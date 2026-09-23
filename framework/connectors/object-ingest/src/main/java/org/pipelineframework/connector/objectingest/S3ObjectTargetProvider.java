package org.pipelineframework.connector.objectingest;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.objectpublish.ObjectTargetProvider;
import org.pipelineframework.objectpublish.PagedObjectTargetProvider;
import org.pipelineframework.objectpublish.PagedObjectPart;
import org.pipelineframework.objectpublish.PagedObjectPartQuery;
import org.pipelineframework.objectpublish.PagedObjectCompositionRequest;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;
import org.pipelineframework.repository.PayloadReference;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

/**
 * Plain AWS SDK S3 object target provider for Object Publish.
 */
public class S3ObjectTargetProvider implements PagedObjectTargetProvider, AutoCloseable {
    static final int DEFAULT_PART_SIZE_BYTES = 8 * 1024 * 1024;
    private static final int MIN_PART_SIZE_BYTES = 5 * 1024 * 1024;

    private final Optional<S3Client> client;
    private final boolean ownsClient;
    private final Executor executor;
    private final boolean ownsExecutor;
    private final int partSizeBytes;
    private final ConcurrentMap<ClientConfiguration, S3Client> resolvedClients = new ConcurrentHashMap<>();
    private final Object lifecycleLock = new Object();
    private boolean closed;

    public S3ObjectTargetProvider() {
        this(Optional.empty(), true, Executors.newVirtualThreadPerTaskExecutor(), true, DEFAULT_PART_SIZE_BYTES);
    }

    public S3ObjectTargetProvider(S3Client client) {
        this(Optional.of(Objects.requireNonNull(client, "client")), false, Executors.newVirtualThreadPerTaskExecutor(), true,
            DEFAULT_PART_SIZE_BYTES);
    }

    S3ObjectTargetProvider(S3Client client, Executor executor, int partSizeBytes) {
        this(Optional.of(Objects.requireNonNull(client, "client")), false, executor, false, partSizeBytes);
    }

    private S3ObjectTargetProvider(
        Optional<S3Client> client,
        boolean ownsClient,
        Executor executor,
        boolean ownsExecutor,
        int partSizeBytes
    ) {
        this.client = client;
        this.ownsClient = ownsClient;
        this.executor = executor;
        this.ownsExecutor = ownsExecutor;
        this.partSizeBytes = Math.max(partSizeBytes, MIN_PART_SIZE_BYTES);
    }

    @Override
    public String providerName() {
        return "s3";
    }

    @Override
    public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            String bucket = required(request, "bucket");
            String key = objectKey(request);
            S3Client s3 = client(request);
            CreateMultipartUploadResponse response = s3.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType(request.contentType())
                .metadata(request.metadata())
                .build());
            return new S3WriteSession(request, s3, bucket, key, response.uploadId(), executor, partSizeBytes);
        }, executor);
    }

    @Override
    public CompletionStage<List<PagedObjectPart>> listParts(PagedObjectPartQuery query) {
        return CompletableFuture.supplyAsync(() -> {
            ObjectWriteOpenRequest lookup = new ObjectWriteOpenRequest(
                query.targetName(), query.target(), query.stagePrefix(),
                "application/octet-stream", Map.of(), "paged-list:" + query.stagePrefix());
            S3Client s3 = client(lookup);
            String bucket = required(lookup, "bucket");
            String prefix = objectKey(lookup);
            List<PagedObjectPart> parts = new ArrayList<>();
            String continuation = null;
            do {
                ListObjectsV2Response listed = s3.listObjectsV2(ListObjectsV2Request.builder()
                    .bucket(bucket).prefix(prefix).continuationToken(continuation).build());
                listed.contents().stream()
                    .filter(object -> object.key().endsWith(".part"))
                    .forEach(object -> {
                        Map<String, String> metadata = s3.headObject(HeadObjectRequest.builder()
                            .bucket(bucket).key(object.key()).build()).metadata();
                        parts.add(new PagedObjectPart(
                            requiredMetadata(metadata, "tpf.page.partkey"),
                            requiredMetadata(metadata, "tpf.page.group"),
                            requiredMetadata(metadata, "tpf.page.finalkey"),
                            requiredMetadata(metadata, "tpf.page.contenttype"),
                            Integer.parseInt(requiredMetadata(metadata, "tpf.page.index")),
                            metadata));
                    });
                continuation = listed.isTruncated() ? listed.nextContinuationToken() : null;
            } while (continuation != null);
            return parts.stream()
                .sorted(java.util.Comparator.comparing(PagedObjectPart::groupKey)
                    .thenComparingInt(PagedObjectPart::pageIndex))
                .toList();
        }, executor);
    }

    @Override
    public CompletionStage<ObjectWriteResult> compose(PagedObjectCompositionRequest request) {
        return CompletableFuture.supplyAsync(() -> composeBlocking(request), executor);
    }

    private ObjectWriteResult composeBlocking(PagedObjectCompositionRequest request) {
        ObjectWriteOpenRequest targetRequest = new ObjectWriteOpenRequest(
            request.targetName(), request.target(), request.objectKey(), request.contentType(),
            request.metadata(), request.idempotencyKey());
        S3Client s3 = client(targetRequest);
        String bucket = required(targetRequest, "bucket");
        String finalKey = objectKey(targetRequest);
        List<String> physicalParts = request.orderedPartKeys().stream()
            .map(key -> objectKey(new ObjectWriteOpenRequest(
                request.targetName(), request.target(), key, request.contentType(), Map.of(), request.idempotencyKey())))
            .toList();
        long partBytes = physicalParts.stream().mapToLong(key -> s3.headObject(
            HeadObjectRequest.builder().bucket(bucket).key(key).build()).contentLength()).sum();
        long totalBytes = Math.addExact(Math.addExact(request.prefix().length, partBytes), request.suffix().length);
        MessageDigest digest = S3WriteSession.sha256Digest();
        try (InputStream composite = new java.security.DigestInputStream(
            new S3PartSequenceInputStream(s3, bucket, request.prefix(), physicalParts, request.suffix()), digest)) {
            s3.putObject(PutObjectRequest.builder()
                    .bucket(bucket).key(finalKey).contentType(request.contentType()).metadata(request.metadata()).build(),
                RequestBody.fromInputStream(composite, totalBytes));
        } catch (IOException failure) {
            throw new java.util.concurrent.CompletionException(failure);
        }
        String checksum = HexFormat.of().formatHex(digest.digest());
        Map<String, String> metadata = new LinkedHashMap<>(request.metadata());
        metadata.put("target", request.targetName());
        metadata.put(S3ObjectSourceProvider.CHECKSUM_KIND_METADATA, S3ObjectSourceProvider.CHECKSUM_KIND_SHA256);
        PayloadReference reference = new PayloadReference(
            "s3", bucket, finalKey, request.contentType(), "raw", checksum, totalBytes,
            null, metadata, Optional.empty());
        return new ObjectWriteResult(reference, totalBytes, checksum, Instant.now());
    }

    private static String requiredMetadata(Map<String, String> metadata, String key) {
        String value = metadata.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Paged S3 part is missing metadata: " + key);
        }
        return value;
    }

    private static final class S3PartSequenceInputStream extends InputStream {
        private final S3Client client;
        private final String bucket;
        private final byte[] suffix;
        private final java.util.Iterator<String> parts;
        private Optional<InputStream> current;
        private boolean suffixOpened;

        private S3PartSequenceInputStream(
            S3Client client, String bucket, byte[] prefix, List<String> parts, byte[] suffix) {
            this.client = client;
            this.bucket = bucket;
            this.current = Optional.of(new ByteArrayInputStream(prefix));
            this.parts = parts.iterator();
            this.suffix = suffix;
        }

        @Override public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
        }

        @Override public int read(byte[] bytes, int offset, int length) throws IOException {
            while (current.isPresent()) {
                int read = current.orElseThrow().read(bytes, offset, length);
                if (read >= 0) return read;
                current.orElseThrow().close();
                current = nextStream();
            }
            return -1;
        }

        private Optional<InputStream> nextStream() {
            if (parts.hasNext()) {
                return Optional.of(client.getObject(
                    GetObjectRequest.builder().bucket(bucket).key(parts.next()).build(),
                    ResponseTransformer.toInputStream()));
            }
            if (!suffixOpened) {
                suffixOpened = true;
                return Optional.of(new ByteArrayInputStream(suffix));
            }
            return Optional.empty();
        }

        @Override public void close() throws IOException {
            if (current.isPresent()) current.orElseThrow().close();
        }
    }

    @Override
    public void close() {
        synchronized (lifecycleLock) {
            if (closed) {
                return;
            }
            closed = true;
            if (ownsClient) {
                resolvedClients.values().forEach(S3Client::close);
                resolvedClients.clear();
            }
        }
        if (ownsExecutor && executor instanceof ExecutorService executorService) {
            executorService.close();
        }
    }

    private S3Client client(ObjectWriteOpenRequest request) {
        rejectEndpointOverride(request);
        synchronized (lifecycleLock) {
            if (closed) {
                throw new IllegalStateException("S3 object target provider is closed");
            }
            return client.orElseGet(() -> resolvedClient(request));
        }
    }

    private S3Client resolvedClient(ObjectWriteOpenRequest request) {
        ClientConfiguration configuration = new ClientConfiguration(
            optional(request, "region").orElse(""),
            Boolean.parseBoolean(optional(request, "pathStyleAccess").orElse("false")));
        return resolvedClients.computeIfAbsent(configuration, configured -> {
            S3ClientBuilder builder = S3Client.builder().httpClientBuilder(UrlConnectionHttpClient.builder());
            if (!configured.region().isBlank()) {
                builder.region(Region.of(configured.region()));
            }
            if (configured.pathStyleAccess()) {
                builder.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());
            }
            return builder.build();
        });
    }

    private void rejectEndpointOverride(ObjectWriteOpenRequest request) {
        if (optional(request, "endpoint").isPresent() || optional(request, "endpointOverride").isPresent()) {
            throw new IllegalArgumentException("S3 endpoint overrides must be configured through an application-provided S3Client");
        }
    }

    private String objectKey(ObjectWriteOpenRequest request) {
        String prefix = optional(request, "prefix").orElse("");
        if (prefix.isBlank()) {
            return request.objectKey();
        }
        String normalizedPrefix = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
        String normalizedKey = request.objectKey().startsWith("/") ? request.objectKey().substring(1) : request.objectKey();
        return normalizedPrefix + "/" + normalizedKey;
    }

    private String required(ObjectWriteOpenRequest request, String key) {
        return optional(request, key)
            .orElseThrow(() -> new IllegalArgumentException(
                "s3 publish target '" + request.targetName() + "' requires location." + key));
    }

    private Optional<String> optional(ObjectWriteOpenRequest request, String key) {
        return location(request.target(), key);
    }

    private static Optional<String> location(PipelineObjectPublishConfig target, String key) {
        Object value = target.location().get(key);
        return value == null || value.toString().isBlank()
            ? Optional.empty()
            : Optional.of(value.toString().trim());
    }

    private record ClientConfiguration(String region, boolean pathStyleAccess) {
    }

    private static final class S3WriteSession implements ObjectWriteSession {
        private final ObjectWriteOpenRequest request;
        private final S3Client client;
        private final String bucket;
        private final String key;
        private final String uploadId;
        private final Executor executor;
        private final int partSizeBytes;
        private final List<CompletedPart> parts = new ArrayList<>();
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final MessageDigest digest = sha256Digest();
        private CompletionStage<Void> operationTail = CompletableFuture.completedStage(null);
        private long writtenBytes;
        private int nextPartNumber = 1;
        private boolean terminalScheduled;
        private boolean completed;

        private S3WriteSession(
            ObjectWriteOpenRequest request,
            S3Client client,
            String bucket,
            String key,
            String uploadId,
            Executor executor,
            int partSizeBytes
        ) {
            this.request = request;
            this.client = client;
            this.bucket = bucket;
            this.key = key;
            this.uploadId = uploadId;
            this.executor = executor;
            this.partSizeBytes = partSizeBytes;
        }

        @Override
        public synchronized CompletionStage<Void> write(ByteBuffer chunk) {
            if (terminalScheduled) {
                return CompletableFuture.failedStage(
                    new IllegalStateException("S3 write session is closing or closed"));
            }
            byte[] bytes = copy(chunk);
            CompletionStage<Void> write = operationTail.thenRunAsync(() -> {
                digest.update(bytes);
                writtenBytes += bytes.length;
                buffer.writeBytes(bytes);
                while (buffer.size() >= partSizeBytes) {
                    uploadBufferedPart(partSizeBytes);
                }
            }, executor);
            operationTail = write;
            return write;
        }

        @Override
        public synchronized CompletionStage<ObjectWriteResult> close(ObjectWriteCloseRequest closeRequest) {
            if (terminalScheduled) {
                return CompletableFuture.failedStage(
                    new IllegalStateException("S3 write session is closing or closed"));
            }
            terminalScheduled = true;
            CompletionStage<ObjectWriteResult> close = operationTail.thenApplyAsync(ignored -> {
                String actualChecksum = HexFormat.of().formatHex(digest.digest());
                validateClose(closeRequest, actualChecksum);
                if (buffer.size() > 0) {
                    uploadBufferedPart(buffer.size());
                }
                if (parts.isEmpty()) {
                    client.abortMultipartUpload(abortRequest());
                    client.putObject(
                        PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType(request.contentType())
                            .metadata(request.metadata())
                            .build(),
                        RequestBody.empty());
                } else {
                    client.completeMultipartUpload(
                        CompleteMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .uploadId(uploadId)
                            .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                            .build());
                }
                completed = true;
                Map<String, String> metadata = new LinkedHashMap<>(request.metadata());
                metadata.putAll(closeRequest.metadata());
                metadata.put("target", request.targetName());
                metadata.put(
                    S3ObjectSourceProvider.CHECKSUM_KIND_METADATA,
                    S3ObjectSourceProvider.CHECKSUM_KIND_SHA256);
                location(request.target(), "region")
                    .ifPresent(region -> metadata.put(S3ObjectSourceProvider.REGION_METADATA, region));
                PayloadReference reference = new PayloadReference(
                    "s3",
                    bucket,
                    key,
                    request.contentType(),
                    "raw",
                    actualChecksum,
                    writtenBytes,
                    null,
                    metadata,
                    Optional.empty());
                return new ObjectWriteResult(reference, writtenBytes, actualChecksum, Instant.now());
            }, executor);
            operationTail = close.thenApply(ignored -> null);
            return close;
        }

        private void validateClose(ObjectWriteCloseRequest closeRequest, String actualChecksum) {
            RuntimeException validationFailure = null;
            if (closeRequest.bytes() != writtenBytes) {
                validationFailure = new IllegalStateException("S3 written byte count mismatch: expected "
                    + closeRequest.bytes() + " but wrote " + writtenBytes);
            } else if (closeRequest.checksum() != null
                    && !closeRequest.checksum().equalsIgnoreCase(actualChecksum)) {
                validationFailure = new IllegalStateException("S3 written payload checksum mismatch");
            }
            if (validationFailure == null) {
                return;
            }
            try {
                client.abortMultipartUpload(abortRequest());
            } catch (RuntimeException abortFailure) {
                validationFailure.addSuppressed(abortFailure);
            } finally {
                completed = true;
            }
            throw validationFailure;
        }

        @Override
        public synchronized CompletionStage<Void> abort(Throwable cause) {
            terminalScheduled = true;
            CompletionStage<Void> abort = operationTail.handle((ignored, failure) -> null).thenRunAsync(() -> {
                if (!completed) {
                    client.abortMultipartUpload(abortRequest());
                    completed = true;
                }
            }, executor);
            operationTail = abort;
            return abort;
        }

        private void uploadBufferedPart(int size) {
            byte[] payload = buffer.toByteArray();
            byte[] part = java.util.Arrays.copyOf(payload, size);
            buffer.reset();
            if (payload.length > size) {
                buffer.writeBytes(java.util.Arrays.copyOfRange(payload, size, payload.length));
            }
            int partNumber = nextPartNumber++;
            UploadPartResponse response = client.uploadPart(
                UploadPartRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .partNumber(partNumber)
                    .contentLength((long) part.length)
                    .build(),
                RequestBody.fromBytes(part));
            parts.add(CompletedPart.builder().partNumber(partNumber).eTag(response.eTag()).build());
        }

        private AbortMultipartUploadRequest abortRequest() {
            return AbortMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(key)
                .uploadId(uploadId)
                .build();
        }

        private static byte[] copy(ByteBuffer chunk) {
            if (chunk == null) {
                return new byte[0];
            }
            ByteBuffer duplicate = chunk.slice();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return bytes;
        }

        private static MessageDigest sha256Digest() {
            try {
                return MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException failure) {
                throw new IllegalStateException("SHA-256 is unavailable", failure);
            }
        }
    }
}
