package org.pipelineframework.connector.objectingest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.connector.MaterializedPayload;
import org.pipelineframework.connector.ObjectReadSession;
import org.pipelineframework.objectingest.ObjectSourceItem;
import org.pipelineframework.objectingest.ObjectSourceProvider;
import org.pipelineframework.repository.PayloadReference;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Plain AWS SDK S3 object source provider.
 */
public class S3ObjectSourceProvider implements ObjectSourceProvider, AutoCloseable {
    static final String CHECKSUM_KIND_METADATA = "tpf.s3.checksum.kind";
    static final String CHECKSUM_KIND_ETAG = "etag";
    static final String CHECKSUM_KIND_SHA256 = "sha256";
    static final String REGION_METADATA = "tpf.s3.region";

    private final Optional<S3Client> client;
    private final S3ReferenceAuthority authority;
    private final boolean ownsClient;
    private final Executor executor;
    private final boolean ownsExecutor;
    private final ConcurrentMap<String, S3Client> resolvedClients = new ConcurrentHashMap<>();
    private final Object lifecycleLock = new Object();
    private boolean closed;
    private CompletionStage<Void> stopStage;

    public S3ObjectSourceProvider() {
        this(Optional.empty(), true, Executors.newVirtualThreadPerTaskExecutor(), true,
            new S3ReferenceAuthority());
    }

    S3ObjectSourceProvider(S3ReferenceAuthority authority) {
        this(Optional.empty(), true, Executors.newVirtualThreadPerTaskExecutor(), true, authority);
    }

    public S3ObjectSourceProvider(S3Client client) {
        this(Optional.of(Objects.requireNonNull(client, "client")), false,
            Executors.newVirtualThreadPerTaskExecutor(), true, new S3ReferenceAuthority());
    }

    S3ObjectSourceProvider(S3Client client, Executor executor) {
        this(Optional.of(Objects.requireNonNull(client, "client")), false, executor, false,
            new S3ReferenceAuthority());
    }

    S3ObjectSourceProvider(S3Client client, Executor executor, S3ReferenceAuthority authority) {
        this(Optional.of(Objects.requireNonNull(client, "client")), false, executor, false, authority);
    }

    S3ObjectSourceProvider(S3Client client, Executor executor, boolean ownsExecutor) {
        this(Optional.of(Objects.requireNonNull(client, "client")), false, executor, ownsExecutor,
            new S3ReferenceAuthority());
    }

    private S3ObjectSourceProvider(
        Optional<S3Client> client,
        boolean ownsClient,
        Executor executor,
        boolean ownsExecutor,
        S3ReferenceAuthority authority
    ) {
        this.client = client;
        this.authority = Objects.requireNonNull(authority, "S3 reference authority must not be null");
        this.ownsClient = ownsClient;
        this.executor = Objects.requireNonNull(executor, "S3 source executor must not be null");
        this.ownsExecutor = ownsExecutor;
    }

    @Override
    public String providerName() {
        return "s3";
    }

    @Override
    public List<ObjectSourceItem> list(PipelineObjectSourceConfig source, int limit) {
        String bucket = required(source, "bucket");
        String prefix = optional(source, "prefix").orElse("");
        int requested = Math.max(1, limit);
        List<ObjectSourceItem> matches = new ArrayList<>();
        String continuationToken = null;
        do {
            ListObjectsV2Response response = client(source).listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix)
                .maxKeys(requested)
                .continuationToken(continuationToken)
                .build());
            for (S3Object item : response.contents()) {
                if (matches(source, item.key())) {
                    matches.add(item(source, bucket, item));
                    if (matches.size() == requested) {
                        return List.copyOf(matches);
                    }
                }
            }
            continuationToken = response.nextContinuationToken();
        } while (continuationToken != null);
        return List.copyOf(matches);
    }

    @Override
    public Optional<String> readText(PipelineObjectSourceConfig source, ObjectSourceItem item, long maxBytes) {
        String bucket = required(source, "bucket");
        Long contentLength = client(source).headObject(HeadObjectRequest.builder()
                .bucket(bucket)
                .key(item.key())
                .build())
            .contentLength();
        if (maxBytes > 0 && contentLength == null) {
            throw new IllegalStateException(
                "Cannot enforce maxBytes limit: S3 object contentLength unavailable for " + item.key());
        }
        if (maxBytes > 0 && contentLength != null && contentLength > maxBytes) {
            throw new IllegalStateException("Object exceeds configured maxBytes: " + item.key());
        }
        ResponseBytes<GetObjectResponse> bytes = client(source).getObjectAsBytes(GetObjectRequest.builder()
            .bucket(bucket)
            .key(item.key())
            .build());
        byte[] payload = bytes.asByteArray();
        if (maxBytes > 0 && payload.length > maxBytes) {
            throw new IllegalStateException("Object exceeds configured maxBytes: " + item.key());
        }
        return Optional.of(new String(payload, source.payload().charset()));
    }

    @Override
    public CompletionStage<MaterializedPayload> materialize(PayloadReference reference, long maxBytes) {
        return CompletableFuture.supplyAsync(() -> materializeBlocking(reference, maxBytes), executor);
    }

    @Override
    public CompletionStage<ObjectReadSession> openRead(PayloadReference reference) {
        return CompletableFuture.supplyAsync(() -> {
            requireMaterializable(reference, Long.MAX_VALUE);
            authority.verify(reference);
            S3Client resolved = client(reference);
            HeadObjectRequest.Builder headRequest = HeadObjectRequest.builder()
                .bucket(reference.container()).key(reference.key());
            if (reference.version() != null) {
                headRequest.versionId(reference.version());
            }
            var head = resolved.headObject(headRequest.build());
            verifyEtag(reference, head.eTag());
            if (head.contentLength() != null && head.contentLength() != reference.sizeBytes()) {
                throw new IllegalStateException("S3 payload size mismatch: " + reference.key());
            }
            String currentEtag = normalizeEtag(head.eTag());
            GetObjectRequest.Builder get = GetObjectRequest.builder()
                .bucket(reference.container()).key(reference.key());
            if (reference.version() != null) {
                get.versionId(reference.version());
            } else if (currentEtag != null) {
                get.ifMatch(quoteEtag(currentEtag));
            }
            ResponseInputStream<GetObjectResponse> stream = resolved.getObject(get.build());
            try {
                verifyUnchangedDuringRead(reference.key(), currentEtag, stream.response().eTag());
                verifyEtag(reference, stream.response().eTag());
                return new S3ReadSession(stream, reference, executor,
                    CHECKSUM_KIND_SHA256.equalsIgnoreCase(checksumKind(reference)));
            } catch (RuntimeException failure) {
                try {
                    stream.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }, executor);
    }

    private static final class S3ReadSession implements ObjectReadSession {
        private final ResponseInputStream<GetObjectResponse> input;
        private final PayloadReference reference;
        private final Executor executor;
        private final Optional<MessageDigest> digest;
        private long delivered;
        private volatile boolean closed;

        private S3ReadSession(ResponseInputStream<GetObjectResponse> input, PayloadReference reference,
                              Executor executor, boolean checksumIsSha256) {
            this.input = input;
            this.reference = reference;
            this.executor = executor;
            try {
                this.digest = checksumIsSha256 ? Optional.of(MessageDigest.getInstance("SHA-256")) : Optional.empty();
            } catch (NoSuchAlgorithmException failure) {
                throw new IllegalStateException("SHA-256 is unavailable", failure);
            }
        }

        @Override
        public CompletionStage<Optional<ByteBuffer>> read(int maxBytes) {
            if (maxBytes < 1 || maxBytes > 64 * 1024) {
                throw new IllegalArgumentException("read size must be between 1 and 65536 bytes");
            }
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this) {
                    if (closed) {
                        throw new IllegalStateException("S3 read session is closed");
                    }
                    try {
                        byte[] chunk = input.readNBytes(maxBytes);
                        if (chunk.length == 0) {
                            if (delivered != reference.sizeBytes()) {
                                throw new IllegalStateException("S3 payload size mismatch: " + reference.key());
                            }
                            digest.ifPresent(value -> {
                                String actual = HexFormat.of().formatHex(value.digest());
                                if (reference.checksum() != null
                                    && !reference.checksum().equalsIgnoreCase(actual)) {
                                    throw new IllegalStateException("S3 payload checksum mismatch: " + reference.key());
                                }
                            });
                            close();
                            return Optional.empty();
                        }
                        if (chunk.length > reference.sizeBytes() - delivered) {
                            throw new IllegalStateException("S3 payload size mismatch: " + reference.key());
                        }
                        delivered += chunk.length;
                        digest.ifPresent(value -> value.update(chunk));
                        return Optional.of(ByteBuffer.wrap(chunk));
                    } catch (IOException failure) {
                        close();
                        throw new java.io.UncheckedIOException(failure);
                    } catch (RuntimeException failure) {
                        close();
                        throw failure;
                    }
                }
            }, executor);
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                input.abort();
            }
        }
    }

    private MaterializedPayload materializeBlocking(PayloadReference reference, long maxBytes) {
        requireMaterializable(reference, maxBytes);
        S3Client resolvedClient = client(reference);
        HeadObjectRequest.Builder headRequest = HeadObjectRequest.builder()
            .bucket(reference.container())
            .key(reference.key());
        if (reference.version() != null) {
            headRequest.versionId(reference.version());
        }
        var head = resolvedClient.headObject(headRequest.build());
        requireWithinLimit(reference.key(), head.contentLength(), maxBytes);
        verifyEtag(reference, head.eTag());
        String currentEtag = normalizeEtag(head.eTag());

        GetObjectRequest.Builder getRequest = GetObjectRequest.builder()
            .bucket(reference.container())
            .key(reference.key());
        if (reference.version() != null) {
            getRequest.versionId(reference.version());
        } else if (currentEtag != null) {
            getRequest.ifMatch(quoteEtag(currentEtag));
        }
        ResponseBytes<GetObjectResponse> response = resolvedClient.getObjectAsBytes(getRequest.build());
        byte[] payload = response.asByteArray();
        requireWithinLimit(reference.key(), (long) payload.length, maxBytes);
        verifyUnchangedDuringRead(reference.key(), currentEtag, response.response().eTag());
        verifyEtag(reference, response.response().eTag());
        verifyContentChecksum(reference, payload);
        String contentType = response.response().contentType() == null
            ? reference.contentType()
            : response.response().contentType();
        return new MaterializedPayload(
            reference, payload, contentType, reference.codec(), materializedChecksum(reference, response, payload));
    }

    @Override
    public void close() {
        stop().toCompletableFuture().join();
    }

    CompletionStage<Void> stop() {
        synchronized (lifecycleLock) {
            if (stopStage != null) {
                return stopStage;
            }
            closed = true;
            if (ownsExecutor && executor instanceof ExecutorService executorService) {
                CompletableFuture<Void> stopped = new CompletableFuture<>();
                stopStage = stopped;
                Thread.ofVirtual().name("tpf-s3-source-stop").start(() -> {
                    try {
                        executorService.close();
                        closeOwnedClients();
                        stopped.complete(null);
                    } catch (Throwable failure) {
                        stopped.completeExceptionally(failure);
                    }
                });
                return stopped;
            }
            closeOwnedClients();
            stopStage = CompletableFuture.completedStage(null);
            return stopStage;
        }
    }

    private void closeOwnedClients() {
        if (ownsClient) {
            resolvedClients.values().forEach(S3Client::close);
            resolvedClients.clear();
        }
    }

    private ObjectSourceItem item(PipelineObjectSourceConfig source, String bucket, S3Object item) {
        String etag = normalizeEtag(item.eTag());
        long size = item.size() == null ? 0L : item.size();
        long lastModified = item.lastModified() == null ? 0L : item.lastModified().toEpochMilli();
        PayloadReference reference = authority.issue(new PayloadReference(
            providerName(),
            bucket,
            item.key(),
            null,
            "raw",
            etag,
            size,
            null,
            referenceMetadata(source),
            Optional.empty()));
        return new ObjectSourceItem(
            providerName(),
            bucket,
            item.key(),
            null,
            etag,
            size,
            lastModified,
            null,
            Map.of(),
            reference,
            null);
    }

    private boolean matches(PipelineObjectSourceConfig source, String key) {
        boolean included = source.filter().include().isEmpty()
            || source.filter().include().stream().anyMatch(pattern -> globMatches(pattern, key));
        boolean excluded = source.filter().exclude().stream().anyMatch(pattern -> globMatches(pattern, key));
        return included && !excluded;
    }

    private S3Client client(PipelineObjectSourceConfig source) {
        rejectEndpointOverride(source);
        return client(optional(source, "region").orElse(""));
    }

    private S3Client client(PayloadReference reference) {
        return client(reference.metadata().getOrDefault(REGION_METADATA, ""));
    }

    private S3Client client(String region) {
        synchronized (lifecycleLock) {
            if (closed) {
                throw new IllegalStateException("S3 object source provider is closed");
            }
            if (client.isPresent()) {
                return client.get();
            }
            return resolvedClients.computeIfAbsent(region, configuredRegion -> {
                S3ClientBuilder builder = S3Client.builder().httpClientBuilder(UrlConnectionHttpClient.builder());
                if (!configuredRegion.isBlank()) {
                    builder.region(Region.of(configuredRegion));
                }
                return builder.build();
            });
        }
    }

    private void requireMaterializable(PayloadReference reference, long maxBytes) {
        if (reference == null) {
            throw new IllegalArgumentException("payload reference must not be null");
        }
        if (maxBytes < 1) {
            throw new IllegalArgumentException("maxBytes must be positive");
        }
        if (!providerName().equalsIgnoreCase(reference.provider())) {
            throw new IllegalArgumentException("S3 operation cannot materialize provider=" + reference.provider());
        }
        if (reference.container() == null || reference.container().isBlank()) {
            throw new IllegalArgumentException("S3 payload reference container must not be blank");
        }
        requireWithinLimit(reference.key(), reference.sizeBytes(), maxBytes);
    }

    private void requireWithinLimit(String key, Long sizeBytes, long maxBytes) {
        if (sizeBytes == null) {
            throw new IllegalStateException("Cannot enforce maxBytes limit: S3 object contentLength unavailable for " + key);
        }
        if (sizeBytes > maxBytes) {
            throw new IllegalStateException("Object exceeds configured maxBytes: " + key);
        }
    }

    private void verifyEtag(PayloadReference reference, String actualEtag) {
        Optional<String> expected = expectedEtag(reference);
        if (expected.isEmpty()) {
            return;
        }
        String normalized = normalizeEtag(actualEtag);
        if (!expected.orElseThrow().equalsIgnoreCase(normalized)) {
            throw new IllegalStateException("S3 payload ETag mismatch: " + reference.key());
        }
    }

    private Optional<String> expectedEtag(PayloadReference reference) {
        return CHECKSUM_KIND_ETAG.equalsIgnoreCase(checksumKind(reference))
            ? Optional.ofNullable(reference.checksum())
            : Optional.empty();
    }

    private void verifyUnchangedDuringRead(String key, String headEtag, String responseEtag) {
        String normalizedResponse = normalizeEtag(responseEtag);
        if (headEtag != null && !headEtag.equalsIgnoreCase(normalizedResponse)) {
            throw new IllegalStateException("S3 payload changed during materialization: " + key);
        }
    }

    private void verifyContentChecksum(PayloadReference reference, byte[] payload) {
        if (!CHECKSUM_KIND_SHA256.equalsIgnoreCase(checksumKind(reference))
                || reference.checksum() == null) {
            return;
        }
        if (!reference.checksum().equalsIgnoreCase(sha256(payload))) {
            throw new IllegalStateException("S3 payload checksum mismatch: " + reference.key());
        }
    }

    private String materializedChecksum(
        PayloadReference reference,
        ResponseBytes<GetObjectResponse> response,
        byte[] payload
    ) {
        return CHECKSUM_KIND_SHA256.equalsIgnoreCase(checksumKind(reference))
            ? sha256(payload)
            : normalizeEtag(response.response().eTag());
    }

    private String checksumKind(PayloadReference reference) {
        String explicit = reference.metadata().get(CHECKSUM_KIND_METADATA);
        if (explicit != null) {
            return explicit;
        }
        return CHECKSUM_KIND_ETAG;
    }

    private String quoteEtag(String etag) {
        return '"' + normalizeEtag(etag) + '"';
    }

    private Map<String, String> referenceMetadata(PipelineObjectSourceConfig source) {
        String region = optional(source, "region").orElse(null);
        return region == null
            ? Map.of("source", source.name(), CHECKSUM_KIND_METADATA, CHECKSUM_KIND_ETAG)
            : Map.of(
                "source", source.name(),
                REGION_METADATA, region,
                CHECKSUM_KIND_METADATA, CHECKSUM_KIND_ETAG);
    }

    private String sha256(byte[] payload) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private void rejectEndpointOverride(PipelineObjectSourceConfig source) {
        if (optional(source, "endpoint").isPresent() || optional(source, "endpointOverride").isPresent()) {
            throw new IllegalArgumentException("S3 endpoint overrides must be configured through an application-provided S3Client");
        }
    }

    private boolean globMatches(String pattern, String key) {
        return FileSystems.getDefault().getPathMatcher("glob:" + pattern).matches(Path.of(key));
    }

    private String required(PipelineObjectSourceConfig source, String key) {
        return optional(source, key)
            .orElseThrow(() -> new IllegalArgumentException("s3 source '" + source.name() + "' requires location." + key));
    }

    private Optional<String> optional(PipelineObjectSourceConfig source, String key) {
        Object value = source.location().get(key);
        return value == null || value.toString().isBlank()
            ? Optional.empty()
            : Optional.of(value.toString().trim());
    }

    private String normalizeEtag(String etag) {
        if (etag == null || etag.isBlank()) {
            return null;
        }
        return etag.replace("\"", "").trim();
    }
}
