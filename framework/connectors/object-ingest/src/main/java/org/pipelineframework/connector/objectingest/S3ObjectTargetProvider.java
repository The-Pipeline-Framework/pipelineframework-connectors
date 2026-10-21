package org.pipelineframework.connector.objectingest;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import org.pipelineframework.connector.DelegatedPayloadTarget;
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
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.UploadPartPresignRequest;

/**
 * Plain AWS SDK S3 object target provider for Object Publish.
 */
public class S3ObjectTargetProvider implements PagedObjectTargetProvider, DelegatedPayloadTarget, AutoCloseable {
    static final int DEFAULT_PART_SIZE_BYTES = 8 * 1024 * 1024;
    private static final int MIN_PART_SIZE_BYTES = 5 * 1024 * 1024;

    private final Optional<S3Client> client;
    private final S3ReferenceAuthority authority;
    private final S3DelegatedUploadToken delegatedTokens = new S3DelegatedUploadToken();
    private final boolean ownsClient;
    private final Executor executor;
    private final boolean ownsExecutor;
    private final int partSizeBytes;
    private final ConcurrentMap<ClientConfiguration, S3Client> resolvedClients = new ConcurrentHashMap<>();
    private final ConcurrentMap<PresignerConfiguration, S3Presigner> presigners = new ConcurrentHashMap<>();
    private final Object lifecycleLock = new Object();
    private boolean closed;

    private record PresignerConfiguration(S3Client client, boolean pathStyleAccess) { }

    public S3ObjectTargetProvider() {
        this(Optional.empty(), true, Executors.newVirtualThreadPerTaskExecutor(), true, DEFAULT_PART_SIZE_BYTES,
            new S3ReferenceAuthority());
    }

    S3ObjectTargetProvider(S3ReferenceAuthority authority) {
        this(Optional.empty(), true, Executors.newVirtualThreadPerTaskExecutor(), true, DEFAULT_PART_SIZE_BYTES,
            authority);
    }

    public S3ObjectTargetProvider(S3Client client) {
        this(Optional.of(Objects.requireNonNull(client, "client")), false, Executors.newVirtualThreadPerTaskExecutor(), true,
            DEFAULT_PART_SIZE_BYTES, new S3ReferenceAuthority());
    }

    S3ObjectTargetProvider(S3Client client, Executor executor, int partSizeBytes) {
        this(Optional.of(Objects.requireNonNull(client, "client")), false, executor, false, partSizeBytes,
            new S3ReferenceAuthority());
    }

    private S3ObjectTargetProvider(
        Optional<S3Client> client,
        boolean ownsClient,
        Executor executor,
        boolean ownsExecutor,
        int partSizeBytes,
        S3ReferenceAuthority authority
    ) {
        this.client = client;
        this.authority = Objects.requireNonNull(authority, "S3 reference authority must not be null");
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
            return new S3WriteSession(request, s3, bucket, key, response.uploadId(), executor, partSizeBytes,
                authority);
        }, executor);
    }

    @Override
    public CompletionStage<Reservation> initiate(DelegatedPayloadTarget.Request request) {
        return CompletableFuture.supplyAsync(() -> {
            if (request.bytes() < 1 || request.sha256() == null
                || !request.sha256().matches("[a-f0-9]{64}")
                || request.idempotencyKey() == null
                || !request.idempotencyKey().matches("[A-Za-z0-9._:-]{1,128}")
                || ((request.bytes() - 1) / partSizeBytes) + 1 > 10_000
                || !request.expiresAt().isAfter(Instant.now())) {
                throw new IllegalArgumentException("invalid delegated S3 upload request");
            }
            String randomKey = "uploads/" + java.util.UUID.randomUUID();
            ObjectWriteOpenRequest open = new ObjectWriteOpenRequest(request.targetName(), request.target(),
                randomKey, request.contentType(), request.metadata(), request.idempotencyKey());
            String bucket = required(open, "bucket");
            String key = objectKey(open);
            S3Client s3 = client(open);
            String markerKey = delegatedMarkerKey(open, request.idempotencyKey());
            try {
                return reusedReservation(s3, bucket, markerKey, request);
            } catch (S3Exception missing) {
                if (missing.statusCode() != 404) throw missing;
            }
            if (request.metadata().size() > 29) {
                throw new IllegalArgumentException("delegated S3 upload metadata exceeds 29 entries");
            }
            Map<String, String> metadata = new LinkedHashMap<>(request.metadata());
            metadata.put("tpf.delegated.sha256", request.sha256());
            metadata.put("tpf.delegated.bytes", Long.toString(request.bytes()));
            metadata.put("tpf.delegated.target", request.targetName());
            String uploadId = s3.createMultipartUpload(CreateMultipartUploadRequest.builder()
                .bucket(bucket).key(key).contentType(request.contentType()).metadata(metadata).build()).uploadId();
            try {
                var claims = new S3DelegatedUploadToken.Claims(request.targetName(), bucket, key, uploadId,
                    request.contentType(), request.bytes(), partSizeBytes, request.sha256(), request.expiresAt(),
                    request.idempotencyKey(), metadata);
                String token = delegatedTokens.issue(claims);
                s3.putObject(PutObjectRequest.builder().bucket(bucket).key(markerKey).ifNoneMatch("*")
                    .contentType("text/plain").build(), RequestBody.fromString(token));
                return new Reservation(token, partSizeBytes, request.expiresAt());
            } catch (S3Exception conflict) {
                abortNewReservation(s3, bucket, key, uploadId, conflict);
                if (conflict.statusCode() == 412) {
                    return reusedReservation(s3, bucket, markerKey, request);
                }
                throw conflict;
            } catch (RuntimeException failure) {
                abortNewReservation(s3, bucket, key, uploadId, failure);
                throw failure;
            }
        }, executor);
    }

    private static void abortNewReservation(S3Client s3, String bucket, String key, String uploadId,
                                            RuntimeException failure) {
        try {
            s3.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket(bucket).key(key)
                .uploadId(uploadId).build());
        } catch (RuntimeException abortFailure) {
            failure.addSuppressed(abortFailure);
        }
    }

    private Reservation reusedReservation(S3Client s3, String bucket, String markerKey,
                                          DelegatedPayloadTarget.Request request) {
        var stored = s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucket).key(markerKey).build());
        String token = stored.asString(StandardCharsets.UTF_8);
        var claims = delegatedTokens.inspect(token);
        String uploadsPrefix = objectKey(new ObjectWriteOpenRequest(request.targetName(), request.target(),
            "uploads/", request.contentType(), Map.of(), request.idempotencyKey()));
        if (!claims.targetName().equals(request.targetName())
            || !claims.bucket().equals(bucket) || !claims.key().startsWith(uploadsPrefix)
            || !claims.contentType().equals(request.contentType())
            || claims.bytes() != request.bytes() || !claims.sha256().equals(request.sha256())
            || !claims.idempotencyKey().equals(request.idempotencyKey())
            || !request.metadata().entrySet().stream().allMatch(entry ->
                entry.getValue().equals(claims.metadata().get(entry.getKey())))) {
            throw new SecurityException("idempotency key was used for another upload reservation");
        }
        if (!claims.expiresAt().isAfter(Instant.now())) {
            try {
                s3.abortMultipartUpload(AbortMultipartUploadRequest.builder().bucket(bucket)
                    .key(claims.key()).uploadId(claims.uploadId()).build());
            } catch (S3Exception failure) {
                if (failure.statusCode() != 404) throw failure;
            }
            s3.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(markerKey)
                .ifMatch('"' + requireEtag(stored.response().eTag()) + '"').build());
            throw S3Exception.builder().statusCode(404).message("expired reservation removed").build();
        }
        return new Reservation(token, claims.partSizeBytes(), claims.expiresAt());
    }

    private String delegatedMarkerKey(ObjectWriteOpenRequest request, String idempotencyKey) {
        MessageDigest digest = S3WriteSession.sha256Digest();
        digest.update(request.targetName().getBytes(StandardCharsets.UTF_8));
        request.metadata().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            digest.update(entry.getKey().getBytes(StandardCharsets.UTF_8));
            digest.update(entry.getValue().getBytes(StandardCharsets.UTF_8));
        });
        digest.update(idempotencyKey.getBytes(StandardCharsets.UTF_8));
        return objectKey(new ObjectWriteOpenRequest(request.targetName(), request.target(),
            ".tpf-delegated/" + HexFormat.of().formatHex(digest.digest()) + ".reservation",
            "text/plain", Map.of(), idempotencyKey));
    }

    @Override
    public CompletionStage<PartCapability> part(PipelineObjectPublishConfig target, String token,
                                                 Map<String, String> expectedOwner, int partNumber) {
        return CompletableFuture.supplyAsync(() -> {
            var claims = verifiedDelegated(target, token, expectedOwner);
            long count = (claims.bytes() + claims.partSizeBytes() - 1) / claims.partSizeBytes();
            if (partNumber < 1 || partNumber > count || partNumber > 10_000) {
                throw new IllegalArgumentException("S3 upload part number is outside reservation");
            }
            ObjectWriteOpenRequest open = delegatedOpen(target, claims);
            S3Client s3 = client(open);
            S3Presigner presigner = presigner(s3,
                Boolean.parseBoolean(location(target, "pathStyleAccess").orElse("false")));
            java.time.Duration duration = java.time.Duration.between(Instant.now(), claims.expiresAt());
            if (duration.compareTo(java.time.Duration.ofMinutes(10)) > 0) {
                duration = java.time.Duration.ofMinutes(10);
            }
            long reservedPartBytes = Math.min(claims.partSizeBytes(),
                claims.bytes() - (partNumber - 1L) * claims.partSizeBytes());
            var signed = presigner.presignUploadPart(UploadPartPresignRequest.builder()
                .signatureDuration(duration)
                .uploadPartRequest(UploadPartRequest.builder().bucket(claims.bucket()).key(claims.key())
                    .uploadId(claims.uploadId()).partNumber(partNumber).contentLength(reservedPartBytes).build())
                .build());
            return new PartCapability(signed.url().toExternalForm(), signed.signedHeaders().entrySet().stream()
                .filter(entry -> !entry.getKey().equalsIgnoreCase("host"))
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey,
                    entry -> String.join(",", entry.getValue()))),
                Instant.now().plus(duration));
        }, executor);
    }

    @Override
    public CompletionStage<ObjectWriteResult> complete(PipelineObjectPublishConfig target, String token,
                                                       Map<String, String> expectedOwner) {
        return CompletableFuture.supplyAsync(() -> completeDelegated(target,
            verifiedDelegated(target, token, expectedOwner)), executor);
    }

    @Override
    public CompletionStage<Void> abort(PipelineObjectPublishConfig target, String token,
                                       Map<String, String> expectedOwner) {
        return CompletableFuture.runAsync(() -> {
            var claims = verifiedDelegated(target, token, expectedOwner);
            S3Client s3 = client(delegatedOpen(target, claims));
            try {
                s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(claims.bucket()).key(claims.key()).uploadId(claims.uploadId()).build());
            } catch (S3Exception failure) {
                if (failure.statusCode() != 404) throw failure;
            }
        }, executor);
    }

    private S3DelegatedUploadToken.Claims verifiedDelegated(PipelineObjectPublishConfig target, String token,
                                                             Map<String, String> expectedOwner) {
        var claims = delegatedTokens.verify(token, Instant.now());
        ObjectWriteOpenRequest open = delegatedOpen(target, claims);
        if (!claims.targetName().equals(target.name()) || !required(open, "bucket").equals(claims.bucket())
            || !claims.key().startsWith(objectKey(new ObjectWriteOpenRequest(target.name(), target,
                "uploads/", claims.contentType(), Map.of(), claims.uploadId())))
            || !expectedOwner.entrySet().stream().allMatch(entry ->
                entry.getValue().equals(claims.metadata().get(entry.getKey())))) {
            throw new SecurityException("S3 upload reservation does not match this target and owner");
        }
        return claims;
    }

    private ObjectWriteOpenRequest delegatedOpen(PipelineObjectPublishConfig target,
                                                  S3DelegatedUploadToken.Claims claims) {
        return new ObjectWriteOpenRequest(target.name(), target, claims.key(), claims.contentType(),
            claims.metadata(), claims.uploadId());
    }

    private ObjectWriteResult completeDelegated(PipelineObjectPublishConfig target,
                                                 S3DelegatedUploadToken.Claims claims) {
        S3Client s3 = client(delegatedOpen(target, claims));
        boolean existing = false;
        String completedEtag = null;
        String completedVersion = null;
        try {
            var head = s3.headObject(HeadObjectRequest.builder().bucket(claims.bucket())
                .key(claims.key()).build());
            existing = true;
            if (head.contentLength() == null || head.contentLength() != claims.bytes()
                || !claims.contentType().equals(head.contentType())
                || !claims.metadata().entrySet().stream().allMatch(entry ->
                    entry.getValue().equals(head.metadata().get(entry.getKey())))) {
                throw new SecurityException("completed S3 upload metadata does not match reservation");
            }
            completedEtag = requireEtag(head.eTag());
            completedVersion = head.versionId();
        } catch (S3Exception failure) {
            if (failure.statusCode() != 404) throw failure;
        }
        if (!existing) {
            List<CompletedPart> parts = new ArrayList<>();
            long total = 0;
            int marker = 0;
            int expectedPart = 1;
            while (true) {
                var listing = ListPartsRequest.builder().bucket(claims.bucket())
                    .key(claims.key()).uploadId(claims.uploadId());
                if (marker > 0) listing.partNumberMarker(marker);
                var listed = s3.listParts(listing.build());
                for (var part : listed.parts()) {
                    if (part.partNumber() != expectedPart++ || part.size() == null
                        || part.size() < 1 || part.size() > claims.partSizeBytes()
                        || (total + part.size() < claims.bytes() && part.size() != claims.partSizeBytes())) {
                        throw new SecurityException("S3 upload has invalid or missing parts");
                    }
                    total += part.size();
                    if (total > claims.bytes()) throw new SecurityException("S3 upload exceeds reserved size");
                    parts.add(CompletedPart.builder().partNumber(part.partNumber()).eTag(part.eTag()).build());
                }
                if (!Boolean.TRUE.equals(listed.isTruncated())) break;
                marker = listed.nextPartNumberMarker();
            }
            if (total != claims.bytes() || parts.isEmpty() || parts.size() > 10_000) {
                throw new SecurityException("S3 upload is incomplete");
            }
            var completed = s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder().bucket(claims.bucket())
                .key(claims.key()).uploadId(claims.uploadId())
                .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build()).build());
            completedEtag = requireEtag(completed.eTag());
            completedVersion = completed.versionId();
            try {
                verifyWrittenVersion(s3, claims.bucket(), claims.key(), completedVersion,
                    completedEtag, claims.bytes(), claims.contentType(), claims.metadata());
            } catch (RuntimeException failure) {
                deleteUnverified(s3, claims, completedVersion, failure);
                throw failure;
            }
        }
        MessageDigest digest = S3WriteSession.sha256Digest();
        long read = 0;
        try (InputStream input = s3.getObject(GetObjectRequest.builder().bucket(claims.bucket())
            .key(claims.key()).versionId(completedVersion).ifMatch('"' + completedEtag + '"').build(),
            ResponseTransformer.toInputStream())) {
            byte[] chunk = new byte[64 * 1024];
            int count;
            while ((count = input.read(chunk)) >= 0) {
                read += count;
                if (read > claims.bytes()) throw new SecurityException("S3 upload exceeds reserved size");
                digest.update(chunk, 0, count);
            }
        } catch (IOException failure) {
            if (!existing) deleteUnverified(s3, claims, completedVersion, failure);
            throw new IllegalStateException("could not verify completed S3 upload", failure);
        } catch (RuntimeException failure) {
            if (!existing) deleteUnverified(s3, claims, completedVersion, failure);
            throw failure;
        }
        String actual = HexFormat.of().formatHex(digest.digest());
        if (read != claims.bytes() || !actual.equals(claims.sha256())) {
            SecurityException failure = new SecurityException("S3 upload size or SHA-256 differs from reservation");
            if (!existing) deleteUnverified(s3, claims, completedVersion, failure);
            throw failure;
        }
        Map<String, String> metadata = new LinkedHashMap<>(claims.metadata());
        metadata.put("target", claims.targetName());
        metadata.put(S3ObjectSourceProvider.CHECKSUM_KIND_METADATA,
            S3ObjectSourceProvider.CHECKSUM_KIND_SHA256);
        metadata.put(S3ObjectSourceProvider.ORIGINAL_ETAG_METADATA, completedEtag);
        location(target, "region").ifPresent(region -> metadata.put(S3ObjectSourceProvider.REGION_METADATA, region));
        location(target, "pathStyleAccess").ifPresent(pathStyle ->
            metadata.put(S3ObjectSourceProvider.PATH_STYLE_METADATA, pathStyle));
        PayloadReference reference = authority.issue(new PayloadReference("s3", claims.bucket(), claims.key(),
            claims.contentType(), "raw", actual, read, completedVersion, metadata, Optional.empty()));
        return new ObjectWriteResult(reference, read, actual, Instant.now());
    }

    private static void deleteUnverified(S3Client s3, S3DelegatedUploadToken.Claims claims,
                                         String version, Throwable failure) {
        try {
            s3.deleteObject(DeleteObjectRequest.builder().bucket(claims.bucket()).key(claims.key())
                .versionId(version).build());
        } catch (RuntimeException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
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
        CreateMultipartUploadResponse created = s3.createMultipartUpload(CreateMultipartUploadRequest.builder()
            .bucket(bucket)
            .key(finalKey)
            .contentType(request.contentType())
            .metadata(request.metadata())
            .build());
        List<CompletedPart> completedParts = new ArrayList<>();
        String completedEtag;
        String completedVersion;
        try (InputStream composite = new java.security.DigestInputStream(
            new S3PartSequenceInputStream(s3, bucket, request.prefix(), physicalParts, request.suffix()), digest)) {
            int partNumber = 1;
            byte[] block;
            while ((block = composite.readNBytes(partSizeBytes)).length > 0) {
                UploadPartResponse uploaded = s3.uploadPart(UploadPartRequest.builder()
                        .bucket(bucket).key(finalKey).uploadId(created.uploadId()).partNumber(partNumber).build(),
                    RequestBody.fromBytes(block));
                completedParts.add(CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(uploaded.eTag())
                    .build());
                partNumber++;
            }
            if (completedParts.isEmpty()) {
                s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(bucket).key(finalKey).uploadId(created.uploadId()).build());
                var completed = s3.putObject(PutObjectRequest.builder()
                        .bucket(bucket).key(finalKey).contentType(request.contentType()).metadata(request.metadata()).build(),
                    RequestBody.empty());
                completedEtag = requireEtag(completed.eTag());
                completedVersion = completed.versionId();
            } else {
                var completed = s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                    .bucket(bucket).key(finalKey).uploadId(created.uploadId())
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
                    .build());
                completedEtag = requireEtag(completed.eTag());
                completedVersion = completed.versionId();
            }
        } catch (IOException | RuntimeException failure) {
            try {
                s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(bucket).key(finalKey).uploadId(created.uploadId()).build());
            } catch (RuntimeException abortFailure) {
                failure.addSuppressed(abortFailure);
            }
            if (failure instanceof RuntimeException runtimeFailure) {
                throw runtimeFailure;
            }
            throw new java.util.concurrent.CompletionException(failure);
        }
        String checksum = HexFormat.of().formatHex(digest.digest());
        Map<String, String> metadata = new LinkedHashMap<>(request.metadata());
        metadata.put("target", request.targetName());
        metadata.put(S3ObjectSourceProvider.CHECKSUM_KIND_METADATA, S3ObjectSourceProvider.CHECKSUM_KIND_SHA256);
        metadata.put(S3ObjectSourceProvider.ORIGINAL_ETAG_METADATA, completedEtag);
        location(request.target(), "region")
            .ifPresent(region -> metadata.put(S3ObjectSourceProvider.REGION_METADATA, region));
        location(request.target(), "pathStyleAccess").ifPresent(pathStyle ->
            metadata.put(S3ObjectSourceProvider.PATH_STYLE_METADATA, pathStyle));
        PayloadReference reference = authority.issue(new PayloadReference(
            "s3", bucket, finalKey, request.contentType(), "raw", checksum, totalBytes,
            completedVersion, metadata, Optional.empty()));
        return new ObjectWriteResult(reference, totalBytes, checksum, Instant.now());
    }

    private static String requiredMetadata(Map<String, String> metadata, String key) {
        String value = metadata.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Paged S3 part is missing metadata: " + key);
        }
        return value;
    }

    private static void verifyWrittenVersion(S3Client s3, String bucket, String key, String version,
                                            String etag, long bytes, String contentType,
                                            Map<String, String> metadata) {
        // The write response, never a later latest-object observation, owns this version.
        // Preserve existing unversioned behavior when S3 returns no version ID.
        if (version == null) return;
        var head = s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).versionId(version).build());
        if (!version.equals(head.versionId()) || head.contentLength() == null || head.contentLength() != bytes
            || !contentType.equals(head.contentType()) || !etag.equals(requireEtag(head.eTag()))
            || !metadata.entrySet().stream().allMatch(entry -> entry.getValue().equals(head.metadata().get(entry.getKey())))) {
            throw new SecurityException("completed S3 version metadata differs from the write");
        }
    }

    private static String requireEtag(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("S3 completed object has no ETag");
        }
        String normalized = value.startsWith("\"") && value.endsWith("\"")
            ? value.substring(1, value.length() - 1) : value;
        if (normalized.isBlank()) throw new IllegalStateException("S3 completed object has no ETag");
        return normalized;
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
            presigners.values().forEach(S3Presigner::close);
            presigners.clear();
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

    private S3Presigner presigner(S3Client s3, boolean pathStyleAccess) {
        synchronized (lifecycleLock) {
            if (closed) throw new IllegalStateException("S3 object target provider is closed");
            return presigners.computeIfAbsent(new PresignerConfiguration(s3, pathStyleAccess),
                configuration -> S3Presigners.forClient(configuration.client(), configuration.pathStyleAccess()));
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
        private final S3ReferenceAuthority authority;
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
            int partSizeBytes,
            S3ReferenceAuthority authority
        ) {
            this.request = request;
            this.client = client;
            this.bucket = bucket;
            this.key = key;
            this.uploadId = uploadId;
            this.executor = executor;
            this.partSizeBytes = partSizeBytes;
            this.authority = authority;
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
                String completedEtag;
                String completedVersion;
                if (parts.isEmpty()) {
                    client.abortMultipartUpload(abortRequest());
                    var response = client.putObject(
                        PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .contentType(request.contentType())
                            .metadata(request.metadata())
                            .build(),
                        RequestBody.empty());
                    completedEtag = requireEtag(response.eTag());
                    completedVersion = response.versionId();
                } else {
                    var response = client.completeMultipartUpload(
                        CompleteMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .uploadId(uploadId)
                            .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                            .build());
                    completedEtag = requireEtag(response.eTag());
                    completedVersion = response.versionId();
                }
                completed = true;
                Map<String, String> metadata = new LinkedHashMap<>(request.metadata());
                metadata.putAll(closeRequest.metadata());
                metadata.put("target", request.targetName());
                metadata.put(
                    S3ObjectSourceProvider.CHECKSUM_KIND_METADATA,
                    S3ObjectSourceProvider.CHECKSUM_KIND_SHA256);
                metadata.put(S3ObjectSourceProvider.ORIGINAL_ETAG_METADATA, completedEtag);
                location(request.target(), "region")
                    .ifPresent(region -> metadata.put(S3ObjectSourceProvider.REGION_METADATA, region));
                location(request.target(), "pathStyleAccess").ifPresent(pathStyle ->
                    metadata.put(S3ObjectSourceProvider.PATH_STYLE_METADATA, pathStyle));
                PayloadReference reference = authority.issue(new PayloadReference(
                    "s3",
                    bucket,
                    key,
                    request.contentType(),
                    "raw",
                    actualChecksum,
                    writtenBytes,
                    completedVersion,
                    metadata,
                    Optional.empty()));
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
