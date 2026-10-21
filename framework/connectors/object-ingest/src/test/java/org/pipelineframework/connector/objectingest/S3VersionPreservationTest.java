package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.config.boundary.PipelineObjectNamingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishGroupingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishPayloadConfig;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.PagedObjectCompositionRequest;
import org.pipelineframework.repository.PayloadReference;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.Part;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

class S3VersionPreservationTest {
    private static final byte[] CONTENT = "owned\n".getBytes(StandardCharsets.UTF_8);
    private static final Map<String, String> OWNER = Map.of("owner", "tenant-a");
    private String previousKey;

    @BeforeEach
    void configureKey() {
        previousKey = System.getProperty("tpf.object.reference.hmac-key");
        System.setProperty("tpf.object.reference.hmac-key", Base64.getEncoder().encodeToString(new byte[32]));
    }

    @AfterEach
    void restoreKey() {
        if (previousKey == null) System.clearProperty("tpf.object.reference.hmac-key");
        else System.setProperty("tpf.object.reference.hmac-key", previousKey);
    }

    @Test
    void delegatedCompletionPinsTheWriteVersionDespiteNewerLatestWithSameEtag() throws Exception {
        S3Client client = delegatedClient("version-one", false, false);
        try (var provider = provider(client)) {
            PayloadReference ref = provider.complete(target(), token(), OWNER).toCompletableFuture().join().reference();
            assertEquals("version-one", ref.version());
            assertEquals(checksum(CONTENT), ref.checksum());
            var heads = ArgumentCaptor.forClass(HeadObjectRequest.class);
            verify(client, org.mockito.Mockito.atLeast(2)).headObject(heads.capture());
            assertEquals("version-one", heads.getAllValues().getLast().versionId());
        }
    }

    @Test
    void repeatedCompletionPinsTheActualExistingHeadVersion() throws Exception {
        S3Client client = delegatedClient("version-one", true, false);
        try (var provider = provider(client)) {
            PayloadReference ref = provider.complete(target(), token(), OWNER).toCompletableFuture().join().reference();
            assertEquals("version-one", ref.version());
        }
        verify(client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }

    @Test
    void corruptCompletedVersionCleanupCannotDeleteTheNewerLatestVersion() throws Exception {
        S3Client client = delegatedClient("version-one", false, true);
        try (var provider = provider(client)) {
            assertThrows(CompletionException.class,
                () -> provider.complete(target(), token(), OWNER).toCompletableFuture().join());
        }
        var deleted = ArgumentCaptor.forClass(DeleteObjectRequest.class);
        verify(client).deleteObject(deleted.capture());
        assertEquals("version-one", deleted.getValue().versionId());
    }

    @Test
    void missingDeletePermissionRemainsSuppressedUnderTheVerificationFailure() {
        S3Client client = delegatedClient("version-one", false, true);
        var denied = S3Exception.builder().statusCode(403).message("denied").build();
        org.mockito.Mockito.doThrow(denied).when(client).deleteObject(any(DeleteObjectRequest.class));
        try (var provider = provider(client)) {
            CompletionException failure = assertThrows(CompletionException.class,
                () -> provider.complete(target(), token(), OWNER).toCompletableFuture().join());
            assertEquals(SecurityException.class, failure.getCause().getClass());
            assertEquals("S3 upload size or SHA-256 differs from reservation", failure.getCause().getMessage());
            assertEquals(List.of(denied), List.of(failure.getCause().getSuppressed()));
        }
    }

    @Test
    void unversionedDelegatedCompletionStillUsesNullVersion() throws Exception {
        S3Client client = delegatedClient(null, false, false);
        try (var provider = provider(client)) {
            assertNull(provider.complete(target(), token(), OWNER).toCompletableFuture().join().reference().version());
        }
    }

    @Test
    void streamCompletionPreservesMultipartAndEmptyPutVersionsWithoutChangingUnversionedResults() {
        for (String version : new String[] {"version-one", null}) {
            for (boolean empty : new boolean[] {false, true}) {
                byte[] bytes = empty ? new byte[0] : CONTENT;
                S3Client client = writeClient(version, bytes.length);
                try (var provider = provider(client)) {
                    var session = provider.open(new ObjectWriteOpenRequest(
                        "invoices", target(), "result", "text/plain", OWNER, "stream")).toCompletableFuture().join();
                    if (!empty) session.write(ByteBuffer.wrap(bytes)).toCompletableFuture().join();
                    var result = session.close(new ObjectWriteCloseRequest(bytes.length, checksum(bytes), Map.of()))
                        .toCompletableFuture().join();
                    assertEquals(version, result.reference().version());
                    assertEquals(checksum(bytes), result.reference().checksum());
                }
            }
        }
    }

    @Test
    void pagedCompositionPreservesMultipartAndEmptyPutVersionsWithoutChangingUnversionedResults() {
        for (String version : new String[] {"version-one", null}) {
            for (boolean empty : new boolean[] {false, true}) {
                byte[] bytes = empty ? new byte[0] : CONTENT;
                S3Client client = writeClient(version, bytes.length);
                try (var provider = provider(client)) {
                    var result = provider.compose(new PagedObjectCompositionRequest("invoices", target(),
                        "result", "text/plain", OWNER, "compose", bytes, List.of("stage/empty.part"), new byte[0]))
                        .toCompletableFuture().join();
                    assertEquals(version, result.reference().version());
                    assertEquals(checksum(bytes), result.reference().checksum());
                }
            }
        }
    }

    @Test
    void typedJsonRetainsVersionAndVersionTamperingIsRejectedBeforeS3() throws Exception {
        S3Client client = delegatedClient("version-one", false, false);
        PayloadReference original;
        try (var provider = provider(client)) {
            original = provider.complete(target(), token(), OWNER).toCompletableFuture().join().reference();
        }
        var mapper = PipelineJson.mapper().copy().registerModule(new com.fasterxml.jackson.datatype.jdk8.Jdk8Module());
        PayloadReference restored = mapper.readValue(mapper.writeValueAsBytes(original), PayloadReference.class);
        assertEquals("version-one", restored.version());
        assertEquals(original, restored);
        new S3ReferenceAuthority().verify(restored);
        PayloadReference tampered = new PayloadReference(restored.provider(), restored.container(), restored.key(),
            restored.contentType(), restored.codec(), restored.checksum(), restored.sizeBytes(), "version-two",
            restored.metadata(), restored.connectorOrigin());
        S3Client untouched = mock(S3Client.class);
        try (var source = new S3ObjectSourceProvider(untouched)) {
            assertThrows(CompletionException.class, () -> source.materialize(tampered, CONTENT.length)
                .toCompletableFuture().join());
        }
        verify(untouched, never()).headObject(any(HeadObjectRequest.class));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private S3Client delegatedClient(String version, boolean existing, boolean corrupt) {
        S3Client client = writeClient(version, CONTENT.length);
        AtomicBoolean completed = new AtomicBoolean(existing);
        when(client.headObject(any(HeadObjectRequest.class))).thenAnswer(invocation -> {
            HeadObjectRequest request = invocation.getArgument(0);
            if (!completed.get()) throw S3Exception.builder().statusCode(404).build();
            // The initial existing-object observation selects version-one. Subsequent reads must pin it.
            String observedVersion = existing || version == null || version.equals(request.versionId())
                ? version : "newer-version-with-same-etag";
            return head(observedVersion, CONTENT.length);
        });
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenAnswer(ignored -> {
            completed.set(true);
            return CompleteMultipartUploadResponse.builder().eTag("same-etag").versionId(version).build();
        });
        when(client.listParts(any(ListPartsRequest.class))).thenReturn(ListPartsResponse.builder()
            .parts(Part.builder().partNumber(1).size((long) CONTENT.length).eTag("part").build()).build());
        when(client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class))).thenAnswer(invocation -> {
            GetObjectRequest request = invocation.getArgument(0);
            boolean wrongLatest = version != null && !version.equals(request.versionId());
            byte[] bytes = corrupt || wrongLatest ? "other\n".getBytes(StandardCharsets.UTF_8) : CONTENT;
            return new ResponseInputStream<>(GetObjectResponse.builder().versionId(request.versionId()).build(),
                AbortableInputStream.create(new ByteArrayInputStream(bytes)));
        });
        return client;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private S3Client writeClient(String version, int bytes) {
        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload").build());
        when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenReturn(UploadPartResponse.builder().eTag("part").build());
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompleteMultipartUploadResponse.builder().eTag("same-etag").versionId(version).build());
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().eTag("same-etag").versionId(version).build());
        when(client.headObject(any(HeadObjectRequest.class))).thenAnswer(invocation -> {
            HeadObjectRequest request = invocation.getArgument(0);
            if (request.key().endsWith(".part")) return head(null, 0);
            assertEquals(version, request.versionId(), "Verification must not race latest");
            return head(version, bytes);
        });
        when(client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
            .thenAnswer(ignored -> new ResponseInputStream<>(GetObjectResponse.builder().build(),
                AbortableInputStream.create(new ByteArrayInputStream(new byte[0]))));
        return client;
    }

    private HeadObjectResponse head(String version, int bytes) {
        return HeadObjectResponse.builder().versionId(version).eTag("same-etag")
            .contentLength((long) bytes).contentType("text/plain").metadata(OWNER).build();
    }

    private S3ObjectTargetProvider provider(S3Client client) {
        return new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024);
    }

    private PipelineObjectPublishConfig target() {
        return new PipelineObjectPublishConfig("invoices", "object", "s3", Optional.of("objects"),
            Map.of("bucket", "payloads", "prefix", "invoices"), PipelineObjectNamingConfig.defaults(),
            PipelineObjectPublishPayloadConfig.defaults(), PipelineObjectPublishGroupingConfig.defaults());
    }

    private String token() {
        return new S3DelegatedUploadToken().issue(new S3DelegatedUploadToken.Claims("invoices", "payloads",
            "invoices/uploads/reserved", "upload", "text/plain", CONTENT.length, 5 * 1024 * 1024,
            checksum(CONTENT), Instant.now().plusSeconds(900), "retry", OWNER));
    }

    private static String checksum(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new AssertionError(failure);
        }
    }
}
