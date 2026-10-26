package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;
import org.pipelineframework.objectpublish.PagedObjectCompositionRequest;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectResult;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.model.CopyPartResult;

class S3ObjectTargetProviderTest {

    @Test
    void streamsMultipartUploadAndCompletesOnClose() {
        S3Client client = mock(S3Client.class);
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompleteMultipartUploadResponse.builder().eTag("completed-etag").versionId("source-version").build());
        when(client.copyObject(any(CopyObjectRequest.class))).thenReturn(CopyObjectResponse.builder()
            .copyObjectResult(CopyObjectResult.builder().eTag("metadata-etag").build()).versionId("metadata-version").build());
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
        when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenReturn(UploadPartResponse.builder().eTag("etag-1").build());
        S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024);
        ObjectWriteSession session = provider.open(openRequest()).toCompletableFuture().join();

        byte[] payload = new byte[5 * 1024 * 1024];
        session.write(ByteBuffer.wrap(payload)).toCompletableFuture().join();
        ObjectWriteResult result = session.close(new ObjectWriteCloseRequest(
            payload.length,
            sha256(payload),
            Map.of("recordCount", "1"))).toCompletableFuture().join();

        ArgumentCaptor<CreateMultipartUploadRequest> createCaptor =
            ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
        verify(client).createMultipartUpload(createCaptor.capture());
        assertEquals("payments", createCaptor.getValue().bucket());
        assertEquals("out/payments.csv", createCaptor.getValue().key());
        verify(client).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        assertEquals("s3", result.reference().provider());
        assertEquals("payments", result.reference().container());
        assertEquals("out/payments.csv", result.reference().key());
        assertEquals("1", result.reference().metadata().get("recordCount"));
        assertTrue(result.reference().metadata().containsKey(S3ReferenceAuthority.CAPABILITY_METADATA));
        assertEquals(
            S3ObjectSourceProvider.CHECKSUM_KIND_SHA256,
            result.reference().metadata().get(S3ObjectSourceProvider.CHECKSUM_KIND_METADATA));
        assertEquals(sha256(payload), result.reference().checksum());
        assertEquals("metadata-etag",
            result.reference().metadata().get(S3ObjectSourceProvider.ORIGINAL_ETAG_METADATA));
        assertEquals("metadata-version", result.reference().version());
        ArgumentCaptor<CopyObjectRequest> copy = ArgumentCaptor.forClass(CopyObjectRequest.class);
        verify(client).copyObject(copy.capture());
        assertEquals("source-version", copy.getValue().sourceVersionId());
        assertEquals("completed-etag", copy.getValue().copySourceIfMatch());
        assertEquals("1", copy.getValue().metadata().get("recordcount"));
        assertEquals("1", S3ObjectTargetProvider.restoreFinalMetadata(copy.getValue().metadata()).get("recordCount"));
    }

    @Test
    void emptyOutputPersistsCloseMetadataWithoutCopyingAnObject() {
        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("empty-upload").build());
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().eTag("empty-etag").versionId("empty-version").build());
        try (var provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024)) {
            var result = provider.open(openRequest()).toCompletableFuture().join()
                .close(new ObjectWriteCloseRequest(0, sha256(new byte[0]), Map.of("recordCount", "0")))
                .toCompletableFuture().join();
            var put = ArgumentCaptor.forClass(PutObjectRequest.class);
            verify(client).putObject(put.capture(), any(RequestBody.class));
            assertEquals("0", S3ObjectTargetProvider.restoreFinalMetadata(put.getValue().metadata()).get("recordCount"));
            assertEquals("empty-version", result.reference().version());
            verify(client, never()).copyObject(any(CopyObjectRequest.class));
        }
    }

    @Test
    void aFreshPagedReaderRestoresCaseSensitiveFinalMetadata() {
        S3Client client = mock(S3Client.class);
        var metadata = Map.of("tpf.page.index", "0", "tpf.page.group", "group", "tpf.page.finalkey", "final.csv",
            "tpf.page.contenttype", "text/csv", "tpf.page.partkey", "page.part", "recordcount", "10",
            S3ObjectTargetProvider.FINAL_METADATA_PREFIX + "7265636f7264436f756e74", "MTA=");
        when(client.listObjectsV2(any(software.amazon.awssdk.services.s3.model.ListObjectsV2Request.class)))
            .thenReturn(software.amazon.awssdk.services.s3.model.ListObjectsV2Response.builder().isTruncated(false)
                .contents(software.amazon.awssdk.services.s3.model.S3Object.builder().key("out/page.part").build()).build());
        when(client.headObject(any(HeadObjectRequest.class))).thenReturn(HeadObjectResponse.builder().metadata(metadata).build());
        try (var reader = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024)) {
            var parts = reader.listParts(new org.pipelineframework.objectpublish.PagedObjectPartQuery(
                "results", openRequest().target(), "stage/")).toCompletableFuture().join();
            assertEquals(1, parts.size());
            assertEquals("10", parts.getFirst().metadata().get("recordCount"));
            assertTrue(parts.getFirst().metadata().keySet().stream().noneMatch(k -> k.startsWith(S3ObjectTargetProvider.FINAL_METADATA_PREFIX)));
        }
    }

    @Test
    void malformedFinalMetadataFailsInsteadOfReturningAnIncorrectCount() {
        assertThrows(IllegalStateException.class, () -> S3ObjectTargetProvider.restoreFinalMetadata(
            Map.of(S3ObjectTargetProvider.FINAL_METADATA_PREFIX + "invalid-hex", "MTA=")));
        assertEquals(Map.of("legacy", "value"), S3ObjectTargetProvider.restoreFinalMetadata(Map.of("legacy", "value")));
    }

    @Test
    void largeOutputUsesVersionPinnedMultipartCopyWithoutDownloadingPayload() {
        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("metadata-upload").build());
        when(client.uploadPartCopy(any(UploadPartCopyRequest.class))).thenReturn(UploadPartCopyResponse.builder()
            .copyPartResult(CopyPartResult.builder().eTag("copied-part").build()).build());
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompleteMultipartUploadResponse.builder().eTag("large-final").versionId("large-version").build());
        var result = S3ObjectTargetProvider.replaceMetadata(client, "bucket", "path/part", "source-version",
            "source-etag", "text/csv", 6_000_000_000L, Map.of("recordcount", "10"));
        var copied = ArgumentCaptor.forClass(UploadPartCopyRequest.class);
        verify(client, org.mockito.Mockito.times(6)).uploadPartCopy(copied.capture());
        assertEquals("bytes=0-1073741823", copied.getAllValues().getFirst().copySourceRange());
        assertTrue(copied.getAllValues().getLast().copySourceRange().endsWith("-5999999999"));
        assertTrue(copied.getAllValues().stream().allMatch(r -> "source-version".equals(r.sourceVersionId())
            && "source-etag".equals(r.copySourceIfMatch())));
        assertEquals("large-version", result.version());
        verify(client, never()).getObject(any(GetObjectRequest.class));
        verify(client, never()).getObject(any(GetObjectRequest.class),
            any(software.amazon.awssdk.core.sync.ResponseTransformer.class));
        verify(client, never()).getObjectAsBytes(any(GetObjectRequest.class));
        verify(client, never()).copyObject(any(CopyObjectRequest.class));
    }

    @Test
    void failedMetadataMultipartCopyIsAbortedAndDoesNotPublishSuccess() {
        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("failed-copy").build());
        when(client.uploadPartCopy(any(UploadPartCopyRequest.class))).thenThrow(new IllegalStateException("source changed"));
        assertThrows(IllegalStateException.class, () -> S3ObjectTargetProvider.replaceMetadata(client,
            "bucket", "part", "source-version", "source-etag", "text/csv", 6_000_000_000L, Map.of()));
        verify(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        verify(client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }

    @Test
    void metadataFailureRetainsCompletedPayloadAndFailsTheWrite() {
        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("data-upload").build());
        when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenReturn(UploadPartResponse.builder().eTag("data-part").build());
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompleteMultipartUploadResponse.builder().eTag("data-etag").versionId("retained-version").build());
        when(client.copyObject(any(CopyObjectRequest.class))).thenThrow(new IllegalStateException("metadata unavailable"));
        try (var provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024)) {
            var session = provider.open(openRequest()).toCompletableFuture().join();
            session.write(ByteBuffer.wrap(new byte[] {1})).toCompletableFuture().join();
            var failed = assertThrows(CompletionException.class, () -> session.close(
                new ObjectWriteCloseRequest(1, sha256(new byte[] {1}), Map.of("recordCount", "1")))
                .toCompletableFuture().join());
            assertEquals("metadata unavailable", failed.getCause().getMessage());
            session.abort(failed).toCompletableFuture().join();
            verify(client, never()).deleteObject(any(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.class));
        }
    }

    @Test
    void encodedCloseMetadataSizeIsValidatedBeforeCompletingPayload() {
        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("oversized-metadata").build());
        RuntimeException abortFailure = new RuntimeException("abort unavailable");
        doThrow(abortFailure).when(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        try (var provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024)) {
            var session = provider.open(openRequest()).toCompletableFuture().join();
            session.write(ByteBuffer.wrap(new byte[] {1})).toCompletableFuture().join();
            var failure = assertThrows(CompletionException.class, () -> session.close(
                new ObjectWriteCloseRequest(1, sha256(new byte[] {1}), Map.of("label", "é".repeat(600))))
                .toCompletableFuture().join());
            assertEquals("S3 stored user metadata exceeds 2048 UTF-8 bytes", failure.getCause().getMessage());
            assertSame(abortFailure, failure.getCause().getSuppressed()[0]);
            verify(client, never()).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
            verify(client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        }
    }

    @Test
    void nonAsciiCloseMetadataWithinStoredSizeLimitRemainsSupported() {
        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("unicode-metadata").build());
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().eTag("empty-etag").build());
        try (var provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024)) {
            provider.open(openRequest()).toCompletableFuture().join()
                .close(new ObjectWriteCloseRequest(0, sha256(new byte[0]), Map.of("finalLabel", "é".repeat(400))))
                .toCompletableFuture().join();
            var put = ArgumentCaptor.forClass(PutObjectRequest.class);
            verify(client).putObject(put.capture(), any(RequestBody.class));
            assertEquals("é".repeat(400),
                S3ObjectTargetProvider.restoreFinalMetadata(put.getValue().metadata()).get("finalLabel"));
        }
    }

    @Test
    void abortsMultipartUploadWhenCallerChecksumDoesNotMatchWrittenBytes() {
        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
        when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
            .thenReturn(UploadPartResponse.builder().eTag("etag-1").build());
        RuntimeException abortFailure = new RuntimeException("abort failed");
        doThrow(abortFailure).when(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024);
        ObjectWriteSession session = provider.open(openRequest()).toCompletableFuture().join();
        byte[] payload = new byte[5 * 1024 * 1024];
        session.write(ByteBuffer.wrap(payload)).toCompletableFuture().join();

        CompletionException failure = assertThrows(CompletionException.class, () -> session.close(
            new ObjectWriteCloseRequest(payload.length, "not-the-digest", Map.of())).toCompletableFuture().join());

        assertEquals("S3 written payload checksum mismatch", failure.getCause().getMessage());
        assertSame(abortFailure, failure.getCause().getSuppressed()[0]);
        verify(client).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
        verify(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        verify(client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }

    @Test
    void trimsConfiguredRegionInPublishedReferenceMetadata() {
        S3Client client = mock(S3Client.class);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().eTag("empty-etag").build());
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
        S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024);
        ObjectWriteSession session = provider.open(openRequest("  eu-west-1  ")).toCompletableFuture().join();

        ObjectWriteResult result = session.close(new ObjectWriteCloseRequest(0, sha256(new byte[0]), Map.of()))
            .toCompletableFuture().join();

        assertEquals("eu-west-1", result.reference().metadata().get(S3ObjectSourceProvider.REGION_METADATA));
    }

    @Test
    void abortCancelsMultipartUpload() {
        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
        S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024);
        ObjectWriteSession session = provider.open(openRequest()).toCompletableFuture().join();

        session.abort(new RuntimeException("failed")).toCompletableFuture().join();

        verify(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    @Test
    void serializesOverlappingWritesBeforeCloseAndRejectsLaterWrites() throws Exception {
        S3Client client = mock(S3Client.class);
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompleteMultipartUploadResponse.builder().eTag("completed-etag").build());
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
        CountDownLatch uploadStarted = new CountDownLatch(1);
        CountDownLatch releaseUpload = new CountDownLatch(1);
        when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenAnswer(ignored -> {
            uploadStarted.countDown();
            releaseUpload.await();
            return UploadPartResponse.builder().eTag("etag-1").build();
        });
        var executor = Executors.newSingleThreadExecutor();
        try {
            S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client, executor, 5 * 1024 * 1024);
            ObjectWriteSession session = provider.open(openRequest()).toCompletableFuture().join();
            byte[] first = new byte[5 * 1024 * 1024];
            byte[] second = new byte[] {7};

            var firstWrite = session.write(ByteBuffer.wrap(first));
            assertTrue(uploadStarted.await(5, TimeUnit.SECONDS));
            var secondWrite = session.write(ByteBuffer.wrap(second));
            var close = session.close(new ObjectWriteCloseRequest(
                first.length + second.length, sha256(first, second), Map.of()));
            CompletionException lateWrite = assertThrows(CompletionException.class, () ->
                session.write(ByteBuffer.wrap(new byte[] {9})).toCompletableFuture().join());
            assertEquals("S3 write session is closing or closed", lateWrite.getCause().getMessage());

            releaseUpload.countDown();
            firstWrite.toCompletableFuture().join();
            secondWrite.toCompletableFuture().join();
            ObjectWriteResult result = close.toCompletableFuture().join();

            assertEquals(first.length + second.length, result.bytes());
            assertEquals(sha256(first, second), result.checksum());
            verify(client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        } finally {
            releaseUpload.countDown();
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void serializesAbortAfterOverlappingWriteAndRejectsLaterWrites() throws Exception {
        S3Client client = mock(S3Client.class);
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
        CountDownLatch uploadStarted = new CountDownLatch(1);
        CountDownLatch releaseUpload = new CountDownLatch(1);
        when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenAnswer(ignored -> {
            uploadStarted.countDown();
            releaseUpload.await();
            return UploadPartResponse.builder().eTag("etag-1").build();
        });
        var executor = Executors.newSingleThreadExecutor();
        try {
            S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client, executor, 5 * 1024 * 1024);
            ObjectWriteSession session = provider.open(openRequest()).toCompletableFuture().join();
            var write = session.write(ByteBuffer.wrap(new byte[5 * 1024 * 1024]));
            assertTrue(uploadStarted.await(5, TimeUnit.SECONDS));

            var abort = session.abort(new RuntimeException("cancelled"));
            CompletionException lateWrite = assertThrows(CompletionException.class, () ->
                session.write(ByteBuffer.wrap(new byte[] {9})).toCompletableFuture().join());
            assertEquals("S3 write session is closing or closed", lateWrite.getCause().getMessage());

            releaseUpload.countDown();
            write.toCompletableFuture().join();
            abort.toCompletableFuture().join();
            verify(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
            verify(client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
        } finally {
            releaseUpload.countDown();
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void rejectsClientResolutionAfterClose() {
        S3ObjectTargetProvider provider = new S3ObjectTargetProvider(mock(S3Client.class), Runnable::run, 5 * 1024 * 1024);
        provider.close();

        CompletionException exception = assertThrows(CompletionException.class,
            () -> provider.open(openRequest()).toCompletableFuture().join());

        assertEquals("S3 object target provider is closed", exception.getCause().getMessage());
    }

    @Test
    void streamComposesS3PagePartsWithFinalObjectParity() throws Exception {
        S3Client client = mock(S3Client.class);
        when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
            .thenReturn(CompleteMultipartUploadResponse.builder().eTag("compose-completed-etag").build());
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("compose-upload").build());
        byte[] first = "first,\"María\nGarcía\"\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] second = "second,Zoë\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        when(client.headObject(any(HeadObjectRequest.class))).thenAnswer(invocation -> {
            HeadObjectRequest request = invocation.getArgument(0);
            return HeadObjectResponse.builder()
                .contentLength(request.key().endsWith("page-0.part") ? (long) first.length : (long) second.length)
                .build();
        });
        when(client.getObject(
            org.mockito.Mockito.<GetObjectRequest>any(),
            any(software.amazon.awssdk.core.sync.ResponseTransformer.class)))
            .thenAnswer(invocation -> {
                software.amazon.awssdk.services.s3.model.GetObjectRequest request = invocation.getArgument(0);
                byte[] bytes = request.key().endsWith("page-0.part") ? first : second;
                return new ResponseInputStream<>(GetObjectResponse.builder().build(),
                    AbortableInputStream.create(new ByteArrayInputStream(bytes)));
            });
        ByteArrayOutputStream published = new ByteArrayOutputStream();
        when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenAnswer(invocation -> {
            RequestBody body = invocation.getArgument(1);
            try (var input = body.contentStreamProvider().newStream()) {
                published.write(input.readAllBytes());
            }
            return UploadPartResponse.builder().eTag("compose-etag").build();
        });
        S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024);
        PipelineObjectPublishConfig target = openRequest("eu-west-1").target();

        ObjectWriteResult result = provider.compose(new PagedObjectCompositionRequest(
            target.name(), target, "payments.csv", "text/csv", Map.of("recordCount", "2"),
            "compose-run", "header\n".getBytes(java.nio.charset.StandardCharsets.UTF_8),
            List.of(".tpf/page-0.part", ".tpf/page-1.part"),
            "footer\n".getBytes(java.nio.charset.StandardCharsets.UTF_8))).toCompletableFuture().join();

        assertEquals("header\nfirst,\"María\nGarcía\"\nsecond,Zoë\nfooter\n",
            published.toString(java.nio.charset.StandardCharsets.UTF_8));
        assertEquals(published.size(), result.bytes());
        assertEquals(sha256(published.toByteArray()), result.checksum());
        assertEquals("eu-west-1", result.reference().metadata().get(S3ObjectSourceProvider.REGION_METADATA));
        verify(client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }

    private ObjectWriteOpenRequest openRequest() {
        return openRequest(null);
    }

    private ObjectWriteOpenRequest openRequest(String region) {
        Map<String, Object> location = new java.util.LinkedHashMap<>();
        location.put("bucket", "payments");
        location.put("prefix", "out");
        if (region != null) {
            location.put("region", region);
        }
        PipelineObjectPublishConfig target = new PipelineObjectPublishConfig(
            "results",
            "object",
            "s3",
            Map.copyOf(location),
            null,
            null);
        return new ObjectWriteOpenRequest(
            target.name(),
            target,
            "payments.csv",
            "text/csv",
            Map.of("groupKey", "payments.csv"),
            "idempotency");
    }

    private static String sha256(byte[]... payloads) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (byte[] payload : payloads) {
                digest.update(payload);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException failure) {
            throw new IllegalStateException(failure);
        }
    }
}
