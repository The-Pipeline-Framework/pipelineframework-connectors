package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.doReturn;

import java.time.Instant;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.connector.DelegatedPayloadTarget;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

class S3DelegatedUploadProviderTest {
    private String previousKey;
    private String previousAccessKey;
    private String previousSecretKey;

    @BeforeEach
    void configureKey() {
        previousKey = System.getProperty("tpf.object.reference.hmac-key");
        previousAccessKey = System.getProperty("aws.accessKeyId");
        previousSecretKey = System.getProperty("aws.secretAccessKey");
        System.setProperty("tpf.object.reference.hmac-key",
            java.util.Base64.getEncoder().encodeToString(new byte[32]));
        System.setProperty("aws.accessKeyId", "test-access-key");
        System.setProperty("aws.secretAccessKey", "test-secret-key");
    }

    @AfterEach
    void restoreKey() {
        if (previousKey == null) System.clearProperty("tpf.object.reference.hmac-key");
        else System.setProperty("tpf.object.reference.hmac-key", previousKey);
        if (previousAccessKey == null) System.clearProperty("aws.accessKeyId");
        else System.setProperty("aws.accessKeyId", previousAccessKey);
        if (previousSecretKey == null) System.clearProperty("aws.secretAccessKey");
        else System.setProperty("aws.secretAccessKey", previousSecretKey);
    }

    @Test
    void reservationIsDurableAndOwnerBoundBeforePartOrAbort() {
        S3Client client = mock(S3Client.class);
        S3ServiceClientConfiguration configuration = mock(S3ServiceClientConfiguration.class);
        when(configuration.region()).thenReturn(Region.EU_WEST_1);
        doReturn(StaticCredentialsProvider.create(AwsBasicCredentials.create("test-access-key", "test-secret-key")))
            .when(configuration).credentialsProvider();
        when(configuration.endpointOverride()).thenReturn(Optional.of(URI.create("http://localhost:4566")));
        when(client.serviceClientConfiguration()).thenReturn(configuration);
        AtomicReference<String> marker = new AtomicReference<>();
        when(client.getObjectAsBytes(any(GetObjectRequest.class))).thenAnswer(ignored -> {
            if (marker.get() == null) {
                throw S3Exception.builder().statusCode(404).message("missing").build();
            }
            return ResponseBytes.fromByteArray(GetObjectResponse.builder().build(),
                marker.get().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        });
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenAnswer(invocation -> {
            RequestBody body = invocation.getArgument(1);
            marker.set(new String(body.contentStreamProvider().newStream().readAllBytes(),
                java.nio.charset.StandardCharsets.UTF_8));
            return PutObjectResponse.builder().build();
        });
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
        S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024);
        PipelineObjectPublishConfig target = new PipelineObjectPublishConfig("invoices", "object", "s3",
            java.util.Optional.of("objects"), Map.of("bucket", "payloads", "prefix", "invoices",
                "region", "eu-west-1"),
            org.pipelineframework.config.boundary.PipelineObjectNamingConfig.defaults(),
            org.pipelineframework.config.boundary.PipelineObjectPublishPayloadConfig.defaults(),
            org.pipelineframework.config.boundary.PipelineObjectPublishGroupingConfig.defaults());
        var request = new DelegatedPayloadTarget.Request("invoices", target, "application/pdf",
            Map.of("owner", "tenant-a"), 6_000_000, "a".repeat(64),
            Instant.now().plusSeconds(900), "retry-1");

        var reserved = provider.initiate(request).toCompletableFuture().join();
        var retried = provider.initiate(request).toCompletableFuture().join();
        assertEquals(5 * 1024 * 1024, reserved.partSizeBytes());
        assertEquals(reserved.token(), retried.token());
        verify(client, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        var part = provider.part(target, reserved.token(), Map.of("owner", "tenant-a"), 1)
            .toCompletableFuture().join();
        assertTrue(part.url().contains("partNumber=1"));
        assertTrue(part.url().contains("uploadId=upload-1"));
        verify(client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
        CompletionException denied = assertThrows(CompletionException.class, () -> provider.abort(target,
            reserved.token(), Map.of("owner", "tenant-b")).toCompletableFuture().join());
        assertEquals(SecurityException.class, denied.getCause().getClass());
        provider.abort(target, reserved.token(), Map.of("owner", "tenant-a")).toCompletableFuture().join();
        verify(client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }

    @Test
    void expiredReservationIsAbortedAndRemovedBeforeReusingIdempotencyKey() {
        S3Client client = mock(S3Client.class);
        Map<String, String> owner = Map.of("owner", "tenant-a");
        String oldToken = new S3DelegatedUploadToken().issue(new S3DelegatedUploadToken.Claims(
            "invoices", "payloads", "invoices/uploads/old", "old-upload", "application/pdf",
            6_000_000, 5 * 1024 * 1024, "a".repeat(64), Instant.now().minusSeconds(1),
            "retry-1", owner));
        when(client.getObjectAsBytes(any(GetObjectRequest.class))).thenReturn(ResponseBytes.fromByteArray(
            GetObjectResponse.builder().eTag("\"old-marker\"").build(),
            oldToken.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(CreateMultipartUploadResponse.builder().uploadId("new-upload").build());
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenReturn(PutObjectResponse.builder().build());
        PipelineObjectPublishConfig target = new PipelineObjectPublishConfig("invoices", "object", "s3",
            java.util.Optional.of("objects"), Map.of("bucket", "payloads", "prefix", "invoices"),
            org.pipelineframework.config.boundary.PipelineObjectNamingConfig.defaults(),
            org.pipelineframework.config.boundary.PipelineObjectPublishPayloadConfig.defaults(),
            org.pipelineframework.config.boundary.PipelineObjectPublishGroupingConfig.defaults());
        var request = new DelegatedPayloadTarget.Request("invoices", target, "application/pdf", owner,
            6_000_000, "a".repeat(64), Instant.now().plusSeconds(900), "retry-1");

        try (S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024)) {
            var fresh = provider.initiate(request).toCompletableFuture().join();
            assertTrue(!oldToken.equals(fresh.token()));
        }
        var abort = org.mockito.ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        verify(client).abortMultipartUpload(abort.capture());
        assertEquals("old-upload", abort.getValue().uploadId());
        var delete = org.mockito.ArgumentCaptor.forClass(DeleteObjectRequest.class);
        verify(client).deleteObject(delete.capture());
        assertEquals("\"old-marker\"", delete.getValue().ifMatch());
    }

    @Test
    void oversizedMetadataIsRejectedBeforeCreatingMultipartUpload() {
        S3Client client = mock(S3Client.class);
        when(client.getObjectAsBytes(any(GetObjectRequest.class)))
            .thenThrow(S3Exception.builder().statusCode(404).message("missing").build());
        Map<String, String> metadata = new java.util.LinkedHashMap<>();
        for (int index = 0; index < 30; index++) metadata.put("key-" + index, "value");
        PipelineObjectPublishConfig target = new PipelineObjectPublishConfig("invoices", "object", "s3",
            java.util.Optional.of("objects"), Map.of("bucket", "payloads"),
            org.pipelineframework.config.boundary.PipelineObjectNamingConfig.defaults(),
            org.pipelineframework.config.boundary.PipelineObjectPublishPayloadConfig.defaults(),
            org.pipelineframework.config.boundary.PipelineObjectPublishGroupingConfig.defaults());
        var request = new DelegatedPayloadTarget.Request("invoices", target, "application/pdf", metadata,
            6_000_000, "a".repeat(64), Instant.now().plusSeconds(900), "retry-1");

        try (S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client, Runnable::run, 5 * 1024 * 1024)) {
            assertThrows(CompletionException.class, () -> provider.initiate(request).toCompletableFuture().join());
        }
        verify(client, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
    }
}
