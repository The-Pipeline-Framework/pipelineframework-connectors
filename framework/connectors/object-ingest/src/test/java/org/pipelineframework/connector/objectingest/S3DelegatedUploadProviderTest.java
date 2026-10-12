package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Map;
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
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
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
}
