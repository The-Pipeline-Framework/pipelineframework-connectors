package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.pipelineframework.config.boundary.PipelineObjectNamingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishGroupingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishPayloadConfig;
import org.pipelineframework.connector.DelegatedPayloadTarget;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

/** Run against a local S3 emulator with TPF_LOCALSTACK_ENDPOINT. */
@EnabledIfEnvironmentVariable(named = "TPF_LOCALSTACK_ENDPOINT", matches = ".+")
class S3DelegatedUploadLocalstackIT {
    @Test
    void presignedPartsCompleteIntoVerifiedOwnedReferenceAndDownload() throws Exception {
        String endpoint = System.getenv("TPF_LOCALSTACK_ENDPOINT");
        String previousKey = System.getProperty("tpf.object.reference.hmac-key");
        System.setProperty("tpf.object.reference.hmac-key",
            java.util.Base64.getEncoder().encodeToString(new byte[32]));
        String bucket = "tpf-delegated-" + UUID.randomUUID();
        try (S3Client client = S3Client.builder().endpointOverride(URI.create(endpoint))
            .region(Region.US_EAST_1).serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(true).build())
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
            .build();
             S3ObjectSourceProvider source = new S3ObjectSourceProvider(client)) {
            client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
            PipelineObjectPublishConfig target = new PipelineObjectPublishConfig("invoices", "object", "s3",
                Optional.of("objects"), Map.of("bucket", bucket, "prefix", "invoices",
                    "region", "us-east-1", "pathStyleAccess", true),
                PipelineObjectNamingConfig.defaults(), PipelineObjectPublishPayloadConfig.defaults(),
                PipelineObjectPublishGroupingConfig.defaults());
            byte[] content = new byte[10 * 1024 * 1024];
            Arrays.fill(content, (byte) 'p');
            String checksum = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
            Map<String, String> owner = Map.of("tpf.payload.owner.tenant", "tenant-a");
            var request = new DelegatedPayloadTarget.Request("invoices", target,
                "application/octet-stream", owner, content.length, checksum, Instant.now().plusSeconds(900),
                "retry-1");
            try (S3ObjectTargetProvider provider = new S3ObjectTargetProvider(client);
                 HttpClient http = HttpClient.newHttpClient()) {
                var reservation = provider.initiate(request).toCompletableFuture().join();
                assertEquals(reservation.token(), provider.initiate(request).toCompletableFuture().join().token());
                assertThrows(CompletionException.class, () -> provider.part(target, reservation.token(),
                    Map.of("tpf.payload.owner.tenant", "tenant-b"), 1).toCompletableFuture().join());
                int partSize = reservation.partSizeBytes();
                var firstPart = provider.part(target, reservation.token(), owner, 1).toCompletableFuture().join();
                assertEquals(URI.create(endpoint).getHost(), URI.create(firstPart.url()).getHost());
                assertTrue(firstPart.url().contains("X-Amz-Credential=test"));
                upload(http, firstPart, Arrays.copyOfRange(content, 0, partSize));
                upload(http, provider.part(target, reservation.token(), owner, 2).toCompletableFuture().join(),
                    Arrays.copyOfRange(content, partSize, content.length));
                var reference = provider.complete(target, reservation.token(), owner)
                    .toCompletableFuture().join().reference();
                assertEquals(checksum, reference.checksum());
                assertEquals(content.length, reference.sizeBytes());
                assertTrue(reference.metadata().containsKey(S3ObjectSourceProvider.ORIGINAL_ETAG_METADATA));
                var download = source.authorizeDownload(reference, Duration.ofMinutes(1))
                    .toCompletableFuture().join();
                assertEquals(URI.create(endpoint).getHost(), URI.create(download.url()).getHost());
                assertTrue(download.url().contains("X-Amz-Credential=test"));
                HttpRequest.Builder get = HttpRequest.newBuilder(URI.create(download.url())).GET();
                download.headers().forEach(get::header);
                var response = http.send(get.build(), HttpResponse.BodyHandlers.ofByteArray());
                assertEquals(200, response.statusCode(), download.url() + " "
                    + new String(response.body(), java.nio.charset.StandardCharsets.UTF_8));
                assertArrayEquals(content, response.body());

                var wrongChecksum = new DelegatedPayloadTarget.Request("invoices", target,
                    "application/octet-stream", owner, content.length, "0".repeat(64),
                    Instant.now().plusSeconds(900), "retry-2");
                var unverified = provider.initiate(wrongChecksum).toCompletableFuture().join();
                upload(http, provider.part(target, unverified.token(), owner, 1).toCompletableFuture().join(),
                    Arrays.copyOfRange(content, 0, partSize));
                upload(http, provider.part(target, unverified.token(), owner, 2).toCompletableFuture().join(),
                    Arrays.copyOfRange(content, partSize, content.length));
                assertThrows(CompletionException.class, () -> provider.complete(target, unverified.token(), owner)
                    .toCompletableFuture().join());
                var unverifiedClaims = new S3DelegatedUploadToken().verify(unverified.token(), Instant.now());
                assertEquals(404, assertThrows(S3Exception.class, () -> client.headObject(HeadObjectRequest.builder()
                    .bucket(bucket).key(unverifiedClaims.key()).build())).statusCode());

                var incomplete = provider.initiate(new DelegatedPayloadTarget.Request("invoices", target,
                    "application/octet-stream", owner, content.length, checksum,
                    Instant.now().plusSeconds(900), "retry-3")).toCompletableFuture().join();
                upload(http, provider.part(target, incomplete.token(), owner, 1).toCompletableFuture().join(),
                    Arrays.copyOfRange(content, 0, partSize));
                assertThrows(CompletionException.class, () -> provider.complete(target, incomplete.token(), owner)
                    .toCompletableFuture().join());
                provider.abort(target, incomplete.token(), owner).toCompletableFuture().join();
            }
        } finally {
            if (previousKey == null) System.clearProperty("tpf.object.reference.hmac-key");
            else System.setProperty("tpf.object.reference.hmac-key", previousKey);
        }
    }

    private static void upload(HttpClient http, DelegatedPayloadTarget.PartCapability capability, byte[] bytes)
        throws Exception {
        HttpRequest.Builder put = HttpRequest.newBuilder(URI.create(capability.url()))
            .PUT(HttpRequest.BodyPublishers.ofByteArray(bytes));
        capability.headers().forEach((name, value) -> {
            // Java HttpClient derives this restricted header from the body publisher.
            if (name.equalsIgnoreCase("content-length")) assertEquals(bytes.length, Long.parseLong(value));
            else put.header(name, value);
        });
        var response = http.send(put.build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), capability.url() + " " + response.body());
    }
}
