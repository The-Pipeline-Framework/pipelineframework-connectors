package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.pipelineframework.config.boundary.PipelineObjectNamingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishGroupingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishPayloadConfig;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.DelegatedPayloadTarget;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.PagedObjectCompositionRequest;
import org.pipelineframework.repository.PayloadReference;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

/** Provider-level local S3 evidence; never a live AWS or consuming-application qualification. */
@EnabledIfEnvironmentVariable(named = "TPF_LOCALSTACK_ENDPOINT", matches = ".+")
class S3VersionPreservationLocalstackIT {
    private static final byte[] CONTENT = "same immutable object\n".getBytes(StandardCharsets.UTF_8);
    private static final Map<String, String> OWNER = Map.of("tpf.payload.owner.tenant", "tenant-a");
    private final AtomicReference<String> responseVersion = new AtomicReference<>();
    private final AtomicReference<String> responseHeader = new AtomicReference<>();
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
    void delegatedReferenceUsesActualSdkAndHttpVersionAndStillReadsOriginalAfterOverwrite() throws Exception {
        try (S3Client client = client(); var targetProvider = new S3ObjectTargetProvider(client);
             var source = new S3ObjectSourceProvider(client); var http = HttpClient.newHttpClient()) {
            String bucket = bucket(client, true);
            var target = target(bucket);
            var reservation = targetProvider.initiate(new DelegatedPayloadTarget.Request("objects", target,
                "text/plain", OWNER, CONTENT.length, checksum(CONTENT), Instant.now().plusSeconds(900), "retry"))
                .toCompletableFuture().join();
            var part = targetProvider.part(target, reservation.token(), OWNER, 1).toCompletableFuture().join();
            HttpRequest.Builder put = HttpRequest.newBuilder(URI.create(part.url()))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(CONTENT));
            part.headers().forEach((name, value) -> {
                if (name.equalsIgnoreCase("content-length")) assertEquals(CONTENT.length, Long.parseLong(value));
                else put.header(name, value);
            });
            assertEquals(200, http.send(put.build(), HttpResponse.BodyHandlers.discarding()).statusCode());
            resetWriteObservation();
            PayloadReference reference = targetProvider.complete(target, reservation.token(), OWNER)
                .toCompletableFuture().join().reference();
            assertActualVersion(client, reference, true);
            assertEquals(reference, targetProvider.complete(target, reservation.token(), OWNER)
                .toCompletableFuture().join().reference());
            var mapper = PipelineJson.mapper().copy().registerModule(new com.fasterxml.jackson.datatype.jdk8.Jdk8Module());
            PayloadReference restored = mapper.readValue(mapper.writeValueAsBytes(reference), PayloadReference.class);
            assertEquals(reference, restored);
            overwriteAndReadOriginal(client, source, restored, CONTENT);
        }
    }

    @Test
    void streamAndPagedWritesPreserveActualMultipartAndEmptyPutVersionsAndUnversionedNulls() throws Exception {
        try (S3Client client = client(); var provider = new S3ObjectTargetProvider(client);
             var source = new S3ObjectSourceProvider(client)) {
            for (boolean versioned : new boolean[] {true, false}) {
                String bucket = bucket(client, versioned);
                var target = target(bucket);
                for (boolean empty : new boolean[] {false, true}) {
                    byte[] bytes = empty ? new byte[0] : CONTENT;
                    var session = provider.open(new ObjectWriteOpenRequest("objects", target, "stream-"+empty,
                        "text/plain", OWNER, "stream-"+empty)).toCompletableFuture().join();
                    if (!empty) session.write(ByteBuffer.wrap(bytes)).toCompletableFuture().join();
                    resetWriteObservation();
                    PayloadReference stream = session.close(new ObjectWriteCloseRequest(
                        bytes.length, checksum(bytes), Map.of())).toCompletableFuture().join().reference();
                    assertActualVersion(client, stream, versioned);
                    if (versioned) overwriteAndReadOriginal(client, source, stream, bytes);
                    resetWriteObservation();
                    client.putObject(request -> request.bucket(bucket).key("objects/stage-"+empty),
                        RequestBody.empty());
                    resetWriteObservation();
                    PayloadReference composed = provider.compose(new PagedObjectCompositionRequest("objects",
                        target, "compose-"+empty, "text/plain", OWNER, "compose-"+empty, bytes,
                        List.of("stage-"+empty), new byte[0])).toCompletableFuture().join().reference();
                    assertActualVersion(client, composed, versioned);
                    if (versioned) overwriteAndReadOriginal(client, source, composed, bytes);
                }
            }
        }
    }

    private void resetWriteObservation() {
        responseVersion.set(null);
        responseHeader.set(null);
    }

    private void assertActualVersion(S3Client client, PayloadReference reference, boolean versioned) {
        var actual = client.headObject(request -> request.bucket(reference.container()).key(reference.key()));
        assertEquals(responseVersion.get(), reference.version());
        assertEquals(responseHeader.get(), reference.version());
        assertEquals(actual.versionId(), reference.version());
        if (versioned) assertNotNull(reference.version());
        else assertNull(reference.version());
        System.out.println("LOCAL_S3_VERSION_MATCH provider=" + reference.version()
            + " sdk=" + responseVersion.get() + " http=" + responseHeader.get() + " head=" + actual.versionId());
    }

    private void overwriteAndReadOriginal(S3Client client, S3ObjectSourceProvider source,
                                         PayloadReference reference, byte[] expected) {
        var replaced = client.putObject(request -> request.bucket(reference.container()).key(reference.key())
            .contentType("text/plain").metadata(OWNER), RequestBody.fromString("newer different bytes"));
        assertNotEquals(reference.version(), replaced.versionId());
        assertArrayEquals(expected, source.materialize(reference, Math.max(1, expected.length))
            .toCompletableFuture().join().bytes());
        var retained = client.getObjectAsBytes(request -> request.bucket(reference.container())
            .key(reference.key()).versionId(reference.version()));
        assertArrayEquals(expected, retained.asByteArray());
    }

    private String bucket(S3Client client, boolean versioned) {
        String bucket = "tpf-version-" + UUID.randomUUID();
        client.createBucket(request -> request.bucket(bucket));
        if (versioned) {
            client.putBucketVersioning(request -> request.bucket(bucket)
                .versioningConfiguration(config -> config.status("Enabled")));
            assertEquals("Enabled", client.getBucketVersioning(request -> request.bucket(bucket)).statusAsString());
        }
        return bucket;
    }

    private S3Client client() {
        URI endpoint = URI.create(System.getenv("TPF_LOCALSTACK_ENDPOINT"));
        assertTrue(List.of("127.0.0.1", "localhost").contains(endpoint.getHost()), "Only a local fixture is allowed");
        return S3Client.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
            .overrideConfiguration(config -> config.addExecutionInterceptor(new ExecutionInterceptor() {
                @Override
                public void afterExecution(Context.AfterExecution context, ExecutionAttributes attributes) {
                    if (context.response() instanceof CompleteMultipartUploadResponse completed) {
                        responseVersion.set(completed.versionId());
                        responseHeader.set(context.httpResponse().firstMatchingHeader("x-amz-version-id").orElse(null));
                    } else if (context.response() instanceof PutObjectResponse put) {
                        responseVersion.set(put.versionId());
                        responseHeader.set(context.httpResponse().firstMatchingHeader("x-amz-version-id").orElse(null));
                    }
                }
            })).build();
    }

    private PipelineObjectPublishConfig target(String bucket) {
        return new PipelineObjectPublishConfig("objects", "object", "s3", Optional.of("objects"),
            Map.of("bucket", bucket, "prefix", "objects", "region", "us-east-1", "pathStyleAccess", true),
            PipelineObjectNamingConfig.defaults(), PipelineObjectPublishPayloadConfig.defaults(),
            PipelineObjectPublishGroupingConfig.defaults());
    }

    private static String checksum(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
