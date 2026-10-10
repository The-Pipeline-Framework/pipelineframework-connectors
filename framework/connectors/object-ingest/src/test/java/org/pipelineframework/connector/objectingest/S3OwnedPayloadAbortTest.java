package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.pipelineframework.connector.ObjectReadSession;
import org.pipelineframework.repository.PayloadReference;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/** Exercises the AWS SDK's documented Apache-client drain-on-close contract. */
class S3OwnedPayloadAbortTest {
    @Test
    void sdkControlDistinguishesDrainingCloseFromAbort() throws Exception {
        DrainingBody closed = new DrainingBody();
        response(closed, "abc123").close();
        assertEquals(3, closed.bytesRead);
        assertEquals(0, closed.aborts);

        DrainingBody aborted = new DrainingBody();
        response(aborted, "abc123").abort();
        assertEquals(0, aborted.bytesRead);
        assertEquals(1, aborted.aborts);
    }

    @Test
    void rejectedGetResponseAbortsWithoutDrainingUntrustedBody() {
        DrainingBody body = new DrainingBody();
        S3Client client = client(body, "changed");
        S3ReferenceAuthority authority = new S3ReferenceAuthority();
        try (S3ObjectSourceProvider provider = new S3ObjectSourceProvider(client, Runnable::run, authority)) {
            CompletionException failure = assertThrows(CompletionException.class,
                () -> provider.openRead(reference(authority)).toCompletableFuture().join());
            assertEquals("S3 payload changed during materialization: invoice.pdf", failure.getCause().getMessage());
            assertEquals(0, body.bytesRead, "rejection must not consume the response body during cleanup");
            assertEquals(1, body.aborts, "rejection must release the provider connection by aborting");
        }
    }

    @Test
    void acceptedGetReadsOnlyDemandedBytesAndCancellationAbortsRemainder() {
        DrainingBody body = new DrainingBody();
        S3ReferenceAuthority authority = new S3ReferenceAuthority();
        S3Client client = client(body, "abc123");
        try (S3ObjectSourceProvider provider = new S3ObjectSourceProvider(client, Runnable::run, authority)) {
            ObjectReadSession session = provider.openRead(reference(authority)).toCompletableFuture().join();
            ArgumentCaptor<GetObjectRequest> request = ArgumentCaptor.forClass(GetObjectRequest.class);
            verify(client).getObject(request.capture());
            assertEquals("\"abc123\"", request.getValue().ifMatch());
            assertEquals(0, body.bytesRead);
            assertEquals(1, session.read(1).toCompletableFuture().join().orElseThrow().remaining());
            assertEquals(1, body.bytesRead);
            session.close();
            assertEquals(1, body.bytesRead);
            assertEquals(1, body.aborts);
        }
    }

    @Test
    void completedGetClosesNormallyWithoutAborting() {
        DrainingBody body = new DrainingBody();
        S3ReferenceAuthority authority = new S3ReferenceAuthority();
        try (S3ObjectSourceProvider provider = new S3ObjectSourceProvider(
            client(body, "abc123"), Runnable::run, authority)) {
            ObjectReadSession session = provider.openRead(reference(authority)).toCompletableFuture().join();
            assertEquals(3, session.read(3).toCompletableFuture().join().orElseThrow().remaining());
            assertEquals(Optional.empty(), session.read(3).toCompletableFuture().join());
            assertEquals(3, body.bytesRead);
            assertEquals(1, body.closes);
            assertEquals(0, body.aborts);
        }
    }

    private S3Client client(DrainingBody body, String getEtag) {
        S3Client client = mock(S3Client.class);
        when(client.headObject(any(HeadObjectRequest.class)))
            .thenReturn(HeadObjectResponse.builder().contentLength(3L).eTag("abc123").build());
        when(client.getObject(any(GetObjectRequest.class))).thenReturn(response(body, getEtag));
        return client;
    }

    private ResponseInputStream<GetObjectResponse> response(DrainingBody body, String etag) {
        return new ResponseInputStream<>(GetObjectResponse.builder().contentLength(3L).eTag(etag).build(),
            AbortableInputStream.create(body, () -> body.aborts++), Duration.ZERO);
    }

    private PayloadReference reference(S3ReferenceAuthority authority) {
        return authority.issue(new PayloadReference("s3", "docs", "invoice.pdf", "application/pdf", "raw",
            "abc123", 3L, null, Map.of(), Optional.empty()));
    }

    private static final class DrainingBody extends InputStream {
        private int bytesRead;
        private int aborts;
        private int closes;

        @Override
        public int read() {
            if (aborts > 0 || bytesRead == 3) {
                return -1;
            }
            return ++bytesRead;
        }

        @Override
        public void close() {
            closes++;
            while (read() != -1) {
                // Apache-client close drains remaining response bytes for connection reuse.
            }
        }
    }
}
