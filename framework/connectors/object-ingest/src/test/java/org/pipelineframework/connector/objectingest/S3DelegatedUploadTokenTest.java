package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class S3DelegatedUploadTokenTest {
    private String previousKey;

    @BeforeEach
    void configureKey() {
        previousKey = System.getProperty("tpf.object.reference.hmac-key");
        System.setProperty("tpf.object.reference.hmac-key",
            java.util.Base64.getEncoder().encodeToString(new byte[32]));
    }

    @AfterEach
    void restoreKey() {
        if (previousKey == null) System.clearProperty("tpf.object.reference.hmac-key");
        else System.setProperty("tpf.object.reference.hmac-key", previousKey);
    }

    @Test
    void rejectsTamperingAndExpiry() {
        var codec = new S3DelegatedUploadToken();
        Instant now = Instant.parse("2026-10-12T10:00:00Z");
        var claims = new S3DelegatedUploadToken.Claims("invoices", "bucket", "prefix/uploads/one",
            "upload-id", "application/pdf", 6_000_000, 5 * 1024 * 1024,
            "a".repeat(64), now.plusSeconds(900), "retry-1", Map.of("owner", "tenant-a"));
        String token = codec.issue(claims);
        assertEquals(claims, codec.verify(token, now));
        char replacement = token.charAt(20) == 'A' ? 'B' : 'A';
        String tampered = token.substring(0, 20) + replacement + token.substring(21);
        assertThrows(SecurityException.class, () -> codec.verify(tampered, now));
        assertThrows(SecurityException.class, () -> codec.verify(token, now.plusSeconds(901)));
    }
}
