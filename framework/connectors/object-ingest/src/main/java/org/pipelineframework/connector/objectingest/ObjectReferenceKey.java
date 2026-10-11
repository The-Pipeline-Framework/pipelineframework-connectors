package org.pipelineframework.connector.objectingest;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Derives provider-specific capability keys from a host-held secret. */
final class ObjectReferenceKey {
    private ObjectReferenceKey() {
    }

    static SecretKeySpec forProvider(String provider) {
        String encoded = System.getProperty("tpf.object.reference.hmac-key");
        if (encoded == null || encoded.isBlank()) {
            encoded = System.getenv("TPF_OBJECT_REFERENCE_HMAC_KEY");
        }
        SecretKeySpec rootKey;
        if (encoded == null || encoded.isBlank()) {
            rootKey = FallbackRoot.KEY;
        } else {
            byte[] root;
            try {
                root = Base64.getDecoder().decode(encoded);
            } catch (IllegalArgumentException failure) {
                throw new IllegalStateException("object reference HMAC key must be Base64", failure);
            }
            try {
                if (root.length < 32) {
                    throw new IllegalStateException("object reference HMAC key must contain at least 32 bytes");
                }
                rootKey = new SecretKeySpec(root, "HmacSHA256");
            } finally {
                java.util.Arrays.fill(root, (byte) 0);
            }
        }
        try {
            Mac derivation = Mac.getInstance("HmacSHA256");
            derivation.init(rootKey);
            return new SecretKeySpec(derivation.doFinal(
                ("tpf.object-reference.v1:" + provider).getBytes(StandardCharsets.UTF_8)), "HmacSHA256");
        } catch (java.security.GeneralSecurityException failure) {
            throw new IllegalStateException("object reference HMAC key derivation unavailable", failure);
        }
    }

    private static final class FallbackRoot {
        private static final SecretKeySpec KEY = create();

        private static SecretKeySpec create() {
            byte[] root = new byte[32];
            new SecureRandom().nextBytes(root);
            try {
                return new SecretKeySpec(root, "HmacSHA256");
            } finally {
                java.util.Arrays.fill(root, (byte) 0);
            }
        }
    }
}
