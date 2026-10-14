package org.pipelineframework.connector.objectingest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.crypto.Mac;

/** Authenticated, short-lived description of an S3 multipart upload. */
final class S3DelegatedUploadToken {
    record Claims(String targetName, String bucket, String key, String uploadId, String contentType,
                  long bytes, int partSizeBytes, String sha256, Instant expiresAt, String idempotencyKey,
                  Map<String, String> metadata) {
        Claims {
            metadata = Map.copyOf(metadata);
        }
    }

    private static final int MAX_TOKEN_BYTES = 4096;
    private static final byte[] DOMAIN = "tpf-s3-delegated-upload-v1".getBytes(StandardCharsets.US_ASCII);

    String issue(Claims claims) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(1);
            out.writeUTF(claims.targetName());
            out.writeUTF(claims.bucket());
            out.writeUTF(claims.key());
            out.writeUTF(claims.uploadId());
            out.writeUTF(claims.contentType());
            out.writeLong(claims.bytes());
            out.writeInt(claims.partSizeBytes());
            out.writeUTF(claims.sha256());
            out.writeLong(claims.expiresAt().getEpochSecond());
            out.writeUTF(claims.idempotencyKey());
            if (claims.metadata().size() > 32) {
                throw new IllegalArgumentException("upload token metadata exceeds 32 entries");
            }
            out.writeInt(claims.metadata().size());
            claims.metadata().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
                try {
                    out.writeUTF(entry.getKey());
                    out.writeUTF(entry.getValue());
                } catch (IOException failure) {
                    throw new IllegalStateException(failure);
                }
            });
            byte[] payload = bytes.toByteArray();
            if (payload.length > MAX_TOKEN_BYTES) throw new IllegalArgumentException("upload token is too large");
            byte[] signed = new byte[payload.length + 32];
            System.arraycopy(payload, 0, signed, 0, payload.length);
            System.arraycopy(mac(payload), 0, signed, payload.length, 32);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(signed);
        } catch (IOException failure) {
            throw new IllegalStateException("could not encode S3 upload token", failure);
        }
    }

    Claims verify(String token, Instant now) {
        Claims claims = inspect(token);
        if (!claims.expiresAt().isAfter(now)) {
            throw new SecurityException("S3 upload token is invalid or expired");
        }
        return claims;
    }

    Claims inspect(String token) {
        if (token == null || token.length() > 6000) throw new SecurityException("invalid S3 upload token");
        try {
            byte[] signed = Base64.getUrlDecoder().decode(token);
            if (signed.length < 33 || signed.length > MAX_TOKEN_BYTES + 32) {
                throw new SecurityException("invalid S3 upload token");
            }
            int length = signed.length - 32;
            byte[] payload = java.util.Arrays.copyOf(signed, length);
            byte[] signature = java.util.Arrays.copyOfRange(signed, length, signed.length);
            if (!MessageDigest.isEqual(mac(payload), signature)) {
                throw new SecurityException("S3 upload token signature mismatch");
            }
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            if (in.readInt() != 1) throw new SecurityException("unsupported S3 upload token");
            String targetName = in.readUTF();
            String bucket = in.readUTF();
            String key = in.readUTF();
            String uploadId = in.readUTF();
            String contentType = in.readUTF();
            long bytes = in.readLong();
            int partSizeBytes = in.readInt();
            String sha256 = in.readUTF();
            Instant expires = Instant.ofEpochSecond(in.readLong());
            String idempotencyKey = in.readUTF();
            int size = in.readInt();
            if (size < 0 || size > 32) throw new SecurityException("invalid S3 upload token metadata");
            Map<String, String> metadata = new LinkedHashMap<>();
            for (int i = 0; i < size; i++) {
                if (metadata.putIfAbsent(in.readUTF(), in.readUTF()) != null) {
                    throw new SecurityException("duplicate S3 upload token metadata");
                }
            }
            if (in.available() != 0) throw new SecurityException("invalid S3 upload token");
            if (bytes < 1 || partSizeBytes < 5 * 1024 * 1024 || !sha256.matches("[a-f0-9]{64}")) {
                throw new SecurityException("invalid S3 upload token content");
            }
            return new Claims(targetName, bucket, key, uploadId, contentType, bytes, partSizeBytes,
                sha256, expires, idempotencyKey, metadata);
        } catch (IOException | IllegalArgumentException failure) {
            throw new SecurityException("invalid S3 upload token", failure);
        }
    }

    private byte[] mac(byte[] payload) {
        try {
            String configured = System.getProperty("tpf.object.reference.hmac-key");
            if (configured == null || configured.isBlank()) {
                configured = System.getenv("TPF_OBJECT_REFERENCE_HMAC_KEY");
            }
            if (configured == null || configured.isBlank()) {
                throw new IllegalStateException("delegated S3 uploads require a durable object reference HMAC key");
            }
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(ObjectReferenceKey.forProvider("s3"));
            mac.update(DOMAIN);
            return mac.doFinal(payload);
        } catch (java.security.GeneralSecurityException failure) {
            throw new IllegalStateException("S3 upload token signing unavailable", failure);
        }
    }
}
