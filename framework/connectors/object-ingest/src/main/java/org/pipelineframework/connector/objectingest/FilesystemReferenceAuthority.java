package org.pipelineframework.connector.objectingest;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.LinkedHashMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.pipelineframework.repository.PayloadReference;

/** Authority shared by the source and target operations of one filesystem connector instance. */
final class FilesystemReferenceAuthority {
    static final String CAPABILITY_METADATA = "tpf.filesystem.capability.hmac-sha256";
    private final SecretKeySpec key;

    FilesystemReferenceAuthority() {
        key = ObjectReferenceKey.forProvider("filesystem");
    }

    String sign(PayloadReference reference) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(key);
            field(mac, reference.provider());
            field(mac, reference.container());
            field(mac, reference.key());
            field(mac, reference.contentType());
            field(mac, reference.codec());
            field(mac, reference.checksum());
            field(mac, Long.toString(reference.sizeBytes()));
            field(mac, reference.version());
            reference.metadata().entrySet().stream()
                .filter(entry -> !CAPABILITY_METADATA.equals(entry.getKey()))
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    field(mac, entry.getKey());
                    field(mac, entry.getValue());
                });
            return HexFormat.of().formatHex(mac.doFinal());
        } catch (java.security.GeneralSecurityException failure) {
            throw new IllegalStateException("Filesystem reference signing is unavailable", failure);
        }
    }

    PayloadReference issue(PayloadReference unsigned) {
        Map<String, String> metadata = new LinkedHashMap<>(unsigned.metadata());
        metadata.remove(CAPABILITY_METADATA);
        metadata.put(CAPABILITY_METADATA, sign(unsigned));
        return new PayloadReference(unsigned.provider(), unsigned.container(), unsigned.key(),
            unsigned.contentType(), unsigned.codec(), unsigned.checksum(), unsigned.sizeBytes(),
            unsigned.version(), metadata, unsigned.connectorOrigin());
    }

    void verify(PayloadReference reference) {
        String supplied = reference.metadata().get(CAPABILITY_METADATA);
        if (supplied == null || !MessageDigest.isEqual(
            supplied.getBytes(StandardCharsets.US_ASCII), sign(reference).getBytes(StandardCharsets.US_ASCII))) {
            throw new IllegalStateException("Filesystem payload locator provenance mismatch: " + reference.key());
        }
    }

    private static void field(Mac mac, String value) {
        byte[] bytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        mac.update(java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        mac.update(bytes);
    }
}
