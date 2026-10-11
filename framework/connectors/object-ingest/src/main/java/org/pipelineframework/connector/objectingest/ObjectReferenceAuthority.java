package org.pipelineframework.connector.objectingest;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.pipelineframework.repository.PayloadReference;

/** Signs and verifies an object locator with a provider-scoped capability. */
final class ObjectReferenceAuthority {
    private final SecretKeySpec key;
    private final String capabilityMetadata;
    private final String errorPrefix;

    ObjectReferenceAuthority(String provider, String capabilityMetadata, String errorPrefix) {
        this.key = ObjectReferenceKey.forProvider(provider);
        this.capabilityMetadata = capabilityMetadata;
        this.errorPrefix = errorPrefix;
    }

    PayloadReference issue(PayloadReference unsigned) {
        Map<String, String> metadata = new LinkedHashMap<>(unsigned.metadata());
        metadata.remove(capabilityMetadata);
        metadata.put(capabilityMetadata, sign(unsigned));
        return new PayloadReference(unsigned.provider(), unsigned.container(), unsigned.key(),
            unsigned.contentType(), unsigned.codec(), unsigned.checksum(), unsigned.sizeBytes(),
            unsigned.version(), metadata, unsigned.connectorOrigin());
    }

    void verify(PayloadReference reference) {
        String supplied = reference.metadata().get(capabilityMetadata);
        if (supplied == null || !MessageDigest.isEqual(
            supplied.getBytes(StandardCharsets.US_ASCII), sign(reference).getBytes(StandardCharsets.US_ASCII))) {
            throw new IllegalStateException(errorPrefix + " payload locator provenance mismatch: " + reference.key());
        }
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
                .filter(entry -> !capabilityMetadata.equals(entry.getKey()))
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    field(mac, entry.getKey());
                    field(mac, entry.getValue());
                });
            return HexFormat.of().formatHex(mac.doFinal());
        } catch (java.security.GeneralSecurityException failure) {
            throw new IllegalStateException(errorPrefix + " reference signing is unavailable", failure);
        }
    }

    private static void field(Mac mac, String value) {
        byte[] bytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        mac.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        mac.update(bytes);
    }
}
