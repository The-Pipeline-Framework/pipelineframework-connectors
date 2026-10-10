package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.HashMap;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.OwnedPayloadTransfer;
import org.pipelineframework.repository.PayloadReference;

class ObjectReferenceKeyTest {
    @Test
    void separatelyConstructedAuthoritiesVerifyTheSameProviderReference() {
        PayloadReference filesystem = reference("filesystem");
        assertDoesNotThrow(() -> new FilesystemReferenceAuthority().verify(
            new FilesystemReferenceAuthority().issue(filesystem)));

        PayloadReference s3 = reference("s3");
        assertDoesNotThrow(() -> new S3ReferenceAuthority().verify(
            new S3ReferenceAuthority().issue(s3)));
    }

    @Test
    void providerKeysRemainDistinct() {
        assertFalse(Arrays.equals(ObjectReferenceKey.forProvider("filesystem").getEncoded(),
            ObjectReferenceKey.forProvider("s3").getEncoded()));
    }

    @Test
    void ownerMetadataCannotBeChangedAfterIssuingReference() {
        FilesystemReferenceAuthority authority = new FilesystemReferenceAuthority();
        PayloadReference unsigned = new PayloadReference("filesystem", "container", "key",
            "application/octet-stream", "raw", null, 1, null,
            Map.of(OwnedPayloadTransfer.OWNER_TENANT, "tenant-a",
                OwnedPayloadTransfer.OWNER_SCOPE, "scope-a"), Optional.empty());
        PayloadReference issued = authority.issue(unsigned);
        Map<String, String> changedMetadata = new HashMap<>(issued.metadata());
        changedMetadata.put(OwnedPayloadTransfer.OWNER_TENANT, "tenant-b");
        PayloadReference changed = new PayloadReference(issued.provider(), issued.container(), issued.key(),
            issued.contentType(), issued.codec(), issued.checksum(), issued.sizeBytes(), issued.version(),
            changedMetadata, issued.connectorOrigin());

        assertThrows(IllegalStateException.class, () -> authority.verify(changed));
    }

    private static PayloadReference reference(String provider) {
        return new PayloadReference(provider, "container", "key", "application/octet-stream", "raw",
            null, 1, null, Map.of(), Optional.empty());
    }
}
