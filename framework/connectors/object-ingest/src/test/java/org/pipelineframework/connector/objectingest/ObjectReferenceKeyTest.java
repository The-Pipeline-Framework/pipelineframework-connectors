package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
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

    private static PayloadReference reference(String provider) {
        return new PayloadReference(provider, "container", "key", "application/octet-stream", "raw",
            null, 1, null, Map.of(), Optional.empty());
    }
}
