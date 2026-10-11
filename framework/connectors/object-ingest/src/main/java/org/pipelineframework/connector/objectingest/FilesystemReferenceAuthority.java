package org.pipelineframework.connector.objectingest;

import org.pipelineframework.repository.PayloadReference;

/** Authority shared by the source and target operations of one filesystem connector instance. */
final class FilesystemReferenceAuthority {
    static final String CAPABILITY_METADATA = "tpf.filesystem.capability.hmac-sha256";
    private final ObjectReferenceAuthority authority =
        new ObjectReferenceAuthority("filesystem", CAPABILITY_METADATA, "Filesystem");

    String sign(PayloadReference reference) {
        return authority.sign(reference);
    }

    PayloadReference issue(PayloadReference unsigned) {
        return authority.issue(unsigned);
    }

    void verify(PayloadReference reference) {
        authority.verify(reference);
    }
}
