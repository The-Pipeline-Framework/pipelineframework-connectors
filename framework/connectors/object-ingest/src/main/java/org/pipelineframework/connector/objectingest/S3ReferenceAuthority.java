package org.pipelineframework.connector.objectingest;

import org.pipelineframework.repository.PayloadReference;

/** Instance-held capability authority for one S3 Connector source/target pair. */
final class S3ReferenceAuthority {
    static final String CAPABILITY_METADATA = "tpf.s3.capability.hmac-sha256";
    private final ObjectReferenceAuthority authority =
        new ObjectReferenceAuthority("s3", CAPABILITY_METADATA, "S3");

    PayloadReference issue(PayloadReference unsigned) {
        return authority.issue(unsigned);
    }

    void verify(PayloadReference reference) {
        authority.verify(reference);
    }
}
