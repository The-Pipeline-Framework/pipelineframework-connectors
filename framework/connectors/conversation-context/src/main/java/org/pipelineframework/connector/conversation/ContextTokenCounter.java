package org.pipelineframework.connector.conversation;

import java.util.Optional;

/** Optional host-supplied tokenizer for the exact model-facing entry representation. */
@FunctionalInterface
public interface ContextTokenCounter {
    int count(String entryId, String provenance, String text, Optional<String> payloadRef);
}
