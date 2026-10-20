package org.pipelineframework.connector.conversation;

import java.util.Objects;
import java.util.Optional;

/** Explicitly eligible model evidence. Input order is application-selected priority. */
public record ContextCandidate(
    ConversationScope scope, String entryId, String provenance, String text, Optional<String> payloadRef, Boolean required
) {
    public ContextCandidate {
        scope = Objects.requireNonNull(scope, "scope must not be null");
        entryId = ConversationValues.identifier(entryId, "entry ID");
        provenance = ConversationValues.identifier(provenance, "provenance");
        text = ConversationValues.boundedText(text, 65_536, "candidate text");
        payloadRef = Objects.requireNonNull(payloadRef, "payload reference must not be null")
            .map(value -> ConversationValues.identifier(value, "payload reference"));
        required = Objects.requireNonNull(required, "required flag must not be null");
        if (text.isBlank() && payloadRef.isEmpty()) {
            throw new IllegalArgumentException("candidate requires text or an owned payload reference");
        }
    }
}
