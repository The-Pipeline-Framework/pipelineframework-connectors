package org.pipelineframework.connector.conversation;

import java.util.Objects;
import java.util.Optional;

/** Selected text and its owned reference if a large observation was abbreviated. */
public record SelectedContextEntry(
    String entryId, String provenance, String text, Optional<String> payloadRef, Integer estimatedTokens, Boolean abbreviated
) {
    public SelectedContextEntry {
        entryId = ConversationValues.identifier(entryId, "entry ID");
        provenance = ConversationValues.identifier(provenance, "provenance");
        text = ConversationValues.boundedText(text, 16_384, "selected text");
        payloadRef = Objects.requireNonNull(payloadRef, "payload reference must not be null");
        estimatedTokens = ConversationValues.positive(estimatedTokens, "estimated tokens");
        abbreviated = Objects.requireNonNull(abbreviated, "abbreviated flag must not be null");
    }
}
