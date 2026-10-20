package org.pipelineframework.connector.conversation;

/** Text-free decision trace for one omitted candidate. */
public record SkippedContextEntry(String entryId, String provenance, Integer sourceBytes,
        Integer measuredTokens, String reason) {
    public SkippedContextEntry {
        entryId = ConversationValues.identifier(entryId, "entry ID");
        provenance = ConversationValues.identifier(provenance, "provenance");
        if (sourceBytes == null || sourceBytes < 0 || measuredTokens == null || measuredTokens < 0) {
            throw new IllegalArgumentException("skip measurements must be non-negative");
        }
        reason = ConversationValues.identifier(reason, "skip reason");
    }
}
