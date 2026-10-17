package org.pipelineframework.connector.conversation;

import java.util.Objects;

/** Bounded newest-first lookup; zero beforeCursor starts from the current end. */
public record HistoryTailRequest(ConversationScope scope, Long beforeCursor, Integer limit) {
    public HistoryTailRequest {
        scope = Objects.requireNonNull(scope, "scope must not be null");
        if (beforeCursor == null || beforeCursor < 0) {
            throw new IllegalArgumentException("before cursor must be non-negative");
        }
        limit = ConversationValues.positive(limit, "tail limit");
        if (limit > 200) {
            throw new IllegalArgumentException("tail limit exceeds 200");
        }
    }
}
