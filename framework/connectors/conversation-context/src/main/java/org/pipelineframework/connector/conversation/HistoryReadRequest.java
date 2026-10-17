package org.pipelineframework.connector.conversation;

import java.util.Objects;

/** Cursor-exclusive page request; the provider checks host-authorised scope. */
public record HistoryReadRequest(ConversationScope scope, Long afterCursor, Integer limit) {
    public HistoryReadRequest {
        scope = Objects.requireNonNull(scope, "scope must not be null");
        if (afterCursor == null || afterCursor < 0) {
            throw new IllegalArgumentException("after cursor must be non-negative");
        }
        limit = ConversationValues.positive(limit, "history limit");
        if (limit > 200) {
            throw new IllegalArgumentException("history limit exceeds 200");
        }
    }
}
