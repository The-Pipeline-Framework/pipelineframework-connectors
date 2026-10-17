package org.pipelineframework.connector.conversation;

import java.util.List;
import java.util.Objects;

/** Ascending presentation of a bounded tail; previousCursor pages backwards exclusively. */
public record HistoryTailPage(ConversationScope scope, List<ConversationTurn> turns,
        Long previousCursor, Boolean hasEarlier) {
    public HistoryTailPage {
        scope = Objects.requireNonNull(scope, "scope must not be null");
        turns = List.copyOf(Objects.requireNonNull(turns, "turns must not be null"));
        if (previousCursor == null || previousCursor < 0 || hasEarlier == null) {
            throw new IllegalArgumentException("tail cursor and hasEarlier must be present");
        }
        long last = 0;
        for (ConversationTurn turn : turns) {
            if (!scope.equals(turn.scope()) || turn.cursor() <= last) {
                throw new IllegalArgumentException("tail contains foreign scope or invalid cursor order");
            }
            last = turn.cursor();
        }
        if (!turns.isEmpty() && !previousCursor.equals(turns.getFirst().cursor())) {
            throw new IllegalArgumentException("previous cursor must identify the first returned turn");
        }
    }
}
