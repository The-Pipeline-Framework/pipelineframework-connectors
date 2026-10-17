package org.pipelineframework.connector.conversation;

import java.util.List;
import java.util.Objects;

/** Stable ascending page with next cursor, including an empty page. */
public record HistoryPage(ConversationScope scope, List<ConversationTurn> turns, Long nextCursor, Boolean hasMore) {
    public HistoryPage {
        scope = Objects.requireNonNull(scope, "scope must not be null");
        turns = List.copyOf(Objects.requireNonNull(turns, "turns must not be null"));
        if (nextCursor == null || nextCursor < 0 || hasMore == null) {
            throw new IllegalArgumentException("page cursor and hasMore must be present");
        }
        long last = 0;
        for (ConversationTurn turn : turns) {
            if (!scope.equals(turn.scope()) || turn.cursor() <= last || turn.cursor() > nextCursor) {
                throw new IllegalArgumentException("page contains foreign scope or invalid cursor order");
            }
            last = turn.cursor();
        }
    }
}
