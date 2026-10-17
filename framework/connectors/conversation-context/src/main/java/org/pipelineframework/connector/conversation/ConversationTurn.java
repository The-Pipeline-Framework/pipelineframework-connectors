package org.pipelineframework.connector.conversation;

import java.util.Objects;
import java.util.Optional;
import java.time.Instant;
import java.time.format.DateTimeParseException;

/** Immutable text-only dialogue evidence, ordered by a store-assigned cursor. */
public record ConversationTurn(
    ConversationScope scope, String eventId, Long cursor, String occurredAt, String speaker, String source,
    String text, Optional<String> payloadRef, Optional<String> replyToEventId
) {
    public ConversationTurn {
        scope = Objects.requireNonNull(scope, "scope must not be null");
        eventId = ConversationValues.identifier(eventId, "event ID");
        if (cursor == null || cursor < 0) {
            throw new IllegalArgumentException("cursor must be non-negative");
        }
        occurredAt = ConversationValues.identifier(occurredAt, "timestamp");
        try {
            occurredAt = Instant.parse(occurredAt).toString();
        } catch (DateTimeParseException invalid) {
            throw new IllegalArgumentException("timestamp must be an ISO-8601 UTC instant", invalid);
        }
        speaker = ConversationValues.identifier(speaker, "speaker");
        source = ConversationValues.identifier(source, "source");
        text = ConversationValues.boundedText(text, 16_384, "turn text");
        payloadRef = Objects.requireNonNull(payloadRef, "payload reference must not be null")
            .map(value -> ConversationValues.identifier(value, "payload reference"));
        replyToEventId = Objects.requireNonNull(replyToEventId, "reply-to event ID must not be null")
            .map(value -> ConversationValues.identifier(value, "reply-to event ID"));
        if (text.isBlank() && payloadRef.isEmpty()) {
            throw new IllegalArgumentException("turn requires text or an owned payload reference");
        }
    }
}
