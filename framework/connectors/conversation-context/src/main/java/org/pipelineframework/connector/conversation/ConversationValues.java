package org.pipelineframework.connector.conversation;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

final class ConversationValues {
    private ConversationValues() {}

    static String identifier(String value, String name) {
        if (value == null || value.isBlank() || value.length() > 160 || !value.equals(value.strip())
                || value.codePoints().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException(name + " must be a non-blank, unpadded identifier of at most 160 characters");
        }
        return value;
    }

    static String boundedText(String value, int maxBytes, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.getBytes(StandardCharsets.UTF_8).length > maxBytes) {
            throw new IllegalArgumentException(name + " exceeds " + maxBytes + " UTF-8 bytes");
        }
        return value;
    }

    static int positive(Integer value, String name) {
        if (value == null || value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
