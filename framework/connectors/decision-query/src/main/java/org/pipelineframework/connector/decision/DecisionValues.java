package org.pipelineframework.connector.decision;

import java.math.BigDecimal;
import java.util.Objects;

import com.fasterxml.jackson.databind.ObjectMapper;

final class DecisionValues {
    private static final ObjectMapper JSON = new ObjectMapper();

    private DecisionValues() { }

    static String text(String value, String label) {
        value = Objects.requireNonNull(value, label + " must not be null").trim();
        if (value.isEmpty()) throw new IllegalArgumentException(label + " must not be blank");
        return value;
    }

    static String token(String value, String label) {
        value = text(value, label);
        if (!value.matches("[A-Za-z][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException(label + " must be a field token: " + value);
        }
        return value;
    }

    static String json(String value, String label) {
        value = text(value, label);
        try {
            JSON.readTree(value);
            return value;
        } catch (Exception failure) {
            throw new IllegalArgumentException(label + " must be valid JSON", failure);
        }
    }

    static BigDecimal probability(BigDecimal value, String label) {
        value = Objects.requireNonNull(value, label + " must not be null");
        if (value.compareTo(BigDecimal.ZERO) < 0 || value.compareTo(BigDecimal.ONE) > 0) {
            throw new IllegalArgumentException(label + " must be between zero and one");
        }
        return value;
    }
}
