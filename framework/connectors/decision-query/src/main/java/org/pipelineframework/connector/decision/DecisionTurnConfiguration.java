package org.pipelineframework.connector.decision;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public record DecisionTurnConfiguration(String request, Optional<Map<String, String>> completion) {
    public DecisionTurnConfiguration {
        request = path(request, "decision request");
        completion = Objects.requireNonNull(completion, "decision completion must not be null").map(Map::copyOf);
    }

    public DecisionCompletionConfiguration projection() {
        Map<String, String> values = new LinkedHashMap<>(completion.orElseThrow(() ->
            new IllegalArgumentException("decision completion projection is required")));
        String field = values.remove("field");
        return new DecisionCompletionConfiguration(field, values);
    }

    static String path(String value, String label) {
        value = Objects.requireNonNull(value, label + " must not be null").trim();
        if (!value.matches("[A-Za-z][A-Za-z0-9]*(?:\\.[A-Za-z][A-Za-z0-9]*)*")) {
            throw new IllegalArgumentException(label + " must be a dotted field path: " + value);
        }
        return value;
    }
}
