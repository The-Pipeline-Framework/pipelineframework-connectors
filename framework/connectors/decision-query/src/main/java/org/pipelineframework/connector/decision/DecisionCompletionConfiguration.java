package org.pipelineframework.connector.decision;

import java.util.LinkedHashMap;
import java.util.Map;

record DecisionCompletionConfiguration(String field, Map<String, String> carry) {
    DecisionCompletionConfiguration {
        field = DecisionTurnConfiguration.path(field, "decision completion field");
        Map<String, String> normalized = new LinkedHashMap<>();
        carry.forEach((target, source) -> normalized.put(
            DecisionTurnConfiguration.path(target, "decision completion output field"),
            DecisionTurnConfiguration.path(source, "decision completion input path")));
        carry = Map.copyOf(normalized);
    }
}
