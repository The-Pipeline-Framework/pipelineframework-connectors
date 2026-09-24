package org.pipelineframework.connector.decision;

import java.util.Objects;

public record DecisionCriterion(String label, String description) {
    public DecisionCriterion {
        label = DecisionValues.text(label, "criterion label");
        description = Objects.requireNonNull(description, "criterion description must not be null").trim();
    }
}
