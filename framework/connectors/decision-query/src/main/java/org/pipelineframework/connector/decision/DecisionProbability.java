package org.pipelineframework.connector.decision;

import java.math.BigDecimal;
import java.util.Objects;

public record DecisionProbability(String label, BigDecimal probability) {
    public DecisionProbability {
        label = DecisionValues.text(label, "probability label");
        probability = DecisionValues.probability(probability, "probability");
    }
}
