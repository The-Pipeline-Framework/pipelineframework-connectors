package org.pipelineframework.connector.decision;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public record DecisionAnswer(
    String name,
    DecisionQuestionType type,
    String selected,
    BigDecimal value,
    BigDecimal confidence,
    List<DecisionProbability> probabilities
) {
    public DecisionAnswer {
        name = DecisionValues.token(name, "answer name");
        type = Objects.requireNonNull(type, "answer type must not be null");
        selected = Objects.requireNonNull(selected, "answer selection must not be null").trim();
        value = Objects.requireNonNull(value, "answer value must not be null");
        confidence = DecisionValues.probability(confidence, "answer confidence");
        probabilities = List.copyOf(Objects.requireNonNull(probabilities, "answer probabilities must not be null"));
        var labels = new HashSet<String>();
        for (DecisionProbability probability : probabilities) {
            if (!labels.add(probability.label())) {
                throw new IllegalArgumentException("answer probability labels must be unique: " + probability.label());
            }
        }
        if (type == DecisionQuestionType.CHOICE && selected.isEmpty()) {
            throw new IllegalArgumentException("choice answer selection must not be blank");
        }
        if (type != DecisionQuestionType.CHOICE && !selected.isEmpty()) {
            throw new IllegalArgumentException(type + " answer must not contain a selection");
        }
        if (type == DecisionQuestionType.NOUL) {
            DecisionValues.probability(value, "noul value");
        }
    }
}
