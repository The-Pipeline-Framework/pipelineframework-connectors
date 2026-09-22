package org.pipelineframework.connector.decision;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public record DecisionQuestion(
    String name,
    DecisionQuestionType type,
    String instructions,
    List<DecisionCriterion> criteria
) {
    public DecisionQuestion {
        name = DecisionValues.token(name, "question name");
        type = Objects.requireNonNull(type, "question type must not be null");
        instructions = Objects.requireNonNull(instructions, "question instructions must not be null").trim();
        criteria = List.copyOf(Objects.requireNonNull(criteria, "question criteria must not be null"));
        int minimum = type == DecisionQuestionType.SCORE ? 2 : type == DecisionQuestionType.CHOICE ? 1 : 0;
        if (criteria.size() < minimum) {
            throw new IllegalArgumentException(type + " question requires at least " + minimum + " criteria");
        }
        if (criteria.size() > 255) {
            throw new IllegalArgumentException("question criteria must not exceed 255 alternatives");
        }
        var labels = new HashSet<String>();
        for (DecisionCriterion criterion : criteria) {
            if (!labels.add(criterion.label())) {
                throw new IllegalArgumentException("question criterion labels must be unique: " + criterion.label());
            }
        }
    }
}
