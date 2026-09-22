package org.pipelineframework.connector.decision;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public record DecisionRequest(String stateJson, List<DecisionQuestion> questions) {
    public DecisionRequest {
        stateJson = DecisionValues.json(stateJson, "decision state");
        questions = List.copyOf(Objects.requireNonNull(questions, "decision questions must not be null"));
        if (questions.isEmpty()) {
            throw new IllegalArgumentException("decision request requires at least one question");
        }
        var names = new HashSet<String>();
        for (DecisionQuestion question : questions) {
            if (!names.add(question.name())) {
                throw new IllegalArgumentException("decision question names must be unique: " + question.name());
            }
        }
    }
}
