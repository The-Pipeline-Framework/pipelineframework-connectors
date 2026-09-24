package org.pipelineframework.connector.decision;

import java.util.List;
import java.util.Objects;

public record DecisionResult(List<DecisionAnswer> answers) {
    public DecisionResult {
        answers = List.copyOf(Objects.requireNonNull(answers, "decision answers must not be null"));
    }
}
