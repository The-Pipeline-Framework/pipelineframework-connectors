package org.pipelineframework.connector.decision;

import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.connector.QueryObservation;

public record DecisionResponse(DecisionResult result, Optional<QueryObservation> observation) {
    public DecisionResponse {
        result = Objects.requireNonNull(result, "decision result must not be null");
        observation = Objects.requireNonNull(observation, "decision observation must not be null");
    }
}
