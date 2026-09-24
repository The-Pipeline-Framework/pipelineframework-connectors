package org.pipelineframework.connector.decision;

import java.util.concurrent.CompletionStage;

@FunctionalInterface
public interface DecisionClient {
    CompletionStage<DecisionResponse> decide(DecisionRequest request);
}
