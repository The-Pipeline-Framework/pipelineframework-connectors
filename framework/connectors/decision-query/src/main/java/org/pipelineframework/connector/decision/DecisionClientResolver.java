package org.pipelineframework.connector.decision;

import java.util.concurrent.CompletionStage;

import org.pipelineframework.connector.ConnectorExecutionContext;

@FunctionalInterface
public interface DecisionClientResolver {
    CompletionStage<DecisionClient> resolve(ConnectorExecutionContext context);
}
