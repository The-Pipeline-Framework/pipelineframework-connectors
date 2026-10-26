package org.pipelineframework.connector.llm;

public record HandoffCall(String binding, String operation, String argumentsJson, String contextJson) { }
