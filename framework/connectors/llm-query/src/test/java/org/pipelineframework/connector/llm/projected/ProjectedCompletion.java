package org.pipelineframework.connector.llm.projected;

import org.pipelineframework.connector.llm.ReviewReady;

public record ProjectedCompletion(String trustedState, ReviewReady review) { }
