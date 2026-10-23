package org.pipelineframework.connector.llm;

import org.pipelineframework.connector.llm.projected.ProjectedCompletion;

public sealed interface ProjectedDecision permits ProjectedDecision.Call, ProjectedDecision.Complete {
    String discriminator();

    record Call(AgentCall value) implements ProjectedDecision {
        @Override public String discriminator() { return "call"; }
    }

    record Complete(ProjectedCompletion value) implements ProjectedDecision {
        @Override public String discriminator() { return "complete"; }
    }
}
