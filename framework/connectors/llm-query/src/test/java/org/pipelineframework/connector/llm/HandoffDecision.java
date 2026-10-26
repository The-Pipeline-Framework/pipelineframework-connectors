package org.pipelineframework.connector.llm;

public sealed interface HandoffDecision
    permits HandoffDecision.Call, HandoffDecision.Handoff, HandoffDecision.Complete {
    String discriminator();

    record Call(AgentCall value) implements HandoffDecision {
        @Override public String discriminator() { return "call"; }
    }

    record Handoff(HandoffCall value) implements HandoffDecision {
        @Override public String discriminator() { return "handoff"; }
    }

    record Complete(CompleteResult value) implements HandoffDecision {
        @Override public String discriminator() { return "complete"; }
    }
}
