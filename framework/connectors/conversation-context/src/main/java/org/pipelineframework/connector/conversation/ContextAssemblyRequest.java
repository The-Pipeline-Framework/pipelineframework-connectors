package org.pipelineframework.connector.conversation;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Model window and overhead are supplied explicitly; no history or model read occurs here. */
public record ContextAssemblyRequest(
    ConversationScope scope, List<ContextCandidate> candidates, Integer contextLimitTokens,
    Integer instructionTokens, Integer callableSchemaTokens, Integer reservedOutputTokens, Integer maxEntryBytes
) {
    public ContextAssemblyRequest {
        scope = Objects.requireNonNull(scope, "scope must not be null");
        candidates = List.copyOf(Objects.requireNonNull(candidates, "candidates must not be null"));
        contextLimitTokens = ConversationValues.positive(contextLimitTokens, "context limit");
        instructionTokens = nonNegative(instructionTokens, "instruction tokens");
        callableSchemaTokens = nonNegative(callableSchemaTokens, "callable schema tokens");
        reservedOutputTokens = nonNegative(reservedOutputTokens, "reserved output tokens");
        maxEntryBytes = ConversationValues.positive(maxEntryBytes, "max entry bytes");
        if (maxEntryBytes > 16_384 || candidates.size() > 1_000) {
            throw new IllegalArgumentException("entry or candidate count exceeds supported bound");
        }
        var identities = new HashSet<String>();
        for (ContextCandidate candidate : candidates) {
            if (!scope.equals(candidate.scope()) || !identities.add(candidate.entryId())) {
                throw new IllegalArgumentException("candidate has foreign scope or repeated entry ID");
            }
        }
    }

    private static int nonNegative(Integer value, String name) {
        if (value == null || value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }
}
