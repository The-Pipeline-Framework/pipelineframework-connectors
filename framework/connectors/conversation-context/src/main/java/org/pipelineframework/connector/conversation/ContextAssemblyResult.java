package org.pipelineframework.connector.conversation;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** A cannot-fit result has no selected text and must not be sent to inference. */
public record ContextAssemblyResult(
    Boolean fits, List<SelectedContextEntry> selected, List<SkippedContextEntry> skipped,
    Integer estimatedInputTokens, Integer reservedOutputTokens, String measurement,
    Optional<String> overflowReason, Optional<Integer> observedInputTokens
) {
    public ContextAssemblyResult {
        fits = Objects.requireNonNull(fits, "fits must not be null");
        selected = List.copyOf(Objects.requireNonNull(selected, "selected must not be null"));
        skipped = List.copyOf(Objects.requireNonNull(skipped, "skipped entries must not be null"));
        estimatedInputTokens = Objects.requireNonNull(estimatedInputTokens, "estimated input tokens must not be null");
        reservedOutputTokens = Objects.requireNonNull(reservedOutputTokens, "reserved output tokens must not be null");
        measurement = Objects.requireNonNull(measurement, "measurement must not be null");
        overflowReason = Objects.requireNonNull(overflowReason, "overflow reason must not be null")
            .map(value -> ConversationValues.identifier(value, "overflow reason"));
        observedInputTokens = Objects.requireNonNull(observedInputTokens, "observed tokens must not be null")
            .map(value -> ConversationValues.positive(value, "observed input tokens"));
        if (fits == overflowReason.isPresent()) {
            throw new IllegalArgumentException("overflow reason must be present only for a cannot-fit result");
        }
    }

    /** Attach a provider-reported count after the one-turn model Query returns. */
    public ContextAssemblyResult withObservedInputTokens(int observed) {
        if (!fits) {
            throw new IllegalStateException("cannot attach model usage to a cannot-fit result");
        }
        return new ContextAssemblyResult(fits, selected, skipped, estimatedInputTokens,
            reservedOutputTokens, measurement, overflowReason, Optional.of(observed));
    }
}
