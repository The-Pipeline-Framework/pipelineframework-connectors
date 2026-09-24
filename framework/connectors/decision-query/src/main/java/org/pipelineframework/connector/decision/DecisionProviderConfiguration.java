package org.pipelineframework.connector.decision;

import java.util.Objects;
import java.util.Optional;

import org.pipelineframework.connector.ConnectionRef;

public record DecisionProviderConfiguration(
    String model,
    Optional<String> baseUrl,
    Optional<ConnectionRef> connection
) {
    public DecisionProviderConfiguration {
        model = DecisionValues.text(model, "decision model");
        baseUrl = Objects.requireNonNull(baseUrl, "decision base URL must not be null")
            .map(value -> DecisionValues.text(value, "decision base URL"));
        connection = Objects.requireNonNull(connection, "decision connection must not be null");
    }
}
