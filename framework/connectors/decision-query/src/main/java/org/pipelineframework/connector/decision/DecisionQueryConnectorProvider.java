package org.pipelineframework.connector.decision;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.ConnectorOperation;
import org.pipelineframework.connector.ConnectorProvider;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorProviderVersion;
import org.pipelineframework.connector.ConnectorRuntimeContext;

/** Stable provider-neutral surface for bounded probabilistic judgments. */
public abstract class DecisionQueryConnectorProvider implements ConnectorProvider<DecisionProviderConfiguration> {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("decision.query");
    private static final ConnectorConfigSchema<DecisionProviderConfiguration> PROVIDER_SCHEMA =
        ConnectorConfigSchema.record(DecisionProviderConfiguration.class, "decision.query.provider", 1);

    private final ConnectorProviderId providerId;
    private final AtomicReference<DecisionClientResolver> clientResolver = new AtomicReference<>();
    private final DecisionQueryOperation operation = new DecisionQueryOperation(this::resolveClient);

    protected DecisionQueryConnectorProvider() { this(PROVIDER_ID); }

    protected DecisionQueryConnectorProvider(ConnectorProviderId providerId) {
        this.providerId = java.util.Objects.requireNonNull(providerId, "decision provider ID must not be null");
    }

    @Override public final ConnectorProviderId id() { return providerId; }
    @Override public final ConnectorProviderVersion version() { return new ConnectorProviderVersion(1, 0); }
    @Override public final Optional<ConnectorConfigSchema<DecisionProviderConfiguration>> configurationSchema() {
        return Optional.of(PROVIDER_SCHEMA);
    }
    @Override public final Collection<? extends ConnectorOperation> operations() { return List.of(operation); }

    @Override
    public final CompletionStage<Void> start(ConnectorRuntimeContext context, DecisionProviderConfiguration config) {
        try {
            clientResolver.set(java.util.Objects.requireNonNull(createClientResolver(config, context)));
            return CompletableFuture.completedFuture(null);
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
    }

    @Override
    public final CompletionStage<Void> stop(ConnectorRuntimeContext context) {
        clientResolver.set(null);
        return CompletableFuture.completedFuture(null);
    }

    protected abstract DecisionClientResolver createClientResolver(
        DecisionProviderConfiguration configuration, ConnectorRuntimeContext context);

    private CompletionStage<DecisionClient> resolveClient(
        org.pipelineframework.connector.ConnectorExecutionContext context) {
        DecisionClientResolver active = clientResolver.get();
        if (active == null) return CompletableFuture.failedStage(
            new IllegalStateException("Decision Query binding is not active"));
        try {
            return java.util.Objects.requireNonNull(active.resolve(context));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
    }
}
