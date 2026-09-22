package org.pipelineframework.connector.decision.jev;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import io.quarkus.arc.Unremovable;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectionResolver;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.connector.ConnectorRuntimeContext;
import org.pipelineframework.connector.decision.DecisionClientResolver;
import org.pipelineframework.connector.decision.DecisionProviderConfiguration;
import org.pipelineframework.connector.decision.DecisionQueryConnectorProvider;

@ApplicationScoped
@Unremovable
public final class JevDecisionQueryConnector extends DecisionQueryConnectorProvider {
    public static final ConnectorProviderId PROVIDER_ID = ConnectorProviderId.of("decision.query.jev");
    private static final String DEFAULT_BASE_URL = "https://api.typesafe.ai";
    private final RuntimeConfiguration runtime;

    public JevDecisionQueryConnector() {
        this(() -> Duration.ofSeconds(10));
    }

    @Inject
    JevDecisionQueryConnector(RuntimeConfiguration runtime) {
        super(PROVIDER_ID);
        this.runtime = runtime;
    }

    @Override
    protected DecisionClientResolver createClientResolver(
        DecisionProviderConfiguration configuration, ConnectorRuntimeContext context) {
        var reference = configuration.connection().orElseThrow(() -> new ConnectionResolutionException(
            "Jev binding requires a deployment-owned connection reference"));
        ConnectionResolver resolver = context.connectionResolver().orElseThrow(() -> new ConnectionResolutionException(
            "No host ConnectionResolver is configured for the Jev binding"));
        String baseUrl = configuration.baseUrl().orElse(DEFAULT_BASE_URL);
        return executionContext -> {
            if (executionContext.tenantId().isEmpty()) {
                return CompletableFuture.failedStage(new ConnectionResolutionException(
                    "Jev connection resolution requires a tenant-aware invocation context"));
            }
            return resolver.resolve(new ConnectionResolutionRequest<>(
                reference, AuthenticatedJevConnection.class, executionContext))
                .thenApply(connection -> new JevDecisionClient(
                    connection, baseUrl, configuration.model(), runtime.requestTimeout()));
        };
    }

    @ConfigMapping(prefix = "pipeline.decision.jev")
    interface RuntimeConfiguration {
        /** Maximum duration of one provider attempt; TPF owns retry policy. */
        @WithDefault("10s")
        Duration requestTimeout();
    }
}
