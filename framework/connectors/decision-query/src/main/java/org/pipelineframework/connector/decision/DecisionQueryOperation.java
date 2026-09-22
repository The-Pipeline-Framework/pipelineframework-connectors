package org.pipelineframework.connector.decision;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.connector.ConnectionResolutionException;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.QueryCapabilities;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;

final class DecisionQueryOperation implements QueryOperation<Object, DecisionTurnConfiguration, Object> {
    private static final ConnectorConfigSchema<DecisionTurnConfiguration> CONFIGURATION_SCHEMA =
        ConnectorConfigSchema.record(DecisionTurnConfiguration.class, "decision.query.turn", 1);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final System.Logger LOG = System.getLogger(DecisionQueryOperation.class.getName());
    private final Function<org.pipelineframework.connector.ConnectorExecutionContext,
        CompletionStage<DecisionClient>> clientResolver;

    DecisionQueryOperation(Function<org.pipelineframework.connector.ConnectorExecutionContext,
        CompletionStage<DecisionClient>> clientResolver) {
        this.clientResolver = Objects.requireNonNull(clientResolver);
    }

    @Override public String id() { return "decide"; }
    @Override public QueryCapabilities capabilities() { return QueryCapabilities.cacheable(); }
    @Override public Optional<ConnectorConfigSchema<DecisionTurnConfiguration>> configurationSchema() {
        return Optional.of(CONFIGURATION_SCHEMA);
    }

    @Override
    public CompletionStage<QueryOutcome<Object>> query(
        QueryInvocation<Object, DecisionTurnConfiguration, Object> invocation) {
        final DecisionRequest request;
        final DecisionProjection projection;
        try {
            Object raw = DecisionProjection.readPath(invocation.input(), invocation.configuration().request());
            request = JSON.convertValue(raw, DecisionRequest.class);
            projection = new DecisionProjection(JSON, invocation.outputType(), invocation.configuration().projection());
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
        return resolve(invocation.executionContext()).thenCompose(client -> decide(client, request, projection,
            invocation.input())).exceptionally(DecisionQueryOperation::failureOutcome);
    }

    private CompletionStage<DecisionClient> resolve(
        org.pipelineframework.connector.ConnectorExecutionContext context) {
        try {
            return Objects.requireNonNull(clientResolver.apply(context));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
    }

    private CompletionStage<QueryOutcome<Object>> decide(
        DecisionClient client, DecisionRequest request, DecisionProjection projection, Object input) {
        CompletionStage<DecisionResponse> stage;
        try {
            stage = Objects.requireNonNull(client.decide(request));
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
        return stage.thenApply(response -> {
            validate(request, response.result());
            return new QueryOutcome.Found<>(projection.materialize(input, response.result()), response.observation());
        });
    }

    static void validate(DecisionRequest request, DecisionResult result) {
        Map<String, DecisionQuestion> questions = new LinkedHashMap<>();
        request.questions().forEach(question -> questions.put(question.name(), question));
        if (result.answers().size() != questions.size()) {
            throw invalid("provider returned a different number of answers");
        }
        for (DecisionAnswer answer : result.answers()) {
            DecisionQuestion question = questions.remove(answer.name());
            if (question == null) throw invalid("provider returned unknown answer " + answer.name());
            if (question.type() != answer.type()) throw invalid("provider changed question type for " + answer.name());
            Map<String, DecisionCriterion> criteria = new LinkedHashMap<>();
            question.criteria().forEach(value -> criteria.put(value.label(), value));
            if (answer.type() == DecisionQuestionType.CHOICE && !criteria.containsKey(answer.selected())) {
                throw invalid("provider invented choice " + answer.selected() + " for " + answer.name());
            }
            if (answer.type() != DecisionQuestionType.NOUL) {
                for (DecisionProbability probability : answer.probabilities()) {
                    if (!criteria.containsKey(probability.label())) {
                        throw invalid("provider returned unknown probability label " + probability.label());
                    }
                }
                if (answer.probabilities().size() != criteria.size()) {
                    throw invalid("provider omitted probabilities for " + answer.name());
                }
            }
            BigDecimal sum = answer.probabilities().stream().map(DecisionProbability::probability)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
            if (!answer.probabilities().isEmpty()
                && sum.subtract(BigDecimal.ONE).abs().compareTo(new BigDecimal("0.000001")) > 0) {
                throw invalid("provider probabilities do not sum to one for " + answer.name());
            }
        }
        if (!questions.isEmpty()) throw invalid("provider omitted answers " + questions.keySet());
    }

    private static DecisionProviderFailureException invalid(String message) {
        return new DecisionProviderFailureException(DecisionProviderFailureException.Kind.TERMINAL,
            "decision-provider-invalid-response", message);
    }

    private static QueryOutcome<Object> failureOutcome(Throwable failure) {
        Throwable cause = unwrap(failure);
        QueryOutcome<Object> outcome;
        if (cause instanceof ConnectionResolutionException connection) {
            outcome = switch (connection.kind()) {
                case AUTHENTICATION_REQUIRED -> new QueryOutcome.AuthenticationRequired<>(
                    "decision-connection-authentication-required");
                case TEMPORARILY_UNAVAILABLE -> new QueryOutcome.TemporarilyUnavailable<>(
                    "decision-connection-temporarily-unavailable");
                case CONFIGURATION -> new QueryOutcome.TerminalFailure<>("decision-connection-misconfigured");
            };
        } else if (cause instanceof DecisionProviderFailureException provider) {
            outcome = switch (provider.kind()) {
                case AUTHENTICATION_REQUIRED -> new QueryOutcome.AuthenticationRequired<>(provider.outcomeCode());
                case TEMPORARILY_UNAVAILABLE -> new QueryOutcome.TemporarilyUnavailable<>(provider.outcomeCode());
                case TERMINAL -> new QueryOutcome.TerminalFailure<>(provider.outcomeCode());
            };
        } else {
            outcome = new QueryOutcome.TerminalFailure<>("decision-query-failed");
        }
        LOG.log(System.Logger.Level.WARNING, "Decision Query failed: " + outcome.code(), cause);
        return outcome;
    }

    private static Throwable unwrap(Throwable failure) {
        Throwable current = Objects.requireNonNull(failure);
        while ((current instanceof java.util.concurrent.CompletionException
            || current instanceof java.util.concurrent.ExecutionException) && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
