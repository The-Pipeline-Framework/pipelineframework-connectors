package org.pipelineframework.connector.decision.jev;

import java.math.BigDecimal;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpTimeoutException;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.pipelineframework.connector.QueryObservation;
import org.pipelineframework.connector.QueryTokenUsage;
import org.pipelineframework.connector.decision.DecisionAnswer;
import org.pipelineframework.connector.decision.DecisionClient;
import org.pipelineframework.connector.decision.DecisionProbability;
import org.pipelineframework.connector.decision.DecisionProviderFailureException;
import org.pipelineframework.connector.decision.DecisionQuestion;
import org.pipelineframework.connector.decision.DecisionQuestionType;
import org.pipelineframework.connector.decision.DecisionRequest;
import org.pipelineframework.connector.decision.DecisionResponse;
import org.pipelineframework.connector.decision.DecisionResult;

final class JevDecisionClient implements DecisionClient {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final AuthenticatedJevConnection connection;
    private final URI endpoint;
    private final String model;
    private final Duration timeout;

    JevDecisionClient(AuthenticatedJevConnection connection, String baseUrl, String model, Duration timeout) {
        this.connection = java.util.Objects.requireNonNull(connection);
        this.endpoint = endpoint(baseUrl);
        this.model = java.util.Objects.requireNonNull(model);
        this.timeout = java.util.Objects.requireNonNull(timeout);
        if (timeout.isZero() || timeout.isNegative()) throw new IllegalArgumentException("Jev timeout must be positive");
    }

    @Override
    public CompletionStage<DecisionResponse> decide(DecisionRequest request) {
        final String body;
        try {
            body = JSON.writeValueAsString(withModel(project(request)));
        } catch (Exception failure) {
            throw invalid("Jev request cannot be serialized", failure);
        }
        return connection.post(endpoint, timeout, body).handle((response, failure) -> {
            if (failure != null) throw transportFailure(failure);
            return decode(request, response);
        });
    }

    private static ObjectNode project(DecisionRequest request) {
        ObjectNode root = JSON.createObjectNode();
        root.set("state", parseState(request.stateJson()));
        ObjectNode questions = root.putObject("questions");
        for (DecisionQuestion question : request.questions()) {
            ObjectNode wire = questions.putObject(question.name());
            wire.put("type", question.type().name().toLowerCase(java.util.Locale.ROOT));
            if (!question.instructions().isEmpty()) wire.put("instructions", question.instructions());
            switch (question.type()) {
                case CHOICE -> {
                    ObjectNode criteria = wire.putObject("criteria");
                    question.criteria().forEach(value -> criteria.put(value.label(), value.description()));
                }
                case SCORE -> {
                    ArrayNode criteria = wire.putArray("criteria");
                    question.criteria().forEach(value -> criteria.add(value.description()));
                }
                case NOUL -> {
                    if (!question.criteria().isEmpty()) {
                        ObjectNode criteria = wire.putObject("criteria");
                        question.criteria().forEach(value -> criteria.put(value.label(), value.description()));
                    }
                }
            }
        }
        return root;
    }

    private ObjectNode withModel(ObjectNode request) {
        request.put("model", model);
        return request;
    }

    private DecisionResponse decode(DecisionRequest request, HttpResponse<String> response) {
        int status = response.statusCode();
        if (status < 200 || status >= 300) throw httpFailure(status);
        final JsonNode root;
        try {
            root = JSON.readTree(response.body());
        } catch (Exception failure) {
            throw invalid("Jev response is not valid JSON", failure);
        }
        Optional<String> responseModel = optionalText(root, "model");
        JsonNode answers = requiredObject(root, "answers");
        if (answers.size() != request.questions().size()) throw invalid("Jev returned an unexpected answer count");
        List<DecisionAnswer> decoded = new ArrayList<>();
        for (DecisionQuestion question : request.questions()) {
            JsonNode answer = answers.get(question.name());
            if (answer == null || !answer.isObject()) throw invalid("Jev omitted answer " + question.name());
            decoded.add(decode(question, answer));
        }
        Optional<QueryTokenUsage> tokens = optionalObject(root, "usage").map(usage -> {
            OptionalLong inputTokens = optionalNonNegativeLong(usage, "input_tokens");
            OptionalLong outputTokens = optionalNonNegativeLong(usage, "output_tokens");
            OptionalLong totalTokens = OptionalLong.empty();
            if (inputTokens.isPresent() && outputTokens.isPresent()) {
                try {
                    totalTokens = OptionalLong.of(Math.addExact(inputTokens.getAsLong(), outputTokens.getAsLong()));
                } catch (ArithmeticException failure) {
                    throw invalid("Jev usage token total exceeds the supported range", failure);
                }
            }
            return new QueryTokenUsage(inputTokens, outputTokens, totalTokens);
        });
        QueryObservation observation = QueryObservation.live(tokens, responseModel, Optional.empty());
        return new DecisionResponse(new DecisionResult(decoded), Optional.of(observation));
    }

    private static DecisionAnswer decode(DecisionQuestion question, JsonNode answer) {
        String type = requiredText(answer, "type");
        if (!type.equals(question.type().name().toLowerCase(java.util.Locale.ROOT))) {
            throw invalid("Jev changed answer type for " + question.name());
        }
        return switch (question.type()) {
            case NOUL -> {
                BigDecimal noul = probability(answer, "noul");
                List<DecisionProbability> probabilities = List.of(
                    new DecisionProbability("false", BigDecimal.ONE.subtract(noul)),
                    new DecisionProbability("true", noul));
                yield new DecisionAnswer(question.name(), question.type(), "", noul,
                    noul.max(BigDecimal.ONE.subtract(noul)), probabilities);
            }
            case CHOICE -> {
                String selected = requiredText(answer, "choice");
                BigDecimal confidence = probability(answer, "confidence");
                List<DecisionProbability> probabilities = probabilities(requiredObject(answer, "probabilities"));
                yield new DecisionAnswer(question.name(), question.type(), selected, BigDecimal.ZERO,
                    confidence, probabilities);
            }
            case SCORE -> {
                BigDecimal score = requiredDecimal(answer, "score");
                BigDecimal confidence = probability(answer, "confidence");
                List<DecisionProbability> probabilities = scoreProbabilities(
                    question, requiredObject(answer, "probabilities"));
                optionalObject(answer, "legend").ifPresent(legend -> validateLegend(question, legend));
                yield new DecisionAnswer(question.name(), question.type(), "", score, confidence, probabilities);
            }
        };
    }

    private static URI endpoint(String baseUrl) {
        String value = java.util.Objects.requireNonNull(baseUrl, "Jev base URL must not be null").trim();
        final URI base;
        try {
            base = URI.create(value);
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("Jev base URL must be a valid absolute URI", failure);
        }
        String scheme = base.getScheme();
        String host = base.getHost();
        if (scheme == null || host == null || base.getUserInfo() != null || base.getQuery() != null
            || base.getFragment() != null) {
            throw new IllegalArgumentException("Jev base URL must be an absolute HTTP(S) URI without credentials, query or fragment");
        }
        boolean loopback = host.equalsIgnoreCase("localhost") || host.equals("127.0.0.1") || host.equals("::1");
        if (!scheme.equalsIgnoreCase("https") && !(scheme.equalsIgnoreCase("http") && loopback)) {
            throw new IllegalArgumentException("Jev base URL must use HTTPS, except for a loopback HTTP endpoint");
        }

        String path = Optional.ofNullable(base.getPath()).orElse("").replaceAll("/+$", "");
        String endpointPath;
        if (path.endsWith("/v1")) {
            endpointPath = path.substring(0, path.length() - 3) + "/alpha/decisions";
        } else if (path.endsWith("/alpha")) {
            endpointPath = path + "/decisions";
        } else if (path.endsWith("/alpha/decisions") || path.endsWith("/v1/systemone")) {
            endpointPath = path;
        } else {
            endpointPath = path + "/v1/systemone";
        }
        try {
            return new URI(base.getScheme(), null, base.getHost(), base.getPort(), endpointPath, null, null);
        } catch (URISyntaxException failure) {
            throw new IllegalArgumentException("Jev base URL cannot be converted to a decision endpoint", failure);
        }
    }

    private static List<DecisionProbability> probabilities(JsonNode node) {
        List<DecisionProbability> result = new ArrayList<>();
        node.fields().forEachRemaining(entry -> result.add(
            new DecisionProbability(entry.getKey(), decimalProbability(entry.getValue(), entry.getKey()))));
        result.sort(Comparator.comparing(DecisionProbability::label));
        return List.copyOf(result);
    }

    private static List<DecisionProbability> scoreProbabilities(DecisionQuestion question, JsonNode node) {
        DecisionProbability[] result = new DecisionProbability[question.criteria().size()];
        node.fields().forEachRemaining(entry -> {
            final int index;
            try {
                if (!entry.getKey().matches("0|[1-9][0-9]*")) throw new NumberFormatException();
                index = Integer.parseInt(entry.getKey());
            } catch (NumberFormatException failure) {
                throw invalid("Jev score probability key must be a criterion index: " + entry.getKey());
            }
            if (index >= result.length) {
                throw invalid("Jev score probability index is outside the supplied rubric: " + entry.getKey());
            }
            result[index] = new DecisionProbability(
                question.criteria().get(index).label(), decimalProbability(entry.getValue(), entry.getKey()));
        });
        List<DecisionProbability> mapped = new ArrayList<>();
        for (DecisionProbability probability : result) {
            if (probability != null) mapped.add(probability);
        }
        return List.copyOf(mapped);
    }

    private static void validateLegend(DecisionQuestion question, JsonNode legend) {
        if (legend.size() != question.criteria().size()) throw invalid("Jev score legend has the wrong size");
        for (int index = 0; index < question.criteria().size(); index++) {
            JsonNode description = legend.get(Integer.toString(index));
            if (description == null || !description.isTextual()
                || !description.textValue().equals(question.criteria().get(index).description())) {
                throw invalid("Jev score legend does not match the supplied rubric");
            }
        }
    }

    private static JsonNode parseState(String json) {
        try { return JSON.readTree(json); }
        catch (Exception failure) { throw invalid("decision state is not valid JSON", failure); }
    }

    private static DecisionProviderFailureException httpFailure(int status) {
        if (status == 401) return failure(DecisionProviderFailureException.Kind.AUTHENTICATION_REQUIRED,
            "jev-authentication-required", "Jev rejected the configured credentials");
        if (status == 408 || status == 429 || status >= 500) return failure(
            DecisionProviderFailureException.Kind.TEMPORARILY_UNAVAILABLE,
            status == 429 ? "jev-rate-limited" : "jev-temporarily-unavailable",
            "Jev request failed with HTTP " + status);
        return failure(DecisionProviderFailureException.Kind.TERMINAL, "jev-request-rejected",
            "Jev request failed with HTTP " + status);
    }

    static DecisionProviderFailureException transportFailure(Throwable failure) {
        Throwable cause = failure;
        while (cause instanceof CompletionException && cause.getCause() != null) cause = cause.getCause();
        String message = cause instanceof HttpTimeoutException ? "Jev request timed out" : "Jev transport failed";
        if (cause instanceof IOException || cause instanceof InterruptedException) {
            return new DecisionProviderFailureException(
                DecisionProviderFailureException.Kind.TEMPORARILY_UNAVAILABLE,
                cause instanceof HttpTimeoutException ? "jev-timeout" : "jev-transport-failed",
                message,
                cause);
        }
        return new DecisionProviderFailureException(
            DecisionProviderFailureException.Kind.TERMINAL,
            "jev-request-failed",
            "Jev request failed",
            cause);
    }

    private static JsonNode requiredObject(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || !value.isObject()) throw invalid("Jev response field " + field + " must be an object");
        return value;
    }

    private static Optional<JsonNode> optionalObject(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || value.isNull()) return Optional.empty();
        if (!value.isObject()) throw invalid("Jev response field " + field + " must be an object");
        return Optional.of(value);
    }

    private static String requiredText(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || !value.isTextual() || value.textValue().isBlank()) {
            throw invalid("Jev response field " + field + " must be non-blank text");
        }
        return value.textValue();
    }

    private static Optional<String> optionalText(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || value.isNull()) return Optional.empty();
        if (!value.isTextual() || value.textValue().isBlank()) {
            throw invalid("Jev response field " + field + " must be non-blank text");
        }
        return Optional.of(value.textValue());
    }

    private static BigDecimal requiredDecimal(JsonNode node, String field) {
        JsonNode value = node == null ? null : node.get(field);
        if (value == null || !value.isNumber()) throw invalid("Jev response field " + field + " must be numeric");
        return value.decimalValue();
    }

    private static BigDecimal probability(JsonNode node, String field) {
        return decimalProbability(node.get(field), field);
    }

    private static BigDecimal decimalProbability(JsonNode value, String field) {
        if (value == null || !value.isNumber()) throw invalid("Jev probability " + field + " must be numeric");
        BigDecimal probability = value.decimalValue();
        if (probability.compareTo(BigDecimal.ZERO) < 0 || probability.compareTo(BigDecimal.ONE) > 0) {
            throw invalid("Jev probability " + field + " must be between zero and one");
        }
        return probability;
    }

    private static OptionalLong optionalNonNegativeLong(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull()) return OptionalLong.empty();
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
            throw invalid("Jev usage field " + field + " must be a non-negative integer");
        }
        return OptionalLong.of(value.longValue());
    }

    private static DecisionProviderFailureException invalid(String message) {
        return failure(DecisionProviderFailureException.Kind.TERMINAL, "decision-provider-invalid-response", message);
    }

    private static DecisionProviderFailureException invalid(String message, Throwable cause) {
        return new DecisionProviderFailureException(DecisionProviderFailureException.Kind.TERMINAL,
            "decision-provider-invalid-response", message, cause);
    }

    private static DecisionProviderFailureException failure(
        DecisionProviderFailureException.Kind kind, String code, String message) {
        return new DecisionProviderFailureException(kind, code, message);
    }
}
