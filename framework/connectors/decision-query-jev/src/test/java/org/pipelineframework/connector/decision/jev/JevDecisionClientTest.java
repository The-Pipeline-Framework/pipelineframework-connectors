package org.pipelineframework.connector.decision.jev;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletionException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.decision.DecisionCriterion;
import org.pipelineframework.connector.decision.DecisionProviderFailureException;
import org.pipelineframework.connector.decision.DecisionQuestion;
import org.pipelineframework.connector.decision.DecisionQuestionType;
import org.pipelineframework.connector.decision.DecisionRequest;

final class JevDecisionClientTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private HttpServer server;

    @AfterEach
    void stopServer() {
        if (server != null) server.stop(0);
    }

    @Test
    void sendsMultipleTypedQuestionsAndPreservesProbabilities() throws Exception {
        AtomicReference<JsonNode> requestBody = new AtomicReference<>();
        String response = """
            {"model":"jev-test","answers":{
              "supplier":{"type":"choice","choice":"EXPLICIT_TEXT","confidence":0.82,
                "probabilities":{"EXPLICIT_TEXT":0.82,"INSUFFICIENT":0.18}},
              "property":{"type":"choice","choice":"P2","confidence":0.7,
                "probabilities":{"P1":0.3,"P2":0.7}}
            },"usage":{"input_tokens":17,"output_tokens":9}}
            """;
        start(requestBody, response, 200);
        JevDecisionClient client = client();
        DecisionRequest request = new DecisionRequest("{\"supplier\":\"Acme\"}", List.of(
            choice("supplier", "EXPLICIT_TEXT", "INSUFFICIENT"), choice("property", "P1", "P2")));

        var result = client.decide(request).toCompletableFuture().join();

        assertEquals(2, result.result().answers().size());
        assertEquals(new BigDecimal("0.7"), result.result().answers().get(1).confidence());
        assertEquals(2, result.result().answers().get(1).probabilities().size());
        assertEquals("jev-test", result.observation().orElseThrow().responseModel().orElseThrow());
        assertEquals("typesafe/jev-1.13", requestBody.get().get("model").textValue());
        assertTrue(requestBody.get().get("questions").has("supplier"));
        assertTrue(requestBody.get().get("questions").has("property"));
    }

    @Test
    void decodesNoulAndMapsScoreIndexesToDeclaredLabels() throws Exception {
        AtomicReference<JsonNode> requestBody = new AtomicReference<>();
        String response = """
            {"answers":{
              "needsReview":{"type":"noul","noul":0.73},
              "urgency":{"type":"score","score":1.4,"confidence":0.6,
                "probabilities":{"0":0.1,"1":0.4,"2":0.5}}
            }}
            """;
        start(requestBody, response, 200);
        JevDecisionClient client = client();
        DecisionQuestion noul = new DecisionQuestion("needsReview", DecisionQuestionType.NOUL, "Review?", List.of(
            new DecisionCriterion("false", "No review"), new DecisionCriterion("true", "Needs review")));
        DecisionQuestion score = new DecisionQuestion("urgency", DecisionQuestionType.SCORE, "Urgency", List.of(
            new DecisionCriterion("LOW", "low"), new DecisionCriterion("MEDIUM", "medium"),
            new DecisionCriterion("HIGH", "high")));

        var result = client.decide(new DecisionRequest("{}", List.of(noul, score))).toCompletableFuture().join();

        assertEquals(new BigDecimal("0.73"), result.result().answers().get(0).value());
        assertEquals(List.of("false", "true"), result.result().answers().get(0).probabilities().stream()
            .map(probability -> probability.label()).toList());
        assertEquals(List.of("LOW", "MEDIUM", "HIGH"), result.result().answers().get(1).probabilities().stream()
            .map(probability -> probability.label()).toList());
        assertEquals(new BigDecimal("0.5"), result.result().answers().get(1).probabilities().get(2).probability());
        assertEquals("noul", requestBody.get().path("questions").path("needsReview").path("type").textValue());
        assertEquals("score", requestBody.get().path("questions").path("urgency").path("type").textValue());
    }

    @Test
    void rejectsInsecureRemoteAndUnsupportedBaseUrisBeforeSending() {
        AuthenticatedJevConnection connection =
            AuthenticatedJevConnection.bearer(HttpClient.newHttpClient(), "secret");

        assertThrows(IllegalArgumentException.class, () -> new JevDecisionClient(
            connection, "http://example.com/api/v1", "typesafe/jev-1.13", Duration.ofSeconds(2)));
        assertThrows(IllegalArgumentException.class, () -> new JevDecisionClient(
            connection, "ftp://example.com/api/v1", "typesafe/jev-1.13", Duration.ofSeconds(2)));
    }

    @Test
    void classifiesNonTransportAsynchronousFailuresAsTerminal() {
        DecisionProviderFailureException failure =
            JevDecisionClient.transportFailure(new CompletionException(new SecurityException("denied")));

        assertEquals(DecisionProviderFailureException.Kind.TERMINAL, failure.kind());
        assertEquals("jev-request-failed", failure.outcomeCode());
    }

    @Test
    void rejectsMalformedResponsesExplicitly() throws Exception {
        start(new AtomicReference<>(), "{\"model\":\"jev-test\",\"answers\":{}}", 200);

        CompletionException failure = assertThrows(CompletionException.class, () -> client()
            .decide(new DecisionRequest("{}", List.of(choice("supplier", "EXPLICIT_TEXT", "INSUFFICIENT"))))
            .toCompletableFuture().join());

        DecisionProviderFailureException providerFailure =
            assertInstanceOf(DecisionProviderFailureException.class, failure.getCause());
        assertEquals(DecisionProviderFailureException.Kind.TERMINAL, providerFailure.kind());
        assertEquals("decision-provider-invalid-response", providerFailure.outcomeCode());
    }

    @Test
    void classifiesRateLimitsAsTemporarilyUnavailable() throws Exception {
        start(new AtomicReference<>(), "{}", 429);

        CompletionException failure = assertThrows(CompletionException.class, () -> client()
            .decide(new DecisionRequest("{}", List.of(choice("supplier", "EXPLICIT_TEXT", "INSUFFICIENT"))))
            .toCompletableFuture().join());

        DecisionProviderFailureException providerFailure =
            assertInstanceOf(DecisionProviderFailureException.class, failure.getCause());
        assertEquals(DecisionProviderFailureException.Kind.TEMPORARILY_UNAVAILABLE, providerFailure.kind());
        assertEquals("jev-rate-limited", providerFailure.outcomeCode());
    }

    private DecisionQuestion choice(String name, String... labels) {
        return new DecisionQuestion(name, DecisionQuestionType.CHOICE, "Choose",
            java.util.Arrays.stream(labels).map(label -> new DecisionCriterion(label, label)).toList());
    }

    private JevDecisionClient client() {
        return new JevDecisionClient(AuthenticatedJevConnection.bearer(HttpClient.newHttpClient(), "secret"),
            "http://127.0.0.1:" + server.getAddress().getPort() + "/api/v1", "typesafe/jev-1.13",
            Duration.ofSeconds(2));
    }

    private void start(AtomicReference<JsonNode> body, String response, int status) throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/alpha/decisions", exchange -> {
            body.set(JSON.readTree(exchange.getRequestBody()));
            byte[] bytes = response.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
    }
}
