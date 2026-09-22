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
        assertEquals("jev-latest", requestBody.get().get("model").textValue());
        assertTrue(requestBody.get().get("questions").has("supplier"));
        assertTrue(requestBody.get().get("questions").has("property"));
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
            "http://127.0.0.1:" + server.getAddress().getPort(), "jev-latest", Duration.ofSeconds(2));
    }

    private void start(AtomicReference<JsonNode> body, String response, int status) throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/systemone", exchange -> {
            body.set(JSON.readTree(exchange.getRequestBody()));
            byte[] bytes = response.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
    }
}
