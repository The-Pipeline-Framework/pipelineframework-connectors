package org.pipelineframework.connector.decision.jev;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** Host-resolved authenticated Jev transport; credentials never enter connector configuration. */
public final class AuthenticatedJevConnection implements org.pipelineframework.connector.ResolvedConnection {
    private final HttpClient client;
    private final String apiKey;

    private AuthenticatedJevConnection(HttpClient client, String apiKey) {
        HttpClient validatedClient = Objects.requireNonNull(client, "Jev HTTP client must not be null");
        if (validatedClient.followRedirects() != HttpClient.Redirect.NEVER) {
            throw new IllegalArgumentException("Jev HTTP client must not follow redirects");
        }
        String validatedApiKey = Objects.requireNonNull(apiKey, "Jev API key must not be null").trim();
        if (validatedApiKey.isEmpty()) throw new IllegalArgumentException("Jev API key must not be blank");
        this.client = validatedClient;
        this.apiKey = validatedApiKey;
    }

    /** Uses a host-owned client whose lifecycle is longer than any resolved connection. */
    public static AuthenticatedJevConnection bearer(HttpClient client, String apiKey) {
        return new AuthenticatedJevConnection(client, apiKey);
    }

    CompletionStage<HttpResponse<String>> post(URI uri, Duration timeout, String body) {
        HttpRequest request = HttpRequest.newBuilder(uri).timeout(timeout)
            .header("Authorization", "Bearer " + apiKey)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("User-Agent", "pipelineframework-decision-query-jev")
            .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
    }
}
