package com.example.nightwatch.client;

import com.example.nightwatch.config.NightwatchProperties;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
public class NightwatchClient {
    private final WebClient webClient;

    public NightwatchClient(WebClient.Builder builder, NightwatchProperties properties) {
        this.webClient = builder
                .baseUrl(stripTrailingSlash(properties.apiUrl()))
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + properties.apiToken())
                .build();
    }

    public Mono<JsonNode> verifyAuth() {
        return getJson("/auth/verify");
    }

    public Mono<JsonNode> createSession(String sessionMode, String scenarioType) {
        return postJson("/sessions", new CreateSessionRequest(sessionMode, scenarioType));
    }

    public Mono<JsonNode> getSession(String sessionId) {
        return getJson("/sessions/" + sessionId);
    }

    public Mono<JsonNode> startSession(String sessionId) {
        return postJson("/sessions/" + sessionId + "/start", null);
    }

    public Mono<JsonNode> getCatalog(String sessionId) {
        return getJson("/sessions/" + sessionId + "/catalog");
    }

    public Mono<JsonNode> listIncidents(String sessionId) {
        return getJson("/sessions/" + sessionId + "/incidents");
    }

    public Mono<JsonNode> getIncident(String sessionId, String incidentId) {
        return getJson("/sessions/" + sessionId + "/incidents/" + incidentId);
    }

    public Mono<JsonNode> getIncidentEvents(String sessionId, String incidentId) {
        return getJson("/sessions/" + sessionId + "/incidents/" + incidentId + "/events");
    }

    public Mono<JsonNode> startAction(String sessionId, String incidentId, String actionId) {
        return postJson(
                "/sessions/" + sessionId + "/incidents/" + incidentId + "/action",
                new StartActionRequest(actionId, "starting next runnable step"));
    }

    public Mono<String> getMarkdownSummary(String sessionId) {
        return webClient.get()
                .uri("/sessions/" + sessionId + "/summary")
                .accept(MediaType.valueOf("text/markdown"))
                .retrieve()
                .onStatus(status -> !status.is2xxSuccessful(), response -> response.bodyToMono(String.class)
                        .defaultIfEmpty("")
                        .map(body -> new NightwatchApiException(response.statusCode(), body)))
                .bodyToMono(String.class)
                .retryWhen(transientRetry());
    }

    public Flux<ServerSentEvent<JsonNode>> streamEvents(String sessionId) {
        return webClient.get()
                .uri("/sessions/" + sessionId + "/stream")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .retrieve()
                .onStatus(status -> !status.is2xxSuccessful(), response -> response.bodyToMono(String.class)
                        .defaultIfEmpty("")
                        .map(body -> new NightwatchApiException(response.statusCode(), body)))
                .bodyToFlux(new org.springframework.core.ParameterizedTypeReference<ServerSentEvent<JsonNode>>() {})
                .retryWhen(transientRetry());
    }

    private Mono<JsonNode> getJson(String path) {
        return webClient.get()
                .uri(path)
                .retrieve()
                .onStatus(status -> !status.is2xxSuccessful(), response -> response.bodyToMono(String.class)
                        .defaultIfEmpty("")
                        .map(body -> new NightwatchApiException(response.statusCode(), body)))
                .bodyToMono(JsonNode.class)
                .retryWhen(transientRetry());
    }

    private Mono<JsonNode> postJson(String path, Object body) {
        WebClient.RequestBodySpec request = webClient.post().uri(path);
        WebClient.RequestHeadersSpec<?> headersSpec = body == null ? request : request.bodyValue(body);
        return headersSpec
                .retrieve()
                .onStatus(status -> !status.is2xxSuccessful(), response -> response.bodyToMono(String.class)
                        .defaultIfEmpty("")
                        .map(responseBody -> new NightwatchApiException(response.statusCode(), responseBody)))
                .bodyToMono(JsonNode.class)
                .retryWhen(transientRetry());
    }

    private Retry transientRetry() {
        return Retry.backoff(3, Duration.ofMillis(500))
                .filter(throwable -> throwable instanceof NightwatchApiException apiException && apiException.isTransient());
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private record CreateSessionRequest(String session_mode, String scenario_type) {}

    private record StartActionRequest(String action_id, String notes) {}
}
