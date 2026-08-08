package com.example.nightwatch.controller;

import com.example.nightwatch.client.NightwatchApiException;
import com.example.nightwatch.client.NightwatchClient;
import com.example.nightwatch.config.NightwatchProperties;
import com.example.nightwatch.model.ActionDefinition;
import com.example.nightwatch.model.Catalog;
import com.example.nightwatch.model.IncidentState;
import com.example.nightwatch.model.JsonSupport;
import com.example.nightwatch.model.Playbook;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

@Service
public class NightwatchControllerService {
    private static final Logger log = LoggerFactory.getLogger(NightwatchControllerService.class);
    private static final List<String> STARTABLE_SESSION_STATUSES = List.of("", "pending", "created", "new", "ready");
    private static final List<String> ACTIVE_SESSION_STATUSES = List.of("active", "running", "started", "in_progress");
    private static final List<String> TERMINAL_SESSION_STATUSES = List.of("finished", "completed", "stopped", "expired", "failed");

    private final NightwatchProperties properties;
    private final NightwatchClient client;
    private final Map<String, IncidentState> incidents = new ConcurrentHashMap<>();
    private final Sinks.Many<String> wakeups = Sinks.many().multicast().directBestEffort();
    private final AtomicBoolean finished = new AtomicBoolean(false);

    private volatile String sessionId;
    private volatile Catalog catalog;
    private volatile Instant lastIncidentListFetch = Instant.EPOCH;

    public NightwatchControllerService(NightwatchProperties properties, NightwatchClient client) {
        this.properties = properties;
        this.client = client;
        this.sessionId = blankToNull(properties.sessionId());
    }

    public Mono<Void> run() {
        return prepareSession()
                .then(refreshCatalog(true))
                .thenMany(Flux.merge(sseLoop(), pollLoop(), schedulerLoop()))
                .then();
    }

    private Mono<Void> prepareSession() {
        return client.verifyAuth()
                .doOnNext(auth -> log.info("Authenticated as {}", JsonSupport.firstText(auth, "subject", "sub").orElse("unknown")))
                .then(Mono.defer(() -> {
                    if (sessionId != null) {
                        return Mono.empty();
                    }
                    return client.createSession(properties.sessionMode(), properties.scenario())
                            .doOnNext(created -> {
                                sessionId = extractId(created).orElseThrow(() -> new IllegalStateException("Missing session id"));
                                log.info("Created session {} for scenario {}", sessionId, properties.scenario());
                            })
                            .then();
                }))
                .then(Mono.defer(() -> client.getSession(sessionId)))
                .flatMap(session -> {
                    String status = JsonSupport.firstText(session, "status", "state").orElse("").toLowerCase();
                    if (STARTABLE_SESSION_STATUSES.contains(status)) {
                        return client.startSession(sessionId)
                                .doOnNext(ignored -> log.info("Started session {}", sessionId))
                                .onErrorResume(NightwatchApiException.class, error -> {
                                    log.warn("Could not start session; continuing with current status: {}", error.getMessage());
                                    return Mono.empty();
                                })
                                .then(waitForActiveSession());
                    }
                    if (TERMINAL_SESSION_STATUSES.contains(status)) {
                        return Mono.error(new IllegalStateException(
                                "Session " + sessionId + " is already " + status
                                        + ". Unset SESSION_ID or use a fresh token/session."));
                    }
                    log.info("Reconnected to session {} with status {}", sessionId, status);
                    return waitForActiveSession();
                })
                .then();
    }

    private Mono<Void> waitForActiveSession() {
        return Mono.defer(() -> client.getSession(sessionId))
                .flatMap(session -> {
                    String status = JsonSupport.firstText(session, "status", "state").orElse("").toLowerCase();
                    if (ACTIVE_SESSION_STATUSES.contains(status)) {
                        log.info("Session {} is active", sessionId);
                        return Mono.<Void>empty();
                    }
                    if (TERMINAL_SESSION_STATUSES.contains(status)) {
                        return Mono.error(new IllegalStateException(
                                "Session " + sessionId + " became " + status + " before controller startup completed."));
                    }
                    log.info("Waiting for session {} to become active; current status={}", sessionId, status);
                    return Mono.delay(Duration.ofSeconds(1)).then(waitForActiveSession());
                });
    }

    private Mono<Void> refreshCatalog(boolean required) {
        return client.getCatalog(sessionId)
                .doOnNext(rawCatalog -> {
                    catalog = new Catalog(rawCatalog);
                    log.info("Loaded catalog with {} playbooks", catalog.playbookCount());
                })
                .then()
                .onErrorResume(error -> {
                    if (required && catalog == null) {
                        log.warn("Catalog unavailable; checking session before retrying: {}", error.getMessage());
                        return failIfSessionEnded()
                                .then(Mono.delay(Duration.ofSeconds(1)))
                                .then(refreshCatalog(true));
                    }
                    log.warn("Catalog refresh failed; keeping cached catalog: {}", error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> failIfSessionEnded() {
        return client.getSession(sessionId)
                .flatMap(session -> {
                    String status = JsonSupport.firstText(session, "status", "state").orElse("").toLowerCase();
                    if (TERMINAL_SESSION_STATUSES.contains(status)) {
                        return Mono.error(new IllegalStateException(
                                "Catalog is unavailable because session " + sessionId + " is " + status
                                        + ". Start a new session and rerun the controller."));
                    }
                    return Mono.empty();
                });
    }

    private Flux<Void> sseLoop() {
        return Flux.defer(() -> client.streamEvents(sessionId)
                        .takeUntil(ignored -> finished.get())
                        .doOnNext(this::handleSseEvent)
                        .thenMany(Flux.<Void>empty()))
                .repeat(() -> !finished.get())
                .onErrorResume(error -> {
                    log.warn("SSE stream failed; reconnecting: {}", error.getMessage());
                    return Mono.delay(Duration.ofSeconds(1)).thenMany(sseLoop());
                });
    }

    private void handleSseEvent(ServerSentEvent<JsonNode> event) {
        String eventName = event.event() == null ? "message" : event.event();
        log.info("SSE event: {}", eventName);

        JsonNode data = event.data();
        if (data != null) {
            extractId(data).ifPresent(incidentId -> {
                IncidentState incident = incidents.get(incidentId);
                if (incident != null) {
                    // Merge state from SSE event (e.g. action_started, action_completed, action_failed)
                    incident.mergeEvents(data);
                    incident.mergeSnapshot(data);
                }
            });
        }

        if ("catalog_updated".equals(eventName)) {
            refreshCatalog(false).subscribe();
        }
        if ("session_finished".equals(eventName)) {
            finished.set(true);
        }
        wakeups.tryEmitNext(eventName);
    }

    private Flux<Void> pollLoop() {
        return Flux.interval(Duration.ZERO, properties.pollInterval())
                .takeUntil(ignored -> finished.get())
                .doOnNext(ignored -> wakeups.tryEmitNext("poll"))
                .thenMany(Flux.empty());
    }

    private Flux<Void> schedulerLoop() {
        return wakeups.asFlux()
                .takeUntil(ignored -> finished.get())
                .concatMap(ignored -> reconcileAndSchedule().onErrorResume(error -> {
                    log.warn("Scheduler pass failed: {}", error.getMessage());
                    return Mono.empty();
                }))
                .thenMany(Flux.defer(() -> logSummary().flux()));
    }

    private Mono<Void> reconcileAndSchedule() {
        return refreshIncidentListIfDue()
                .thenMany(Flux.fromIterable(incidents.values()))
                .concatMap(this::refreshAndScheduleIncident)
                .then(client.getSession(sessionId))
                .doOnNext(session -> {
                    String status = JsonSupport.firstText(session, "status", "state").orElse("").toLowerCase();
                    if (TERMINAL_SESSION_STATUSES.contains(status)) {
                        finished.set(true);
                    }
                })
                .then();
    }

    private Mono<Void> refreshIncidentListIfDue() {
        if (Duration.between(lastIncidentListFetch, Instant.now()).compareTo(properties.incidentRefetchInterval()) < 0) {
            return Mono.empty();
        }
        return client.listIncidents(sessionId)
                .doOnNext(payload -> {
                    lastIncidentListFetch = Instant.now();
                    for (JsonNode incident : JsonSupport.arrayOrObjectValues(payload, "incidents", "data", "items")) {
                        extractId(incident).ifPresent(incidentId -> incidents
                                .computeIfAbsent(incidentId, IncidentState::new)
                                .mergeSnapshot(incident));
                    }
                })
                .then()
                .onErrorResume(NightwatchApiException.class, error -> {
                    log.warn("Incident list unavailable: {}", error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> refreshAndScheduleIncident(IncidentState incidentState) {
        Mono<Void> refresh = Duration.between(incidentState.lastFetchAt(), Instant.now())
                        .compareTo(properties.incidentRefetchInterval()) < 0
                ? Mono.empty()
                : client.getIncident(sessionId, incidentState.incidentId())
                        .map(payload -> JsonSupport.objectAt(payload, "incident"))
                        .doOnNext(incidentState::mergeSnapshot)
                        .then(client.getIncidentEvents(sessionId, incidentState.incidentId()))
                        .doOnNext(incidentState::mergeEvents)
                        .then()
                        .onErrorResume(NightwatchApiException.class, error -> {
                            log.warn("Incident {} unavailable: {}", incidentState.incidentId(), error.getMessage());
                            return Mono.empty();
                        });

        return refresh.then(Mono.defer(() -> {
            if (incidentState.isTerminal()) {
                return Mono.empty();
            }
            return scheduleIncident(incidentState);
        }));
    }

    private Mono<Void> scheduleIncident(IncidentState incidentState) {
        Catalog currentCatalog = catalog;
        if (currentCatalog == null) {
            return Mono.empty();
        }
        Optional<Playbook> maybePlaybook = currentCatalog.findPlaybook(incidentState.snapshot());
        if (maybePlaybook.isEmpty()) {
            log.warn("No playbook found for incident {}", incidentState.incidentId());
            return Mono.empty();
        }

        Playbook playbook = maybePlaybook.get();
        int runningCount = incidentState.running().size();
        boolean serialRunning = incidentState.running().stream().anyMatch(actionId -> isSerial(actionId, playbook));
        if (serialRunning || runningCount >= 2) {
            return Mono.empty();
        }

        Mono<Void> chain = Mono.empty();
        int plannedRunningCount = runningCount;
        for (ActionDefinition action : playbook.actions()) {
            if (incidentState.completed().contains(action.id()) || incidentState.running().contains(action.id())) {
                continue;
            }
            if (!incidentState.completed().containsAll(action.dependencies())) {
                continue;
            }
            if (action.serial() && plannedRunningCount > 0) {
                break;
            }
            if (!action.serial() && plannedRunningCount >= 2) {
                break;
            }
            chain = chain.then(startAction(incidentState, action));
            plannedRunningCount++;
            if (action.serial() || plannedRunningCount >= 2) {
                break;
            }
        }
        return chain;
    }

    private Mono<Void> startAction(IncidentState incidentState, ActionDefinition action) {
        // Optimistically mark as started locally to prevent concurrent scheduling loops from retrying it
        incidentState.markStarted(action.id());

        return client.startAction(sessionId, incidentState.incidentId(), action.id())
                .doOnNext(ignored -> log.info("Started action {} for incident {}", action.id(), incidentState.incidentId()))
                .then()
                .onErrorResume(NightwatchApiException.class, error -> {
                    log.warn("Action {} rejected for incident {}: {}", action.id(), incidentState.incidentId(), error.getMessage());
                    return refreshCatalog(false)
                            .then(client.getIncident(sessionId, incidentState.incidentId()))
                            .map(payload -> JsonSupport.objectAt(payload, "incident"))
                            .doOnNext(incidentState::mergeSnapshot)
                            .then();
                });
    }

    private Mono<Void> logSummary() {
        return client.getMarkdownSummary(sessionId)
                .doOnNext(summary -> log.info("Session summary:\n{}", summary))
                .then();
    }

    private boolean isSerial(String actionId, Playbook playbook) {
        return playbook.actions().stream().anyMatch(action -> action.id().equals(actionId) && action.serial());
    }

    private Optional<String> extractId(JsonNode node) {
        return JsonSupport.firstText(node, "session_id", "incident_id", "id");
    }

    private String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
