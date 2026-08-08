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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private static final Set<String> STARTABLE_SESSION_STATUSES = Set.of("", "pending", "created", "new", "ready");
    private static final Set<String> ACTIVE_SESSION_STATUSES = Set.of("active", "running", "started", "in_progress");
    private static final Set<String> TERMINAL_SESSION_STATUSES = Set.of("finished", "completed", "stopped", "expired", "failed");

    private final NightwatchProperties properties;
    private final NightwatchClient client;
    // All known incidents for this session, keyed by incident_id
    private final Map<String, IncidentState> incidents = new ConcurrentHashMap<>();
    // Shared wakeup sink — both SSE events and poll ticks feed into this
    private final Sinks.Many<String> wakeups = Sinks.many().multicast().directBestEffort();
    private final AtomicBoolean finished = new AtomicBoolean(false);

    private volatile String sessionId;
    // Cached catalog — updated on startup and whenever catalog_updated is received
    private volatile Catalog catalog;
    private volatile Instant lastIncidentListFetch = Instant.EPOCH;
    private volatile Instant lastSessionCheck = Instant.EPOCH;

    public NightwatchControllerService(NightwatchProperties properties, NightwatchClient client) {
        this.properties = properties;
        this.client = client;
        this.sessionId = blankToNull(properties.sessionId());
    }

    // --- Entry point ---

    public Mono<Void> run() {
        return prepareSession()                                          // Step 1: auth + session setup
                .then(refreshCatalog(true))                             // Step 2: fetch catalog
                .thenMany(Flux.merge(sseLoop(), pollLoop(), schedulerLoop())) // Step 3: run unattended
                .then();
    }

    // --- Step 1: Session setup ---

    // Verifies auth, creates or reconnects to a session, and waits for it to become active
    private Mono<Void> prepareSession() {
        return client.verifyAuth()
                .doOnNext(auth -> log.info("Authenticated as {}", JsonSupport.firstText(auth, "subject", "sub").orElse("unknown")))
                .then(Mono.defer(this::ensureSessionCreated))
                .then(Mono.defer(() -> client.getSession(sessionId))) // defer so updated sessionId is read
                .flatMap(this::startOrWaitForActive)
                .then();
    }

    private Mono<Void> ensureSessionCreated() {
        if (sessionId != null) {
            return Mono.empty();
        }
        return client.createSession(properties.sessionMode(), properties.scenario())
                .doOnNext(created -> {
                    sessionId = extractId(created).orElseThrow(() -> new IllegalStateException("Missing session id"));
                    log.info("Created session {} for scenario {}", sessionId, properties.scenario());
                })
                .then();
    }

    private Mono<Void> startOrWaitForActive(JsonNode session) {
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
                    "Session " + sessionId + " is already " + status + ". Unset SESSION_ID or use a fresh token/session."));
        }
        log.info("Reconnected to session {} with status {}", sessionId, status);
        return waitForActiveSession();
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

    // --- Step 2: Catalog ---

    // Fetches and caches the catalog. On failure, keeps the last known good catalog unless none has ever loaded.
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
                        return client.getSession(sessionId).flatMap(session -> {
                            String status = JsonSupport.firstText(session, "status", "state").orElse("").toLowerCase();
                            if (TERMINAL_SESSION_STATUSES.contains(status)) {
                                return Mono.error(new IllegalStateException(
                                        "Catalog unavailable because session " + sessionId + " is " + status
                                                + ". Start a new session and rerun the controller."));
                            }
                            return Mono.delay(Duration.ofSeconds(1)).then(refreshCatalog(true));
                        });
                    }
                    log.warn("Catalog refresh failed; keeping cached catalog: {}", error.getMessage());
                    return Mono.empty();
                });
    }

    // --- Step 3: Run unattended ---

    // SSE stream: connects to the event stream and wakes up the scheduler on each event.
    // HTTP is the source of truth — events here are wake-up signals only, not state updates.
    // Reconnects automatically on failure.
    private Flux<Void> sseLoop() {
        return Flux.defer(() -> client.streamEvents(sessionId)
                        .takeUntil(ignored -> finished.get())
                        .doOnNext(this::handleSseEvent)
                        .then()
                        .onErrorResume(error -> {
                            log.warn("SSE stream failed; reconnecting: {}", error.getMessage());
                            return Mono.delay(Duration.ofSeconds(1)).then();
                        })
                        .flux())
                .repeat(() -> !finished.get());
    }

    private void handleSseEvent(ServerSentEvent<JsonNode> event) {
        String eventName = event.event() == null ? "message" : event.event();
        // "message" is the default SSE type — typically a keepalive with no payload
        if ("message".equals(eventName)) {
            log.debug("SSE keepalive");
        } else {
            log.info("SSE event: {}", eventName);
        }

        // Refresh catalog in the background when it changes
        if ("catalog_updated".equals(eventName)) {
            refreshCatalog(false).subscribe(null, e -> log.warn("Catalog refresh after catalog_updated failed: {}", e.getMessage()));
        }
        // Signal end of session
        if ("session_finished".equals(eventName)) {
            finished.set(true);
        }
        // Wake up the scheduler — actual state will be fetched from HTTP
        wakeups.tryEmitNext(eventName);
    }

    // Poll loop: fallback wakeup when SSE is slow or missing events
    private Flux<Void> pollLoop() {
        return Flux.interval(Duration.ZERO, properties.pollInterval())
                .takeUntil(ignored -> finished.get())
                .doOnNext(ignored -> wakeups.tryEmitNext("poll"))
                .thenMany(Flux.empty());
    }

    // Scheduler loop: on each wakeup (from SSE or poll), run a full reconcile pass.
    // Uses concatMap to serialize passes — one at a time, no overlap.
    private Flux<Void> schedulerLoop() {
        return wakeups.asFlux()
                .takeUntil(ignored -> finished.get())
                .concatMap(ignored -> reconcileAndSchedule().onErrorResume(error -> {
                    log.warn("Scheduler pass failed: {}", error.getMessage());
                    return Mono.empty();
                }))
                .thenMany(Flux.defer(() -> logSummary().flux()));
    }

    // One reconcile pass: refresh incident list, refresh+schedule each incident, check session status
    private Mono<Void> reconcileAndSchedule() {
        return refreshIncidentListIfDue()
                .thenMany(Flux.fromIterable(incidents.values()))
                .concatMap(this::refreshAndScheduleIncident)
                .then(checkSessionIfDue());
    }

    // Periodically checks session status to detect end-of-run without relying solely on SSE
    private Mono<Void> checkSessionIfDue() {
        if (Duration.between(lastSessionCheck, Instant.now()).compareTo(properties.pollInterval()) < 0) {
            return Mono.empty();
        }
        return client.getSession(sessionId)
                .doOnNext(session -> {
                    lastSessionCheck = Instant.now();
                    String status = JsonSupport.firstText(session, "status", "state").orElse("").toLowerCase();
                    if (TERMINAL_SESSION_STATUSES.contains(status)) {
                        finished.set(true);
                    }
                })
                .then();
    }

    // Detects new incidents appearing in the session (rate-limited to incidentRefetchInterval)
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

    // Fetches fresh incident state from HTTP, then attempts to schedule the next valid action(s)
    private Mono<Void> refreshAndScheduleIncident(IncidentState incidentState) {
        if (!isRefreshDue(incidentState)) {
            return incidentState.isTerminal() ? Mono.empty() : scheduleIncident(incidentState);
        }
        return client.getIncident(sessionId, incidentState.incidentId())
                .map(payload -> JsonSupport.objectAt(payload, "incident"))
                .doOnNext(incidentState::mergeSnapshot)
                .then(client.getIncidentEvents(sessionId, incidentState.incidentId()))
                .doOnNext(incidentState::mergeEvents)
                .then(Mono.defer(() -> incidentState.isTerminal() ? Mono.empty() : scheduleIncident(incidentState)))
                .onErrorResume(NightwatchApiException.class, error -> {
                    log.warn("Incident {} unavailable: {}", incidentState.incidentId(), error.getMessage());
                    return Mono.empty();
                });
    }

    private boolean isRefreshDue(IncidentState incidentState) {
        return Duration.between(incidentState.lastFetchAt(), Instant.now())
                .compareTo(properties.incidentRefetchInterval()) >= 0;
    }

    // --- Core scheduling logic ---

    // Chooses valid actions in correct order and starts them.
    // Rules: dependencies satisfied, serial actions run alone, max 2 parallel per incident.
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
        // Nothing can start alongside a serial action
        if (serialRunning || runningCount >= 2) {
            return Mono.empty();
        }

        List<ActionDefinition> toStart = new ArrayList<>();
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
            toStart.add(action);
            plannedRunningCount++;
            if (action.serial() || plannedRunningCount >= 2) {
                break;
            }
        }

        if (toStart.isEmpty()) {
            return Mono.empty();
        }
        if (toStart.size() == 1) {
            return startAction(incidentState, toStart.get(0));
        }
        // Start eligible parallel actions concurrently
        return Mono.when(toStart.stream().map(a -> startAction(incidentState, a)).toList());
    }

    private Mono<Void> startAction(IncidentState incidentState, ActionDefinition action) {
        // Optimistically mark as started locally to prevent concurrent scheduling loops from retrying it
        incidentState.markStarted(action.id());

        return client.startAction(sessionId, incidentState.incidentId(), action.id())
                .doOnNext(ignored -> log.info("Started action {} for incident {}", action.id(), incidentState.incidentId()))
                .then()
                .onErrorResume(NightwatchApiException.class, error -> {
                    // On rejection, re-fetch catalog and full incident state (snapshot + events) before the next
                    // scheduling pass. mergeSnapshot clears action sets, so mergeEvents must follow to rebuild them.
                    log.warn("Action {} rejected for incident {}: {}", action.id(), incidentState.incidentId(), error.getMessage());
                    return refreshCatalog(false)
                            .then(client.getIncident(sessionId, incidentState.incidentId()))
                            .map(payload -> JsonSupport.objectAt(payload, "incident"))
                            .doOnNext(incidentState::mergeSnapshot)
                            .then(client.getIncidentEvents(sessionId, incidentState.incidentId()))
                            .doOnNext(incidentState::mergeEvents)
                            .then();
                });
    }

    // --- End of run ---

    private Mono<Void> logSummary() {
        return client.getMarkdownSummary(sessionId)
                .doOnNext(summary -> log.info("Session summary:\n{}", summary))
                .then();
    }

    // --- Helpers ---

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
