package com.example.nightwatch.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

public class IncidentState {
    private static final Set<String> TERMINAL_STATUSES = Set.of("resolved", "expired", "finished", "failed", "closed");

    private final String incidentId;
    private JsonNode snapshot;
    private final Set<String> completed = new HashSet<>();
    private final Set<String> running = new HashSet<>();
    private final Set<String> failed = new HashSet<>();
    private volatile Instant lastFetchAt = Instant.EPOCH;

    public IncidentState(String incidentId) {
        this.incidentId = incidentId;
    }

    public String incidentId() {
        return incidentId;
    }

    public JsonNode snapshot() {
        return snapshot;
    }

    public Set<String> completed() {
        return completed;
    }

    public Set<String> running() {
        return running;
    }

    public Instant lastFetchAt() {
        return lastFetchAt;
    }

    public boolean isTerminal() {
        if (snapshot == null) return false;
        String status = JsonSupport.firstText(snapshot, "status").orElse("").toLowerCase();
        return TERMINAL_STATUSES.contains(status);
    }

    // Called from the incident list refresh — updates the snapshot for type/status lookups
    // but does NOT update lastFetchAt. Only the full per-incident refresh (mergeSnapshot)
    // advances the timer, so isRefreshDue() correctly reflects when we last fetched events.
    public void mergeListSnapshot(JsonNode snapshot) {
        this.snapshot = snapshot;
    }

    // Called from the full per-incident refresh (getIncident + getIncidentEvents).
    // Updates lastFetchAt to gate the next full refresh cycle.
    public void mergeSnapshot(JsonNode snapshot) {
        this.snapshot = snapshot;
        this.lastFetchAt = Instant.now();
        // Action state (completed/running/failed) is intentionally NOT cleared here.
        // The events endpoint returns the full history on every call, so mergeEvents()
        // is the sole authority on action transitions. Clearing here would cause a window
        // where state is empty between mergeSnapshot and mergeEvents, leading to
        // spurious re-submissions of already-running or already-completed actions.
    }

    public void markStarted(String actionId) {
        running.add(actionId);
    }

    // Called from the SSE event handler when an action event fires for this incident.
    // Forces the next scheduler pass to re-fetch events immediately instead of waiting
    // for the full incidentRefetchInterval, closing the ~5s gap between action completion
    // and the next action starting.
    public void forceRefreshDue() {
        this.lastFetchAt = Instant.EPOCH;
    }

    public void mergeEvents(JsonNode eventsPayload) {
        for (JsonNode event : JsonSupport.arrayOrObjectValues(eventsPayload, "events", "data", "items")) {
            JsonSupport.firstText(event, "action_id", "action", "id", "name").ifPresent(actionId -> {
                String eventType = JsonSupport.firstText(event, "event", "type", "status", "state")
                        .orElse("")
                        .toLowerCase();
                if (eventType.contains("completed") || eventType.contains("succeeded") || eventType.equals("success")) {
                    running.remove(actionId);
                    failed.remove(actionId);
                    completed.add(actionId);
                } else if (eventType.contains("failed") || eventType.equals("error")) {
                    running.remove(actionId);
                    failed.add(actionId);
                } else if (eventType.contains("started") || eventType.contains("running")) {
                    running.add(actionId);
                }
            });
        }
    }
}
