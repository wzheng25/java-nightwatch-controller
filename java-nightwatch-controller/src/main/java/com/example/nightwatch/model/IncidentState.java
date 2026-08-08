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
    private Instant lastFetchAt = Instant.EPOCH;

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

    public void mergeSnapshot(JsonNode snapshot) {
        this.snapshot = snapshot;
        this.lastFetchAt = Instant.now();
        completed.clear();
        running.clear();
        failed.clear();
    }

    public void markStarted(String actionId) {
        running.add(actionId);
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
