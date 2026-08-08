package com.example.nightwatch.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IncidentState {
    private static final Set<String> TERMINAL_STATUSES = Set.of("resolved", "expired", "finished", "failed", "closed");
    private static final Set<String> RUNNING_STATUSES = Set.of("running", "started", "in_progress", "pending");
    private static final Set<String> SUCCESS_STATUSES = Set.of("completed", "succeeded", "success", "done");
    private static final Set<String> FAILED_STATUSES = Set.of("failed", "error");

    private final String incidentId;
    private JsonNode snapshot;
    private final Set<String> completed = new HashSet<>();
    private final Set<String> running = new HashSet<>();
    private final Set<String> failed = new HashSet<>();
    private final Map<String, Integer> attempts = new HashMap<>();
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
        String status = JsonSupport.firstText(snapshot, "status", "state").orElse("").toLowerCase();
        return TERMINAL_STATUSES.contains(status)
                || !snapshot.path("resolved_at").isMissingNode()
                || !snapshot.path("expired_at").isMissingNode();
    }

    public void mergeSnapshot(JsonNode snapshot) {
        this.snapshot = snapshot;
        this.lastFetchAt = Instant.now();
        completed.clear();
        running.clear();
        failed.clear();

        for (JsonNode action : JsonSupport.arrayOrObjectValues(snapshot, "actions", "action_runs", "runs", "steps")) {
            JsonSupport.firstText(action, "action_id", "id", "name").ifPresent(actionId -> {
                String status = JsonSupport.firstText(action, "status", "state").orElse("").toLowerCase();
                if (SUCCESS_STATUSES.contains(status) || !action.path("completed_at").isMissingNode()) {
                    completed.add(actionId);
                } else if (RUNNING_STATUSES.contains(status)) {
                    running.add(actionId);
                } else if (FAILED_STATUSES.contains(status)) {
                    failed.add(actionId);
                }
            });
        }
    }

    public void markStarted(String actionId) {
        running.add(actionId);
        attempts.merge(actionId, 1, Integer::sum);
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
