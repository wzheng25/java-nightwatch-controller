package com.example.nightwatch.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Catalog {
    private final JsonNode raw;
    private final Map<String, Playbook> playbooks;
    private final Map<String, ActionDefinition> globalActions;

    public Catalog(JsonNode raw) {
        this.raw = raw;
        this.globalActions = parseGlobalActions(raw.path("actions"));
        this.playbooks = parsePlaybooks(raw.path("catalog"));
    }

    public int playbookCount() {
        return playbooks.size();
    }

    public Optional<Playbook> findPlaybook(JsonNode incident) {
        String type = incident.path("type").asText(null);
        return type != null ? Optional.ofNullable(playbooks.get(type)) : Optional.empty();
    }

    public JsonNode raw() {
        return raw;
    }

    private Map<String, ActionDefinition> parseGlobalActions(JsonNode actionsNode) {
        Map<String, ActionDefinition> result = new HashMap<>();
        if (!actionsNode.isObject()) return result;
        actionsNode.fields().forEachRemaining(entry -> {
            boolean serial = "serial".equalsIgnoreCase(entry.getValue().path("execution").asText(""));
            result.put(entry.getKey(), new ActionDefinition(entry.getKey(), List.of(), serial, entry.getValue()));
        });
        return result;
    }

    private Map<String, Playbook> parsePlaybooks(JsonNode catalogNode) {
        Map<String, Playbook> result = new HashMap<>();
        if (!catalogNode.isObject()) return result;
        catalogNode.fields().forEachRemaining(entry ->
            buildPlaybook(entry.getKey(), entry.getValue())
                .ifPresent(p -> result.put(entry.getKey(), p)));
        return result;
    }

    private Optional<Playbook> buildPlaybook(String incidentType, JsonNode node) {
        List<ActionDefinition> actions = new ArrayList<>();
        for (JsonNode actionRef : node.path("resolution_actions")) {
            String id = actionRef.path("action_id").asText(null);
            if (id == null || id.isBlank()) continue;

            List<String> deps = parseDeps(actionRef.path("depends_on"));
            ActionDefinition global = globalActions.get(id);
            boolean serial = global != null && global.serial();
            actions.add(new ActionDefinition(id, deps, serial, actionRef));
        }
        return actions.isEmpty() ? Optional.empty() : Optional.of(new Playbook(incidentType, actions, node));
    }

    private List<String> parseDeps(JsonNode depsNode) {
        if (!depsNode.isArray()) return List.of();
        List<String> result = new ArrayList<>();
        depsNode.forEach(dep -> result.add(dep.asText()));
        return result;
    }
}
