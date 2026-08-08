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
        this.globalActions = extractGlobalActions(raw);
        this.playbooks = extractPlaybooks(raw);
    }

    public int playbookCount() {
        return playbooks.size();
    }

    public Optional<Playbook> findPlaybook(JsonNode incident) {
        return JsonSupport.firstText(incident, "type", "incident_type", "kind", "family", "name")
                .flatMap(key -> Optional.ofNullable(playbooks.get(key)));
    }

    public JsonNode raw() {
        return raw;
    }

    private Map<String, Playbook> extractPlaybooks(JsonNode catalog) {
        Map<String, Playbook> results = new HashMap<>();
        JsonNode source = catalog.path("catalog");
        if (source == null) {
            return results;
        }

        if (source.isObject()) {
            source.fields().forEachRemaining(entry -> addPlaybook(results, entry.getKey(), entry.getValue()));
        } else if (source.isArray()) {
            source.forEach(item -> addPlaybook(results, null, item));
        }
        return results;
    }

    private void addPlaybook(Map<String, Playbook> results, String fallbackKey, JsonNode node) {
        if (!node.isObject()) {
            return;
        }
        String incidentType = JsonSupport.firstText(node, "incident_type", "type", "kind", "id")
                .orElse(fallbackKey);
        if (incidentType == null || incidentType.isBlank()) {
            return;
        }

        List<ActionDefinition> actions = JsonSupport.arrayOrObjectValues(
                node, "resolution_actions", "actions", "steps", "playbook")
                .stream()
                .filter(JsonNode::isObject)
                .map(this::normalizeAction)
                .filter(action -> !action.id().isBlank() && !"null".equals(action.id()))
                .toList();
        if (!actions.isEmpty()) {
            results.put(incidentType, new Playbook(incidentType, actions, node));
        }
    }

    private Map<String, ActionDefinition> extractGlobalActions(JsonNode catalog) {
        Map<String, ActionDefinition> results = new HashMap<>();
        JsonNode source = firstPresent(catalog, "actions", "action_definitions");
        if (source == null) {
            return results;
        }
        if (source.isObject()) {
            source.fields().forEachRemaining(entry -> {
                ActionDefinition action = normalizeStandaloneAction(entry.getValue(), entry.getKey());
                if (!action.id().isBlank()) {
                    results.put(action.id(), action);
                }
            });
        } else if (source.isArray()) {
            source.forEach(item -> {
                ActionDefinition action = normalizeStandaloneAction(item, null);
                if (!action.id().isBlank()) {
                    results.put(action.id(), action);
                }
            });
        }
        return results;
    }

    private ActionDefinition normalizeAction(JsonNode node) {
        return normalizeAction(node, null);
    }

    private ActionDefinition normalizeAction(JsonNode node, String fallbackId) {
        ActionDefinition localAction = normalizeStandaloneAction(node, fallbackId);
        ActionDefinition globalAction = globalActions.get(localAction.id());
        if (globalAction == null) {
            return localAction;
        }

        List<String> dependencies = localAction.dependencies().isEmpty()
                ? globalAction.dependencies()
                : localAction.dependencies();
        boolean serial = localAction.serial() || globalAction.serial();
        return new ActionDefinition(localAction.id(), dependencies, serial, node);
    }

    private ActionDefinition normalizeStandaloneAction(JsonNode node, String fallbackId) {
        String id = JsonSupport.firstText(node, "action_id", "id", "name").orElse(fallbackId == null ? "" : fallbackId);
        List<String> dependencies = dependencies(node);
        boolean serial = JsonSupport.firstBoolean(node, "serial", "exclusive", "run_alone")
                || "serial".equalsIgnoreCase(node.path("execution").asText(""));
        return new ActionDefinition(id, dependencies, serial, node);
    }

    private List<String> dependencies(JsonNode node) {
        JsonNode deps = firstPresent(node, "dependencies", "depends_on", "requires");
        if (deps == null) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        if (deps.isTextual()) {
            values.add(deps.asText());
        } else if (deps.isArray()) {
            deps.forEach(dep -> values.add(dep.asText()));
        }
        return values;
    }

    private JsonNode firstPresent(JsonNode node, String... fieldNames) {
        for (String fieldName : fieldNames) {
            JsonNode value = node.path(fieldName);
            if (!value.isMissingNode() && !value.isNull()) {
                return value;
            }
        }
        return null;
    }
}
