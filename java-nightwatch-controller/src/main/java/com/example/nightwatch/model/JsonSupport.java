package com.example.nightwatch.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public final class JsonSupport {
    private JsonSupport() {}

    public static Optional<String> firstText(JsonNode node, String... fieldNames) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return Optional.empty();
        }
        for (String fieldName : fieldNames) {
            JsonNode value = node.path(fieldName);
            if (!value.isMissingNode() && !value.isNull() && !value.asText().isBlank()) {
                return Optional.of(value.asText());
            }
        }
        return Optional.empty();
    }

    public static List<JsonNode> arrayOrObjectValues(JsonNode node, String... fieldNames) {
        for (String fieldName : fieldNames) {
            JsonNode value = node.path(fieldName);
            if (value.isArray()) {
                List<JsonNode> items = new ArrayList<>();
                value.forEach(items::add);
                return items;
            }
            if (value.isObject()) {
                List<JsonNode> items = new ArrayList<>();
                Iterator<JsonNode> iterator = value.elements();
                while (iterator.hasNext()) {
                    items.add(iterator.next());
                }
                return items;
            }
        }
        if (node.isArray()) {
            List<JsonNode> items = new ArrayList<>();
            node.forEach(items::add);
            return items;
        }
        return List.of();
    }

    public static JsonNode objectAt(JsonNode node, String fieldName) {
        JsonNode value = node.path(fieldName);
        return value.isObject() ? value : node;
    }

}
