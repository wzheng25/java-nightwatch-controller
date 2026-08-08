package com.example.nightwatch.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public record Playbook(String incidentType, List<ActionDefinition> actions, JsonNode raw) {}
