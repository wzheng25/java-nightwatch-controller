package com.example.nightwatch.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;

public record ActionDefinition(String id, List<String> dependencies, boolean serial, JsonNode raw) {}
