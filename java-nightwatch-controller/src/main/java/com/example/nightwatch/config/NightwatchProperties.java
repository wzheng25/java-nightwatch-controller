package com.example.nightwatch.config;

import jakarta.validation.constraints.NotBlank;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "nightwatch")
public record NightwatchProperties(
        @NotBlank String apiUrl,
        @NotBlank String apiToken,
        String sessionMode,
        String scenario,
        String sessionId,
        Duration pollInterval,
        Duration incidentRefetchInterval,
        Duration requestTimeout) {

    public NightwatchProperties {
        if (sessionMode == null || sessionMode.isBlank()) {
            sessionMode = scenario.startsWith("practice-") ? "practice" : "challenge";
        }
    }
}
