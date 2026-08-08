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
        if (apiUrl == null || apiUrl.isBlank()) {
            apiUrl = "https://nightwatch.jata.lol";
        }
        if (scenario == null || scenario.isBlank()) {
            scenario = "practice-starter";
        }
        if (sessionMode == null || sessionMode.isBlank()) {
            sessionMode = scenario.startsWith("practice-") ? "practice" : "challenge";
        }
        if (pollInterval == null) {
            pollInterval = Duration.ofSeconds(2);
        }
        if (incidentRefetchInterval == null) {
            incidentRefetchInterval = Duration.ofMillis(5100);
        }
        if (requestTimeout == null) {
            requestTimeout = Duration.ofSeconds(20);
        }
    }
}
