# Java Nightwatch Controller

Gradle + Spring Boot implementation of the Jataware Nightwatch takehome assignment.

This is a small command-line Spring Boot app, not a web server. It starts, creates or reconnects to a Nightwatch session, listens to the SSE stream, reconciles state through HTTP, and starts valid remediation actions until the session finishes.

## Design

The app follows the assignment's key rule: HTTP is the source of truth. SSE events are only wake-up signals.

Main classes:

- `NightwatchApplication` starts the Spring Boot command-line app.
- `NightwatchProperties` maps environment variables into config.
- `NightwatchClient` wraps all HTTP and SSE API calls with `WebClient`.
- `NightwatchControllerService` owns the run loop, polling fallback, reconciliation, and scheduling.
- `Catalog`, `Playbook`, and `ActionDefinition` normalize catalog JSON.
- `IncidentState` tracks completed/running/failed actions per incident.

The scheduler enforces these local rules before posting an action:

- dependencies must be completed
- serial actions run alone
- max two parallel actions per incident
- terminal incidents receive no more work
- API rejections trigger fresh catalog and incident fetches instead of restarting
- catalog outages keep using the last known good catalog

## Requirements

- Java 21
- Gradle installed locally, or add a Gradle wrapper before submitting

## Configuration

Required:

```bash
export API_TOKEN="your-token"
```

Optional:

```bash
export API_URL="https://nightwatch.jata.lol"
export SESSION_MODE="practice"
export SCENARIO="practice-starter"
export SESSION_ID="existing-session-id"
export POLL_INTERVAL="2s"
export INCIDENT_REFETCH_INTERVAL="5100ms"
export REQUEST_TIMEOUT="20s"
```

## Run

From this directory:

```bash
./gradlew bootRun
```

Or build a jar:

```bash
./gradlew bootJar
java -jar build/libs/java-nightwatch-controller-0.1.0.jar
```

## Suggested Practice Flow

1. Start with `SCENARIO=practice-starter` to confirm auth, session setup, catalog parsing, and action posting.
2. Move to `practice-medium` to validate delayed visibility and retryable failures.
3. Move to `practice-hard` to validate overlapping incidents and catalog updates.
4. Use final/gauntlet only after the service can run unattended.

## Assumptions and Tradeoffs

**SSE as wakeup only.** The controller treats SSE events purely as wakeup signals, not as state updates. All incident and action state is read from HTTP (`GET /incidents/{id}` and `GET /incidents/{id}/events`). This matches the API spec ("HTTP is the source of truth").

**Optimistic action tracking.** When an action is submitted, it is immediately marked as running locally. This prevents the scheduler from re-submitting it before the API reflects the change. If the submission is rejected, the incident is re-fetched and the local state is corrected.

**Catalog caching.** If the catalog endpoint is unavailable, the last successfully fetched catalog is reused. This allows incident scheduling to continue during short catalog outages.

**Per-incident refresh throttle.** Each incident's state is re-fetched at most once per `INCIDENT_REFETCH_INTERVAL` (default 5100ms), respecting the API's 5-second-per-endpoint rate limit. On tiers with many concurrent incidents all due for refresh simultaneously, multiple per-incident calls can fire back-to-back within a single scheduler pass. Adding a global per-endpoint call counter was skipped as unnecessary complexity for the expected incident density.

**Catalog schema.** The catalog response uses exactly the fields documented in the OpenAPI spec: `actions` (map of action definitions with `execution`/`duration_sec`), `catalog` (map of incident types with `resolution_actions` containing `action_id` and `depends_on`). No field aliases or fallbacks are used.
