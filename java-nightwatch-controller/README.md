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
gradle bootRun
```

Or build a jar:

```bash
gradle bootJar
java -jar build/libs/java-nightwatch-controller-0.1.0.jar
```

## Suggested Practice Flow

1. Start with `SCENARIO=practice-starter` to confirm auth, session setup, catalog parsing, and action posting.
2. Move to `practice-medium` to validate delayed visibility and retryable failures.
3. Move to `practice-hard` to validate overlapping incidents and catalog updates.
4. Use final/gauntlet only after the service can run unattended.

## Assumptions and Tradeoffs

The controller uses the OpenAPI v2 setup request fields `session_mode` and `scenario_type`, unwraps incident detail responses from the `incident` property, and submits actions with `action_id` plus `notes`.

Catalog details were omitted from the pasted Swagger export, so the normalization layer still accepts common field aliases like `id`, `session_id`, `incident_id`, `type`, `incident_type`, `actions`, `steps`, `dependencies`, and `depends_on`.

After the first live practice run, tighten `Catalog`, `IncidentState`, and request bodies if the real Swagger schema uses different exact names.
