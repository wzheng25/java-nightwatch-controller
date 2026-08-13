# Java Nightwatch Controller

Gradle + Spring Boot implementation of the Jataware Nightwatch takehome assignment.

This is a small command-line Spring Boot app, not a web server. It starts, creates or reconnects to a Nightwatch session, listens to the SSE stream, reconciles state through HTTP, and starts valid remediation actions until the session finishes.

## Requirements

- Java 21
```bash
export API_TOKEN="your-token"
export SCENARIO="our selected scenario"
```

## Run

From this directory: ~/java-nightwatch-controller
```bash
./gradlew bootRun
```

Or build a jar:
```bash
./gradlew bootJar
java -jar build/libs/java-nightwatch-controller-0.1.0.jar
```

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

## Assumptions and Tradeoffs

**HTTP as source of truth**: SSE is like a wakeup notification only, not an confirmation for state transitions/changes. Actual state transitions of incidents will be determined by a HTTP call, which will add latency in confirmation and also action completion or action starting. 
**SSE + polling redundancy**: The controller uses SSE for low-latency wakeups and polling as a safety net. This improves reliability if events are missed, but creates extra scheduler work and periodic API traffic even when nothing changes.

For this assignment, I prioritized correctness and reliability over speed of events response. There was also more emphasis on getting a working solution out there (MVP) rather than a refined product. Most of the core logic for authentication, reading from SSE and poll loops, scheduling incident work, and posting incident updates are all done in 1 Java class. But a cleaner approach would be to separate some of the responsibilities into different classes.

Quick and dirty design decisions:
**In-memory state only**: Incident state, catalog, and progress tracking live in memory. This keeps the controller simple, but restarting the process loses local scheduling history and relies on the API state to recover.
**No persistence**: if my process crashed, all the current states from my machine would get lost. The controller would need to restart and re-discover incidents from scratch & "running" actions would be stuck until events correct the state.

**No observability**: no metrics, dashboards, alerting. In production, we'd want Prometheus counters on actions submitted, failed, retried, incidents resolved/expired
**No structured logging**: we should add correlation IDs on each incident, each action, each event
**No health endpoint**: nothing to tell orchestrator (Kubernetes, etc) whether service is healthy
**No dead letter handling**: if an incident keeps failing its actions, it just expires. there's no escalation path

**Used JsonNode to capture response fields**: faster to parse inline than to define typed DTOs. A schema change for the responses on incident APIs would fail/break my workflow at runtime.
**Hardcoded concurrency setting**: should be configurable and tunable for different environments/spaces

**No unit test cases and suites**: given more time, we should have unit test cases testing each dependency, serial/parallel rules, etc.
**No WireMock integration tests**: we should have tests that stub the API, simulate catalog outages, 429 errors, partial failures, and verify controller recovery
