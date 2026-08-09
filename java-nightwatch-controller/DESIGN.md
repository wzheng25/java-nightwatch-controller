# Design Notes and Evolution

This document covers the key design decisions in the controller, the tradeoffs each one involves, and the problems that came up during testing and how they were resolved.

---

## Core Design: HTTP as the Source of Truth

The Nightwatch API provides two channels for observability: an SSE stream (`GET /sessions/{id}/stream`) and a set of HTTP endpoints for sessions, incidents, and events.

The controller treats **SSE as a wakeup signal only**. When an SSE event arrives, the scheduler runs a reconcile pass that fetches fresh state from HTTP. The event payload is not used to update action or incident state.

**Why:** SSE delivery is best-effort. Events can arrive out of order, be duplicated, or be missed if the stream reconnects. Using them as state updates would require implementing idempotency logic, ordering guarantees, and gap detection — effectively re-implementing the HTTP endpoints. The HTTP endpoints already give a consistent, authoritative view.

**Tradeoff:** Every state transition requires at least one HTTP round-trip after the SSE event arrives. Under normal latency this is 200–800ms. If SSE events fire faster than HTTP calls complete, some events are processed as pure wakeups with no effect (the next poll or SSE event triggers the useful pass). This is acceptable.

---

## SSE Data as a Scheduling Hint

The strict "SSE is wakeup only" principle was relaxed in one narrow way: if an SSE event's data payload contains an `incident_id` field, the controller resets `lastFetchAt` for that incident to `Instant.EPOCH` via `forceRefreshDue()`.

This does not update any state. It only affects *when* the next HTTP fetch for that incident is due. Without it, the typical flow after an action completes is:

1. SSE `action_completed` fires → wakeup emitted
2. Scheduler runs. `isRefreshDue` is false (last fetch was ~1-2s ago, interval is 5.1s)
3. `scheduleIncident` runs against stale state that still shows the action as running
4. Nothing starts. Wait up to 5.1s for `isRefreshDue` to become true
5. Full refresh → action seen as completed → next action starts

With `forceRefreshDue`, the gap collapses from up to 5.1s to under 1s. Observed in gauntlet logs: action-to-action transitions went from ~5s to same-second.

**Tradeoff:** If the SSE event data does not contain `incident_id`, this is a no-op. The behavior degrades gracefully to the old 5.1s window.

---

## Optimistic Action Tracking

When the controller decides to start an action, it calls `markStarted(actionId)` — adding the action to the local `running` set — before the HTTP POST completes.

**Why:** The scheduler can run multiple passes in quick succession (from SSE bursts). Without optimistic tracking, the first pass would submit the action, and before the API confirms it, a second pass would see no running actions and submit the same action again. The API would return a 400 on the duplicate. With `markStarted`, the second pass sees the action already in `running` and skips it.

**Tradeoff:** If the POST fails (network error, 5xx), the action stays in `running` indefinitely until the next `mergeEvents` call processes the full event history. During that window — up to `incidentRefetchInterval` (5.1s) — the scheduler thinks the action is running and won't start anything else for that incident. In practice this is a short stall, not a stuck incident.

---

## mergeEvents as the Sole Authority on Action State

`IncidentState` maintains three sets: `completed`, `running`, and `failed`. These are updated only by `mergeEvents()`, which processes the full event history returned by `GET /incidents/{id}/events`.

An early version of `mergeSnapshot` cleared these sets before each full refresh. This caused a race condition:

1. `mergeSnapshot` called → clears `running = {}`
2. Before `mergeEvents` is called, the scheduler runs (on another wakeup)
3. Scheduler sees action not in `running` or `completed` → submits it again → 400

The fix: `mergeSnapshot` does not touch action sets. `mergeEvents` is called immediately after and is the only thing that transitions action state. Since the events endpoint returns the full history on every call, `mergeEvents` is idempotent and does not need to be applied incrementally.

---

## mergeListSnapshot vs mergeSnapshot

The incident list (`GET /sessions/{id}/incidents`) is polled on a separate timer (`lastIncidentListFetch`). It returns a lightweight snapshot of each incident — enough to extract the incident ID and update the local type/status view.

An early version called `mergeSnapshot` from the list refresh. `mergeSnapshot` also advances `lastFetchAt`, which gates when the next full per-incident refresh (`getIncident` + `getIncidentEvents`) runs. This caused a critical regression:

1. List refresh runs → `mergeSnapshot` called → `lastFetchAt` updated to now
2. Action completes remotely
3. `isRefreshDue` returns false for the next 5.1s (timer was just reset by the list refresh)
4. `scheduleIncident` runs against stale state → action still appears in `running`
5. Incident stays blocked for up to 5.1s. In one test run, both incidents in a session expired before the next action could start.

The fix: split into two methods:
- `mergeListSnapshot` — updates the snapshot, does **not** touch `lastFetchAt`
- `mergeSnapshot` — updates the snapshot **and** advances `lastFetchAt`

The list refresh calls `mergeListSnapshot`. Only the full per-incident refresh calls `mergeSnapshot`.

---

## Catalog Caching and Graceful Degradation

The catalog (`GET /sessions/{id}/catalog`) is fetched once at startup and cached in a `volatile Catalog` field. It is refreshed in the background when `catalog_updated` is received via SSE.

**On startup failure:** The catalog frequently returns 404 for the first several seconds after a session becomes active — the scenario has not yet populated it. The controller retries in a loop (with a session-status check to detect a dead session) until the catalog is available. Startup blocks until the first successful load.

**On runtime failure:** If a catalog refresh fails mid-run (transient error), the cached catalog is kept and scheduling continues. This allows incidents to be handled during short catalog outages. A warning is logged.

**Catalog updates and scheduling lag:** When `catalog_updated` fires, the controller starts the HTTP refresh asynchronously and immediately emits a wakeup to the scheduler. If the scheduler runs that pass before the catalog finishes loading, it uses the old catalog. An incident whose playbook just appeared in the new catalog would be skipped until the next wakeup (up to `POLL_INTERVAL` away). To close this gap, the controller emits a **second** wakeup (`catalog_refreshed`) via `doOnTerminate` after the refresh completes. This ensures the next scheduling pass always uses the updated catalog.

---

## Rate Limiting and the incidentRefetchInterval

The API rate-limits each endpoint. To stay within limits, per-incident full refreshes (`getIncident` + `getIncidentEvents`) are throttled to at most once per `INCIDENT_REFETCH_INTERVAL` (default 5100ms — 100ms above the 5s window to give a small buffer).

The timer is per-incident (`lastFetchAt` in `IncidentState`). The incident list refresh has its own timer (`lastIncidentListFetch`). These are independent, so a list poll does not consume the per-incident refresh budget.

`forceRefreshDue()` bypasses this timer by resetting `lastFetchAt` to `Instant.EPOCH`. It is called from the SSE event handler when the event data contains an `incident_id`. This trades a potential extra HTTP call for lower action-to-action latency. The API still has its rate limit enforced by `transientRetry` (429s are retried with backoff), so a burst from many simultaneous resets degrades gracefully rather than failing.

---

## Parallel Incident Refresh (flatMap vs concatMap)

Within a reconcile pass, incidents are processed with `flatMap(concurrency=3)` rather than `concatMap` (sequential).

**Why:** In high-density scenarios (nightmare, gauntlet), many incidents can be due for a full refresh simultaneously. Each refresh requires two sequential HTTP calls. With `concatMap` and 10 incidents all due at once, a single pass serializes ~20 HTTP calls. At 500ms–1s per call this adds up to 10–20s of serial processing, during which new incidents that just appeared cannot be scheduled. Observed: a 21s discovery lag for a batch of new incidents in nightmare mode.

With `flatMap(3)`, up to 3 incidents have HTTP calls in flight simultaneously. A pass with 10 incidents takes roughly 10/3 × 1s ≈ 3–4s instead of ~10s.

**Tradeoff:** Concurrent HTTP calls increase instantaneous load on the API. Concurrency 3 was chosen (rather than higher) to reduce the chance of hitting rate limits when many incidents arrive simultaneously. In one gauntlet run, concurrency 4 triggered a "Retries exhausted: 3/3" error when 4 incidents appeared at once, adding a ~14s delay. Concurrency 3 reduces that risk.

Each `IncidentState` is independent — there is no shared mutable state between different incidents' refresh chains — so parallelism does not introduce correctness issues.

---

## Retry Behavior

All HTTP calls use `Retry.backoff(3, Duration.ofMillis(500))` filtered to transient status codes (429, 500, 502, 503, 504). Non-transient errors (400, 404, 409, etc.) are not retried.

- **429 rate limit:** Retried with exponential backoff. Up to 3.5s of backoff before the attempt is abandoned.
- **400 on action start:** Treated as a non-transient rejection. The error handler re-fetches both the catalog and the full incident state (snapshot + events) so the scheduler corrects its local view before the next pass.
- **409 on session-finished API calls:** Expected at end-of-run when the session is closed. Logged as a warning; the `finished` flag suppresses further scheduling.
- **Scheduler pass failures:** If a full reconcile pass fails (e.g., all retries on a list fetch exhausted), the error is logged and the pass is dropped. The next wakeup triggers a fresh pass. No incident is permanently blocked by a single pass failure.

---

## Connection and Request Timeouts

`NightwatchClient` applies both a TCP connect timeout and an HTTP response timeout via Reactor Netty's `HttpClient`, configured from `REQUEST_TIMEOUT` (default 20s). An earlier version used `WebClient` defaults with no timeout, which could cause the application to hang indefinitely on a slow or dropped connection.

---

## SSE Reconnection

The SSE stream is wrapped in `Flux.defer(...).repeat(() -> !finished.get())`. If the stream closes or errors, the controller logs a warning, waits 1 second, and reconnects. The reconnect delay prevents a tight retry loop if the server is rejecting connections.

SSE is treated as optional: if the stream is unavailable, the poll loop continues to emit wakeups every `POLL_INTERVAL`. All correctness guarantees come from the HTTP state; SSE only reduces latency.

---

## What Remains at the Performance Ceiling

After all the above changes, the remaining gap between observed scores and the estimated best is driven by factors outside the controller's control:

- **Scenario startup delay:** The catalog returns 404 for ~10s after the session becomes active.
- **Playbook availability lag:** Some incident types appear in the incident list before their playbook is added to the catalog (via `catalog_updated`). The controller has no playbook for them until the catalog refreshes; it logs "No playbook found" and skips them until the next pass.
- **Transient API failures:** Occasional 5xx errors during high-load bursts exhaust the retry budget and drop a pass, adding ~4–15s of delay for the affected incidents.
- **Scenario timing:** In nightmare and gauntlet modes, some incidents are fired late enough in the session that even perfect scheduling cannot resolve them before the session ends.
