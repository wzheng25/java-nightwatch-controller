# Application Flow

## Startup

1. `GET /auth/verify` — confirms the token is valid, logs the authenticated subject
2. `POST /sessions` — creates a new session (skipped if `SESSION_ID` env var is already set)
3. `POST /sessions/{id}/start` — starts the session if its status is startable (e.g. `pending`)
4. Polls `GET /sessions/{id}` in a loop until status becomes `active`
5. `GET /sessions/{id}/catalog` — loads the playbook catalog into memory; retries every ~2s if the catalog returns 404 (common at startup before the scenario is ready)

---

## Main Loop

Once the session is active, three streams run concurrently via `Flux.merge(...)`:

### sseLoop

Connects to `GET /sessions/{id}/stream` and listens for SSE events. On each event:

- Emits a wakeup to trigger the scheduler
- If `catalog_updated`: fetches the catalog in the background, then emits a **second** wakeup after the refresh completes so the next scheduler pass is guaranteed to use the updated catalog
- If `incident_started`: resets `lastIncidentListFetch` to `Instant.EPOCH` so the incident list is fetched immediately on the next scheduler pass
- If the event data carries an `incident_id`: calls `forceRefreshDue()` on that incident so `isRefreshDue` returns true immediately, closing the gap between action completion and the next action starting
- If `session_finished`: sets the finished flag and stops

If the stream drops, waits 1 second and reconnects. Repeats until finished.

### pollLoop

Every `POLL_INTERVAL` (default 2s), emits a `poll` wakeup. This is the fallback for cases where SSE is slow or misses events.

### schedulerLoop

Listens on the shared wakeup sink (fed by both SSE events and poll ticks). On each wakeup, runs one reconcile pass. Passes are **serialized** via `concatMap` — only one runs at a time:

```
reconcileAndSchedule()
  ├── refreshIncidentListIfDue()     GET /incidents            (rate-limited; bypassed when lastIncidentListFetch=EPOCH)
  ├── for each known incident (up to 3 in parallel via flatMap):
  │     refreshAndScheduleIncident()
  │       ├── if refresh is due (isRefreshDue or forceRefreshDue was called):
  │       │     GET /incidents/{id}          → mergeSnapshot()
  │       │     GET /incidents/{id}/events   → mergeEvents()
  │       └── scheduleIncident()
  └── checkSessionIfDue()            GET /sessions/{id}        (at most once per POLL_INTERVAL)
```

Incidents are refreshed with `flatMap(concurrency=3)` so up to three incidents can have their HTTP calls in flight simultaneously. This matters during high-density scenarios where many incidents all become due for refresh at the same time.

---

## scheduleIncident

For each non-terminal incident, looks up the incident type in the catalog to find its playbook, then walks the action list applying these rules:

1. Skip actions already **completed** or **running**
2. Skip actions whose **dependencies** are not all in `completed`
3. If any **serial** action is currently running, nothing else can start
4. At most **2 parallel** actions may run per incident at once

Actions that pass all checks are submitted via `POST /incidents/{id}/action`. Each action is **optimistically** added to the local `running` set before posting, so concurrent scheduler passes don't double-submit it.

If a submission is **rejected** (4xx), the catalog and full incident state (snapshot + events) are re-fetched to correct local state before the next pass.

---

## Incident State

Each incident has an `IncidentState` object tracking:

- `snapshot` — the most recent `GET /incidents/{id}` JSON, used for type/status lookups
- `completed`, `running`, `failed` — action sets, updated exclusively by `mergeEvents()`
- `lastFetchAt` — timestamp of the last full per-incident refresh (controls `isRefreshDue`)

Two distinct snapshot update paths exist:

- **`mergeListSnapshot`** — called from the incident list refresh. Updates the snapshot but does **not** advance `lastFetchAt`. This prevents the list poll from resetting the per-incident refresh timer.
- **`mergeSnapshot`** — called from the full per-incident refresh. Updates the snapshot **and** advances `lastFetchAt` to gate the next full refresh cycle.

`mergeEvents()` is the **sole authority** on action state transitions. Action sets are never cleared by `mergeSnapshot` — doing so would open a window between the snapshot fetch and the events fetch where all actions appear unstarted, causing duplicate submissions.

---

## Termination

When the session status becomes `finished` or `stopped`, the finished flag is set. All three loops detect it and stop. The scheduler then fetches and logs the end-of-run summary from `GET /sessions/{id}/summary`.
