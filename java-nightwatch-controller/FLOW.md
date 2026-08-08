# Application Flow

## Startup

1. `GET /auth/verify` — confirms the token is valid, logs the authenticated subject
2. `POST /sessions` — creates a new session (skipped if `SESSION_ID` env var is already set)
3. `POST /sessions/{id}/start` — starts the session if its status is startable (e.g. `pending`)
4. Polls `GET /sessions/{id}` in a loop until status becomes `active`
5. `GET /sessions/{id}/catalog` — loads the playbook catalog into memory

---

## Main Loop

Once the session is active, three streams run concurrently via `Flux.merge(...)`:

### sseLoop

Connects to `GET /sessions/{id}/stream` and listens for SSE events. On each event:

- Emits a wakeup to trigger the scheduler
- If `catalog_updated`: fetches the catalog in the background
- If `session_finished`: sets the finished flag and stops

If the stream drops, waits 1 second and reconnects. Repeats until finished.

### pollLoop

Every `POLL_INTERVAL` (default 2s), emits a `poll` wakeup. This is the fallback for cases where SSE is slow or misses events.

### schedulerLoop

Listens on the shared wakeup sink (fed by both SSE events and poll ticks). On each wakeup, runs one reconcile pass:

```
reconcileAndSchedule()
  ├── refreshIncidentListIfDue()     GET /incidents            (at most once per 5.1s)
  ├── for each known incident:
  │     refreshAndScheduleIncident()
  │       ├── if refresh is due:
  │       │     GET /incidents/{id}          → mergeSnapshot()
  │       │     GET /incidents/{id}/events   → mergeEvents()
  │       └── scheduleIncident()
  └── checkSessionIfDue()            GET /sessions/{id}        (at most once per 5.1s)
```

---

## scheduleIncident

For each non-terminal incident, looks up the incident type in the catalog to find its playbook, then walks the action list applying these rules:

1. Skip actions already **completed** or **running**
2. Skip actions whose **dependencies** are not all in `completed`
3. If any **serial** action is currently running, nothing else can start
4. At most **2 parallel** actions may run per incident at once

Actions that pass all checks are submitted via `POST /incidents/{id}/action`. Each action is optimistically added to the local `running` set before posting, so concurrent scheduler passes don't double-submit it.

If a submission is **rejected** (4xx), the catalog and incident state are re-fetched to correct local state before the next pass.

---

## Termination

When the session status becomes `finished` or `stopped`, the finished flag is set. All three loops detect it and stop. The scheduler then fetches and logs the end-of-run summary from `GET /sessions/{id}/summary`.
