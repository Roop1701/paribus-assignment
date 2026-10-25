# Hospital Bulk Processing System — Technical Documentation

Submission for the Paribus Python Challenge (Senior Python Developer
assignment). This document covers architecture, the API contract, design
decisions and their rationale, the batch state machine, testing strategy,
and deployment.

| | |
|---|---|
| **Live base URL** | https://paribus-assignment-xp3l.onrender.com |
| **Swagger UI** | https://paribus-assignment-xp3l.onrender.com/docs |
| **ReDoc** | https://paribus-assignment-xp3l.onrender.com/redoc |
| **Upstream (given)** | https://hospital-directory.onrender.com/docs |
| **Stack** | Python 3.9+, FastAPI, httpx (async), pytest |

---

## 1. Problem & Approach

The given Hospital Directory API manages individual hospital records and
supports batch semantics: hospitals created with a `creation_batch_id` are
inactive until the batch is activated in one call. This service adds bulk
processing on top:

1. Accept and validate a CSV upload (`name,address,phone`, max 20 rows).
2. Generate a batch UUID.
3. `POST /hospitals/` upstream for each row, carrying the batch ID.
4. When **every** row is created, `PATCH /hospitals/batch/{id}/activate`.
5. Expose comprehensive results, live progress, pause/resume.

The core design choice: the bulk endpoint returns **202 immediately** and
processes in the background. Clients follow progress by polling or by SSE,
and the status endpoint is the single source of truth. This keeps the
upload request fast and makes progress tracking, pause, and resume natural
rather than bolted on.

## 2. Architecture

Layered structure — each layer has one responsibility and depends only
downward:

```
app/
├── api/      hospital_bulk_route.py   HTTP layer: input validation, response
│             error_handlers.py        shaping (incl. SSE), error envelope
├── core/     config.py                every tunable in one place (env-overridable)
├── dto/      hospital_bulk_dto.py     pydantic request/response models
├── dao/      batch_dao.py             batch state store (async surface)
│             hospital_api_dao.py      upstream HTTP client + retry policy
└── module/   csv_parser.py            CSV validation (collects all errors)
              bulk_engine.py           orchestration: create rows -> activate
```

- **api** never contains business logic; it validates input and shapes
  responses. Errors are normalized into the response envelope by dedicated
  exception handlers.
- **module/bulk_engine** owns orchestration: concurrency, pause flags, run
  timers, the SSE pub/sub broker, and the batch state machine.
- **dao/batch_dao** is the only code that touches stored batch state. It is
  in-memory (explicitly acceptable per the assignment) but exposes the
  async method surface a database-backed store would have
  (`create / get / update_row / finish / …`), so swapping in Postgres is a
  one-file change — routes and engine would not change.
- **dao/hospital_api_dao** is the only code that talks to the upstream API;
  the retry/backoff policy lives here.

### State split

Persistent-style state (batches, rows, statuses, accumulated processing
time) lives behind the DAO. Runtime-only state (asyncio tasks, pause
flags, run timers, SSE subscriber queues) lives in the engine — it is
meaningless across restarts and deliberately kept out of the store.

## 3. API Reference

Every response — success and error — uses one envelope: `status` first,
payload under `data`.

| Method | Path | `status` → `data` |
|---|---|---|
| `POST` | `/hospitals/bulk` | **202** `"processing"` → `{batch_id, total_hospitals}` |
| `GET` | `/hospitals/bulk/{batch_id}` | batch status → full processing results |
| `GET` | `/hospitals/bulk/{batch_id}/stream` | SSE progress stream (see below) |
| `POST` | `/hospitals/bulk/{batch_id}/pause` | **202** `"pausing"` → `{batch_id, total_hospitals}` |
| `POST` | `/hospitals/bulk/{batch_id}/resume` | **202** `"processing"` → `{batch_id, total_hospitals}` |
| `POST` | `/hospitals/bulk/validate` | `"valid"` / `"invalid"` → `{total_hospitals, errors}` |
| `GET` | `/health` | `"ok"` → `{version}` |

### Bulk upload

```bash
curl -F "file=@hospitals.csv;type=text/csv" \
  https://paribus-assignment-xp3l.onrender.com/hospitals/bulk
```

```json
{"status": "processing", "data": {"batch_id": "3e0fd1aa-…", "total_hospitals": 8}}
```

### Status polling — the comprehensive results

```json
{
  "status": "completed",
  "data": {
    "batch_id": "3e0fd1aa-…",
    "total_hospitals": 8,
    "processed_hospitals": 8,
    "failed_hospitals": 0,
    "processing_time_seconds": 10.33,
    "batch_activated": true,
    "hospitals": [
      {"row": 1, "hospital_id": 311, "name": "General Hospital",
       "status": "created_and_activated", "error": null}
    ]
  }
}
```

### SSE stream

`GET /hospitals/bulk/{batch_id}/stream` (`text/event-stream`). Events:

- `snapshot` — current full status, sent immediately on connect;
- `progress` — one per row as it lands: row status → row info + live counts;
- `completed` / `paused` — terminal full status; the stream then closes.

Event payloads reuse the same envelope as the poll endpoint, so one parser
serves both. A `: keep-alive` comment is emitted every 15s while quiet.

### Errors

```json
{"status": "error", "data": {"message": "CSV validation failed",
  "errors": [{"row": 2, "error": "name is required"}]}}
```

- `404` unknown batch · `409` conflicting action (resume while processing,
  pause when not processing) · `413` oversized file · `422` CSV or request
  validation · `500` catch-all. Framework validation errors are reshaped
  into the same envelope by exception handlers.

## 4. Batch State Machine

```
                 ┌────────────── resume ───────────────┐
                 ▼                                      │
 upload ──► processing ──► completed                    │
                 │    ├──► completed_with_errors ───────┤   (some rows failed;
                 │    ├──► activation_failed ───────────┤    not activated)
                 │    └──► paused ──────────────────────┘
                 └── pause requested ──► (in-flight rows finish first)
```

Row lifecycle: `pending → created → created_and_activated`, or `→ failed`.

- **Activation is all-or-nothing** (per the assignment workflow): it runs
  only when every row was created. A batch with any failed row stays
  inactive and resumable.
- **Resume** resets only `failed` rows to `pending` and relaunches; rows
  already created are never re-sent (no duplicates upstream). If only the
  activation call failed, resume retries just the activation.
- **Pause is cooperative**: rows already sent upstream complete (an HTTP
  call cannot be unsent); rows not yet started stay `pending`. Verified
  live: pausing an 8-row batch mid-flight landed 5 created / 3 pending, and
  resume finished all 8 with exactly 8 upstream creates total.

## 5. Rate Limiting & Resilience Toward the Upstream

The upstream advertises no rate limits (probed: 12 parallel creates all
returned 200, no limiter headers), so the client side is deliberately
polite and defensive:

- **Bounded concurrency** — one global semaphore caps in-flight upstream
  calls (default 5) across *all* batches, not per batch.
- **Retry with exponential backoff + jitter** — transient failures (429,
  5xx, network errors) retry up to 3 times: 0.5s → 1s → 2s (capped 8s),
  plus random jitter so concurrent workers desynchronize.
- **`Retry-After` honored** — when present, it overrides the computed
  backoff (we wait at least that long).
- **No wasted retries** — any other 4xx is a permanent row failure,
  recorded immediately with the upstream's message.
- **Graceful degradation** — when retries exhaust, only that row fails;
  the batch completes as `completed_with_errors` and can be resumed after
  the storm passes.
- Generous per-request timeout (30s) absorbs the upstream's Render
  cold starts.

## 6. CSV Validation

Shared by `POST /hospitals/bulk` (rejects the upload with per-row errors)
and `POST /hospitals/bulk/validate` (dry run). All problems are collected
in one pass rather than failing fast. Rules:

- header must be exactly `name,address[,phone]` — case-insensitive,
  unknown/duplicate columns rejected;
- max **20** data rows (assignment constraint); empty file / header-only
  rejected;
- per row: `name` and `address` required non-blank; `phone` optional
  (blank → null); field-count mismatches and over-length fields reported
  with their 1-based row number;
- UTF-8 (BOM tolerated), quoted fields and embedded commas supported,
  blank lines ignored; 256 KB upload cap.

## 7. Configuration

All tunables live in `app/core/config.py`.

| Variable | Default | Meaning |
|---|---|---|
| `HOSPITAL_API_BASE_URL` | **required** | Upstream base URL; startup fails fast with a clear error when unset — no URL hardcoded in code |
| `MAX_CONCURRENT_UPSTREAM_CALLS` | `5` | Global in-flight call cap |
| `MAX_RETRIES` | `3` | Retries per upstream call |
| `UPSTREAM_TIMEOUT_SECONDS` | `30` | Per-request timeout |

## 8. Testing

`pytest` — **47 tests, ~1s**, fully hermetic: the upstream is faked with an
`httpx.MockTransport` handler (`tests/conftest.py`) supporting failure
injection (always-fail, fail-N-times, configurable status, `Retry-After`,
response delay, concurrency tracking).

Coverage by area:

- **CSV parsing (17)** — header/row/field rules, BOM, CRLF, quoted commas,
  20-row boundary (both sides), encoding failures.
- **Engine (13)** — happy path with upstream truth-check; semaphore bound
  actually observed (max in-flight ≤ 5); 429 + `Retry-After` retry;
  retry exhaustion; permanent-4xx fast-fail; resume retries only failed
  rows; activation-only resume; pause leaves pending rows and resume
  creates each row exactly once; conflict rules; progress event ordering;
  processing-time accumulation.
- **API (17)** — full lifecycle through the HTTP stack; envelope shape and
  key order pinned; validation rejections with per-row errors; 404/409/422
  error envelopes; pause/resume over HTTP; SSE event sequence
  (`snapshot → progress → completed`) and stream closure on finished
  batches; fail-fast startup without the upstream URL.

The deployed instance was additionally verified end-to-end against the
real upstream: bulk create → activation confirmed by querying the Hospital
Directory directly → pause/resume mid-flight → cleanup.

## 9. Deployment

- **Render web service** — build `pip install -r requirements.txt`, start
  `uvicorn app.main:app --host 0.0.0.0 --port $PORT`, env var
  `HOSPITAL_API_BASE_URL` set in the dashboard.
- **Single worker by design**: batch state is in-memory, so one process
  must own it. Scaling out is the documented storage swap (below), not a
  worker-count change.
- Free-tier note: the instance spins down when idle; first request may
  take ~30–60s.

## 10. Known Limitations & Next Steps

- **In-memory state** — lost on restart/redeploy; acceptable per the
  assignment. The DAO already has an async, database-shaped surface, so a
  Postgres swap is contained to `batch_dao.py` (plus a Redis snapshot
  cache if read volume warranted it).
- **Single instance** — SSE pub/sub is in-process; multi-instance would
  move it to Redis pub/sub alongside the storage swap.
- **No auth** — out of scope for the assignment; the natural addition is
  an API key dependency on the router.
- **Schema migrations, structured logging/metrics** — would accompany a
  production storage move.

## 11. Requirement Coverage

| Requirement | Status |
|---|---|
| `POST /hospitals/bulk` multipart CSV upload | ✅ |
| CSV `name,address,phone`, phone optional | ✅ |
| Batch UUID; per-row upstream create with batch ID | ✅ |
| Activate only after all rows created | ✅ |
| Comprehensive processing results | ✅ (status endpoint + SSE terminal event) |
| Max 20 hospitals per CSV | ✅ (enforced + boundary-tested) |
| Deployed publicly | ✅ Render |
| *Bonus:* performance optimization | ✅ async concurrency under a bounded semaphore |
| *Bonus:* progress tracking | ✅ polling + SSE |
| *Bonus:* resume capability | ✅ failed-rows-only, activation-only, and pause resume |
| *Bonus:* CSV validation endpoint | ✅ `/hospitals/bulk/validate` |
| *Bonus:* comprehensive testing | ✅ 47 hermetic tests |
| Beyond spec | pause API, uniform response envelope, graceful rate-limit handling, fail-fast config |
