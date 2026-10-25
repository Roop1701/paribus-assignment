# Hospital Bulk Processing System

Bulk processing API for the Paribus Python Challenge. Accepts CSV uploads of
hospitals and processes them against the deployed
[Hospital Directory API](https://hospital-directory.onrender.com/docs): each
row is created under one batch ID (inactive), then the whole batch is
activated in a single call once every row succeeded. Processing runs in the
background; progress is available by polling and by SSE streaming, and failed
batches can be resumed.

## Quickstart (Python 3.9+)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pytest                        # test suite (47 tests)

export HOSPITAL_API_BASE_URL=https://hospital-directory.onrender.com
uvicorn app.main:app          # API at http://localhost:8000/docs
```

## Endpoints

Every response uses a `{status, data}` envelope: `status` describes the
state/outcome and comes first, `data` carries the payload. SSE event
payloads use the same envelope, so pollers and streamers parse identically.
Errors (404/409/413/422/500) are enveloped too: `status` is `"error"` and
`data` carries `message` plus, where applicable, a per-row/field `errors`
list (`app/api/error_handlers.py`).

| Method | Path | `status` → `data` |
|---|---|---|
| `POST` | `/hospitals/bulk` | **202** `"processing"` → `{batch_id, total_hospitals}` |
| `GET` | `/hospitals/bulk/{batch_id}` | batch status → full processing results |
| `GET` | `/hospitals/bulk/{batch_id}/stream` | SSE: `snapshot`/`completed`/`paused` events carry batch status → results; `progress` events carry row status → row info |
| `POST` | `/hospitals/bulk/{batch_id}/pause` | **202** `"pausing"` → `{batch_id, total_hospitals}`; cooperative — in-flight rows finish, the rest stay pending |
| `POST` | `/hospitals/bulk/{batch_id}/resume` | **202** `"processing"` → `{batch_id, total_hospitals}`; retries only the failed/pending rows / failed activation |
| `POST` | `/hospitals/bulk/validate` | `"valid"`/`"invalid"` → `{total_hospitals, errors}`; dry run, no hospitals created |
| `GET` | `/health` | `"ok"` → `{version}` |

CSV format: `name,address,phone` — `phone` optional (column or value), max
**20** data rows, UTF-8 (BOM tolerated), quoted fields supported.

```bash
curl -F "file=@hospitals.csv;type=text/csv" http://localhost:8000/hospitals/bulk
curl http://localhost:8000/hospitals/bulk/<batch_id>            # poll
curl -N http://localhost:8000/hospitals/bulk/<batch_id>/stream  # SSE
```

Terminal statuses: `completed` (all rows activated) ·
`completed_with_errors` (some rows failed → not activated, resumable) ·
`activation_failed` (rows created, activate call failed, resumable) ·
`paused` (pause requested; untouched rows stay pending, resumable).

## Design

```
app/
├── api/        hospital_bulk_route.py   routes: validation + response shaping (incl. SSE)
├── core/       config.py                every tunable in one place (env-overridable)
├── dto/        hospital_bulk_dto.py     pydantic request/response models
├── dao/        batch_dao.py             in-memory batch store (async surface)
│               hospital_api_dao.py      upstream HTTP client with retry policy
└── module/     csv_parser.py            CSV validation (collects all errors)
                bulk_engine.py           orchestration: create rows -> activate batch
```

**Background processing (202).** `POST /hospitals/bulk` validates the file
up front, rejects it entirely on any error (422 with per-row errors), then
runs the batch as an asyncio task and returns immediately — the status of a
batch is never ambiguous: the poll endpoint is the single source of truth.

**Rate limiting toward the upstream.** The directory API advertises no rate
limits, so the client side is deliberately polite and defensive:

- a global semaphore caps in-flight upstream calls (default 5, shared across
  all batches, `MAX_CONCURRENT_UPSTREAM_CALLS`);
- transient failures (429, 5xx, network errors) retry up to 3 times with
  exponential backoff + jitter, honouring a `Retry-After` header if present;
- other 4xx responses are permanent row failures — no pointless retries.

**All-or-nothing activation.** Per the assignment workflow, activation runs
only when *every* row was created. A batch with failures stays inactive and
resumable; `resume` re-runs only the failed/pending rows (never re-creating
successful ones, so no duplicates upstream) and then activates.

**Pause/resume.** `pause` is cooperative: rows already sent upstream finish,
rows not yet started stay `pending`, and the batch lands in `paused`.
`resume` picks up exactly where it stopped — each row is created at most
once.

**Progress tracking.** Row updates are published to per-batch subscriber
queues (`ProgressBroker` in the engine); the SSE endpoint replays a snapshot
on connect and then streams events, so pollers and streamers see the same
data.

**In-memory persistence** (acceptable per the assignment) sits behind
`BatchDao`, which keeps the async method surface a database-backed store
would have — swapping in Postgres/Redis later touches one file.

## Configuration (env vars)

| Variable | Default | Meaning |
|---|---|---|
| `HOSPITAL_API_BASE_URL` | **required, no default** | Upstream base URL (e.g. `https://hospital-directory.onrender.com`); startup fails fast when unset |
| `MAX_CONCURRENT_UPSTREAM_CALLS` | `5` | Global in-flight call cap |
| `MAX_RETRIES` | `3` | Retries per upstream call |
| `UPSTREAM_TIMEOUT_SECONDS` | `30` | Per-request timeout (Render cold starts) |

## Testing

`pytest` — 46 tests, ~1s. The upstream is faked with an `httpx.MockTransport`
(`tests/conftest.py`) with failure injection, covering: CSV validation edge
cases; the full happy path; bounded concurrency; 429/`Retry-After` retries;
retry exhaustion; permanent-4xx fast-fail; partial failure without
activation; pause/resume semantics (failed/pending rows only,
activation-only resume, no duplicate creates); conflict/404 handling; and
SSE event ordering.
