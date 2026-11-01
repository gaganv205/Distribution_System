# Distributed ML Inference & Job Processing System

An asynchronous ML inference service. Clients submit jobs over REST, get a UUID back
immediately, and poll for the result. A pool of independent workers, which can run on
any number of machines, pull jobs from a shared Redis priority queue and run the model.

```
            ┌──────────────┐  POST /jobs (202 + job_id)
 Client ───▶│  FastAPI API │  GET  /jobs/{id}  (poll)
            └──────┬───────┘  DELETE /jobs/{id} (cancel)
                   │ writes job + enqueues / reads status
            ┌──────▼───────────────────────────────┐
            │ Redis                                 │
            │  job:{id}         HASH  job record    │
            │  jobs:pending     ZSET  priority queue│
            │  jobs:processing  ZSET  leases        │
            └──────▲───────────▲───────────▲────────┘
                   │ claim / complete (atomic Lua)
              ┌────┴───┐  ┌────┴───┐  ┌────┴───┐
              │worker 1│  │worker 2│  │worker N│   each loads the model
              └────────┘  └────────┘  └────────┘
```

## Run it

Commands are shown without inline `#` comments because macOS zsh treats them as arguments.

**Docker (API + Redis + 3 workers).** Docker Desktop must be open.
```bash
docker compose up --build -d
```
Then open **http://localhost:8000** for the live dashboard, or **http://localhost:8000/docs** for the API.

Add or remove workers with no code change:
```bash
docker compose up --scale worker=6 -d
```
```bash
docker compose stop worker
```
```bash
docker compose start worker
```
Stop everything:
```bash
docker compose down
```

**Locally (macOS/Linux):**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
python scripts/train_model.py
```
Start Redis (`docker run -d --name redis -p 6379:6379 redis:7-alpine` or `brew install redis && redis-server`), then in separate terminals:
```bash
uvicorn app.api:app --port 8000
```
```bash
INFERENCE_DELAY_SECONDS=1 python -m app.worker
```
Each extra terminal running the worker command is one more worker.

**Tests** (no Redis needed; fakeredis runs the real Lua scripts):
```bash
pytest -q
```

## Live dashboard

`http://localhost:8000/dashboard` (also the root URL) polls `GET /dashboard/data` twice a second and shows:

- **Queue:** pending jobs in dispatch order (#1 runs next), priority, time waiting, retries, with a cancel button.
- **Workers:** one card per live worker process, idle or busy, the job it is running, and jobs done.
- **Recently finished:** the last 20 completed, failed or cancelled jobs with the prediction or error.
- **Buttons** to submit high, normal or low jobs, or a burst of 20.

Workers appear via heartbeats: each writes `worker:{id}` to Redis every second with a 10s expiry. A stopped worker deletes its key; a crashed one disappears when the key expires.

## API

| Method | Path | Purpose | Codes |
|---|---|---|---|
| POST | `/jobs` | Submit job `{"input":{"features":[...4 floats]}, "priority":"high\|normal\|low"}` | 202, 422 |
| GET | `/jobs/{id}` | Status and result | 200, 404, 422 |
| DELETE | `/jobs/{id}` | Cancel (PENDING only) | 200, 404, 409 |
| GET | `/stats` | Queue depth: pending, running | 200 |
| GET | `/health` | Checks Redis is reachable | 200, 503 |
| GET | `/dashboard` | Live HTML dashboard | 200 |
| GET | `/dashboard/data` | JSON snapshot: queue, running, workers, recent, counts | 200, 503 |

Job lifecycle: `PENDING → RUNNING → COMPLETED | FAILED`, or `PENDING → CANCELLED`.
A job whose worker dies goes `RUNNING → PENDING` again (up to `JOB_MAX_ATTEMPTS`).

## Configuration (env vars)

| Variable | Default | Meaning |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | Shared state |
| `MODEL_PATH` | `model/model.joblib` | Model artifact |
| `INFERENCE_DELAY_SECONDS` | `0` | Simulated model latency |
| `PRIORITY_AGING_MS` | `30000` | Head start per priority level (anti-starvation) |
| `JOB_LEASE_SECONDS` | `60` | Time a worker may hold a job before it's presumed dead |
| `JOB_MAX_ATTEMPTS` | `3` | Retries after worker loss |
| `RESULT_TTL_SECONDS` | `86400` | Finished jobs expire after this |

## Project layout

```
app/
  api.py      HTTP layer: validation, routing. Never runs the model.
  store.py    Job manager: Redis data model, atomic Lua scripts, priority queue, leases
  worker.py   Inference worker: claim → predict → complete, plus the crash reaper
  model.py    The ML model (scikit-learn, Iris). Swap for any model.
  schemas.py  Pydantic models, priorities, job statuses
  static/dashboard.html  Live dashboard (plain HTML/JS, no build step)
  config.py   Env-var settings
scripts/      train_model.py, demo.py
tests/        API, queue semantics (priority, aging, crash recovery, zombie workers), dashboard
```

## Known limits / next steps

- Clients and the dashboard poll. Webhooks or Server-Sent Events would remove polling.
- Workers poll Redis every 200 ms when idle. Fine at this scale; BZPOPMIN or pub/sub would cut idle traffic.
- Lease must be longer than the slowest inference. Long jobs need a heartbeat that extends the lease.
- Redis is a single point of failure. Production would use Redis Sentinel/replication or a managed service (ElastiCache).
- Lua scripts touch `job:*` keys not passed in `KEYS`, so this targets single-node Redis, not Redis Cluster.
- No auth or rate limiting on the API.
