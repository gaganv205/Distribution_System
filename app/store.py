"""
JobStore: the job manager. All shared state lives in Redis so any number of
API and worker processes (on any number of machines) can cooperate.

Redis layout
  job:{id}            HASH  - the job record (status, input, result, ...)
  jobs:pending        ZSET  - queue; member=job id, score=dispatch order (lowest first)
  jobs:processing     ZSET  - claimed jobs; score=lease deadline in epoch ms
  jobs:recent         LIST  - ids of the last 50 finished jobs (for the dashboard)
  jobs:counters       HASH  - totals: completed / failed / cancelled
  worker:{id}         HASH  - worker heartbeat; expires if the worker stops reporting

Multi-step state changes run as Lua scripts, which Redis executes atomically.
That is what stops two workers from claiming the same job.
"""
import json
import time
import uuid

import redis

from .schemas import PRIORITY_WEIGHT, JobOut, JobStatus, Priority

PENDING = "jobs:pending"
PROCESSING = "jobs:processing"
RECENT = "jobs:recent"
COUNTERS = "jobs:counters"
RECENT_MAX = 50


def job_key(job_id: str) -> str:
    return f"job:{job_id}"


def worker_key(worker_id: str) -> str:
    return f"worker:{worker_id}"


# Pop the highest-priority job and record it as leased to this worker, in one atomic step.
CLAIM_LUA = """
local popped = redis.call('ZPOPMIN', KEYS[1])
if #popped == 0 then return false end
local id = popped[1]
redis.call('ZADD', KEYS[2], tonumber(ARGV[1]) + tonumber(ARGV[2]), id)
local key = 'job:' .. id
redis.call('HSET', key, 'status', 'RUNNING', 'worker_id', ARGV[3], 'started_at', ARGV[4])
redis.call('HINCRBY', key, 'attempts', 1)
return id
"""

# Finish a job, but only if this worker still owns the lease.
# Stops a slow "zombie" worker from overwriting a job that was already re-assigned.
FINISH_LUA = """
local key = 'job:' .. ARGV[1]
if redis.call('HGET', key, 'worker_id') ~= ARGV[2] then return 0 end
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then return 0 end
redis.call('HSET', key, 'status', ARGV[3], ARGV[4], ARGV[5], 'finished_at', ARGV[6])
redis.call('EXPIRE', key, tonumber(ARGV[7]))
redis.call('HINCRBY', KEYS[3], string.lower(ARGV[3]), 1)
redis.call('LPUSH', KEYS[2], ARGV[1])
redis.call('LTRIM', KEYS[2], 0, tonumber(ARGV[8]) - 1)
return 1
"""

# Find jobs whose lease expired (worker crashed or hung): requeue them, or fail them
# if they've used up their attempts.
REAP_LUA = """
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local requeued, failed = 0, 0
for _, id in ipairs(expired) do
  redis.call('ZREM', KEYS[1], id)
  local key = 'job:' .. id
  local attempts = tonumber(redis.call('HGET', key, 'attempts') or '0')
  if attempts >= tonumber(ARGV[2]) then
    redis.call('HSET', key, 'status', 'FAILED', 'finished_at', ARGV[3],
               'error', 'exceeded max attempts (worker lost or timed out)')
    redis.call('EXPIRE', key, tonumber(ARGV[4]))
    redis.call('HINCRBY', KEYS[4], 'failed', 1)
    redis.call('LPUSH', KEYS[3], id)
    redis.call('LTRIM', KEYS[3], 0, tonumber(ARGV[5]) - 1)
    failed = failed + 1
  else
    redis.call('HSET', key, 'status', 'PENDING', 'worker_id', '')
    redis.call('ZADD', KEYS[2], tonumber(redis.call('HGET', key, 'score')), id)
    requeued = requeued + 1
  end
end
return {requeued, failed}
"""


def _f(v):
    return float(v) if v not in (None, "") else None


class JobNotFound(Exception):
    pass


class JobNotCancellable(Exception):
    pass


class JobStore:
    def __init__(self, client: redis.Redis, *, aging_ms: int = 30000, lease_seconds: int = 60,
                 max_attempts: int = 3, result_ttl: int = 86400):
        self.r = client
        self.aging_ms = aging_ms
        self.lease_ms = lease_seconds * 1000
        self.max_attempts = max_attempts
        self.result_ttl = result_ttl
        self._claim = self.r.register_script(CLAIM_LUA)
        self._finish = self.r.register_script(FINISH_LUA)
        self._reap = self.r.register_script(REAP_LUA)

    @classmethod
    def from_settings(cls, s) -> "JobStore":
        client = redis.Redis.from_url(s.redis_url, decode_responses=True)
        return cls(client, aging_ms=s.aging_ms, lease_seconds=s.lease_seconds,
                   max_attempts=s.max_attempts, result_ttl=s.result_ttl)

    # ---------- used by the API ----------

    def enqueue(self, input_data: dict, priority: Priority, now: float | None = None) -> JobOut:
        now = time.time() if now is None else now
        job_id = str(uuid.uuid4())
        # Lower score = dispatched first. Older jobs have lower timestamps (FIFO), and
        # each priority level subtracts a head start. A low job that has waited longer
        # than the head start overtakes a new high job, so nothing starves.
        score = int(now * 1000) - PRIORITY_WEIGHT[priority] * self.aging_ms
        record = {
            "job_id": job_id,
            "status": JobStatus.PENDING.value,
            "priority": priority.value,
            "input": json.dumps(input_data),
            "created_at": now,
            "attempts": 0,
            "worker_id": "",
            "score": score,
        }
        pipe = self.r.pipeline(transaction=True)  # MULTI/EXEC: record + queue entry together
        pipe.hset(job_key(job_id), mapping=record)
        pipe.zadd(PENDING, {job_id: score})
        pipe.execute()
        return self.get(job_id)

    def get(self, job_id: str) -> JobOut:
        data = self.r.hgetall(job_key(job_id))
        if not data:
            raise JobNotFound(job_id)
        return JobOut(
            job_id=data["job_id"],
            status=data["status"],
            priority=data["priority"],
            input=json.loads(data["input"]),
            created_at=float(data["created_at"]),
            started_at=_f(data.get("started_at")),
            finished_at=_f(data.get("finished_at")),
            attempts=int(data.get("attempts", 0)),
            worker_id=data.get("worker_id") or None,
            result=json.loads(data["result"]) if data.get("result") else None,
            error=data.get("error") or None,
        )

    def cancel(self, job_id: str) -> JobOut:
        if not self.r.exists(job_key(job_id)):
            raise JobNotFound(job_id)
        # ZREM is atomic: either we remove it from the queue, or a worker already claimed it.
        if self.r.zrem(PENDING, job_id) == 0:
            raise JobNotCancellable(self.get(job_id).status.value)
        key = job_key(job_id)
        pipe = self.r.pipeline(transaction=True)
        pipe.hset(key, mapping={"status": JobStatus.CANCELLED.value, "finished_at": time.time()})
        pipe.expire(key, self.result_ttl)
        pipe.hincrby(COUNTERS, "cancelled", 1)
        pipe.lpush(RECENT, job_id)
        pipe.ltrim(RECENT, 0, RECENT_MAX - 1)
        pipe.execute()
        return self.get(job_id)

    def stats(self) -> dict:
        return {"pending": self.r.zcard(PENDING), "running": self.r.zcard(PROCESSING)}

    def ping(self) -> bool:
        return bool(self.r.ping())

    # ---------- used by workers ----------

    def claim(self, worker_id: str, now: float | None = None) -> str | None:
        now = time.time() if now is None else now
        return self._claim(keys=[PENDING, PROCESSING],
                           args=[int(now * 1000), self.lease_ms, worker_id, now]) or None

    def complete(self, job_id: str, worker_id: str, result: dict) -> bool:
        return self._finish_job(job_id, worker_id, JobStatus.COMPLETED, "result", json.dumps(result))

    def fail(self, job_id: str, worker_id: str, error: str) -> bool:
        return self._finish_job(job_id, worker_id, JobStatus.FAILED, "error", error)

    def _finish_job(self, job_id, worker_id, status, field, value) -> bool:
        ok = self._finish(keys=[PROCESSING, RECENT, COUNTERS],
                          args=[job_id, worker_id, status.value, field, value, time.time(),
                                self.result_ttl, RECENT_MAX])
        return bool(ok)

    def reap_expired(self, now: float | None = None) -> tuple[int, int]:
        now = time.time() if now is None else now
        requeued, failed = self._reap(keys=[PROCESSING, PENDING, RECENT, COUNTERS],
                                      args=[int(now * 1000), self.max_attempts, now,
                                            self.result_ttl, RECENT_MAX])
        return int(requeued), int(failed)

    # ---------- worker heartbeats (for visibility) ----------

    def heartbeat(self, worker_id: str, state: dict, ttl: int = 10) -> None:
        """A worker reports it is alive. If it stops reporting, the key expires
        and it disappears from the dashboard automatically."""
        key = worker_key(worker_id)
        pipe = self.r.pipeline(transaction=True)
        pipe.hset(key, mapping={k: ("" if v is None else v) for k, v in state.items()})
        pipe.expire(key, ttl)
        pipe.execute()

    def remove_worker(self, worker_id: str) -> None:
        self.r.delete(worker_key(worker_id))

    def list_workers(self) -> list[dict]:
        keys = sorted(self.r.scan_iter(match="worker:*", count=200))
        if not keys:
            return []
        pipe = self.r.pipeline(transaction=False)
        for k in keys:
            pipe.hgetall(k)
        workers = []
        for data in pipe.execute():
            if not data:
                continue  # expired between SCAN and HGETALL
            workers.append({
                "worker_id": data.get("worker_id"),
                "state": data.get("state", "idle"),
                "job_id": data.get("job_id") or None,
                "job_priority": data.get("job_priority") or None,
                "job_started_at": _f(data.get("job_started_at")),
                "jobs_done": int(data.get("jobs_done") or 0),
                "started_at": _f(data.get("started_at")),
                "last_seen": _f(data.get("last_seen")),
            })
        return workers

    # ---------- everything the dashboard shows, in one call ----------

    def snapshot(self, pending_limit: int = 100, recent_limit: int = 20) -> dict:
        pending_ids = self.r.zrange(PENDING, 0, pending_limit - 1)
        running = self.r.zrange(PROCESSING, 0, -1, withscores=True)
        recent_ids = self.r.lrange(RECENT, 0, recent_limit - 1)

        pipe = self.r.pipeline(transaction=False)
        for jid in pending_ids:
            pipe.hmget(job_key(jid), "priority", "created_at", "attempts")
        for jid, _ in running:
            pipe.hmget(job_key(jid), "priority", "started_at", "worker_id", "attempts")
        for jid in recent_ids:
            pipe.hmget(job_key(jid), "status", "priority", "worker_id", "started_at",
                       "finished_at", "result", "error", "attempts")
        pipe.zcard(PENDING)
        pipe.hgetall(COUNTERS)
        rows = pipe.execute()
        pending_total, counters = rows[-2], rows[-1]
        rows = rows[:-2]

        pending = []
        for pos, (jid, (prio, created, attempts)) in enumerate(zip(pending_ids, rows), start=1):
            if prio is None:
                continue
            pending.append({"job_id": jid, "position": pos, "priority": prio,
                            "created_at": _f(created), "attempts": int(attempts or 0)})
        rows = rows[len(pending_ids):]

        running_out = []
        for (jid, deadline_ms), (prio, started, wid, attempts) in zip(running, rows):
            if prio is None:
                continue
            running_out.append({"job_id": jid, "priority": prio, "started_at": _f(started),
                                "worker_id": wid or None, "attempts": int(attempts or 0),
                                "lease_deadline": deadline_ms / 1000})
        rows = rows[len(running):]

        recent = []
        for jid, (status, prio, wid, started, finished, result, error, attempts) in zip(recent_ids, rows):
            if status is None:
                continue  # record expired
            res = json.loads(result) if result else None
            recent.append({"job_id": jid, "status": status, "priority": prio,
                           "worker_id": wid or None, "started_at": _f(started),
                           "finished_at": _f(finished), "attempts": int(attempts or 0),
                           "label": res.get("label") if res else None,
                           "confidence": res.get("confidence") if res else None,
                           "error": error or None})

        return {
            "server_time": time.time(),
            "counts": {
                "pending": int(pending_total),
                "running": len(running_out),
                "completed": int(counters.get("completed", 0)),
                "failed": int(counters.get("failed", 0)),
                "cancelled": int(counters.get("cancelled", 0)),
            },
            "pending": pending,
            "running": running_out,
            "recent": recent,
            "workers": self.list_workers(),
        }
