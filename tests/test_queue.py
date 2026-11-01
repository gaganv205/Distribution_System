"""Queue semantics: priority order, starvation prevention, crash recovery, lease safety."""
from app.schemas import Priority
from app.worker import Worker

INPUT = {"features": [5.1, 3.5, 1.4, 0.2]}


def test_priority_order(store):
    t = 1_000_000.0
    low = store.enqueue(INPUT, Priority.low, now=t).job_id
    normal = store.enqueue(INPUT, Priority.normal, now=t + 1).job_id
    high = store.enqueue(INPUT, Priority.high, now=t + 2).job_id
    assert [store.claim("w") for _ in range(3)] == [high, normal, low]


def test_fifo_within_same_priority(store):
    a = store.enqueue(INPUT, Priority.normal, now=100.0).job_id
    b = store.enqueue(INPUT, Priority.normal, now=101.0).job_id
    assert store.claim("w") == a and store.claim("w") == b


def test_aging_prevents_starvation(store):
    # aging_ms=30s per level, so high gets a 60s head start over low.
    old_low = store.enqueue(INPUT, Priority.low, now=0.0).job_id
    store.enqueue(INPUT, Priority.high, now=61.0)  # new high job, 61s later
    assert store.claim("w") == old_low  # waited long enough to overtake


def test_each_job_claimed_once(store):
    ids = {store.enqueue(INPUT, Priority.normal).job_id for _ in range(20)}
    claimed = [store.claim(f"w{i % 4}") for i in range(25)]
    got = [c for c in claimed if c]
    assert len(got) == 20 and set(got) == ids  # no duplicates, none lost


def test_crashed_worker_job_is_requeued(store, model):
    job_id = store.enqueue(INPUT, Priority.normal, now=0.0).job_id
    assert store.claim("dead-worker", now=0.0) == job_id  # claims, then "crashes"
    assert store.reap_expired(now=30.0) == (0, 0)          # lease (60s) not expired yet
    assert store.reap_expired(now=61.0) == (1, 0)          # expired: requeued
    assert store.get(job_id).status.value == "PENDING"

    Worker(store, model, worker_id="healthy").run_once()
    job = store.get(job_id)
    assert job.status.value == "COMPLETED" and job.worker_id == "healthy" and job.attempts == 2


def test_zombie_worker_cannot_overwrite(store):
    job_id = store.enqueue(INPUT, Priority.normal, now=0.0).job_id
    store.claim("slow", now=0.0)
    store.reap_expired(now=61.0)          # slow worker presumed dead
    store.claim("fast", now=62.0)
    assert store.complete(job_id, "fast", {"label": "ok"}) is True
    assert store.complete(job_id, "slow", {"label": "stale"}) is False  # rejected
    assert store.get(job_id).result == {"label": "ok"}


def test_fails_after_max_attempts(store):
    job_id = store.enqueue(INPUT, Priority.normal, now=0.0).job_id
    t = 0.0
    for _ in range(3):
        store.claim("w", now=t)
        t += 61
        store.reap_expired(now=t)
    job = store.get(job_id)
    assert job.status.value == "FAILED" and job.attempts == 3
    assert "max attempts" in job.error


def test_model_error_fails_job_without_retry(store, model):
    job_id = store.enqueue({"features": [float("nan")] * 4}, Priority.normal).job_id
    Worker(store, model, worker_id="w").run_once()
    job = store.get(job_id)
    assert job.status.value == "FAILED" and "finite" in job.error
    assert store.stats() == {"pending": 0, "running": 0}


def test_cancel_races_with_claim(store):
    job_id = store.enqueue(INPUT, Priority.normal).job_id
    store.claim("w")
    import pytest
    from app.store import JobNotCancellable
    with pytest.raises(JobNotCancellable):
        store.cancel(job_id)
