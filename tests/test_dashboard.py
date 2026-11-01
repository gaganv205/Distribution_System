"""Dashboard data: queue order, running jobs, worker heartbeats, recent history, counters."""
from app.schemas import Priority
from app.worker import Worker

INPUT = {"features": [5.1, 3.5, 1.4, 0.2]}


def test_dashboard_page_and_redirect(client):
    r = client.get("/dashboard")
    assert r.status_code == 200 and "Inference job queue" in r.text
    assert client.get("/", follow_redirects=False).headers["location"] == "/dashboard"


def test_snapshot_shows_queue_in_dispatch_order(client, store):
    low = store.enqueue(INPUT, Priority.low, now=100.0).job_id
    high = store.enqueue(INPUT, Priority.high, now=101.0).job_id
    d = client.get("/dashboard/data").json()
    assert [j["job_id"] for j in d["pending"]] == [high, low]
    assert [j["position"] for j in d["pending"]] == [1, 2]
    assert d["counts"]["pending"] == 2


def test_running_job_and_worker_heartbeat_visible(store, model):
    job_id = store.enqueue(INPUT, Priority.normal).job_id
    w = Worker(store, model, worker_id="w-test")
    store.claim("w-test")
    w.beat("busy", store.get(job_id), force=True)
    d = store.snapshot()
    assert d["running"][0]["job_id"] == job_id and d["running"][0]["worker_id"] == "w-test"
    assert d["workers"][0]["worker_id"] == "w-test"
    assert d["workers"][0]["state"] == "busy" and d["workers"][0]["job_id"] == job_id


def test_worker_returns_to_idle_and_counts_jobs(store, worker):
    store.enqueue(INPUT, Priority.normal)
    store.enqueue(INPUT, Priority.normal)
    worker.run_once(); worker.run_once()
    w = store.snapshot()["workers"][0]
    assert w["state"] == "idle" and w["job_id"] is None and w["jobs_done"] == 2


def test_dead_worker_disappears_when_heartbeat_expires(store):
    store.heartbeat("ghost", {"worker_id": "ghost", "state": "idle"}, ttl=1)
    assert len(store.list_workers()) == 1
    store.r.delete("worker:ghost")  # what Redis does when the TTL runs out
    assert store.list_workers() == []


def test_recent_history_and_counters(client, store, worker):
    a = store.enqueue(INPUT, Priority.normal).job_id
    b = store.enqueue({"features": [float("nan")] * 4}, Priority.normal).job_id
    c = store.enqueue(INPUT, Priority.low).job_id
    worker.run_once(); worker.run_once()
    client.delete(f"/jobs/{c}")
    d = client.get("/dashboard/data").json()
    assert [j["job_id"] for j in d["recent"]] == [c, b, a]  # newest first
    assert [j["status"] for j in d["recent"]] == ["CANCELLED", "FAILED", "COMPLETED"]
    assert d["recent"][2]["label"] == "setosa"
    assert d["counts"] == {"pending": 0, "running": 0, "completed": 1, "failed": 1, "cancelled": 1}


def test_reaper_failure_is_counted(store):
    store.max_attempts = 1
    job_id = store.enqueue(INPUT, Priority.normal, now=0.0).job_id
    store.claim("w", now=0.0)
    store.reap_expired(now=61.0)
    d = store.snapshot()
    assert d["counts"]["failed"] == 1 and d["recent"][0]["job_id"] == job_id
