import uuid

SETOSA = [5.1, 3.5, 1.4, 0.2]


def submit(client, features=SETOSA, priority="normal"):
    r = client.post("/jobs", json={"input": {"features": features}, "priority": priority})
    assert r.status_code == 202, r.text
    return r.json()


def test_submit_returns_pending_job_with_uuid(client):
    r = client.post("/jobs", json={"input": {"features": SETOSA}})
    assert r.status_code == 202
    body = r.json()
    uuid.UUID(body["job_id"])  # valid UUID
    assert body["status"] == "PENDING"
    assert body["priority"] == "normal"
    assert r.headers["Location"] == f"/jobs/{body['job_id']}"


def test_full_lifecycle(client, worker):
    job = submit(client)
    assert worker.run_once() is True
    done = client.get(f"/jobs/{job['job_id']}").json()
    assert done["status"] == "COMPLETED"
    assert done["result"]["label"] == "setosa"
    assert done["worker_id"] == "w1"
    assert done["attempts"] == 1
    assert done["started_at"] <= done["finished_at"]


def test_validation_rejects_bad_input(client):
    assert client.post("/jobs", json={"input": {"features": [1, 2, 3]}}).status_code == 422
    assert client.post("/jobs", json={"input": {"features": SETOSA}, "priority": "urgent"}).status_code == 422
    assert client.post("/jobs", json={}).status_code == 422


def test_unknown_and_malformed_ids(client):
    assert client.get(f"/jobs/{uuid.uuid4()}").status_code == 404
    assert client.get("/jobs/not-a-uuid").status_code == 422


def test_cancel_pending_job(client, worker):
    job = submit(client)
    r = client.delete(f"/jobs/{job['job_id']}")
    assert r.status_code == 200 and r.json()["status"] == "CANCELLED"
    assert worker.run_once() is False  # nothing left to run


def test_cannot_cancel_finished_job(client, worker):
    job = submit(client)
    worker.run_once()
    assert client.delete(f"/jobs/{job['job_id']}").status_code == 409


def test_stats_and_health(client, worker):
    submit(client); submit(client)
    assert client.get("/stats").json() == {"pending": 2, "running": 0}
    assert client.get("/health").json() == {"status": "ok"}
