"""HTTP layer. It validates requests and reads/writes job records. It never runs the model,
so it stays fast no matter how slow inference is.
    uvicorn app.api:app --host 0.0.0.0 --port 8000"""
from pathlib import Path
from uuid import UUID

from fastapi import FastAPI, HTTPException, Response, status
from fastapi.responses import HTMLResponse, RedirectResponse

from .config import load_settings
from .schemas import JobCreate, JobOut
from .store import JobNotCancellable, JobNotFound, JobStore

DASHBOARD_HTML = (Path(__file__).parent / "static" / "dashboard.html").read_text(encoding="utf-8")


def create_app(store: JobStore | None = None) -> FastAPI:
    app = FastAPI(
        title="ML Inference Job Service",
        description="Submit inference jobs, get a job ID, poll for the result.",
        version="1.0.0",
    )
    store = store or JobStore.from_settings(load_settings())
    app.state.store = store

    @app.post("/jobs", response_model=JobOut, status_code=status.HTTP_202_ACCEPTED)
    def submit_job(body: JobCreate, response: Response):
        """Queue a job and return immediately. 202 = accepted, not finished."""
        job = store.enqueue(body.input.model_dump(), body.priority)
        response.headers["Location"] = f"/jobs/{job.job_id}"
        return job

    @app.get("/jobs/{job_id}", response_model=JobOut)
    def get_job(job_id: UUID):
        """Poll this until status is COMPLETED, FAILED or CANCELLED."""
        try:
            return store.get(str(job_id))
        except JobNotFound:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")

    @app.delete("/jobs/{job_id}", response_model=JobOut)
    def cancel_job(job_id: UUID):
        """Cancel a job that is still PENDING. Running jobs can't be cancelled."""
        try:
            return store.cancel(str(job_id))
        except JobNotFound:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
        except JobNotCancellable as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, f"job is {exc}, only PENDING jobs can be cancelled")

    @app.get("/stats")
    def stats():
        return store.stats()

    @app.get("/health")
    def health():
        try:
            store.ping()
        except Exception:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "redis unreachable")
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse("/dashboard")

    @app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
    def dashboard():
        """Live view of the queue, workers and finished jobs."""
        return DASHBOARD_HTML

    @app.get("/dashboard/data")
    def dashboard_data():
        """Everything the dashboard shows, in one call. Polled about twice a second."""
        try:
            return store.snapshot()
        except Exception:
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "redis unreachable")

    return app


app = create_app()
