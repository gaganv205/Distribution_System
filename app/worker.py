"""Inference worker. Run as many copies as you like, on any machine that can reach Redis:
    python -m app.worker
Each worker loops: claim job -> run model -> write result. It never talks to the API.
It also sends a heartbeat to Redis so the dashboard can show it (idle/busy, current job)."""
import logging
import os
import signal
import socket
import time
import uuid

from .config import load_settings
from .model import IrisModel
from .store import JobStore

log = logging.getLogger("worker")


class Worker:
    def __init__(self, store: JobStore, model, worker_id: str | None = None,
                 inference_delay: float = 0.0, poll_interval: float = 0.2, reap_interval: float = 5.0,
                 heartbeat_interval: float = 1.0, heartbeat_ttl: int = 10):
        self.store = store
        self.model = model
        self.worker_id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:4]}"
        self.inference_delay = inference_delay
        self.poll_interval = poll_interval
        self.reap_interval = reap_interval
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_ttl = heartbeat_ttl
        self.jobs_done = 0
        self.started_at = time.time()
        self._stopping = False
        self._last_reap = 0.0
        self._last_beat = 0.0

    def stop(self, *_):
        log.info("%s: shutdown requested, finishing current job first", self.worker_id)
        self._stopping = True

    def beat(self, state: str = "idle", job=None, force: bool = False):
        """Report status to Redis. Throttled to once per heartbeat_interval unless forced
        (state changes are always sent immediately)."""
        now = time.time()
        if not force and now - self._last_beat < self.heartbeat_interval:
            return
        self._last_beat = now
        try:
            self.store.heartbeat(self.worker_id, {
                "worker_id": self.worker_id,
                "state": state,
                "job_id": job.job_id if job else "",
                "job_priority": job.priority.value if job else "",
                "job_started_at": now if job else "",
                "jobs_done": self.jobs_done,
                "started_at": self.started_at,
                "last_seen": now,
            }, ttl=self.heartbeat_ttl)
        except Exception as exc:  # visibility must never break processing
            log.debug("heartbeat failed: %s", exc)

    def run_once(self) -> bool:
        """Process at most one job. Returns True if a job was processed."""
        job_id = self.store.claim(self.worker_id)
        if job_id is None:
            return False
        job = self.store.get(job_id)
        self.beat("busy", job, force=True)
        log.info("%s: running job %s (priority=%s, attempt=%d)",
                 self.worker_id, job_id, job.priority.value, job.attempts)
        try:
            if self.inference_delay:
                time.sleep(self.inference_delay)  # simulate a slow model
            result = self.model.predict(job.input["features"])
        except Exception as exc:  # bad input or model error: fail now, don't retry
            ok = self.store.fail(job_id, self.worker_id, f"{type(exc).__name__}: {exc}")
            log.warning("%s: job %s failed: %s", self.worker_id, job_id, exc)
        else:
            ok = self.store.complete(job_id, self.worker_id, result)
            log.info("%s: job %s -> %s", self.worker_id, job_id, result["label"])
        if ok:
            self.jobs_done += 1
        else:
            log.warning("%s: lost lease on job %s; result discarded", self.worker_id, job_id)
        self.beat("idle", force=True)
        return True

    def maybe_reap(self):
        """Any worker can recover jobs abandoned by crashed workers. No coordinator needed."""
        now = time.time()
        if now - self._last_reap >= self.reap_interval:
            self._last_reap = now
            requeued, failed = self.store.reap_expired()
            if requeued or failed:
                log.warning("%s: reaper requeued=%d failed=%d", self.worker_id, requeued, failed)

    def run_forever(self):
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)
        log.info("%s: started", self.worker_id)
        self.beat("idle", force=True)
        try:
            while not self._stopping:
                self.maybe_reap()
                self.beat("idle")
                if not self.run_once():
                    time.sleep(self.poll_interval)  # queue empty: back off briefly
        finally:
            self.store.remove_worker(self.worker_id)  # clean exit: vanish from dashboard now
            log.info("%s: stopped", self.worker_id)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    s = load_settings()
    Worker(JobStore.from_settings(s), IrisModel(s.model_path),
           inference_delay=s.inference_delay).run_forever()


if __name__ == "__main__":
    main()
