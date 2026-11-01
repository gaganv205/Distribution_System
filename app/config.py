"""All settings come from environment variables so the same image runs as API or worker."""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    redis_url: str
    model_path: str
    inference_delay: float   # artificial delay to simulate a heavy model
    aging_ms: int            # how much "head start" each priority level gets
    lease_seconds: int       # how long a worker may hold a job before it's presumed dead
    max_attempts: int        # retries before a job is marked FAILED
    result_ttl: int          # finished jobs are deleted from Redis after this many seconds


def load_settings() -> Settings:
    return Settings(
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        model_path=os.getenv("MODEL_PATH", "model/model.joblib"),
        inference_delay=float(os.getenv("INFERENCE_DELAY_SECONDS", "0")),
        aging_ms=int(os.getenv("PRIORITY_AGING_MS", "30000")),
        lease_seconds=int(os.getenv("JOB_LEASE_SECONDS", "60")),
        max_attempts=int(os.getenv("JOB_MAX_ATTEMPTS", "3")),
        result_ttl=int(os.getenv("RESULT_TTL_SECONDS", "86400")),
    )
