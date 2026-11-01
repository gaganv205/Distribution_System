"""Request/response models and the job lifecycle definitions."""
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class Priority(str, Enum):
    high = "high"
    normal = "normal"
    low = "low"


# Higher weight = processed sooner. Used to compute the queue score.
PRIORITY_WEIGHT = {Priority.high: 2, Priority.normal: 1, Priority.low: 0}


class JobStatus(str, Enum):
    PENDING = "PENDING"      # waiting in the queue
    RUNNING = "RUNNING"      # claimed by a worker
    COMPLETED = "COMPLETED"  # result available
    FAILED = "FAILED"        # error, or retries exhausted
    CANCELLED = "CANCELLED"  # cancelled by the client before it started


class InferenceInput(BaseModel):
    features: list[float] = Field(
        ...,
        min_length=4,
        max_length=4,
        description="Iris measurements in cm: sepal length, sepal width, petal length, petal width",
        examples=[[5.1, 3.5, 1.4, 0.2]],
    )


class JobCreate(BaseModel):
    input: InferenceInput
    priority: Priority = Priority.normal


class JobOut(BaseModel):
    job_id: str
    status: JobStatus
    priority: Priority
    input: dict[str, Any]
    created_at: float
    started_at: float | None = None
    finished_at: float | None = None
    attempts: int = 0
    worker_id: str | None = None
    result: dict[str, Any] | None = None
    error: str | None = None
