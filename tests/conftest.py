import fakeredis
import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.model import IrisModel
from app.store import JobStore
from app.worker import Worker


@pytest.fixture
def store():
    # fakeredis runs real Lua, so the atomic scripts are actually exercised
    return JobStore(fakeredis.FakeRedis(decode_responses=True),
                    aging_ms=30000, lease_seconds=60, max_attempts=3)


@pytest.fixture(scope="session")
def model(tmp_path_factory):
    return IrisModel(str(tmp_path_factory.mktemp("m") / "model.joblib"))


@pytest.fixture
def client(store):
    return TestClient(create_app(store))


@pytest.fixture
def worker(store, model):
    return Worker(store, model, worker_id="w1")
