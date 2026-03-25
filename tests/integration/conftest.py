"""Integration test fixtures.

Services must be running before invoking pytest:
  - Postgres on :5433          (via docker compose up -d --wait)
  - qiita-control-plane on :8080
  - qiita-compute-orchestrator on :8081
  - qiita-data-plane on :50051
"""

import httpx
import pytest


@pytest.fixture(scope="session")
def postgres_url() -> str:
    return "postgresql://qiita:qiita@localhost:5433/qiita_test"


@pytest.fixture(scope="session")
def control_plane_url() -> str:
    return "http://localhost:8080"


@pytest.fixture(scope="session")
def compute_orchestrator_url() -> str:
    return "http://localhost:8081"


@pytest.fixture(scope="session")
def data_plane_location() -> str:
    return "grpc://localhost:50051"


@pytest.fixture(scope="session")
def http() -> httpx.Client:
    """Shared HTTP client for the full test session."""
    with httpx.Client(timeout=10) as client:
        yield client
