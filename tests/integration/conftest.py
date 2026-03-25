"""Integration test fixtures. Requires docker compose up -d --wait before running."""
import pytest


@pytest.fixture(scope="session")
def postgres_url() -> str:
    return "postgresql://qiita:qiita@localhost:5433/qiita_test"
