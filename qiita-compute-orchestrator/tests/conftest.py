"""Shared test fixtures for qiita-compute-orchestrator.

Sets dev-mode env vars before any test imports the FastAPI app so the
lifespan handler can resolve `Settings.from_env()` without a real token
file at /etc/qiita/cp-to-co.token.
"""

import os

from qiita_common.duckdb_miint import setup_miint_test_env

os.environ.setdefault("QIITA_ALLOW_TOKEN_ENV", "true")
os.environ.setdefault("CP_TO_CO_TOKEN", "test-cp-to-co-token")
os.environ.setdefault("CO_TO_CP_TOKEN", "test-co-to-cp-token")
# Pull miint from the team mirror (carries the up-to-date build incl.
# rype_index_create; the community build lags) into a per-component private
# extension dir. Shared with the control-plane conftest via the helper.
setup_miint_test_env("orchestrator")

import pytest  # noqa: E402
from helpers import TEST_SEQUENCES  # noqa: E402


@pytest.fixture
def fasta_file(tmp_path):
    """Create a 5-sequence FASTA file. Returns (path, sequences dict)."""
    path = tmp_path / "test.fasta"
    with open(path, "w") as f:
        for name, seq in TEST_SEQUENCES.items():
            f.write(f">{name}\n{seq}\n")
    return path, TEST_SEQUENCES


@pytest.fixture
def cp_to_co_token() -> str:
    return os.environ["CP_TO_CO_TOKEN"]
