"""Shared test fixtures for qiita-compute-orchestrator.

Sets dev-mode env vars before any test imports the FastAPI app so the
lifespan handler can resolve `Settings.from_env()` without a real token
file at /etc/qiita/cp-to-co.token.
"""

import os

os.environ.setdefault("QIITA_ALLOW_TOKEN_ENV", "true")
os.environ.setdefault("CP_TO_CO_TOKEN", "test-cp-to-co-token")
os.environ.setdefault("CO_TO_CP_TOKEN", "test-co-to-cp-token")
# Pull miint from the team mirror rather than the DuckDB community channel: the
# mirror carries the up-to-date build that includes rype_index_create (the
# community build lags behind). MIINT_EXTENSION_REPO is read at miint.py import
# time and also flips open_conn() to allow unsigned extensions, so it must be set
# before any test imports the miint helper — hence here, at conftest top, like
# the token vars above. Suite-wide (not per-test) keeps every connection on the
# same build + signed/unsigned setting, avoiding a mixed-build clash in DuckDB's
# shared on-disk extension directory.
os.environ.setdefault("MIINT_EXTENSION_REPO", "https://ftp.microbio.me/pub/miint")

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
