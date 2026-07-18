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


@pytest.fixture(scope="session", autouse=True)
def _stage_miint_extension():
    """Production stages miint into MIINT_EXTENSION_DIRECTORY at deploy; the
    cluster runtime (native jobs, the probe) is then LOAD-only via
    `open_miint_conn`. Mirror that here: install once into the per-component
    temp extension dir (`setup_miint_test_env` above) so LOAD-only callers find
    it. Plain INSTALL (not the deploy's FORCE) so the stable temp dir caches
    across runs — first run downloads from the mirror, later runs are instant.
    Also installs the GPL-boundary tool host once, mirroring what the deploy's
    `stage_miint_extension` does (bowtie2 alignment and friends run behind it), so
    real-miint smokes find it pre-installed exactly as a native job does at runtime.
    Kept in step with the integration conftest's identical fixture."""
    import duckdb
    from qiita_common.duckdb_miint import (
        miint_connect_config,
        miint_install_sql,
        miint_load_sql,
    )

    with duckdb.connect(":memory:", config=miint_connect_config()) as conn:
        conn.execute(miint_install_sql())
        conn.execute(miint_load_sql())
        row = conn.execute("SELECT install_gpl_boundary()").fetchone()

    # miint is a CORE dependency (see CLAUDE.md): miint_job_env() — used by the
    # SlurmBackend and the compute-readiness probe — now REQUIRES
    # MIINT_GPL_BOUNDARY_PATH, mirroring the deploy (native jobs get an ephemeral
    # HOME, so the boundary must be pointed at explicitly). install_gpl_boundary()
    # reports where it installed the binary; point the var there so submit/probe
    # tests resolve exactly the boundary a real job would. setdefault → an explicit
    # override (e.g. a real deploy env) still wins.
    if row and row[0]:
        os.environ.setdefault("MIINT_GPL_BOUNDARY_PATH", row[0]["path"])


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
