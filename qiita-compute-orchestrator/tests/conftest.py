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

import subprocess  # noqa: E402
import sys  # noqa: E402
import textwrap  # noqa: E402
from pathlib import Path  # noqa: E402

import pytest  # noqa: E402
from helpers import KREPP_TREE_NEWICK, TEST_SEQUENCES, krepp_genomes  # noqa: E402


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
        gpl_boundary_is_outdated,
        miint_connect_config,
        miint_install_sql,
        miint_load_sql,
    )

    with duckdb.connect(":memory:", config=miint_connect_config()) as conn:
        conn.execute(miint_install_sql())
        conn.execute(miint_load_sql())
        row = conn.execute("SELECT install_gpl_boundary()").fetchone()
        # install_gpl_boundary() keeps a cached binary of any version; replace one
        # older than the miint functions under test accept.
        if row and row[0] and gpl_boundary_is_outdated(row[0]["version"]):
            row = conn.execute("SELECT install_gpl_boundary(true)").fetchone()

    # miint is a CORE dependency (see CLAUDE.md): miint_job_env() — used by the
    # SlurmBackend and the compute-readiness probe — now REQUIRES
    # MIINT_GPL_BOUNDARY_PATH, mirroring the deploy (native jobs get an ephemeral
    # HOME, so the boundary must be pointed at explicitly). install_gpl_boundary()
    # reports where it installed the binary; point the var there so submit/probe
    # tests resolve exactly the boundary a real job would. setdefault → an explicit
    # override (e.g. a real deploy env) still wins.
    boundary_path = row[0].get("path") if row and row[0] else None
    if boundary_path:
        os.environ.setdefault("MIINT_GPL_BOUNDARY_PATH", boundary_path)


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


# --- a small krepp index (tests/test_krepp_build.py, tests/jobs/test_krepp_index_*) ---
#
# Built once per session by the staged miint, in a subprocess with a timeout:
# krepp_index_create can hang (duckdb-miint#318), and a hang must fail this fixture
# rather than the whole run. Four random 20 kb genomes, the tree below numbered
# edge_id = node_index (as Qiita numbers an undecorated tree), two partitions (m=4,
# r=0 and r=1), h=9 so each `inc` file is 0.5 MB.

_KREPP_BUILD_TIMEOUT_S = 300

_KREPP_BUILD = textwrap.dedent(
    """
    import sys
    from qiita_compute_orchestrator.miint import open_miint_conn
    out, nwk, genomes = sys.argv[1], sys.argv[2], sys.argv[3]
    conn = open_miint_conn()
    conn.execute(
        "CREATE TABLE ref AS SELECT read_id, sequence1 FROM read_csv(?, delim='\\t',"
        " header=false, columns={'read_id': 'VARCHAR', 'sequence1': 'VARCHAR'})",
        [genomes],
    )
    conn.execute(
        "CREATE TABLE tree AS SELECT * REPLACE (node_index AS edge_id) FROM read_newick(?)",
        [nwk],
    )
    for r in (0, 1):
        status = conn.execute(
            f"SELECT status FROM krepp_index_create('ref', '{out}', tree_table := 'tree',"
            f" k := 19, w := 25, h := 9, m := 4, r := {r}, frac := false)"
        ).fetchone()[0]
        assert status == "ok", status
    """
)


@pytest.fixture(scope="session")
def krepp_built_index(tmp_path_factory) -> Path:
    """`index/` of a two-partition (m=4, r=0 and r=1) index over four random genomes."""
    root = tmp_path_factory.mktemp("krepp-index")
    nwk = root / "tree.nwk"
    nwk.write_text(KREPP_TREE_NEWICK)
    genomes = root / "genomes.tsv"
    genomes.write_text("".join(f"{k}\t{v}\n" for k, v in krepp_genomes().items()))
    out = root / "index"
    subprocess.run(
        [sys.executable, "-c", _KREPP_BUILD, str(out), str(nwk), str(genomes)],
        check=True,
        timeout=_KREPP_BUILD_TIMEOUT_S,
        capture_output=True,
    )
    return out
