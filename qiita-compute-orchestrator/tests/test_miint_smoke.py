"""Smoke guard: the miint build the jobs depend on installs from the mirror,
loads, and is actually usable.

A consumer running a stale or wrong miint build (an old cached extension, or a
community-vs-mirror mismatch) should fail here, loudly, instead of at the first
reference-load SLURM job. The test exercises the real, single-sourced install
path (``miint_install_sql`` + ``miint_connect_config``) and then runs
``read_fastx`` — the core ingest call ``stage_local_fasta`` / ``reference load``
issue — in the exact shape the jobs use, so a build that can't serve it is
caught at test time. (``max_batch_bytes`` is one such job-issued parameter; the
point is the current build is installed and runs, not any single param.)

Lives in the orchestrator test tree (not ``qiita-common/tests``) because
exercising the install needs ``duckdb``, a runtime dep of this component, not of
the pure-Python ``qiita-common`` contract layer.
"""

from __future__ import annotations

import duckdb
import pytest
from qiita_common.duckdb_miint import miint_connect_config, miint_install_sql


@pytest.fixture
def miint_conn():
    conn = duckdb.connect(":memory:", config=miint_connect_config())
    conn.execute(miint_install_sql())
    conn.execute("LOAD miint;")
    yield conn
    conn.close()


def test_miint_installs_loads_and_read_fastx_runs(miint_conn, fasta_file):
    """The mirror build installs + loads and serves the jobs' read_fastx call
    (the exact shape stage_local_fasta.py / reference_load.py issue) without a
    BinderException — i.e. this host runs a current, usable miint."""
    fasta_path, _records = fasta_file
    rows = miint_conn.execute(
        "SELECT read_id, sequence1 FROM read_fastx(?, max_batch_bytes:='64MB')",
        [str(fasta_path)],
    ).fetchall()
    assert rows  # the fixture FASTA carries at least one record
