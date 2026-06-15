"""Smoke guard: the staged miint build the jobs depend on LOADs and is usable.

A consumer running a stale or wrong miint build (a mis-staged extension dir, a
community-vs-mirror mismatch) should fail here, loudly, instead of at the first
reference-load SLURM job. The conftest's session fixture stages the build once
(mirroring the deploy), and this test then opens a connection through the real
production helper ``open_miint_conn`` (LOAD-only, exactly what the native jobs
do) and runs ``read_fastx`` — the core ingest call ``stage_local_fasta`` /
``reference load`` issue — in the exact shape the jobs use, so a build that
can't serve it is caught at test time. (``max_batch_bytes`` is one such
job-issued parameter; the point is the staged build LOADs and runs, not any
single param.)

Lives in the orchestrator test tree (not ``qiita-common/tests``) because
exercising the connection needs ``duckdb``, a runtime dep of this component, not
of the pure-Python ``qiita-common`` contract layer.
"""

from __future__ import annotations

import pytest

from qiita_compute_orchestrator.miint import open_miint_conn


@pytest.fixture
def miint_conn():
    conn = open_miint_conn()
    yield conn
    conn.close()


def test_miint_loads_and_read_fastx_runs(miint_conn, fasta_file):
    """The staged build LOADs and serves the jobs' read_fastx call (the exact
    shape stage_local_fasta.py / reference_load.py issue) without a
    BinderException — i.e. this host LOADs a current, usable miint."""
    fasta_path, _records = fasta_file
    rows = miint_conn.execute(
        "SELECT read_id, sequence1 FROM read_fastx(?, max_batch_bytes:='64MB')",
        [str(fasta_path)],
    ).fetchall()
    assert rows  # the fixture FASTA carries at least one record
