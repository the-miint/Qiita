"""Smoke guard: the installed miint build must provide ``read_fastx`` with the
``max_batch_bytes`` named parameter that ``stage_local_fasta`` and the CLI's
``reference load`` both rely on.

This is the durable guard for F10 (docs/design/reference-load-resilience.md): a
consumer running a miint build whose ``read_fastx`` predates ``max_batch_bytes``
fails here, loudly, instead of at the first reference-load SLURM job with a
``Binder Error: Invalid named parameter "max_batch_bytes"``. It exercises the
real install path (``miint_install_sql`` + ``miint_connect_config``), so it also
catches community-vs-mirror drift — the install is single-sourced to the team
mirror, so every component runs the same build.

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


def test_read_fastx_accepts_max_batch_bytes(miint_conn, fasta_file):
    """Binds + runs without a BinderException → the param exists in this build.
    The exact call shape stage_local_fasta.py / reference_load.py issue."""
    fasta_path, _records = fasta_file
    rows = miint_conn.execute(
        "SELECT read_id, sequence1 FROM read_fastx(?, max_batch_bytes:='64MB')",
        [str(fasta_path)],
    ).fetchall()
    assert rows  # the fixture FASTA carries at least one record
