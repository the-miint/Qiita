"""Integration test: feature_idx-scoped ticket → open_doget_stream → DuckDB.

Proves the compute-side streaming foundation: a native build job can pull
a shard's reference sequence chunks from the live data plane over Arrow Flight,
scoped to a feature_idx subset, and reassemble them in DuckDB — without reading
staging Parquet. The ticket is signed directly with the data plane's HMAC secret
(the CP route that mints it has its own DB-tier tests); the point exercised here
is the DP-side DoGet + the orchestrator's open_doget_stream consumer.

Relies on the shared module-scoped `data_plane` fixture in conftest.py (starts
the pre-built binary and owns schema creation); rows are seeded via a
short-lived DuckLake connection after the data plane is live, exactly like
test_doget.py.
"""

import duckdb
import pytest
from qiita_common.api_paths import LOOPBACK_HOST

from qiita_compute_orchestrator.data_plane_client import open_doget_stream

from conftest import ducklake_connect

# Out-of-band feature_idx values for this module's seeded rows and the reference
# they belong to. The subset excludes _EXCLUDED to prove scoping.
_REF_IDX = 5
_SUBSET = [800001, 800002]
_EXCLUDED = 800003


@pytest.fixture(scope="module", autouse=True)
def _seed_reference_rows(data_plane):
    """Seed multi-chunk reference sequences + membership against the live
    DuckLake (tables created by the data plane)."""
    conn = ducklake_connect(data_plane["data_path"])
    try:
        conn.execute(
            "INSERT INTO qiita_lake.reference_sequence_chunks VALUES "
            "(800001, 0, 'ATCG'), (800001, 1, 'ATCG'), "  # reassembles to ATCGATCG
            "(800002, 0, 'GCTA'), "  # reassembles to GCTA
            "(800003, 0, 'TTTT')"  # NOT in the ticket's feature_idx subset
        )
        conn.execute(
            "INSERT INTO qiita_lake.reference_membership VALUES "
            "(5, 800001), (5, 800002), (5, 800003)"
        )
    finally:
        conn.close()


def _sign(filter_dict, secret_bytes):
    from qiita_control_plane.auth.tickets import sign_ticket

    return sign_ticket(
        table="reference_sequence_chunks", filter=filter_dict, secret=secret_bytes
    )


def test_open_doget_stream_scoped_to_feature_subset(data_plane):
    """A feature_idx-scoped ticket (the shape POST /reference/{idx}/ticket/doget
    mints) streams only the subset's chunks; the excluded feature is absent, and
    chunks reassemble in chunk_index order."""
    ticket = _sign(
        {"reference_idx": [_REF_IDX], "feature_idx": _SUBSET},
        data_plane["secret"],
    )
    url = f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"
    conn = duckdb.connect()
    try:
        with open_doget_stream(conn, data_plane_url=url, ticket_bytes=ticket) as rel:
            seqs = dict(
                conn.execute(
                    f"SELECT feature_idx, string_agg(chunk_data, '' ORDER BY chunk_index) "
                    f"FROM {rel} GROUP BY feature_idx"
                ).fetchall()
            )
    finally:
        conn.close()

    assert seqs == {800001: "ATCGATCG", 800002: "GCTA"}
    assert _EXCLUDED not in seqs
