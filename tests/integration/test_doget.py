"""Integration test: sign ticket in Python → DoGet via pyarrow.flight → verify Arrow data.

Relies on the shared `data_plane` fixture in conftest.py, which starts the
pre-built binary and lets it own schema creation. Test rows are seeded via
a short-lived DuckDB connection after the data plane is live.
"""

import pyarrow.flight as flight
import pytest

from conftest import ducklake_connect

# Deterministic, out-of-band feature_idx values for this module's seeded rows.
SEED_FEATURE_IDXS = [700001, 700002]


@pytest.fixture(scope="module", autouse=True)
def _seed_reference_rows(data_plane):
    """Seed reference rows against the live DuckLake (tables created by data plane)."""
    conn = ducklake_connect(data_plane["data_path"])
    try:
        conn.execute(
            "INSERT INTO qiita_lake.reference_sequences VALUES "
            "(700001, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID, 8), "
            "(700002, 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22'::UUID, 8)"
        )
        conn.execute(
            "INSERT INTO qiita_lake.reference_sequence_chunks VALUES "
            "(700001, 0, 'ATCGATCG'), (700002, 0, 'GCTAGCTA')"
        )
        conn.execute(
            "INSERT INTO qiita_lake.reference_membership VALUES "
            "(1, 700001), (1, 700002)"
        )
    finally:
        conn.close()


@pytest.fixture
def flight_client(data_plane):
    client = flight.FlightClient(f"grpc://127.0.0.1:{data_plane['port']}")
    yield client
    client.close()


def _sign_ticket(table, filter_dict, secret_bytes):
    from qiita_control_plane.auth.tickets import sign_ticket

    return sign_ticket(table=table, filter=filter_dict, secret=secret_bytes)


def test_doget_reference_sequences(data_plane, flight_client):
    """DoGet with a valid ticket returns Arrow data from reference_sequences."""
    ticket_bytes = _sign_ticket(
        "reference_sequences",
        {"feature_idx": SEED_FEATURE_IDXS},
        data_plane["secret"],
    )
    reader = flight_client.do_get(flight.Ticket(ticket_bytes))
    table = reader.read_all()

    assert table.num_rows == 2
    assert set(table.column_names) >= {
        "feature_idx",
        "sequence_hash",
        "sequence_length_bp",
    }
    assert sorted(table.column("feature_idx").to_pylist()) == SEED_FEATURE_IDXS


def test_doget_tampered_ticket(data_plane, flight_client):
    """DoGet with a tampered ticket must fail with Unauthenticated."""
    ticket_bytes = bytearray(
        _sign_ticket("reference_sequences", {"feature_idx": [1]}, data_plane["secret"])
    )
    ticket_bytes[10] ^= 0xFF

    with pytest.raises(flight.FlightUnauthenticatedError):
        flight_client.do_get(flight.Ticket(bytes(ticket_bytes))).read_all()


def test_doget_empty_result(data_plane, flight_client):
    """DoGet for non-existent feature_idx returns empty table, not error."""
    ticket_bytes = _sign_ticket(
        "reference_sequences",
        {"feature_idx": [999999]},
        data_plane["secret"],
    )
    reader = flight_client.do_get(flight.Ticket(ticket_bytes))
    assert reader.read_all().num_rows == 0
