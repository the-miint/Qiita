"""End-to-end integration test for the upload domain.

Exercises the full mint-slot + DoPut path:

    1. POST /api/v1/upload via the in-process CP (mints qiita.upload row,
       returns a signed DoPut ticket).
    2. pyarrow.flight.FlightClient.do_put against the real data plane
       subprocess, streaming an Arrow batch against the ticket.
    3. Parse the data plane's PutResult body (sha256, row_count,
       bytes_received, staging_path).
    4. POST /api/v1/upload/{idx}/done forwarding those claims; CP
       transitions the row pending → ready.
    5. GET /api/v1/upload/{idx} confirms ready + the recorded claim.

Cross-language ticket verification is exercised implicitly: Python's
`sign_doput` produced the ticket bytes, Rust's `verify_doput` consumed
them on the data-plane side. If either side drifts, this test breaks.
"""

import base64

import pyarrow as pa
import pyarrow.flight as flight
import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    LOOPBACK_HOST,
    URL_UPLOAD_BY_IDX,
    URL_UPLOAD_DONE,
    URL_UPLOAD_PREFIX,
)
from qiita_control_plane.config import Settings as CPSettings
from qiita_control_plane.main import app as cp_app


@pytest.fixture
async def cp_client(postgres_pool, hmac_secret, human_admin_session):
    """ASGITransport-driven AsyncClient onto the CP with the integration
    pool + hmac_secret wired up. Uses the human_admin_session PAT (which
    carries TICKET_DOPUT per the session fixture)."""
    cp_app.state.pool = postgres_pool
    cp_app.state.settings = CPSettings(
        database_url="unused-in-test",
        hmac_secret_key=hmac_secret,
        data_plane_url="grpc://unused:0",
    )
    async with AsyncClient(
        transport=ASGITransport(app=cp_app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as client:
        yield client


@pytest.fixture
def flight_client(data_plane):
    client = flight.FlightClient(f"grpc://{LOOPBACK_HOST}:{data_plane['port']}")
    yield client
    client.close()


def _sample_record_batch() -> pa.RecordBatch:
    """Tiny content-agnostic batch — DoPut is schema-agnostic."""
    return pa.record_batch(
        {
            "read_id": ["seq-1", "seq-2", "seq-3"],
            "seq_length": pa.array([12, 34, 56], type=pa.int64()),
        }
    )


def _do_put_one_batch(
    client: flight.FlightClient,
    ticket_bytes: bytes,
    batch: pa.RecordBatch,
) -> dict:
    """Drive one DoPut from a single RecordBatch; return the PutResult body."""
    descriptor = flight.FlightDescriptor.for_command(ticket_bytes)
    writer, reader = client.do_put(descriptor, batch.schema)
    try:
        writer.write_batch(batch)
        writer.done_writing()
        # The server's PutResult arrives on the reader side as a Buffer
        # carrying the app_metadata bytes (which the data-plane stuffed
        # with a JSON object).
        put_metadata = reader.read()
    finally:
        writer.close()

    assert put_metadata is not None, "data plane returned no PutResult"
    import json as _json

    return _json.loads(bytes(put_metadata).decode())


async def test_doput_happy_path_round_trip(
    postgres_pool, cp_client, flight_client, data_plane
):
    """Mint slot → DoPut Arrow batch → done callback → verify ready state.

    Lock the cross-component contract: the CP-minted ticket verifies on
    the Rust side, the staged Parquet lands at the canonical path, the
    data plane's sha256 / row_count claims survive the round trip into
    the qiita.upload row.
    """
    # 1) Mint the upload slot.
    create_resp = await cp_client.post(
        URL_UPLOAD_PREFIX, json={"description": "doput e2e"}
    )
    assert create_resp.status_code == 201, create_resp.text
    upload_idx = create_resp.json()["upload_idx"]
    ticket_bytes = base64.b64decode(create_resp.json()["doput_ticket"])

    # 2) DoPut via the real data plane subprocess.
    batch = _sample_record_batch()
    put_body = _do_put_one_batch(flight_client, ticket_bytes, batch)

    assert put_body["upload_idx"] == upload_idx
    assert put_body["row_count"] == 3
    assert put_body["bytes_received"] > 0
    assert len(put_body["sha256"]) == 64  # 64 hex chars
    # PutResult body deliberately omits the server-side path — the client
    # never sees / depends on the staging layout. The test derives the
    # expected path from the (root, upload_idx) convention, which both CP
    # and DP know but the client doesn't.
    assert "staging_path" not in put_body, (
        "staging_path leaked in PutResult — clients should not see server paths"
    )
    expected_path = (
        f"{data_plane['upload_staging_root']}/uploads/{upload_idx}/upload.parquet"
    )

    # 3) Verify the file actually landed at that path with mode 440.
    import os
    import stat

    st = os.stat(expected_path)
    assert stat.S_IMODE(st.st_mode) == 0o440, (
        f"expected mode 440, got {oct(stat.S_IMODE(st.st_mode))}"
    )
    assert st.st_size == put_body["bytes_received"]

    # 4) Forward the claim to /done.
    done_resp = await cp_client.post(
        URL_UPLOAD_DONE.format(upload_idx=upload_idx),
        json={
            "sha256": put_body["sha256"],
            "row_count": put_body["row_count"],
            "bytes_received": put_body["bytes_received"],
        },
    )
    assert done_resp.status_code == 200, done_resp.text
    assert done_resp.json()["status"] == "ready"

    # 5) GET confirms the recorded claim.
    get_resp = await cp_client.get(URL_UPLOAD_BY_IDX.format(upload_idx=upload_idx))
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["status"] == "ready"
    assert body["sha256"] == put_body["sha256"]
    assert body["row_count"] == 3

    # Cleanup
    await postgres_pool.execute(
        "DELETE FROM qiita.upload WHERE upload_idx = $1", upload_idx
    )


async def test_doput_rejects_tampered_ticket(postgres_pool, cp_client, flight_client):
    """A ticket whose payload bytes are altered post-signing must trip the
    Rust verifier — surfaces as a Flight Unauthenticated/error on do_put."""
    create_resp = await cp_client.post(URL_UPLOAD_PREFIX, json={})
    upload_idx = create_resp.json()["upload_idx"]
    ticket_bytes = bytearray(base64.b64decode(create_resp.json()["doput_ticket"]))
    # Flip a payload byte (post the version + length prefix).
    ticket_bytes[10] ^= 0xFF

    batch = _sample_record_batch()
    # pyarrow surfaces gRPC errors variably (FlightError subclasses or
    # ArrowInvalid for INVALID_ARGUMENT). Catch the union.
    with pytest.raises((flight.FlightError, pa.ArrowInvalid)):
        _do_put_one_batch(flight_client, bytes(ticket_bytes), batch)

    # The upload row stays pending — nothing transitioned it.
    row = await postgres_pool.fetchrow(
        "SELECT status FROM qiita.upload WHERE upload_idx = $1", upload_idx
    )
    assert row["status"] == "pending"

    await postgres_pool.execute(
        "DELETE FROM qiita.upload WHERE upload_idx = $1", upload_idx
    )


async def test_doput_rejects_missing_ticket(flight_client):
    """A DoPut without the signed ticket on the descriptor.cmd must be
    rejected. pyarrow's `for_path` builds a path-style descriptor; the
    data plane's handler expects cmd-style and refuses cleanly."""
    batch = _sample_record_batch()
    descriptor = flight.FlightDescriptor.for_path("not-a-command")
    with pytest.raises((flight.FlightError, pa.ArrowInvalid)):
        writer, reader = flight_client.do_put(descriptor, batch.schema)
        try:
            writer.write_batch(batch)
            writer.done_writing()
            reader.read()
        finally:
            writer.close()
