"""CLI-side tests for `qiita reference load`.

Drives the programmatic entry point `cli.reference_load.do_reference_load`
with a mocked httpx transport + a fake Flight client. The full
integration path (real CP + real DP subprocess + DuckLake) lives in
`tests/integration/test_e2e_reference.py`; this file exercises only the
CLI orchestration: the call sequence, error propagation, and the
Arrow-conversion helpers.

The Flight client is faked because pyarrow.flight requires a running
gRPC server; tests at this tier should not spawn one. The fake records
the FlightDescriptor.cmd bytes (the signed DoPut ticket) and returns a
canned PutResult, which the CLI then forwards to /upload/{idx}/done.
"""

from __future__ import annotations

import base64
import json

import duckdb
import httpx
import pytest
from qiita_common.api_paths import (
    URL_REFERENCE_BY_IDX,
    URL_REFERENCE_PREFIX,
    URL_UPLOAD_DONE,
    URL_UPLOAD_PREFIX,
    URL_WORK_TICKET_BY_IDX,
    URL_WORK_TICKET_PREFIX,
)

# =============================================================================
# Fakes
# =============================================================================


class _FakeWriter:
    def __init__(self):
        self.batches = []
        self.done = False
        self.closed = False

    def write_batch(self, batch):
        self.batches.append(batch)

    def done_writing(self):
        self.done = True

    def close(self):
        self.closed = True


class _FakeReader:
    def __init__(self, put_metadata_bytes: bytes):
        self._payload = put_metadata_bytes

    def read(self):
        # pyarrow exposes the metadata as a Buffer-like — for the CLI's
        # use, returning the raw bytes object works because the helper
        # wraps `bytes(put_metadata)` before decoding.
        return self._payload


class FakeFlightClient:
    """Records each DoPut invocation and returns a scripted PutResult.
    The CLI calls `client.do_put(descriptor, schema)` → (writer, reader);
    we capture the ticket bytes from descriptor.cmd and return canned
    metadata."""

    def __init__(self):
        self.calls: list[bytes] = []
        # `responses` is a list of (upload_idx, sha256) tuples consumed
        # in order, one per do_put call. Empty → the CLI's invariant
        # check (put_body['upload_idx'] == upload_idx) drives the value.
        self.responses: list[dict] = []
        self._next_upload_idx = 1

    def queue_response(self, upload_idx: int, *, sha256: str = "a" * 64, row_count: int = 1):
        self.responses.append(
            {
                "upload_idx": upload_idx,
                "sha256": sha256,
                "row_count": row_count,
                "bytes_received": 1024,
            }
        )

    def do_put(self, descriptor, schema):
        self.calls.append(bytes(descriptor.command))
        if not self.responses:
            raise RuntimeError("FakeFlightClient: no scripted response remaining")
        body = self.responses.pop(0)
        return _FakeWriter(), _FakeReader(json.dumps(body).encode())

    def close(self):
        pass


@pytest.fixture
def upload_state():
    """Track minted slots: maps upload_idx → status. Lets the route
    fixture transition pending → ready on /done."""
    return {"next_idx": 100, "slots": {}}


@pytest.fixture
def reference_state():
    """Controls the `is_host` the mock GET /reference/{idx} reports — used by
    the `--host` + `--reference-idx` bind-path verification tests. Default
    false; tests that bind to a host reference flip it to true."""
    return {"is_host": False}


@pytest.fixture
def cp_transport(upload_state, reference_state):
    """Mock the CP REST surface the CLI hits: POST /reference,
    GET /reference/{idx}, POST /upload, POST /upload/{idx}/done,
    POST /work-ticket, GET /work-ticket/{idx}. Returns the AsyncTransport +
    the captured call log."""
    calls: list[tuple[str, str, dict | None]] = []
    work_tickets: dict[int, dict] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        calls.append((request.method, request.url.path, body))
        path = request.url.path
        if path == URL_REFERENCE_PREFIX and request.method == "POST":
            return httpx.Response(
                201,
                json={
                    "reference_idx": 999,
                    "name": body["name"],
                    "version": body["version"],
                    "kind": body["kind"],
                    "status": "pending",
                    "created_by_idx": 1,
                    "created_at": "2026-05-20T00:00:00Z",
                },
            )
        if path.startswith(f"{URL_REFERENCE_PREFIX}/") and request.method == "GET":
            ref_idx = int(path.split("/")[-1])
            return httpx.Response(
                200,
                json={
                    "reference_idx": ref_idx,
                    "name": "existing",
                    "version": "1.0",
                    "kind": "sequence_reference",
                    "status": "active",
                    "is_host": reference_state["is_host"],
                    "created_by_idx": 1,
                    "created_at": "2026-05-20T00:00:00Z",
                },
            )
        if path == URL_UPLOAD_PREFIX and request.method == "POST":
            upload_idx = upload_state["next_idx"]
            upload_state["next_idx"] += 1
            upload_state["slots"][upload_idx] = "pending"
            # Token bytes mimic the CP's signed payload shape; the fake
            # flight client doesn't verify, just records.
            ticket_bytes = f"signed-ticket-for-{upload_idx}".encode()
            return httpx.Response(
                201,
                json={
                    "upload_idx": upload_idx,
                    "doput_ticket": base64.b64encode(ticket_bytes).decode(),
                },
            )
        if path.startswith(f"{URL_UPLOAD_PREFIX}/") and path.endswith("/done"):
            upload_idx = int(path.split("/")[-2])
            upload_state["slots"][upload_idx] = "ready"
            return httpx.Response(
                200,
                json={
                    "upload_idx": upload_idx,
                    "status": "ready",
                    "sha256": body["sha256"],
                    "row_count": body["row_count"],
                    "bytes_received": body["bytes_received"],
                    "created_by_idx": 1,
                    "created_at": "2026-05-20T00:00:00Z",
                    "completed_at": "2026-05-20T00:00:01Z",
                },
            )
        if path == URL_WORK_TICKET_PREFIX and request.method == "POST":
            idx = 4242
            work_tickets[idx] = {"work_ticket_idx": idx, "state": "completed"}
            return httpx.Response(202, json={"work_ticket_idx": idx, "state": "pending"})
        if path.startswith(f"{URL_WORK_TICKET_PREFIX}/") and request.method == "GET":
            idx = int(path.split("/")[-1])
            return httpx.Response(200, json=work_tickets.get(idx, {"state": "completed"}))
        return httpx.Response(404, text=f"unhandled mock path: {request.method} {path}")

    transport = httpx.MockTransport(handler)
    return transport, calls


@pytest.fixture
def fasta_file(tmp_path):
    """Tiny FASTA the FASTA→Arrow helper can convert. Two records is
    enough; we don't assert on miint's output here, only that the helper
    completes and yields a Parquet."""
    path = tmp_path / "in.fasta"
    path.write_text(">r1\nACGT\n>r2\nTTTT\n")
    return path


@pytest.fixture
def taxonomy_file(tmp_path):
    path = tmp_path / "tax.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE t (feature_id VARCHAR, taxonomy VARCHAR)")
        conn.execute("INSERT INTO t VALUES ('r1', 'd__Bacteria; p__; c__; o__; f__; g__; s__')")
        conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    return path


# =============================================================================
# Happy path
# =============================================================================


async def test_do_reference_load_happy_path(
    fasta_file, taxonomy_file, tmp_path, cp_transport, upload_state
):
    """Happy path: create reference, upload FASTA + taxonomy, submit
    work_ticket with both handles in action_context, watch poll returns
    `completed` on first read. Asserts the call sequence + the
    action_context shape."""
    from qiita_control_plane.cli.reference_load import do_reference_load

    transport, calls = cp_transport
    flight_client = FakeFlightClient()
    flight_client.queue_response(100)  # FASTA upload — assumes first slot is 100
    flight_client.queue_response(101)  # taxonomy upload

    async with httpx.AsyncClient(transport=transport, base_url="http://cp.test") as http:
        result = await do_reference_load(
            http=http,
            token="test-pat",
            flight_client=flight_client,
            fasta_path=fasta_file,
            taxonomy_path=taxonomy_file,
            name="cli-test",
            version="1.0",
            kind="sequence_reference",
            watch=True,
            poll_interval_seconds=0.01,
        )

    assert result["reference_idx"] == 999
    assert result["work_ticket_idx"] == 4242
    assert result["upload_idxs"] == {"fasta": 100, "taxonomy": 101}
    assert result["work_ticket"]["state"] == "completed"

    # Call order: reference → fasta upload → fasta done → taxonomy upload →
    # taxonomy done → work-ticket submit → at least one work-ticket GET.
    method_paths = [(m, p) for (m, p, _b) in calls]
    assert method_paths[0] == ("POST", URL_REFERENCE_PREFIX)
    assert method_paths[1] == ("POST", URL_UPLOAD_PREFIX)
    assert method_paths[2] == ("POST", URL_UPLOAD_DONE.format(upload_idx=100))
    assert method_paths[3] == ("POST", URL_UPLOAD_PREFIX)
    assert method_paths[4] == ("POST", URL_UPLOAD_DONE.format(upload_idx=101))
    assert method_paths[5] == ("POST", URL_WORK_TICKET_PREFIX)
    assert any(
        m == "GET" and p == URL_WORK_TICKET_BY_IDX.format(work_ticket_idx=4242)
        for m, p in method_paths
    )

    # Work-ticket submission body carries the upload handles, NOT
    # filesystem paths.
    submit_call = next(c for c in calls if c[1] == URL_WORK_TICKET_PREFIX and c[0] == "POST")
    assert submit_call[2]["action_context"] == {"fasta_upload_idx": 100, "taxonomy_upload_idx": 101}
    assert submit_call[2]["scope_target"] == {"kind": "reference", "reference_idx": 999}

    # Default (non-host): the plain reference-add action, and the created
    # reference carries is_host=false.
    assert submit_call[2]["action_id"] == "reference-add"
    create_call = next(c for c in calls if c[1] == URL_REFERENCE_PREFIX and c[0] == "POST")
    assert create_call[2]["is_host"] is False

    # Two DoPut calls fired with distinct ticket payloads.
    assert flight_client.calls == [b"signed-ticket-for-100", b"signed-ticket-for-101"]
    # Slots both ended at ready (the /done call fired for each).
    assert upload_state["slots"] == {100: "ready", 101: "ready"}


async def test_do_reference_load_host_sets_is_host_and_selects_host_action(
    fasta_file, taxonomy_file, tmp_path, cp_transport, upload_state
):
    """`--host` creates the reference with is_host=true and submits the
    `host-reference-add` action (which appends the rype-index build) instead
    of `reference-add`. Taxonomy is supplied — host references require it."""
    from qiita_control_plane.cli.reference_load import do_reference_load

    transport, calls = cp_transport
    flight_client = FakeFlightClient()
    flight_client.queue_response(100)  # FASTA
    flight_client.queue_response(101)  # taxonomy

    async with httpx.AsyncClient(transport=transport, base_url="http://cp.test") as http:
        result = await do_reference_load(
            http=http,
            token="t",
            flight_client=flight_client,
            fasta_path=fasta_file,
            taxonomy_path=taxonomy_file,
            name="host-ref",
            version="1.0",
            host=True,
            watch=False,
        )

    assert result["reference_idx"] == 999
    create_call = next(c for c in calls if c[1] == URL_REFERENCE_PREFIX and c[0] == "POST")
    assert create_call[2]["is_host"] is True
    submit_call = next(c for c in calls if c[1] == URL_WORK_TICKET_PREFIX and c[0] == "POST")
    assert submit_call[2]["action_id"] == "host-reference-add"
    # action_context still carries both upload handles.
    assert submit_call[2]["action_context"] == {"fasta_upload_idx": 100, "taxonomy_upload_idx": 101}


async def test_do_reference_load_host_requires_taxonomy(fasta_file, tmp_path, cp_transport):
    """`--host` without `--taxonomy` is a contract violation — a host
    reference's rype index needs the taxonomy mapping authority. The check
    fires before any network call (no reference created, no upload)."""
    from qiita_control_plane.cli.reference_load import do_reference_load

    transport, calls = cp_transport

    async with httpx.AsyncClient(transport=transport, base_url="http://cp.test") as http:
        with pytest.raises(ValueError, match="taxonomy"):
            await do_reference_load(
                http=http,
                token="t",
                flight_client=FakeFlightClient(),
                fasta_path=fasta_file,
                name="host-ref",
                version="1.0",
                host=True,
                watch=False,
            )

    # Fail-fast: nothing hit the wire.
    assert calls == []


async def test_do_reference_load_host_with_reference_idx_rejects_non_host(
    fasta_file, taxonomy_file, tmp_path, cp_transport, reference_state
):
    """`--host --reference-idx N` against a reference whose is_host=false is
    rejected: is_host is write-once at creation, so running host-reference-add
    against a non-host reference would be a silent mismatch. The CLI GETs the
    reference, sees is_host=false, and raises before any upload / work-ticket."""
    from qiita_control_plane.cli.reference_load import do_reference_load

    transport, calls = cp_transport
    reference_state["is_host"] = False  # the bound reference is NOT a host ref

    async with httpx.AsyncClient(transport=transport, base_url="http://cp.test") as http:
        with pytest.raises(ValueError, match="is_host=false"):
            await do_reference_load(
                http=http,
                token="t",
                flight_client=FakeFlightClient(),
                fasta_path=fasta_file,
                taxonomy_path=taxonomy_file,
                reference_idx=42,
                host=True,
                watch=False,
            )

    # Verified the reference, then bailed: no upload, no work-ticket submit.
    method_paths = [(m, p) for (m, p, _b) in calls]
    assert ("GET", URL_REFERENCE_BY_IDX.format(reference_idx=42)) in method_paths
    assert not any(p == URL_UPLOAD_PREFIX for (_m, p) in method_paths)
    assert not any(p == URL_WORK_TICKET_PREFIX for (_m, p) in method_paths)


async def test_do_reference_load_host_with_reference_idx_allows_host_ref(
    fasta_file, taxonomy_file, tmp_path, cp_transport, reference_state
):
    """`--host --reference-idx N` against a genuine host reference proceeds —
    the legitimate re-run / regenerate-index flow. No POST /reference (binding),
    but the host-reference-add action is submitted."""
    from qiita_control_plane.cli.reference_load import do_reference_load

    transport, calls = cp_transport
    reference_state["is_host"] = True  # the bound reference IS a host ref
    flight_client = FakeFlightClient()
    flight_client.queue_response(100)  # FASTA
    flight_client.queue_response(101)  # taxonomy

    async with httpx.AsyncClient(transport=transport, base_url="http://cp.test") as http:
        result = await do_reference_load(
            http=http,
            token="t",
            flight_client=flight_client,
            fasta_path=fasta_file,
            taxonomy_path=taxonomy_file,
            reference_idx=42,
            host=True,
            watch=False,
        )

    assert result["reference_idx"] == 42
    assert not any(p == URL_REFERENCE_PREFIX and m == "POST" for (m, p, _b) in calls), (
        "binding to an existing reference must not POST /reference"
    )
    submit_call = next(c for c in calls if c[1] == URL_WORK_TICKET_PREFIX and c[0] == "POST")
    assert submit_call[2]["action_id"] == "host-reference-add"


def test_handler_host_without_taxonomy_exits_nonzero(monkeypatch, tmp_path, capsys):
    """End-to-end arg plumbing: `qiita reference load --host` (no --taxonomy)
    threads `host=True` into the entry point, whose taxonomy guard surfaces as
    exit 1 with a stderr line. Locks both the `--host` flag and its wiring."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import user as _user

    monkeypatch.setattr(_common, "read_token", lambda: "test-pat")
    fasta = tmp_path / "x.fasta"
    fasta.write_text(">a\nACGT\n")

    rc = _user.main(
        [
            "--base-url",
            "http://localhost:8080",
            "reference",
            "load",
            "--name",
            "h",
            "--version",
            "1.0",
            "--host",
            "--fasta",
            str(fasta),
            "--data-plane-url",
            "grpc://localhost:0",
            "--no-watch",
        ]
    )
    assert rc == 1
    assert "taxonomy" in capsys.readouterr().err


async def test_do_reference_load_skips_creation_when_reference_idx_set(
    fasta_file, tmp_path, cp_transport
):
    """With --reference-idx, POST /reference is skipped — the CLI binds
    to an existing reference instead of creating one."""
    from qiita_control_plane.cli.reference_load import do_reference_load

    transport, calls = cp_transport
    flight_client = FakeFlightClient()
    flight_client.queue_response(100)

    async with httpx.AsyncClient(transport=transport, base_url="http://cp.test") as http:
        result = await do_reference_load(
            http=http,
            token="t",
            flight_client=flight_client,
            fasta_path=fasta_file,
            reference_idx=42,
            watch=False,
        )

    assert result["reference_idx"] == 42
    assert not any(p == URL_REFERENCE_PREFIX for (_m, p, _b) in calls), (
        "POST /reference should not fire when --reference-idx is supplied"
    )


async def test_do_reference_load_no_watch_returns_without_polling(
    fasta_file, tmp_path, cp_transport
):
    """--no-watch returns immediately after work_ticket submission; no
    GET /work-ticket/{idx} fires."""
    from qiita_control_plane.cli.reference_load import do_reference_load

    transport, calls = cp_transport
    flight_client = FakeFlightClient()
    flight_client.queue_response(100)

    async with httpx.AsyncClient(transport=transport, base_url="http://cp.test") as http:
        result = await do_reference_load(
            http=http,
            token="t",
            flight_client=flight_client,
            fasta_path=fasta_file,
            name="cli-test",
            version="1.0",
            watch=False,
        )

    assert "work_ticket" not in result
    assert not any(m == "GET" for (m, _p, _b) in calls)


# =============================================================================
# Failure paths — explicit, no silent retry
# =============================================================================


async def test_do_reference_load_fail_loud_on_doput_error(fasta_file, tmp_path, cp_transport):
    """A Flight error mid-DoPut propagates verbatim. The CLI must NOT
    retry silently — the operator sees the original failure and decides
    whether to re-run with a fresh upload slot."""
    from qiita_control_plane.cli.reference_load import do_reference_load

    transport, calls = cp_transport

    class _BrokenFlightClient(FakeFlightClient):
        def do_put(self, descriptor, schema):
            raise RuntimeError("simulated network drop mid-DoPut")

    flight_client = _BrokenFlightClient()

    async with httpx.AsyncClient(transport=transport, base_url="http://cp.test") as http:
        with pytest.raises(RuntimeError, match="simulated network drop mid-DoPut"):
            await do_reference_load(
                http=http,
                token="t",
                flight_client=flight_client,
                fasta_path=fasta_file,
                name="cli-test",
                version="1.0",
                watch=False,
            )

    # POST /reference + POST /upload fired; /done never did, and no
    # work-ticket was submitted. The upload row stays at status='pending'
    # — the operator can clean up manually or wait for a sweep.
    method_paths = [(m, p) for (m, p, _b) in calls]
    assert ("POST", URL_REFERENCE_PREFIX) in method_paths
    assert ("POST", URL_UPLOAD_PREFIX) in method_paths
    assert not any("/done" in p for (_m, p) in method_paths)
    assert not any(p == URL_WORK_TICKET_PREFIX for (_m, p) in method_paths)


async def test_do_reference_load_rejects_ambiguous_reference_selection(fasta_file, tmp_path):
    """Both --reference-idx AND --name/--version is a contract violation —
    the caller must pick one. ValueError surfaces directly."""
    from qiita_control_plane.cli.reference_load import do_reference_load

    async with httpx.AsyncClient(base_url="http://x") as http:
        with pytest.raises(ValueError, match="exactly one of"):
            await do_reference_load(
                http=http,
                token="t",
                flight_client=FakeFlightClient(),
                fasta_path=fasta_file,
                name="x",
                version="1.0",
                reference_idx=42,
            )


# =============================================================================
# Arrow streaming helpers
# =============================================================================


def test_blob_upload_stream_chunks_bytes(tmp_path):
    """Newick / jplace inputs stream as chunked `(chunk_index, chunk_data
    BLOB)` Arrow batches. The helper must walk the source file in
    bounded reads and emit ordered chunks whose concatenation
    round-trips to the source bytes."""
    from qiita_control_plane.cli.reference_load import _blob_upload_stream

    src = tmp_path / "tree.nwk"
    src.write_text("(a:0.1,b:0.2);")
    with _blob_upload_stream(src) as stream:
        schema_names = stream.schema.names
        assert schema_names == ["chunk_index", "chunk_data"]
        batches = list(stream.batches)

    # Concatenate every batch's chunk_data column in chunk_index order.
    pairs: list[tuple[int, bytes]] = []
    for batch in batches:
        for idx, data in zip(
            batch.column("chunk_index").to_pylist(),
            batch.column("chunk_data").to_pylist(),
        ):
            pairs.append((idx, data))
    pairs.sort()
    reassembled = b"".join(data for _idx, data in pairs)
    assert reassembled == b"(a:0.1,b:0.2);"


def test_blob_upload_stream_decompresses_gzip(tmp_path):
    """`.gz` inputs are read transparently — chunk_data carries the
    decompressed bytes so the server's stitched temp file is valid
    plaintext for miint's `read_newick`/`read_jplace`. Matches the FASTA
    streamer's treatment; GG2 backbone ships the tree as `.nwk.gz`."""
    import gzip

    from qiita_control_plane.cli.reference_load import _blob_upload_stream

    payload = b"((seq1:0.1,seq2:0.2):0.3,seq3:0.4);"
    src = tmp_path / "tree.nwk.gz"
    with gzip.open(src, "wb") as f:
        f.write(payload)

    with _blob_upload_stream(src) as stream:
        batches = list(stream.batches)
    pairs: list[tuple[int, bytes]] = []
    for batch in batches:
        for idx, data in zip(
            batch.column("chunk_index").to_pylist(),
            batch.column("chunk_data").to_pylist(),
        ):
            pairs.append((idx, data))
    pairs.sort()
    reassembled = b"".join(data for _idx, data in pairs)
    assert reassembled == payload


def test_passthrough_parquet_stream_iterates_source_batches(taxonomy_file):
    """Passthrough streamer must yield every input row through its own
    Parquet batches without dropping or reordering."""
    from qiita_control_plane.cli.reference_load import _passthrough_parquet_stream

    with _passthrough_parquet_stream(taxonomy_file) as stream:
        batches = list(stream.batches)
    with duckdb.connect(":memory:") as conn:
        src_rows = conn.execute(f"SELECT * FROM read_parquet('{taxonomy_file}')").fetchall()
    streamed_rows: list[tuple] = []
    for batch in batches:
        streamed_rows.extend(tuple(row.values()) for row in batch.to_pylist())
    assert streamed_rows == src_rows


def test_open_upload_stream_rejects_unknown_role(tmp_path):
    """A typo'd role surfaces as ValueError — the CLI doesn't silently
    treat unknown roles as passthrough."""
    from qiita_control_plane.cli.reference_load import _open_upload_stream

    with pytest.raises(ValueError, match="unknown upload role"):
        with _open_upload_stream(tmp_path / "x", role="qiime2_artifact"):
            pass


def test_fasta_upload_stream_chunks_via_read_fastx(tmp_path):
    """FASTA streams as chunked `(read_id, chunk_index, chunk_data)` Arrow
    batches via miint read_fastx — schema names + reassembled per-read
    sequence round-trip to the source records (read_id = header first token)."""
    from qiita_control_plane.cli.reference_load import _fasta_upload_stream

    src = tmp_path / "in.fasta"
    src.write_text(">r1 desc dropped\nACGTACGT\n>r2\nTTTT\n")
    with _fasta_upload_stream(src) as stream:
        assert stream.schema.names == ["read_id", "chunk_index", "chunk_data"]
        batches = list(stream.batches)

    by_read: dict[str, list[tuple[int, str]]] = {}
    for batch in batches:
        rows = batch.to_pylist()
        for row in rows:
            by_read.setdefault(row["read_id"], []).append((row["chunk_index"], row["chunk_data"]))
    reassembled = {rid: "".join(d for _i, d in sorted(parts)) for rid, parts in by_read.items()}
    assert reassembled == {"r1": "ACGTACGT", "r2": "TTTT"}


def test_fasta_upload_stream_rejects_empty_file(tmp_path):
    """An empty FASTA is rejected with a clear message instead of a raw
    read_fastx 'Empty file' error (matching stage_local_fasta's guard)."""
    from qiita_control_plane.cli.reference_load import _fasta_upload_stream

    empty = tmp_path / "empty.fasta"
    empty.write_text("")
    with pytest.raises(ValueError, match="no records"):
        with _fasta_upload_stream(empty):
            pass


# Smoke check that the asyncio entry point is callable from a sync test
# context — user.py's CLI handler does `asyncio.run(_run_reference_load(...))`.
def test_handler_returns_nonzero_on_bad_args(monkeypatch, tmp_path, capsys):
    """`qiita reference load` with neither --reference-idx nor
    --name/--version surfaces as exit 1 with a stderr line — argparse
    accepts the args, but the entry point's XOR check fires."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import user as _user

    monkeypatch.setattr(_common, "read_token", lambda: "test-pat")
    fasta = tmp_path / "x.fasta"
    fasta.write_text(">a\nACGT\n")

    # No --name / --version / --reference-idx supplied. Use http://localhost
    # so the base-URL validator's "no plain http to non-localhost" check
    # passes without --insecure (we're driving the XOR check, not the URL
    # gate, so the host doesn't matter beyond passing validation).
    rc = _user.main(
        [
            "--base-url",
            "http://localhost:8080",
            "reference",
            "load",
            "--fasta",
            str(fasta),
            "--data-plane-url",
            "grpc://localhost:0",
            "--no-watch",
        ]
    )
    assert rc == 1
    captured = capsys.readouterr()
    assert "exactly one of" in captured.err
