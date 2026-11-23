"""Tests for qiita_compute_orchestrator.data_plane_client.

Exercises the CO→CP ticket-fetch adapter with httpx.MockTransport so no real
HTTP server is needed — the wire shape (URL, JSON body with/without feature_idx,
the base64-encoded `ticket` response field) is captured here; the CP-side route
has its own DB-tier tests upstream. The streaming half (open_doget_stream)
is exercised end-to-end against a live data plane in the integration suite.

`open_reference_chunk_stream` (the composed seam the shard builders import) is
unit-tested here with its two underlying calls monkeypatched — the composition
(ticket fetch → stream) is the unit; the end-to-end stream is the integration
suite's job.
"""

from __future__ import annotations

import base64
import json
from contextlib import asynccontextmanager, contextmanager

import httpx
import pytest
from qiita_common.api_paths import (
    URL_ALIGNMENT_DOGET,
    URL_ASSEMBLY_DOGET,
    URL_READ_MASKED_DOGET,
    URL_REFERENCE_DOGET,
)
from qiita_common.assembly_constants import (
    ASSEMBLED_SEQUENCE_CHUNKS_TABLE,
    ASSEMBLED_SEQUENCE_TABLE,
)

import qiita_compute_orchestrator.data_plane_client as dpc
from qiita_compute_orchestrator.data_plane_client import (
    fetch_alignment_doget_ticket,
    fetch_assembly_doget_ticket,
    fetch_read_masked_doget_ticket,
    fetch_reference_doget_ticket,
)


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url="http://cp.test",
        transport=httpx.MockTransport(handler),
        headers={"Authorization": "Bearer test-sa-pat"},
    )


_RAW_TICKET = b"\x01\x00\x00\x00\x08payload-bytes-and-mac"


def _ticket_response() -> httpx.Response:
    return httpx.Response(
        201,
        content=json.dumps({"ticket": base64.b64encode(_RAW_TICKET).decode()}),
        headers={"content-type": "application/json"},
    )


async def test_fetch_with_feature_idx_sends_subset_and_returns_raw_bytes():
    """feature_idx present → body carries {table, feature_idx}; the base64 ticket
    is decoded back to the raw signed bytes open_doget_stream wraps."""
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return _ticket_response()

    async with _client(handler) as http:
        result = await fetch_reference_doget_ticket(
            http=http,
            reference_idx=7,
            table="reference_sequence_chunks",
            feature_idx=[11, 22],
        )

    assert result == _RAW_TICKET
    assert len(captured) == 1
    assert captured[0].url.path == URL_REFERENCE_DOGET.format(reference_idx=7)
    assert json.loads(captured[0].content) == {
        "table": "reference_sequence_chunks",
        "feature_idx": [11, 22],
    }


async def test_fetch_without_feature_idx_omits_the_key():
    """feature_idx None → the key is omitted (whole-reference ticket), never
    sent as an empty list (which the CP rejects)."""
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return _ticket_response()

    async with _client(handler) as http:
        result = await fetch_reference_doget_ticket(
            http=http,
            reference_idx=7,
            table="reference_sequence_chunks",
        )

    assert result == _RAW_TICKET
    assert json.loads(captured[0].content) == {"table": "reference_sequence_chunks"}


@pytest.mark.parametrize("status", [404, 409, 403, 500])
async def test_fetch_raises_on_non_2xx(status):
    """Any non-2xx (missing 404, wrong-status 409, missing-scope 403, 5xx) raises
    HTTPStatusError for the caller to map to a BackendFailure."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=json.dumps({"detail": "nope"}))

    async with _client(handler) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_reference_doget_ticket(
                http=http, reference_idx=7, table="reference_sequence_chunks"
            )


async def test_open_reference_chunk_stream_composes_ticket_and_stream(monkeypatch):
    """The composed seam fetches a `feature_idx`-scoped ticket (over a CP client
    that is closed BEFORE the stream opens), then streams that ticket against the
    settings' data_plane_url, yielding the registered relation."""
    captured: dict = {}

    @asynccontextmanager
    async def fake_make_cp_client():
        captured["cp_client_open"] = True
        yield object()  # the http client; fetch is stubbed so it's never used
        captured["cp_client_closed"] = True

    async def fake_fetch(*, http, reference_idx, table, feature_idx):
        captured["fetch"] = {
            "reference_idx": reference_idx,
            "table": table,
            "feature_idx": feature_idx,
        }
        return b"signed-ticket-bytes"

    @contextmanager
    def fake_stream(conn, *, data_plane_url, ticket_bytes, relation):
        captured["stream"] = {
            "conn": conn,
            "data_plane_url": data_plane_url,
            "ticket_bytes": ticket_bytes,
            "relation": relation,
        }
        yield relation

    class _Settings:
        data_plane_url = "grpc://dp.test:50051"

    monkeypatch.setattr(dpc, "make_cp_client", fake_make_cp_client)
    monkeypatch.setattr(dpc, "fetch_reference_doget_ticket", fake_fetch)
    monkeypatch.setattr(dpc, "open_doget_stream", fake_stream)
    monkeypatch.setattr(dpc, "get_settings", lambda: _Settings())

    sentinel_conn = object()
    async with dpc.open_reference_chunk_stream(
        sentinel_conn, reference_idx=9, feature_idx=[11, 22], relation="reference_chunks"
    ) as rel:
        assert rel == "reference_chunks"
        # The CP client is closed before the stream body runs (ticket already minted).
        assert captured["cp_client_closed"] is True

    assert captured["fetch"] == {
        "reference_idx": 9,
        "table": "reference_sequence_chunks",
        "feature_idx": [11, 22],
    }
    assert captured["stream"]["conn"] is sentinel_conn
    assert captured["stream"]["data_plane_url"] == "grpc://dp.test:50051"
    assert captured["stream"]["ticket_bytes"] == b"signed-ticket-bytes"
    assert captured["stream"]["relation"] == "reference_chunks"


async def test_open_reference_chunk_stream_passes_none_feature_idx(monkeypatch):
    """A whole-reference stream (feature_idx=None) flows None straight through to
    the ticket fetch (never coerced to `[]`, which the CP rejects)."""
    captured: dict = {}

    @asynccontextmanager
    async def fake_make_cp_client():
        yield object()

    async def fake_fetch(*, http, reference_idx, table, feature_idx):
        captured["feature_idx"] = feature_idx
        return b"t"

    @contextmanager
    def fake_stream(conn, *, data_plane_url, ticket_bytes, relation):
        yield relation

    class _Settings:
        data_plane_url = "grpc://dp.test:50051"

    monkeypatch.setattr(dpc, "make_cp_client", fake_make_cp_client)
    monkeypatch.setattr(dpc, "fetch_reference_doget_ticket", fake_fetch)
    monkeypatch.setattr(dpc, "open_doget_stream", fake_stream)
    monkeypatch.setattr(dpc, "get_settings", lambda: _Settings())

    async with dpc.open_reference_chunk_stream(object(), reference_idx=3, feature_idx=None) as rel:
        assert rel == "reference_chunks"
    assert captured["feature_idx"] is None


# ---------------------------------------------------------------------------
# Alignment DoGet (feature-table job) — mints by work_ticket_idx only; the CP
# derives alignment_idx + the cohort from the ticket's action_context.
# ---------------------------------------------------------------------------


async def test_fetch_alignment_doget_sends_work_ticket_idx_and_columns():
    """The body carries the scope key and the projection, and nothing else.

    Still no table / alignment_idx / cohort — the CP reads those from
    action_context. `columns` is the one thing the caller genuinely owns: only
    the consumer knows which columns it will bind, so it names them and the CP
    validates and signs them. The base64 ticket is decoded back to raw bytes.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return _ticket_response()

    async with _client(handler) as http:
        result = await fetch_alignment_doget_ticket(
            http=http, work_ticket_idx=42, columns=("feature_idx", "cigar")
        )

    assert result == _RAW_TICKET
    assert len(captured) == 1
    assert captured[0].url.path == URL_ALIGNMENT_DOGET
    assert json.loads(captured[0].content) == {
        "work_ticket_idx": 42,
        "columns": ["feature_idx", "cigar"],
        # Always on the wire, and False unless the caller asked for the de novo
        # arm: the CP defaults it too, so an omitted key would mean the same thing
        # — but sending it is what makes a wrong default here a visible diff
        # rather than a silently reference-only combined table.
        "denovo": False,
    }


@pytest.mark.parametrize("status", [404, 422, 403, 500])
async def test_fetch_alignment_doget_raises_on_non_2xx(status):
    """Any non-2xx (missing 404, bad-scope 422, missing-scope 403, 5xx) raises
    HTTPStatusError for the caller to map to a BackendFailure."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=json.dumps({"detail": "nope"}))

    async with _client(handler) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_alignment_doget_ticket(
                http=http, work_ticket_idx=42, columns=("feature_idx",)
            )


async def test_open_alignment_stream_composes_ticket_and_stream(monkeypatch):
    """The composed seam fetches a work-ticket-scoped alignment ticket (over a CP
    client closed BEFORE the stream opens), then streams that ticket against the
    settings' data_plane_url, yielding the registered relation."""
    captured: dict = {}

    @asynccontextmanager
    async def fake_make_cp_client():
        captured["cp_client_open"] = True
        yield object()
        captured["cp_client_closed"] = True

    async def fake_fetch(*, http, work_ticket_idx, columns, denovo):
        captured["work_ticket_idx"] = work_ticket_idx
        captured["columns"] = list(columns)
        captured["denovo"] = denovo
        return b"signed-alignment-ticket"

    @contextmanager
    def fake_stream(conn, *, data_plane_url, ticket_bytes, relation):
        captured["stream"] = {
            "conn": conn,
            "data_plane_url": data_plane_url,
            "ticket_bytes": ticket_bytes,
            "relation": relation,
        }
        yield relation

    class _Settings:
        data_plane_url = "grpc://dp.test:50051"

    monkeypatch.setattr(dpc, "make_cp_client", fake_make_cp_client)
    monkeypatch.setattr(dpc, "fetch_alignment_doget_ticket", fake_fetch)
    monkeypatch.setattr(dpc, "open_doget_stream", fake_stream)
    monkeypatch.setattr(dpc, "get_settings", lambda: _Settings())

    sentinel_conn = object()
    async with dpc.open_alignment_stream(
        sentinel_conn, work_ticket_idx=42, columns=("feature_idx", "position")
    ) as rel:
        assert rel == "alignment"
        assert captured["cp_client_closed"] is True

    assert captured["work_ticket_idx"] == 42
    # The projection is the caller's, carried through untouched — the seam does
    # not substitute a default of its own, which is what keeps the consumer the
    # single source of truth for which columns it binds.
    assert captured["columns"] == ["feature_idx", "position"]
    assert captured["stream"]["conn"] is sentinel_conn
    assert captured["stream"]["data_plane_url"] == "grpc://dp.test:50051"
    assert captured["stream"]["ticket_bytes"] == b"signed-alignment-ticket"
    assert captured["stream"]["relation"] == "alignment"


async def test_open_reference_sequences_stream_mints_whole_reference(monkeypatch):
    """The lengths stream mints a `reference_sequences` ticket for the WHOLE
    reference (feature_idx=None — coverage needs every contig's length, including
    unaligned ones) and streams it as `reference_lengths`."""
    captured: dict = {}

    @asynccontextmanager
    async def fake_make_cp_client():
        yield object()

    async def fake_fetch(*, http, reference_idx, table, feature_idx):
        captured["fetch"] = {
            "reference_idx": reference_idx,
            "table": table,
            "feature_idx": feature_idx,
        }
        return b"signed-lengths-ticket"

    @contextmanager
    def fake_stream(conn, *, data_plane_url, ticket_bytes, relation):
        captured["relation"] = relation
        captured["ticket_bytes"] = ticket_bytes
        yield relation

    class _Settings:
        data_plane_url = "grpc://dp.test:50051"

    monkeypatch.setattr(dpc, "make_cp_client", fake_make_cp_client)
    monkeypatch.setattr(dpc, "fetch_reference_doget_ticket", fake_fetch)
    monkeypatch.setattr(dpc, "open_doget_stream", fake_stream)
    monkeypatch.setattr(dpc, "get_settings", lambda: _Settings())

    async with dpc.open_reference_sequences_stream(object(), reference_idx=9) as rel:
        assert rel == "reference_lengths"

    assert captured["fetch"] == {
        "reference_idx": 9,
        "table": "reference_sequences",
        "feature_idx": None,
    }
    assert captured["ticket_bytes"] == b"signed-lengths-ticket"


# ---------------------------------------------------------------------------
# Assembly DoGet (one run's contigs) — the body names the RUN, never features.
# ---------------------------------------------------------------------------


async def test_fetch_assembly_doget_sends_the_run_pair_and_table():
    """The body carries `(prep_sample_idx, processing_idx)` and the table, and
    nothing else — a job names the run, never a contig."""
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return _ticket_response()

    async with _client(handler) as http:
        result = await fetch_assembly_doget_ticket(
            http=http,
            prep_sample_idx=11,
            processing_idx=22,
            table=ASSEMBLED_SEQUENCE_TABLE,
        )

    assert result == _RAW_TICKET
    assert len(captured) == 1
    assert captured[0].url.path == URL_ASSEMBLY_DOGET
    assert json.loads(captured[0].content) == {
        "prep_sample_idx": 11,
        "processing_idx": 22,
        "table": ASSEMBLED_SEQUENCE_TABLE,
    }


@pytest.mark.parametrize("status", [404, 422, 403, 500])
async def test_fetch_assembly_doget_raises_on_non_2xx(status):
    """Any non-2xx (404 a pair that never assembled, 422 an unknown table, 403
    missing scope, 5xx) raises HTTPStatusError for the caller to map to a
    BackendFailure."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=json.dumps({"detail": "nope"}))

    async with _client(handler) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_assembly_doget_ticket(
                http=http,
                prep_sample_idx=11,
                processing_idx=22,
                table=ASSEMBLED_SEQUENCE_CHUNKS_TABLE,
            )


async def test_open_assembly_chunk_stream_composes_ticket_and_stream(monkeypatch):
    """The composed seam mints a run-scoped ticket for the CHUNKS table (over a CP
    client closed BEFORE the stream opens), then streams it against the settings'
    data_plane_url. The table is the seam's, not the caller's — a job asks for
    "this run's contigs" and names neither the table nor a feature."""
    captured: dict = {}

    @asynccontextmanager
    async def fake_make_cp_client():
        captured["cp_client_open"] = True
        yield object()
        captured["cp_client_closed"] = True

    async def fake_fetch(*, http, prep_sample_idx, processing_idx, table):
        captured["fetch"] = {
            "prep_sample_idx": prep_sample_idx,
            "processing_idx": processing_idx,
            "table": table,
        }
        return b"signed-assembly-ticket"

    @contextmanager
    def fake_stream(conn, *, data_plane_url, ticket_bytes, relation):
        captured["stream"] = {
            "conn": conn,
            "data_plane_url": data_plane_url,
            "ticket_bytes": ticket_bytes,
            "relation": relation,
        }
        yield relation

    class _Settings:
        data_plane_url = "grpc://dp.test:50051"

    monkeypatch.setattr(dpc, "make_cp_client", fake_make_cp_client)
    monkeypatch.setattr(dpc, "fetch_assembly_doget_ticket", fake_fetch)
    monkeypatch.setattr(dpc, "open_doget_stream", fake_stream)
    monkeypatch.setattr(dpc, "get_settings", lambda: _Settings())

    sentinel_conn = object()
    async with dpc.open_assembly_chunk_stream(
        sentinel_conn, prep_sample_idx=11, processing_idx=22
    ) as rel:
        assert rel == "assembly_chunks"
        assert captured["cp_client_closed"] is True

    assert captured["fetch"] == {
        "prep_sample_idx": 11,
        "processing_idx": 22,
        "table": ASSEMBLED_SEQUENCE_CHUNKS_TABLE,
    }
    assert captured["stream"]["conn"] is sentinel_conn
    assert captured["stream"]["data_plane_url"] == "grpc://dp.test:50051"
    assert captured["stream"]["ticket_bytes"] == b"signed-assembly-ticket"
    assert captured["stream"]["relation"] == "assembly_chunks"


# ---------------------------------------------------------------------------
# read_masked DoGet (per-sample masked reads) — mints by (prep_sample_idx,
# mask_idx). Unlike the block/alignment mints, BOTH scope keys ride the wire:
# the scope is one pair, not a member list there would be a reason to keep
# CP-side, and the pair is the data plane's read_masked macro arguments.
# ---------------------------------------------------------------------------


async def test_fetch_read_masked_doget_sends_both_scope_keys():
    """The body carries exactly `prep_sample_idx` + `mask_idx`, and the base64
    ticket is decoded back to raw bytes.

    Whole-body equality, not a subset: the contract this holds is "the CO sends
    these two keys and no others". The CP's own `extra="forbid"` is pinned CP-side
    in `tests/routes/test_read_masked.py` and is not observable from here.
    """
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return _ticket_response()

    async with _client(handler) as http:
        result = await fetch_read_masked_doget_ticket(http=http, prep_sample_idx=7, mask_idx=3)

    assert result == _RAW_TICKET
    assert len(captured) == 1
    assert captured[0].url.path == URL_READ_MASKED_DOGET
    assert json.loads(captured[0].content) == {"prep_sample_idx": 7, "mask_idx": 3}


async def test_fetch_read_masked_doget_sends_a_requested_projection():
    """A `columns` list rides the body in the caller's order; omitting it (above)
    sends no key at all, which the CP reads as "the full shape"."""
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return _ticket_response()

    async with _client(handler) as http:
        await fetch_read_masked_doget_ticket(
            http=http, prep_sample_idx=7, mask_idx=3, columns=["read_id", "sequence_idx"]
        )

    assert json.loads(captured[0].content) == {
        "prep_sample_idx": 7,
        "mask_idx": 3,
        "columns": ["read_id", "sequence_idx"],
    }


@pytest.mark.parametrize("status", [404, 409, 422, 403, 500])
async def test_fetch_read_masked_doget_raises_on_non_2xx(status):
    """Any non-2xx raises HTTPStatusError for the caller to map to a
    BackendFailure.

    409 is the mask-completion gate (the sample is 'pending', 'invalidated', or has
    no gate row under this mask_idx). It must surface rather than be swallowed into
    an empty stream — an absent pass-set and an empty pass-set are different facts.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=json.dumps({"detail": "nope"}))

    async with _client(handler) as http:
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_read_masked_doget_ticket(http=http, prep_sample_idx=7, mask_idx=3)


async def test_open_read_masked_stream_composes_ticket_and_stream(monkeypatch):
    """The composed seam fetches a `(prep_sample_idx, mask_idx)`-scoped ticket over
    a CP client that is CLOSED before the stream opens, then streams it against the
    settings' data_plane_url, yielding the registered relation."""
    captured: dict = {}

    @asynccontextmanager
    async def fake_make_cp_client():
        captured["cp_client_open"] = True
        yield object()
        captured["cp_client_closed"] = True

    async def fake_fetch(*, http, prep_sample_idx, mask_idx, columns):
        captured["prep_sample_idx"] = prep_sample_idx
        captured["mask_idx"] = mask_idx
        captured["columns"] = columns
        return b"signed-read-masked-ticket"

    def fake_count(*, data_plane_url, ticket_bytes):
        captured["count"] = {"data_plane_url": data_plane_url, "ticket_bytes": ticket_bytes}
        return 41

    @contextmanager
    def fake_stream(conn, *, data_plane_url, ticket_bytes, relation, expected_rows):
        captured["stream"] = {
            "conn": conn,
            "data_plane_url": data_plane_url,
            "ticket_bytes": ticket_bytes,
            "relation": relation,
            "expected_rows": expected_rows,
        }
        yield relation

    class _Settings:
        data_plane_url = "grpc://dp.test:50051"

    monkeypatch.setattr(dpc, "make_cp_client", fake_make_cp_client)
    monkeypatch.setattr(dpc, "fetch_read_masked_doget_ticket", fake_fetch)
    monkeypatch.setattr(dpc, "count_masked_reads", fake_count)
    monkeypatch.setattr(dpc, "open_doget_stream", fake_stream)
    monkeypatch.setattr(dpc, "get_settings", lambda: _Settings())

    sentinel_conn = object()
    async with dpc.open_read_masked_stream(sentinel_conn, prep_sample_idx=7, mask_idx=3) as rel:
        assert rel == "masked_reads"
        assert captured["cp_client_closed"] is True

    assert captured["prep_sample_idx"] == 7
    assert captured["mask_idx"] == 3
    assert captured["columns"] is None
    assert captured["stream"]["conn"] is sentinel_conn
    assert captured["stream"]["data_plane_url"] == "grpc://dp.test:50051"
    assert captured["stream"]["ticket_bytes"] == b"signed-read-masked-ticket"
    assert captured["stream"]["relation"] == "masked_reads"
    # The count is asked under the ticket the stream uses, and checked by the stream.
    assert captured["count"] == {
        "data_plane_url": "grpc://dp.test:50051",
        "ticket_bytes": b"signed-read-masked-ticket",
    }
    assert captured["stream"]["expected_rows"] == 41


async def test_open_read_masked_stream_relation_name_is_overridable(monkeypatch):
    """A caller-supplied relation name reaches `open_doget_stream`, so a consumer
    that already binds `masked_reads` can name this one differently."""
    captured: dict = {}

    @asynccontextmanager
    async def fake_make_cp_client():
        yield object()

    async def fake_fetch(*, http, prep_sample_idx, mask_idx, columns):
        captured.setdefault("fetched", []).append(columns)
        return b"projected" if columns is not None else b"unprojected"

    def fake_count(*, data_plane_url, ticket_bytes):
        captured["count_ticket"] = ticket_bytes
        return 3

    @contextmanager
    def fake_stream(conn, *, data_plane_url, ticket_bytes, relation, expected_rows):
        captured["relation"] = relation
        captured["stream_ticket"] = ticket_bytes
        yield relation

    class _Settings:
        data_plane_url = "grpc://dp.test:50051"

    monkeypatch.setattr(dpc, "make_cp_client", fake_make_cp_client)
    monkeypatch.setattr(dpc, "fetch_read_masked_doget_ticket", fake_fetch)
    monkeypatch.setattr(dpc, "count_masked_reads", fake_count)
    monkeypatch.setattr(dpc, "open_doget_stream", fake_stream)
    monkeypatch.setattr(dpc, "get_settings", lambda: _Settings())

    async with dpc.open_read_masked_stream(
        object(),
        prep_sample_idx=7,
        mask_idx=3,
        relation="denovo_reads",
        columns=["read_id", "sequence_idx"],
    ) as rel:
        assert rel == "denovo_reads"

    assert captured["relation"] == "denovo_reads"
    # `count_masked` refuses a projected ticket: the projection streams, and an
    # unprojected ticket for the same pair is what is counted.
    assert captured["fetched"] == [["read_id", "sequence_idx"], None]
    assert captured["stream_ticket"] == b"projected"
    assert captured["count_ticket"] == b"unprojected"


async def test_open_read_masked_stream_mint_failure_never_opens_the_stream(monkeypatch):
    """A refused mint propagates, and no Flight stream is opened and no relation
    registered on the way out.

    This is the composed seam's half of the 409 contract. The fetcher test above
    pins that a non-2xx raises; what a consumer depends on is that the raise
    happens BEFORE anything is bound, so a refused mask-completion gate cannot be
    mistaken for a stream that yielded no rows.
    """
    opened: list[str] = []

    @asynccontextmanager
    async def fake_make_cp_client():
        yield object()

    async def fake_fetch(*, http, prep_sample_idx, mask_idx, columns):
        raise httpx.HTTPStatusError(
            "409", request=httpx.Request("POST", "http://cp.test"), response=httpx.Response(409)
        )

    @contextmanager
    def fake_stream(conn, *, data_plane_url, ticket_bytes, relation, expected_rows):
        opened.append(relation)
        raise AssertionError("stream opened despite a refused mint")

    class _Settings:
        data_plane_url = "grpc://dp.test:50051"

    monkeypatch.setattr(dpc, "make_cp_client", fake_make_cp_client)
    monkeypatch.setattr(dpc, "fetch_read_masked_doget_ticket", fake_fetch)
    monkeypatch.setattr(dpc, "open_doget_stream", fake_stream)
    monkeypatch.setattr(dpc, "get_settings", lambda: _Settings())

    with pytest.raises(httpx.HTTPStatusError):
        async with dpc.open_read_masked_stream(object(), prep_sample_idx=7, mask_idx=3):
            raise AssertionError("body ran despite a refused mint")

    assert opened == []


# `registered_counted` — the row-count check `open_read_masked_stream` applies. Run on
# a plain DuckDB connection over an in-memory reader: what is under test is the count
# of rows DuckDB pulls, not the transport.


def _int_reader(n: int, *, batch: int = 3):
    import pyarrow as pa  # noqa: PLC0415

    table = pa.table({"sequence_idx": pa.array(range(n), pa.int64())})
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches(max_chunksize=batch))


def test_registered_counted_passes_a_stream_drained_in_full():
    import duckdb  # noqa: PLC0415

    with duckdb.connect() as conn:
        with dpc.registered_counted(conn, "r", _int_reader(7), expected_rows=7) as rel:
            conn.execute(f"CREATE TABLE t AS SELECT * FROM {rel}")
        assert conn.execute("SELECT count(*) FROM t").fetchone() == (7,)


def test_registered_counted_refuses_a_stream_that_ends_short():
    """Seven rows counted, five on the wire, and the stream ends without an error."""
    import duckdb  # noqa: PLC0415

    with duckdb.connect() as conn:
        with pytest.raises(dpc.StreamRowCountMismatch, match="delivered 5 rows.* counts 7"):
            with dpc.registered_counted(conn, "r", _int_reader(5), expected_rows=7) as rel:
                conn.execute(f"CREATE TABLE t AS SELECT * FROM {rel}")


def test_registered_counted_refuses_a_stream_longer_than_counted():
    import duckdb  # noqa: PLC0415

    with duckdb.connect() as conn:
        with pytest.raises(dpc.StreamRowCountMismatch, match="delivered 8 rows.* counts 7"):
            with dpc.registered_counted(conn, "r", _int_reader(8), expected_rows=7) as rel:
                conn.execute(f"CREATE TABLE t AS SELECT * FROM {rel}")


def test_registered_counted_refuses_a_body_that_does_not_drain():
    """The count is of rows that left the reader, so a body reading part of the
    relation fails the check rather than passing on the rows it skipped."""
    import duckdb  # noqa: PLC0415

    with duckdb.connect() as conn:
        with pytest.raises(dpc.StreamRowCountMismatch):
            with dpc.registered_counted(
                conn, "r", _int_reader(30_000), expected_rows=30_000
            ) as rel:
                conn.execute(f"SELECT * FROM {rel} LIMIT 1").fetchall()


def test_registered_counted_lets_the_body_exception_through():
    """A body that raises is not checked: its own exception reaches the caller, not a
    count mismatch over the rows it never read."""
    import duckdb  # noqa: PLC0415

    with duckdb.connect() as conn:
        with pytest.raises(ValueError, match="from the body"):
            with dpc.registered_counted(conn, "r", _int_reader(7), expected_rows=7):
                raise ValueError("from the body")
