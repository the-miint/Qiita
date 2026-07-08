"""Tests for qiita_compute_orchestrator.data_plane_client.

Exercises the CO→CP ticket-fetch adapter with httpx.MockTransport so no real
HTTP server is needed — the wire shape (URL, JSON body with/without feature_idx,
the base64-encoded `ticket` response field) is captured here; the CP-side route
has its own DB-tier tests upstream. The streaming half (stream_reference_chunks)
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
from qiita_common.api_paths import URL_REFERENCE_DOGET

import qiita_compute_orchestrator.data_plane_client as dpc
from qiita_compute_orchestrator.data_plane_client import fetch_reference_doget_ticket


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
    is decoded back to the raw signed bytes stream_reference_chunks wraps."""
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
    monkeypatch.setattr(dpc, "stream_reference_chunks", fake_stream)
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
    monkeypatch.setattr(dpc, "stream_reference_chunks", fake_stream)
    monkeypatch.setattr(dpc, "get_settings", lambda: _Settings())

    async with dpc.open_reference_chunk_stream(object(), reference_idx=3, feature_idx=None) as rel:
        assert rel == "reference_chunks"
    assert captured["feature_idx"] is None
