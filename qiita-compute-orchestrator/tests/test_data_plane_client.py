"""Tests for qiita_compute_orchestrator.data_plane_client.fetch_reference_doget_ticket.

Exercises the CO→CP ticket-fetch adapter with httpx.MockTransport so no real
HTTP server is needed — the wire shape (URL, JSON body with/without feature_idx,
the base64-encoded `ticket` response field) is captured here; the CP-side route
has its own DB-tier tests upstream. The streaming half (stream_reference_chunks)
is exercised end-to-end against a live data plane in the integration suite.
"""

from __future__ import annotations

import base64
import json

import httpx
import pytest
from qiita_common.api_paths import URL_REFERENCE_DOGET

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
