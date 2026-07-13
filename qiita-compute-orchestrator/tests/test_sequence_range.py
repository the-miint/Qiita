"""Tests for qiita_compute_orchestrator.sequence_range.mint_sequence_range.

Exercises the four status-code branches with httpx.MockTransport so
no real HTTP server is needed. The wire shape (URL, JSON body,
response field names) is captured here — the CP-side route lives
upstream in qiita-control-plane and has its own tests; this file
defends our orchestrator-side adapter against drift.
"""

from __future__ import annotations

import json

import httpx
import pytest
from qiita_common.api_paths import (
    URL_SEQUENCE_RANGE_BY_PREP_SAMPLE,
    URL_SEQUENCE_RANGE_PREFIX,
)

from qiita_compute_orchestrator.sequence_range import (
    MintedSequenceRange,
    PrepSampleNotEligibleForSequenceRange,
    SequenceRangeAlreadyExists,
    get_sequence_range,
    mint_sequence_range,
)


def _client(handler) -> httpx.AsyncClient:
    """An AsyncClient with the MockTransport pointed at handler."""
    return httpx.AsyncClient(
        base_url="http://cp.test",
        transport=httpx.MockTransport(handler),
        headers={"Authorization": "Bearer test-sa-pat"},
    )


def _wire_range_json(
    prep_sample_idx: int = 42,
    start: int = 1000,
    stop: int = 1099,
    minted_by_work_ticket_idx: int | None = 7,
    minted_by_work_ticket_state: str | None = None,
) -> str:
    """The CP's SequenceRange wire JSON — shared by the mint (201) and the
    read-back (200) handlers so the two payload shapes can't drift apart."""
    return json.dumps(
        {
            "prep_sample_idx": prep_sample_idx,
            "sequence_idx_start": start,
            "sequence_idx_stop": stop,
            "minted_by_work_ticket_idx": minted_by_work_ticket_idx,
            "minted_by_work_ticket_state": minted_by_work_ticket_state,
            "created_at": "2026-05-15T00:00:00+00:00",
        }
    )


async def test_mint_returns_range_on_201():
    """Happy path: 201 with the wire SequenceRange JSON shape parses
    into MintedSequenceRange."""
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            201,
            content=_wire_range_json(),
            headers={"content-type": "application/json"},
        )

    async with _client(handler) as http:
        result = await mint_sequence_range(
            http=http, prep_sample_idx=42, count=100, work_ticket_idx=7
        )

    assert result == MintedSequenceRange(
        prep_sample_idx=42,
        sequence_idx_start=1000,
        sequence_idx_stop=1099,
        minted_by_work_ticket_idx=7,
    )
    # Verify wire shape: URL + JSON body the CP route expects.
    assert len(captured) == 1
    req = captured[0]
    assert req.url.path == URL_SEQUENCE_RANGE_PREFIX
    assert req.method == "POST"
    body = json.loads(req.content)
    assert body == {"prep_sample_idx": 42, "count": 100, "work_ticket_idx": 7}


async def test_mint_raises_already_exists_on_409():
    """409 means the prep_sample already has a range from a prior
    attempt. The helper raises SequenceRangeAlreadyExists; the caller
    maps to a permanent BackendFailure."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            content=json.dumps({"detail": "prep_sample_idx 42 already has a sequence_range"}),
            headers={"content-type": "application/json"},
        )

    async with _client(handler) as http:
        with pytest.raises(SequenceRangeAlreadyExists) as ei:
            await mint_sequence_range(http=http, prep_sample_idx=42, count=100, work_ticket_idx=7)
    assert ei.value.prep_sample_idx == 42
    assert ei.value.count == 100
    assert "already has a sequence_range" in str(ei.value)


async def test_mint_raises_not_eligible_on_404():
    """404 means the prep_sample doesn't exist or has the wrong
    processing_kind. Surfaces as PrepSampleNotEligibleForSequenceRange."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            content=json.dumps({"detail": "prep_sample_idx 42 not found"}),
            headers={"content-type": "application/json"},
        )

    async with _client(handler) as http:
        with pytest.raises(PrepSampleNotEligibleForSequenceRange) as ei:
            await mint_sequence_range(http=http, prep_sample_idx=42, count=100, work_ticket_idx=7)
    assert ei.value.prep_sample_idx == 42


async def test_mint_raises_http_error_on_5xx():
    """5xx (DB error, infra issue) bubbles up as httpx.HTTPStatusError.
    The caller maps it to whatever BackendFailure kind is appropriate
    (UNKNOWN_TRANSIENT for 5xx typically)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"internal error")

    async with _client(handler) as http:
        with pytest.raises(httpx.HTTPStatusError) as ei:
            await mint_sequence_range(http=http, prep_sample_idx=42, count=100, work_ticket_idx=7)
    assert ei.value.response.status_code == 500


async def test_mint_raises_http_error_on_401():
    """401 = bad/missing token. Surfaces as HTTPStatusError. The
    orchestrator's auth wiring (Settings.co_to_cp_token) is what
    populates the Authorization header; a 401 here indicates a
    misconfigured deployment, not a transient failure."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, content=b"unauthorized")

    async with _client(handler) as http:
        with pytest.raises(httpx.HTTPStatusError) as ei:
            await mint_sequence_range(http=http, prep_sample_idx=42, count=100, work_ticket_idx=7)
    assert ei.value.response.status_code == 401


# ---------------------------------------------------------------------------
# get_sequence_range — read-back for the ingest_reads reuse path
# ---------------------------------------------------------------------------


async def test_get_returns_range_on_200():
    """200 with the wire SequenceRange JSON parses into MintedSequenceRange,
    and the GET hits /sequence-range/{prep_sample_idx}."""
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200,
            content=_wire_range_json(),
            headers={"content-type": "application/json"},
        )

    async with _client(handler) as http:
        result = await get_sequence_range(http=http, prep_sample_idx=42)

    assert result == MintedSequenceRange(
        prep_sample_idx=42,
        sequence_idx_start=1000,
        sequence_idx_stop=1099,
        minted_by_work_ticket_idx=7,
    )
    assert len(captured) == 1
    req = captured[0]
    assert req.method == "GET"
    assert req.url.path == URL_SEQUENCE_RANGE_BY_PREP_SAMPLE.format(prep_sample_idx=42)


async def test_get_returns_none_on_404():
    """404 means no range exists yet — the helper returns None (not an
    exception) so the caller can fall through to a fresh mint."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            content=json.dumps({"detail": "no sequence_range for prep_sample_idx 42"}),
            headers={"content-type": "application/json"},
        )

    async with _client(handler) as http:
        assert await get_sequence_range(http=http, prep_sample_idx=42) is None


async def test_get_raises_http_error_on_5xx():
    """5xx (DB error, infra) bubbles up as httpx.HTTPStatusError, mapped to
    a BackendFailure by the caller."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"internal error")

    async with _client(handler) as http:
        with pytest.raises(httpx.HTTPStatusError) as ei:
            await get_sequence_range(http=http, prep_sample_idx=42)
    assert ei.value.response.status_code == 500
