"""Tests for qiita_compute_orchestrator.sequencing_run.fetch_sequenced_pool_preflight.

Exercises the status-code branches of the SA-only preflight read with
httpx.MockTransport so no real HTTP server is needed. The wire shape
(URL, response field names, base64 blob encoding) is captured here — the
CP-side route lives upstream in qiita-control-plane and has its own
tests; this file defends our orchestrator-side adapter against drift.

The non-JSON 404 case covers the detail-extraction guard: the 404 branch
reads ``resp.json().get("detail")`` under ``except (ValueError,
AttributeError)``, so a body that isn't JSON must still raise the typed
NotFound with ``detail=None`` rather than letting the parse error escape.
"""

from __future__ import annotations

import base64
import json

import httpx
import pytest
from qiita_common.api_paths import URL_SEQUENCED_POOL_PREFLIGHT
from qiita_common.models import SequencedPoolPreflightResponse

from qiita_compute_orchestrator.sequencing_run import (
    SequencedPoolPreflightNotFound,
    fetch_sequenced_pool_preflight,
)

_BLOB = b"SQLite format 3\x00 fake preflight bytes"


def _client(handler) -> httpx.AsyncClient:
    """An AsyncClient with the MockTransport pointed at handler."""
    return httpx.AsyncClient(
        base_url="http://cp.test",
        transport=httpx.MockTransport(handler),
        headers={"Authorization": "Bearer test-sa-pat"},
    )


async def test_fetch_returns_preflight_on_200():
    """Happy path: 200 with the base64-encoded blob parses into
    SequencedPoolPreflightResponse with raw bytes."""
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200,
            content=json.dumps(
                {
                    "run_preflight_blob": base64.b64encode(_BLOB).decode("ascii"),
                    "run_preflight_filename": "run.preflight.db",
                }
            ),
            headers={"content-type": "application/json"},
        )

    async with _client(handler) as http:
        result = await fetch_sequenced_pool_preflight(
            http=http, sequencing_run_idx=7, sequenced_pool_idx=3
        )

    assert isinstance(result, SequencedPoolPreflightResponse)
    assert result.run_preflight_blob == _BLOB
    assert result.run_preflight_filename == "run.preflight.db"
    # Verify wire shape: the parameterized URL the CP route expects.
    assert len(captured) == 1
    req = captured[0]
    assert req.url.path == URL_SEQUENCED_POOL_PREFLIGHT.format(
        sequencing_run_idx=7, sequenced_pool_idx=3
    )
    assert req.method == "GET"


async def test_fetch_raises_not_found_on_404_with_detail():
    """404 with a JSON detail surfaces as SequencedPoolPreflightNotFound
    carrying that detail."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            content=json.dumps({"detail": "sequenced_pool 3 has no preflight blob"}),
            headers={"content-type": "application/json"},
        )

    async with _client(handler) as http:
        with pytest.raises(SequencedPoolPreflightNotFound) as ei:
            await fetch_sequenced_pool_preflight(
                http=http, sequencing_run_idx=7, sequenced_pool_idx=3
            )
    assert ei.value.sequencing_run_idx == 7
    assert ei.value.sequenced_pool_idx == 3
    assert ei.value.detail == "sequenced_pool 3 has no preflight blob"
    assert "no preflight blob" in str(ei.value)


async def test_fetch_raises_not_found_on_404_non_json_body():
    """404 with a non-JSON body must not crash the detail-extraction:
    the ``except ValueError, AttributeError`` swallows the json() parse
    error and the exception is raised with detail=None."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, content=b"<html>not found</html>")

    async with _client(handler) as http:
        with pytest.raises(SequencedPoolPreflightNotFound) as ei:
            await fetch_sequenced_pool_preflight(
                http=http, sequencing_run_idx=7, sequenced_pool_idx=3
            )
    assert ei.value.detail is None


async def test_fetch_raises_http_error_on_5xx():
    """5xx bubbles up as httpx.HTTPStatusError for the caller to map to a
    transient BackendFailure."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"internal error")

    async with _client(handler) as http:
        with pytest.raises(httpx.HTTPStatusError) as ei:
            await fetch_sequenced_pool_preflight(
                http=http, sequencing_run_idx=7, sequenced_pool_idx=3
            )
    assert ei.value.response.status_code == 500


async def test_fetch_raises_http_error_on_401():
    """401 = bad/missing SA token. Surfaces as HTTPStatusError, signalling
    a misconfigured deployment rather than a transient failure."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, content=b"unauthorized")

    async with _client(handler) as http:
        with pytest.raises(httpx.HTTPStatusError) as ei:
            await fetch_sequenced_pool_preflight(
                http=http, sequencing_run_idx=7, sequenced_pool_idx=3
            )
    assert ei.value.response.status_code == 401
