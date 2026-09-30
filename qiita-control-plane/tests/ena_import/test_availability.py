"""Tests for `EnaAvailabilityClient` (`ena_import.availability`), the Browser
API seam `batch._process_one_study` uses to re-check a held run.

Network-free: every `httpx.AsyncClient` here is built on an `httpx.MockTransport`
handler, never a live request.
"""

from __future__ import annotations

import json

import httpx
import pytest
from qiita_common.models.ena import UnknownEnaBrowserStatusError

from qiita_control_plane.ena_import.availability import EnaAvailabilityClient


def _client(handler) -> EnaAvailabilityClient:
    transport = httpx.MockTransport(handler)
    http_client = httpx.AsyncClient(
        base_url="https://www.ebi.ac.uk/ena/browser/api", transport=transport
    )
    return EnaAvailabilityClient(http_client=http_client)


def _summary_response(*, status: int, description: str) -> httpx.Response:
    return httpx.Response(
        200,
        content=json.dumps(
            {
                "summaries": [{"status": status, "statusDescription": description}],
                "total": "1",
            }
        ).encode(),
        headers={"content-type": "application/json"},
    )


async def test_check_run_public_returns_none():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/summary/SRR096342")
        return _summary_response(status=4, description="public")

    async with _client(handler) as client:
        assert await client.check_run("SRR096342") is None


async def test_check_run_suppressed_returns_description():
    def handler(request: httpx.Request) -> httpx.Response:
        return _summary_response(status=5, description="suppressed")

    async with _client(handler) as client:
        assert await client.check_run("ERR000130") == "suppressed"


async def test_check_run_http_500_raises():
    """A held run's summary returns HTTP 500 (observed live for a nonexistent
    accession; ENA's docs say released data can't be un-published, so for a run
    imported while public this means withdrawn or a transient fault -- either
    way, this fails the item the same as any other non-200 rather than writing
    a flag)."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"{}")

    async with _client(handler) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await client.check_run("SRR999999999")


async def test_check_run_other_non_200_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, content=b"{}")

    async with _client(handler) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await client.check_run("PRJEB1,SRR096342")


async def test_check_run_unrecognized_status_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        return _summary_response(status=1, description="received")

    async with _client(handler) as client:
        with pytest.raises(UnknownEnaBrowserStatusError):
            await client.check_run("SRR096342")


async def test_check_run_malformed_body_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=json.dumps({"summaries": [], "total": "0"}).encode(),
            headers={"content-type": "application/json"},
        )

    async with _client(handler) as client:
        with pytest.raises(ValueError, match="expected exactly 1"):
            await client.check_run("SRR096342")


async def test_check_run_unparseable_json_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not json")

    async with _client(handler) as client:
        with pytest.raises(Exception):  # noqa: B017 -- httpx/json raise different types by version
            await client.check_run("SRR096342")


async def test_close_is_noop_for_a_caller_owned_http_client():
    """`close()` must not close a caller-supplied httpx.AsyncClient -- only one
    this instance constructed itself."""
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    client = EnaAvailabilityClient(http_client=http_client)
    await client.close()
    assert not http_client.is_closed
    await http_client.aclose()
