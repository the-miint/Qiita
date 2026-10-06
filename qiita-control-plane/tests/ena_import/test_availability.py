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


def _summaries_response(statuses: dict[str, int]) -> httpx.Response:
    return httpx.Response(
        200,
        content=json.dumps(
            {
                "summaries": [
                    {
                        "accession": accession,
                        "status": status,
                        "statusDescription": {4: "public", 5: "suppressed"}[status],
                    }
                    for accession, status in statuses.items()
                ],
                "total": str(len(statuses)),
            }
        ).encode(),
        headers={"content-type": "application/json"},
    )


async def test_check_runs_asks_for_runs_in_one_request():
    paths = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return _summaries_response({"SRR096342": 4, "ERR000130": 5})

    async with _client(handler) as client:
        statuses = await client.check_runs(["SRR096342", "ERR000130"])

    assert statuses == {"SRR096342": None, "ERR000130": "suppressed"}
    assert paths == ["/ena/browser/api/summary/SRR096342,ERR000130"]


async def test_check_runs_splits_a_long_list_into_batches(monkeypatch):
    monkeypatch.setattr("qiita_control_plane.ena_import.availability._BATCH_SIZE", 2)
    batches = []

    def handler(request: httpx.Request) -> httpx.Response:
        accessions = request.url.path.rsplit("/", 1)[1].split(",")
        batches.append(accessions)
        return _summaries_response(dict.fromkeys(accessions, 4))

    async with _client(handler) as client:
        statuses = await client.check_runs(["A1", "A2", "A3"])

    assert statuses == {"A1": None, "A2": None, "A3": None}
    assert batches == [["A1", "A2"], ["A3"]]


async def test_check_runs_falls_back_to_single_runs_when_a_batch_returns_500():
    """One accession ENA can't serve makes the whole batch 500 (probed
    2026-10-05), so the batch is re-asked one run at a time."""

    def handler(request: httpx.Request) -> httpx.Response:
        accessions = request.url.path.rsplit("/", 1)[1].split(",")
        if len(accessions) > 1:
            return httpx.Response(500, content=b"{}")
        return _summaries_response({accessions[0]: 5})

    async with _client(handler) as client:
        statuses = await client.check_runs(["SRR1", "SRR2"])

    assert statuses == {"SRR1": "suppressed", "SRR2": "suppressed"}


async def test_check_runs_raises_when_a_single_run_still_returns_500():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/SRR000000001"):
            return httpx.Response(500, content=b"{}")
        accessions = request.url.path.rsplit("/", 1)[1].split(",")
        if len(accessions) > 1:
            return httpx.Response(500, content=b"{}")
        return _summaries_response({accessions[0]: 4})

    async with _client(handler) as client:
        with pytest.raises(httpx.HTTPStatusError, match="SRR000000001"):
            await client.check_runs(["SRR096342", "SRR000000001"])


@pytest.mark.parametrize("first_failure", ["transport", "503"])
async def test_check_runs_retries_a_transient_failure_once(first_failure):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            if first_failure == "transport":
                raise httpx.ConnectError("refused", request=request)
            return httpx.Response(503, content=b"{}")
        return _summaries_response({"SRR096342": 4})

    async with _client(handler) as client:
        assert await client.check_runs(["SRR096342"]) == {"SRR096342": None}
    assert calls == 2


async def test_check_runs_raises_after_a_second_transient_failure():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, content=b"{}")

    async with _client(handler) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await client.check_runs(["SRR096342"])


async def test_check_runs_raises_when_a_summary_is_missing():
    def handler(request: httpx.Request) -> httpx.Response:
        return _summaries_response({"SRR096342": 4})

    async with _client(handler) as client:
        with pytest.raises(ValueError, match="ERR000130"):
            await client.check_runs(["SRR096342", "ERR000130"])
