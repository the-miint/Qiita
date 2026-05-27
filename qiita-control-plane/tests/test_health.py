"""Tests for the aggregated /health module.

Covers each probe independently with stubs, the aggregator's
strict-all-ok rule, and the cache TTL behavior. No real DB / gRPC /
HTTP — the goal is to pin the contract, not to integration-test the
network stack (those tiers live in `test-integration`).
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import grpc
import httpx
import pytest
from grpc_health.v1 import health_pb2

from qiita_control_plane import health as health_module

# ---------------------------------------------------------------------------
# Test scaffolding
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_cache():
    """Each test starts with a clean module-level cache so the TTL
    behavior of one test doesn't leak into the next."""
    health_module.reset_cache_for_tests()
    yield
    health_module.reset_cache_for_tests()


class _FakePool:
    """Minimal asyncpg.Pool surface for the cp probe."""

    def __init__(self, *, fetchval_returns=1, fetchval_raises: Exception | None = None):
        self.fetchval_returns = fetchval_returns
        self.fetchval_raises = fetchval_raises
        self.calls: list[str] = []

    async def fetchval(self, query: str):
        self.calls.append(query)
        if self.fetchval_raises is not None:
            raise self.fetchval_raises
        return self.fetchval_returns


def _patch_httpx_async_client(monkeypatch, handler):
    """Replace httpx.AsyncClient inside the health module so the CO
    probe uses our MockTransport. We monkeypatch the symbol the
    module imported, not the original — so the patch's reach is
    bounded to this module."""
    transport = httpx.MockTransport(handler)
    real_cls = health_module.httpx.AsyncClient

    def factory(*args, **kwargs):
        kwargs.setdefault("transport", transport)
        return real_cls(*args, **kwargs)

    monkeypatch.setattr(health_module.httpx, "AsyncClient", factory)


def _patch_grpc_channel(monkeypatch, *, check_result=None, check_raises: Exception | None = None):
    """Replace grpc.aio.insecure_channel inside the health module so
    the DP probe doesn't actually open a connection. Returns the
    stub's `Check` AsyncMock so tests can inspect call args."""
    check_mock = AsyncMock()
    if check_raises is not None:
        check_mock.side_effect = check_raises
    else:
        check_mock.return_value = check_result

    class _FakeStub:
        def __init__(self, _channel):
            self.Check = check_mock

    @asynccontextmanager
    async def fake_channel(target):
        yield object()  # channel sentinel — the stub ignores it

    monkeypatch.setattr(health_module.grpc.aio, "insecure_channel", fake_channel)
    monkeypatch.setattr(health_module.health_pb2_grpc, "HealthStub", _FakeStub)
    return check_mock


# ---------------------------------------------------------------------------
# _probe_cp
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_probe_cp_ok():
    pool = _FakePool()
    assert await health_module._probe_cp(pool) == "ok"
    assert pool.calls == ["SELECT 1"]


@pytest.mark.asyncio
async def test_probe_cp_db_error_is_degraded():
    pool = _FakePool(fetchval_raises=RuntimeError("connection refused"))
    assert await health_module._probe_cp(pool) == "degraded"


# ---------------------------------------------------------------------------
# _probe_co
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_probe_co_unconfigured_when_url_missing():
    assert await health_module._probe_co(None) == "unconfigured"
    assert await health_module._probe_co("") == "unconfigured"


@pytest.mark.asyncio
async def test_probe_co_ok_when_orchestrator_returns_status_ok(monkeypatch):
    seen_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        return httpx.Response(200, json={"status": "ok", "service": "qiita-compute-orchestrator"})

    _patch_httpx_async_client(monkeypatch, handler)
    result = await health_module._probe_co("http://orchestrator.invalid:8081")
    assert result == "ok"
    assert seen_paths == ["/health"]


@pytest.mark.asyncio
async def test_probe_co_degraded_when_orchestrator_self_reports_not_ok(monkeypatch):
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"status": "degraded", "service": "qiita-compute-orchestrator"}
        )

    _patch_httpx_async_client(monkeypatch, handler)
    assert await health_module._probe_co("http://orchestrator.invalid:8081") == "degraded"


@pytest.mark.asyncio
async def test_probe_co_unreachable_on_non_200(monkeypatch):
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="service unavailable")

    _patch_httpx_async_client(monkeypatch, handler)
    assert await health_module._probe_co("http://orchestrator.invalid:8081") == "unreachable"


@pytest.mark.asyncio
async def test_probe_co_unreachable_on_transport_error(monkeypatch):
    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    _patch_httpx_async_client(monkeypatch, handler)
    assert await health_module._probe_co("http://orchestrator.invalid:8081") == "unreachable"


# ---------------------------------------------------------------------------
# _probe_dp
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_probe_dp_unconfigured_when_url_missing():
    assert await health_module._probe_dp(None) == "unconfigured"
    assert await health_module._probe_dp("") == "unconfigured"


@pytest.mark.asyncio
async def test_probe_dp_ok_when_serving(monkeypatch):
    response = health_pb2.HealthCheckResponse(status=health_pb2.HealthCheckResponse.SERVING)
    check_mock = _patch_grpc_channel(monkeypatch, check_result=response)
    assert await health_module._probe_dp("grpc://data-plane.invalid:50051") == "ok"
    # Service field should be empty string by convention (overall-server check).
    assert check_mock.await_args.args[0].service == ""


@pytest.mark.asyncio
async def test_probe_dp_degraded_when_not_serving(monkeypatch):
    response = health_pb2.HealthCheckResponse(status=health_pb2.HealthCheckResponse.NOT_SERVING)
    _patch_grpc_channel(monkeypatch, check_result=response)
    assert await health_module._probe_dp("grpc://data-plane.invalid:50051") == "degraded"


@pytest.mark.asyncio
async def test_probe_dp_unreachable_on_rpc_error(monkeypatch):
    err = grpc.aio.AioRpcError(
        code=grpc.StatusCode.UNAVAILABLE,
        initial_metadata=grpc.aio.Metadata(),
        trailing_metadata=grpc.aio.Metadata(),
        details="connection refused",
    )
    _patch_grpc_channel(monkeypatch, check_raises=err)
    assert await health_module._probe_dp("grpc://data-plane.invalid:50051") == "unreachable"


@pytest.mark.asyncio
async def test_probe_dp_strips_grpc_scheme(monkeypatch):
    """grpc.aio.insecure_channel takes `host:port`, not `grpc://host:port`.
    Verify the scheme is stripped before the call."""
    captured_targets: list[str] = []

    @asynccontextmanager
    async def fake_channel(target):
        captured_targets.append(target)
        yield object()

    class _FakeStub:
        def __init__(self, _):
            pass

        async def Check(self, _request):
            return health_pb2.HealthCheckResponse(status=health_pb2.HealthCheckResponse.SERVING)

    monkeypatch.setattr(health_module.grpc.aio, "insecure_channel", fake_channel)
    monkeypatch.setattr(health_module.health_pb2_grpc, "HealthStub", _FakeStub)
    await health_module._probe_dp("grpc://data-plane.invalid:50051")
    await health_module._probe_dp("grpcs://data-plane.invalid:50051")
    assert captured_targets == [
        "data-plane.invalid:50051",
        "data-plane.invalid:50051",
    ]


# ---------------------------------------------------------------------------
# _aggregate
# ---------------------------------------------------------------------------


def test_aggregate_all_ok_is_ok():
    assert health_module._aggregate({"cp": "ok", "co": "ok", "dp": "ok"}) == "ok"


def test_aggregate_unconfigured_does_not_demote():
    """A CP-only dev instance with co/dp unconfigured stays overall ok."""
    assert (
        health_module._aggregate({"cp": "ok", "co": "unconfigured", "dp": "unconfigured"}) == "ok"
    )


def test_aggregate_any_non_ok_configured_service_demotes():
    assert health_module._aggregate({"cp": "ok", "co": "degraded", "dp": "ok"}) == "degraded"
    assert health_module._aggregate({"cp": "ok", "co": "ok", "dp": "unreachable"}) == "degraded"
    assert health_module._aggregate({"cp": "degraded", "co": "ok", "dp": "ok"}) == "degraded"


# ---------------------------------------------------------------------------
# aggregate_health (cached entry point)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_aggregate_health_returns_breakdown(monkeypatch):
    pool = _FakePool()

    def co_handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok", "service": "qiita-compute-orchestrator"})

    _patch_httpx_async_client(monkeypatch, co_handler)
    _patch_grpc_channel(
        monkeypatch,
        check_result=health_pb2.HealthCheckResponse(status=health_pb2.HealthCheckResponse.SERVING),
    )

    resp = await health_module.aggregate_health(
        pool=pool,
        compute_orchestrator_url="http://orchestrator.invalid:8081",
        data_plane_url="grpc://data-plane.invalid:50051",
    )
    assert resp.status == "ok"
    assert resp.service == "qiita-control-plane"
    assert resp.services == {"cp": "ok", "co": "ok", "dp": "ok"}


@pytest.mark.asyncio
async def test_aggregate_health_caches_within_ttl(monkeypatch):
    """Second call inside the TTL window returns the cached response
    without re-probing — the fake pool's `fetchval` call count is the
    canary."""
    pool = _FakePool()

    def co_handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok", "service": "qiita-compute-orchestrator"})

    _patch_httpx_async_client(monkeypatch, co_handler)
    _patch_grpc_channel(
        monkeypatch,
        check_result=health_pb2.HealthCheckResponse(status=health_pb2.HealthCheckResponse.SERVING),
    )

    await health_module.aggregate_health(
        pool=pool,
        compute_orchestrator_url="http://orchestrator.invalid:8081",
        data_plane_url="grpc://data-plane.invalid:50051",
    )
    await health_module.aggregate_health(
        pool=pool,
        compute_orchestrator_url="http://orchestrator.invalid:8081",
        data_plane_url="grpc://data-plane.invalid:50051",
    )
    # The cache should have suppressed the second probe round.
    assert pool.calls == ["SELECT 1"]


@pytest.mark.asyncio
async def test_aggregate_health_refreshes_after_ttl(monkeypatch):
    """Once the TTL elapses, the next call re-probes."""
    pool = _FakePool()

    def co_handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok", "service": "qiita-compute-orchestrator"})

    _patch_httpx_async_client(monkeypatch, co_handler)
    _patch_grpc_channel(
        monkeypatch,
        check_result=health_pb2.HealthCheckResponse(status=health_pb2.HealthCheckResponse.SERVING),
    )

    await health_module.aggregate_health(
        pool=pool,
        compute_orchestrator_url="http://orchestrator.invalid:8081",
        data_plane_url="grpc://data-plane.invalid:50051",
    )
    # Force-expire the cache rather than waiting real wall-clock time.
    health_module._cache.expires_at = time.monotonic() - 1
    await health_module.aggregate_health(
        pool=pool,
        compute_orchestrator_url="http://orchestrator.invalid:8081",
        data_plane_url="grpc://data-plane.invalid:50051",
    )
    assert pool.calls == ["SELECT 1", "SELECT 1"]


@pytest.mark.asyncio
async def test_aggregate_health_concurrent_callers_share_one_probe_round(monkeypatch):
    """A thundering herd of concurrent visitors should trigger exactly
    one probe round, not N. The fake pool's call count is the canary."""
    pool = _FakePool()

    def co_handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok", "service": "qiita-compute-orchestrator"})

    _patch_httpx_async_client(monkeypatch, co_handler)
    _patch_grpc_channel(
        monkeypatch,
        check_result=health_pb2.HealthCheckResponse(status=health_pb2.HealthCheckResponse.SERVING),
    )

    results = await asyncio.gather(
        *[
            health_module.aggregate_health(
                pool=pool,
                compute_orchestrator_url="http://orchestrator.invalid:8081",
                data_plane_url="grpc://data-plane.invalid:50051",
            )
            for _ in range(10)
        ]
    )
    assert all(r.status == "ok" for r in results)
    assert pool.calls == ["SELECT 1"]
