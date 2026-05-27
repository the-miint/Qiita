"""Cross-service `/health` aggregator.

The CP's public `/health` endpoint is the signal the landing page's
status pills (and any external monitoring) read. To make a green
badge mean "the user-facing system is alive" rather than just "the
CP can reach its DB," this module probes three services in parallel:

- **cp**: `SELECT 1` against the CP's Postgres pool.
- **co**: HTTP `GET {compute_orchestrator_url}/health` (returns 200
  with `{"status":"ok",...}` when alive).
- **dp**: gRPC `grpc.health.v1.Health/Check` against the data plane
  (returns `SERVING` when alive). Uses `grpcio-health-checking`'s
  generated stubs — does *not* require gRPC reflection on the DP
  (reflection is a separate concern; see issue #54 for the
  grpcurl-via-Makefile path that does).

Each probe has its own short timeout so a wedged downstream can't
drag the badge into "checking…" forever. Failures classify as
`unreachable` (transport / timeout / non-2xx) or `degraded` (probe
returned a recognized "not ok" state). Services with no URL
configured (e.g. a CP-only dev instance) report `unconfigured` —
counted as informational, *not* against the overall aggregate.

The full response is cached for `_CACHE_TTL_SECONDS` to keep the
landing page lightweight under traffic. A single `asyncio.Lock`
serializes cache-miss probes so a thundering herd of concurrent
visitors triggers one probe round, not N.

Aggregation rule (kept strict for v1):
    overall = "ok" iff every configured service is "ok",
              else "degraded".
Anything else lives in `services` for richer consumers.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

import asyncpg
import grpc
import httpx
from grpc_health.v1 import health_pb2, health_pb2_grpc
from qiita_common.models import HealthResponse

# Per-probe timeout. ~1s is comfortably above a healthy round-trip
# (DB `SELECT 1` is ms, CO `/health` is single-digit ms, DP gRPC
# `Health.Check` is ms) and below the threshold where the landing
# page badge starts to feel laggy. A wedged downstream surfaces as
# `unreachable` after this cap instead of hanging the response.
_PROBE_TIMEOUT_SECONDS = 1.0

# Cache TTL. A landing page refresh + a few visitors should share
# one probe round. Too short and we re-probe on every visit; too
# long and a recovery isn't visible promptly. 5s is the
# operator-vs-truthful trade-off.
_CACHE_TTL_SECONDS = 5.0

# Per-service status strings. The probe helpers return one of these;
# the aggregate maps {ok, unconfigured} → "ok" and anything else →
# "degraded". `unconfigured` exists so a CP-only dev instance
# doesn't show "degraded" forever — it just shows the field as
# `unconfigured` and the overall summary stays "ok".
_STATUS_OK = "ok"
_STATUS_DEGRADED = "degraded"
_STATUS_UNREACHABLE = "unreachable"
_STATUS_UNCONFIGURED = "unconfigured"

# Slugs the per-service breakdown is keyed on. The landing page's
# JS reads these names to populate its three pills; renaming a key
# breaks that contract. Kept short to keep the JSON tight.
_KEY_CP = "cp"
_KEY_CO = "co"
_KEY_DP = "dp"


@dataclass
class _Cache:
    """Module-level cache state. Replaced wholesale on every refresh
    so a reader holding `_cache` snapshots a coherent
    (expires_at, response) pair without lock acquisition."""

    expires_at: float = 0.0
    response: HealthResponse | None = None


_cache = _Cache()
_cache_lock = asyncio.Lock()


def reset_cache_for_tests() -> None:
    """Clear the module-level cache. Test-only seam — production code
    relies on `_CACHE_TTL_SECONDS` to expire the cache naturally."""
    global _cache
    _cache = _Cache()


async def aggregate_health(
    *,
    pool: asyncpg.Pool,
    compute_orchestrator_url: str | None,
    data_plane_url: str | None,
) -> HealthResponse:
    """Return the cached `/health` response, refreshing if stale.

    The cache is keyed on time alone — every visitor sees the same
    breakdown until the TTL elapses. Concurrent cache-miss requests
    are serialized so we never trigger more than one probe round per
    refresh window.
    """
    now = time.monotonic()
    if _cache.response is not None and _cache.expires_at > now:
        return _cache.response
    async with _cache_lock:
        # Double-check after acquiring the lock — another coroutine
        # may have refreshed between our first check and here.
        now = time.monotonic()
        if _cache.response is not None and _cache.expires_at > now:
            return _cache.response
        response = await _probe_all(
            pool=pool,
            compute_orchestrator_url=compute_orchestrator_url,
            data_plane_url=data_plane_url,
        )
        _cache.expires_at = now + _CACHE_TTL_SECONDS
        _cache.response = response
        return response


async def _probe_all(
    *,
    pool: asyncpg.Pool,
    compute_orchestrator_url: str | None,
    data_plane_url: str | None,
) -> HealthResponse:
    """Run the three probes in parallel and assemble the response.

    `asyncio.gather` with `return_exceptions=True` insulates the
    aggregator from a probe helper raising — each helper already
    catches its own exceptions, but defensively trapping here means
    a future regression that lets one escape won't 500 `/health`.
    """
    cp_result, co_result, dp_result = await asyncio.gather(
        _probe_cp(pool),
        _probe_co(compute_orchestrator_url),
        _probe_dp(data_plane_url),
        return_exceptions=True,
    )
    services = {
        _KEY_CP: _coerce_unreachable(cp_result),
        _KEY_CO: _coerce_unreachable(co_result),
        _KEY_DP: _coerce_unreachable(dp_result),
    }
    overall = _aggregate(services)
    return HealthResponse(
        status=overall,
        service="qiita-control-plane",
        services=services,
    )


def _coerce_unreachable(value: Any) -> str:
    """Turn an unexpected exception from `_probe_*` into the typed
    `unreachable` state. Each probe helper handles its own normal-
    failure paths and returns a string; this branch only fires on a
    bug-class escape (helper itself raised). Surface as
    `unreachable` rather than 500 the parent — operators investigate
    via logs, visitors see a red pill instead of a page failure."""
    if isinstance(value, str):
        return value
    return _STATUS_UNREACHABLE


def _aggregate(services: dict[str, str]) -> str:
    """Strict aggregate: every configured service must be `ok`.
    `unconfigured` is informational (not configured ≠ broken) and is
    excluded from the aggregate. Anything else (`degraded`,
    `unreachable`) flips overall to `degraded`."""
    for status in services.values():
        if status == _STATUS_UNCONFIGURED:
            continue
        if status != _STATUS_OK:
            return _STATUS_DEGRADED
    return _STATUS_OK


# ---------------------------------------------------------------------------
# Per-service probes
# ---------------------------------------------------------------------------


async def _probe_cp(pool: asyncpg.Pool) -> str:
    """`SELECT 1` against the CP's own Postgres pool with a bounded
    timeout. Identical semantics to the old single-probe `/health` —
    the new aggregator keeps this check exactly as it was."""
    try:
        await asyncio.wait_for(pool.fetchval("SELECT 1"), timeout=_PROBE_TIMEOUT_SECONDS)
    except Exception:
        # Catch-all by design: any failure (TimeoutError, asyncpg
        # operational error, etc.) maps to the typed `degraded` state.
        # The caller doesn't classify by exception kind.
        return _STATUS_DEGRADED
    return _STATUS_OK


async def _probe_co(url: str | None) -> str:
    """HTTP `GET {url}/health` with a short timeout.

    Three failure surfaces:
      - `unconfigured` — no URL set (CP-only dev / test instance).
      - `unreachable` — transport error, non-2xx response, timeout.
      - `degraded` — 200 response but the orchestrator self-reports
        non-`ok` status in its body.
    """
    if not url:
        return _STATUS_UNCONFIGURED
    try:
        async with httpx.AsyncClient(timeout=_PROBE_TIMEOUT_SECONDS) as client:
            resp = await client.get(f"{url.rstrip('/')}/health")
    except Exception:
        # Catch-all by design: transport error, timeout, DNS — all
        # map to the typed `unreachable` state for the caller.
        return _STATUS_UNREACHABLE
    if resp.status_code != 200:
        return _STATUS_UNREACHABLE
    try:
        body = resp.json()
    except ValueError:
        return _STATUS_UNREACHABLE
    if isinstance(body, dict) and body.get("status") == _STATUS_OK:
        return _STATUS_OK
    return _STATUS_DEGRADED


async def _probe_dp(url: str | None) -> str:
    """gRPC `grpc.health.v1.Health/Check` against the data plane.

    Strips the `grpc://` / `grpcs://` scheme prefix so the bare
    `host:port` reaches `grpc.aio.insecure_channel`. Service name in
    the request is the empty string — the convention for "overall
    server health" rather than a per-service check.

    Failure surfaces match `_probe_co`: `unconfigured` if no URL,
    `unreachable` on transport / RPC error / timeout, `degraded` if
    the server replied with anything other than `SERVING`.
    """
    if not url:
        return _STATUS_UNCONFIGURED
    target = url.removeprefix("grpc://").removeprefix("grpcs://")
    try:
        async with grpc.aio.insecure_channel(target) as channel:
            stub = health_pb2_grpc.HealthStub(channel)
            response = await asyncio.wait_for(
                stub.Check(health_pb2.HealthCheckRequest(service="")),
                timeout=_PROBE_TIMEOUT_SECONDS,
            )
    except Exception:
        # Catch-all by design: gRPC AioRpcError, TimeoutError, DNS,
        # all map to `unreachable` for the caller.
        return _STATUS_UNREACHABLE
    if response.status == health_pb2.HealthCheckResponse.SERVING:
        return _STATUS_OK
    return _STATUS_DEGRADED
