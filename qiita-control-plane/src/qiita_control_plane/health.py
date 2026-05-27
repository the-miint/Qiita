"""Cross-service `/health` aggregator.

The CP's public `/health` endpoint is the signal the landing page's
status pills (and any external monitoring) read. To make a green
badge mean "the user-facing system is alive" rather than just "the
CP can reach its DB," this module probes three services in parallel:

- **cp**: `SELECT 1` against the CP's Postgres pool.
- **co**: HTTP `GET {compute_orchestrator_url}/health` (the CO
  returns 200 with `{"status":"ok",...}` when its FastAPI process
  is responsive).
- **dp**: gRPC `grpc.health.v1.Health/Check` against the data plane
  (returns `SERVING` when alive). Uses the prebuilt
  `grpc_health.v1` stubs; does not require gRPC reflection on the
  DP — that's a separate operator-tooling concern for `grpcurl`.

Each probe has its own short timeout so a wedged downstream can't
drag the badge into "checking…" forever. Failures classify as
`UNREACHABLE` (transport / timeout / non-2xx / unparseable body) or
`DEGRADED` (probe completed but the responding service reported a
non-ok state). Services with no URL configured (e.g. a CP-only dev
instance) report `UNCONFIGURED` — informational, not counted
against the overall aggregate.

The full response is cached for `_CACHE_TTL_SECONDS` to keep the
landing page lightweight under traffic. A single `asyncio.Lock`
(lazily-created per running event loop so pytest-asyncio's
per-test loop swaps don't poison the lock) serializes cache-miss
probes so a thundering herd of concurrent visitors triggers one
probe round, not N.

Aggregation rule (kept strict for v1):
    overall = OK iff every configured service is OK,
              else DEGRADED.
Anything else lives in `services` for richer consumers.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

import asyncpg
import grpc
import httpx
from grpc_health.v1 import health_pb2, health_pb2_grpc
from qiita_common.models import HealthResponse, HealthStatus

# Per-probe timeout. ~1s is comfortably above a healthy round-trip
# (DB `SELECT 1` is ms, CO `/health` is single-digit ms, DP gRPC
# `Health.Check` is ms) and below the threshold where the landing
# page badge starts to feel laggy. A wedged downstream surfaces as
# UNREACHABLE after this cap instead of hanging the response.
_PROBE_TIMEOUT_SECONDS = 1.0

# Cache TTL. A landing page refresh + a few visitors should share
# one probe round. Too short and we re-probe on every visit; too
# long and a recovery isn't visible promptly. 5s is the
# operator-vs-truthful trade-off.
_CACHE_TTL_SECONDS = 5.0

# Slugs the per-service breakdown is keyed on. The landing-page JS
# reads these names to populate its three pills; renaming a key
# breaks that wire contract. Kept short to keep the JSON tight.
_KEY_CP = "cp"
_KEY_CO = "co"
_KEY_DP = "dp"


_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Cache:
    """Immutable snapshot of one cache state: the response and when
    it expires. The module-level `_cache` reference is rebound
    wholesale on every refresh (frozen=True forces it), so a reader
    that reads `_cache` once and inspects both fields on the same
    snapshot never sees a partial update — the new `_Cache` is
    constructed in full before the rebind, and the rebind itself is
    a single attribute write (GIL-atomic on CPython)."""

    expires_at: float = 0.0
    response: HealthResponse | None = None


_cache = _Cache()

# `asyncio.Lock` is bound to whatever event loop is running when
# `__init__` runs, which under pytest-asyncio is per-test. Module-
# import-time construction would bind to the wrong loop for every
# subsequent test. The lock is lazily created inside `aggregate_health`
# and re-created when the running loop changes — production runs a
# single uvicorn loop, so the re-create branch only fires in tests.
_cache_lock: asyncio.Lock | None = None
_cache_lock_loop: asyncio.AbstractEventLoop | None = None


def _get_lock() -> asyncio.Lock:
    """Return an `asyncio.Lock` bound to the currently-running loop,
    creating it on first call or after a loop swap."""
    global _cache_lock, _cache_lock_loop
    loop = asyncio.get_running_loop()
    if _cache_lock is None or _cache_lock_loop is not loop:
        _cache_lock = asyncio.Lock()
        _cache_lock_loop = loop
    return _cache_lock


def reset_cache_for_tests() -> None:
    """Clear the module-level cache. Test-only seam — production code
    relies on `_CACHE_TTL_SECONDS` to expire the cache naturally."""
    global _cache
    _cache = _Cache()


async def aggregate_health(
    *,
    pool: asyncpg.Pool,
    compute_orchestrator_url: str | None,
    data_plane_url: str,
) -> HealthResponse:
    """Return the cached `/health` response, refreshing if stale.

    The cache is keyed on time alone — every visitor sees the same
    breakdown until the TTL elapses. Concurrent cache-miss requests
    are serialized so we never trigger more than one probe round per
    refresh window.
    """
    global _cache
    now = time.monotonic()
    if _cache.response is not None and _cache.expires_at > now:
        return _cache.response
    async with _get_lock():
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
        _cache = _Cache(expires_at=now + _CACHE_TTL_SECONDS, response=response)
        return response


async def _probe_all(
    *,
    pool: asyncpg.Pool,
    compute_orchestrator_url: str | None,
    data_plane_url: str,
) -> HealthResponse:
    """Run the three probes in parallel and assemble the response.

    The probes always return a `HealthStatus` value — they catch their
    own normal-failure surfaces (transport, timeout, parse error) and
    map each to a typed state. `asyncio.gather` is called *without*
    `return_exceptions=True`: any future regression that lets a probe
    raise should 500 `/health` and surface a traceback in the journal,
    not be silently demoted to UNREACHABLE.
    """
    cp_result, co_result, dp_result = await asyncio.gather(
        _probe_cp(pool),
        _probe_co(compute_orchestrator_url),
        _probe_dp(data_plane_url),
    )
    services = {
        _KEY_CP: cp_result.value,
        _KEY_CO: co_result.value,
        _KEY_DP: dp_result.value,
    }
    overall = _aggregate(services)
    return HealthResponse(
        status=overall.value,
        service="qiita-control-plane",
        services=services,
    )


def _aggregate(services: dict[str, str]) -> HealthStatus:
    """Strict aggregate: every configured service must be OK.
    UNCONFIGURED is informational (not configured ≠ broken) and is
    excluded from the aggregate. Anything else (DEGRADED,
    UNREACHABLE) flips overall to DEGRADED."""
    for status in services.values():
        if status == HealthStatus.UNCONFIGURED.value:
            continue
        if status != HealthStatus.OK.value:
            return HealthStatus.DEGRADED
    return HealthStatus.OK


# ---------------------------------------------------------------------------
# Per-service probes
# ---------------------------------------------------------------------------


async def _probe_cp(pool: asyncpg.Pool) -> HealthStatus:
    """`SELECT 1` against the CP's own Postgres pool with a bounded
    timeout. Identical semantics to the old single-probe `/health` —
    the new aggregator keeps this check exactly as it was."""
    try:
        await asyncio.wait_for(pool.fetchval("SELECT 1"), timeout=_PROBE_TIMEOUT_SECONDS)
    except Exception:
        # Catch-all by design: any failure (TimeoutError, asyncpg
        # operational error, etc.) maps to DEGRADED. The caller
        # doesn't classify by exception kind, but operators do need
        # the traceback to diagnose — log before swallowing.
        _logger.warning("CP /health probe (cp) failed", exc_info=True)
        return HealthStatus.DEGRADED
    return HealthStatus.OK


async def _probe_co(url: str | None) -> HealthStatus:
    """HTTP `GET {url}/health` with a short timeout.

    Failure surfaces:
      - UNCONFIGURED — no URL set (CP-only dev / test instance).
      - UNREACHABLE — transport error, non-2xx response, timeout,
        unparseable body. All collapsed because the caller doesn't
        distinguish; a curiosity-driven traceback is in the log.
      - DEGRADED — 200 response but the orchestrator self-reports
        non-`ok` status in its body.

    The single `try` covers both the HTTP round-trip and the JSON
    decode: both failures mean "we couldn't read a useful answer
    from the CO," which is exactly UNREACHABLE.
    """
    if not url:
        return HealthStatus.UNCONFIGURED
    try:
        async with httpx.AsyncClient(timeout=_PROBE_TIMEOUT_SECONDS) as client:
            resp = await client.get(f"{url.rstrip('/')}/health")
        if resp.status_code != 200:
            _logger.warning("CP /health probe (co) saw non-200: %d from %s", resp.status_code, url)
            return HealthStatus.UNREACHABLE
        body = resp.json()
    except Exception:
        _logger.warning("CP /health probe (co) failed", exc_info=True)
        return HealthStatus.UNREACHABLE
    if isinstance(body, dict) and body.get("status") == HealthStatus.OK.value:
        return HealthStatus.OK
    return HealthStatus.DEGRADED


async def _probe_dp(url: str) -> HealthStatus:
    """gRPC `grpc.health.v1.Health/Check` against the data plane.

    `data_plane_url` is non-Optional in `Settings` with a default of
    `grpc://localhost:50051`, so this probe never sees an unconfigured
    state — the parameter is typed `str` to match. Strips the
    `grpc://` scheme prefix so the bare `host:port` reaches
    `grpc.aio.insecure_channel`. Service name in the request is the
    empty string — the convention for "overall server health" rather
    than a per-service check.

    Rejects `grpcs://` explicitly rather than silently downgrading
    to plaintext. TLS for the DP is terminated at nginx in the
    production deploy; a direct TLS dial isn't a supported path here.
    """
    if url.startswith("grpcs://"):
        raise ValueError(
            "data plane probe does not support grpcs:// — nginx terminates TLS in "
            "the production deploy; configure DATA_PLANE_URL with grpc:// for the "
            "127.0.0.1 dial path."
        )
    target = url.removeprefix("grpc://")
    try:
        async with grpc.aio.insecure_channel(target) as channel:
            stub = health_pb2_grpc.HealthStub(channel)
            response = await asyncio.wait_for(
                stub.Check(health_pb2.HealthCheckRequest(service="")),
                timeout=_PROBE_TIMEOUT_SECONDS,
            )
    except Exception:
        _logger.warning("CP /health probe (dp) failed", exc_info=True)
        return HealthStatus.UNREACHABLE
    if response.status == health_pb2.HealthCheckResponse.SERVING:
        return HealthStatus.OK
    return HealthStatus.DEGRADED
