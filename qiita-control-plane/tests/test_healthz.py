"""Tests for the `/healthz` liveness route.

Pure unit, no DB marker — `/healthz` touches no dependencies (that's
the whole point: it's a cheap liveness probe distinct from the
CP+CO+DP aggregator at `/health`). Drives the route via httpx +
ASGITransport against the module-level app, no lifespan / postgres /
auth fixtures needed. See issue #67: the compute-orchestrator
readiness checker GETs this path and the CP must actually serve it.
"""

from httpx import ASGITransport, AsyncClient

from qiita_control_plane.main import app


async def test_healthz_returns_200_ok():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/healthz")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["service"] == "qiita-control-plane"


async def test_healthz_needs_no_auth_header():
    """The readiness checker sends a Bearer token but liveness ignores
    it — an unauthenticated GET must still 200. Guards against the
    route accidentally acquiring an auth dependency later."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/healthz")  # no Authorization header
    assert response.status_code == 200


async def test_healthz_omits_aggregate_services_breakdown():
    """Liveness reports no per-service breakdown — that's `/health`'s
    job. A populated `services` here would mean the route is doing the
    heavier aggregation it exists to avoid."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/healthz")
    assert response.json().get("services") is None
