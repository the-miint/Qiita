"""The authed httpx client for orchestrator → control-plane calls.

`make_cp_client` builds the AsyncClient (compute service-account PAT in
the Authorization header, base_url = the CP) that orchestrator-side CP
callers construct per `execute()` invocation. It lives in its own module,
outside any single caller, so no caller owns the others' HTTP plumbing.
"""

from __future__ import annotations

from typing import Any

import httpx

from .config import Settings, get_settings


def make_cp_client(
    settings: Settings | None = None,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> httpx.AsyncClient:
    """Build an authed AsyncClient pointed at the control plane.

    The compute service-account PAT goes into the Authorization header
    on every request. Caller is responsible for `async with`-style
    lifetime management; one client per execute() invocation is the
    expected pattern (cheap to construct, no connection pooling
    benefit at our call rate).

    `settings` is injectable for tests; production code passes nothing
    and get_settings() (config.py) resolves either the lifespan-installed
    cached value (orchestrator service) or a fresh Settings.from_env()
    (SLURM launcher / CLI). See config.py module header for the
    asymmetric resolution rationale.

    `transport` is injectable for tests so an integration suite can
    swap in an `httpx.ASGITransport(app=cp_app)` and exercise the full
    Settings → headers → httpx → CP route → DB path in-process without
    a uvicorn subprocess. Production code passes nothing; httpx uses
    its default network transport against `settings.cp_url`.
    """
    if settings is None:
        settings = get_settings()
    kwargs: dict[str, Any] = {
        "base_url": settings.cp_url,
        "headers": {"Authorization": f"Bearer {settings.co_to_cp_token}"},
        # 30s caters to a slow nextval/setval/INSERT under contention;
        # the CP route is bounded by the advisory lock + a few ms of
        # plpgsql, so a longer timeout would only mask infra issues.
        "timeout": httpx.Timeout(30.0),
    }
    if transport is not None:
        kwargs["transport"] = transport
    return httpx.AsyncClient(**kwargs)
