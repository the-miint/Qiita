"""Health-check models shared across the three services."""

from enum import StrEnum

from pydantic import BaseModel


class HealthStatus(StrEnum):
    """Health states used in `HealthResponse.status` and the per-service
    entries inside `HealthResponse.services`.

    Closed set — both the CP aggregator and the landing-page JS pin
    against these literal values, so adding or renaming a member is a
    wire contract change.

    - `OK`: probe succeeded.
    - `DEGRADED`: probe succeeded but the responding service self-
      reported a non-ok state (200 with `status != "ok"`, gRPC
      `Health.Check` returning a state other than `SERVING`, etc.).
    - `UNREACHABLE`: probe failed at the transport layer (timeout,
      connection refused, non-2xx response, parse error). The
      service may be alive but we can't tell.
    - `UNCONFIGURED`: no URL is configured for this service (e.g. a
      CP-only dev instance). Informational — does NOT demote the
      overall aggregate.
    """

    OK = "ok"
    DEGRADED = "degraded"
    UNREACHABLE = "unreachable"
    UNCONFIGURED = "unconfigured"


class HealthResponse(BaseModel):
    """Health-check response shared across the three services.

    `status` and `service` are the original v1 surface — a binary
    `ok` / `degraded` summary and the responding service's name.
    Every existing consumer (the `make verify-health` Makefile target,
    the landing-page JS, monitoring scrapes) reads only these two
    fields and stays compatible.

    `services` is an optional per-component breakdown the control
    plane populates when its `/health` aggregates its own DB probe
    with downstream probes against the orchestrator and the data
    plane. The orchestrator's `/health` leaves it `None` — its
    aggregate is the single `status` field. Keys are component slugs
    (`cp` / `co` / `dp`); values are per-service status strings drawn
    from `HealthStatus`. We intentionally keep this as `dict[str,
    str]` rather than a typed Pydantic submodel so adding a new
    service slug doesn't force a wire-shape revision — both the JS
    and the CP have to know keys anyway, so a typed submodel would
    add code surface without preventing the lockstep change.
    """

    status: str
    service: str
    services: dict[str, str] | None = None
