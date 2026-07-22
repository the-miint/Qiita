"""Config-level `EnaResolver` selection. The HTTP fallback is a
swap here, never a callers change — mirrors
`qiita_compute_orchestrator.main._build_backend`'s factory-raises-on-unknown
pattern for `ComputeBackend`."""

from __future__ import annotations

from .http_resolver import HttpEnaResolver
from .miint_resolver import MiintEnaResolver
from .resolver import EnaResolver

BACKEND_MIINT = "miint"
BACKEND_HTTP = "http"


def get_resolver(backend: str = BACKEND_MIINT) -> EnaResolver:
    """Return the `EnaResolver` implementation for `backend`.

    `"miint"` (default) drives DuckDB + the miint extension
    (`MiintEnaResolver`); `"http"` is the experimental, off-by-default
    plain-ENA-Portal-API fallback (`HttpEnaResolver`). Raises `ValueError`
    on an unrecognized backend name — never silently falls back to a
    default."""
    if backend == BACKEND_MIINT:
        return MiintEnaResolver()
    if backend == BACKEND_HTTP:
        return HttpEnaResolver()
    raise ValueError(
        f"unknown ENA resolver backend={backend!r}; expected {BACKEND_MIINT!r} or {BACKEND_HTTP!r}"
    )
