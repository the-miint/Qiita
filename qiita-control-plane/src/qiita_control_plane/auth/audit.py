"""Append-only audit-event writer with a runtime leak-guard.

`record_event` writes to qiita.auth_events. The leak-guard recursively walks
the `detail` dict and raises ValueError at the call site if any key is in a
forbidden set or any string value matches a token-shape pattern. Fails
closed: a malformed audit attempt aborts the surrounding operation rather
than silently logging a leak.

Forbidden behaviors caught at write time:
- Keys: token, plaintext, bearer, jwt (case-insensitive)
- String values starting with `qk_` (our opaque-token prefix)
- String values shaped like a JWT: starting with `ey` and containing two dots
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from typing import Any

from qiita_common.auth_constants import AuthEventType

from . import TOKEN_PREFIX

# Forbidden top-level keys in detail. Case-insensitive match.
_FORBIDDEN_KEYS = frozenset({"token", "plaintext", "bearer", "jwt"})

# Substring patterns: we want to catch tokens embedded inside larger strings
# (e.g. a misguided log message like "revoking qk_..."), not just strings that
# ARE the token. Bias toward false-positives, not false-negatives — the lower
# bound is well below TOKEN_BODY_LEN deliberately.
_QK_TOKEN_SHAPE = re.compile(rf"{re.escape(TOKEN_PREFIX)}[A-Za-z0-9_\-]{{30,}}")
# JWT-shape: "ey" (base64url of "{") followed by header.payload.signature.
_JWT_SHAPE = re.compile(r"\bey[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\b")


def _check_for_leaks(value: Any, path: str = "") -> None:
    """Recurse through the detail value, raising ValueError on any leak."""
    if isinstance(value, dict):
        for k, v in value.items():
            if isinstance(k, str) and k.lower() in _FORBIDDEN_KEYS:
                raise ValueError(f"audit detail contains forbidden key {k!r} at {path or '<root>'}")
            _check_for_leaks(v, f"{path}.{k}" if path else k)
        return
    if isinstance(value, list | tuple):
        for i, v in enumerate(value):
            _check_for_leaks(v, f"{path}[{i}]")
        return
    if isinstance(value, str):
        if _QK_TOKEN_SHAPE.search(value):
            raise ValueError(
                f"audit detail value at {path or '<root>'} appears to contain a qk_ token"
            )
        if _JWT_SHAPE.search(value):
            raise ValueError(f"audit detail value at {path or '<root>'} appears to contain a JWT")
    # Other scalar types (int, bool, None, float) are fine.


def sha256_hex(s: str) -> str:
    """Helper for callers who want to log a hashed identifier instead of the
    cleartext (e.g. attempted email on a collision)."""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


async def record_event(
    pool_or_conn,
    *,
    event_type: AuthEventType | str,
    principal_idx: int | None = None,
    actor_principal_idx: int | None = None,
    detail: dict[str, Any] | None = None,
) -> int:
    """Insert an auth_events row. Returns the new event_idx.

    Raises ValueError immediately if `detail` contains any forbidden key or
    a value that looks like a leaked token / JWT — this fails the surrounding
    operation rather than silently logging the leak.

    `pool_or_conn` accepts either an asyncpg.Pool or a Connection — the same
    `.fetchval(...)` API works for both, so callers can write inside an
    existing transaction by passing the connection.

    `event_type` accepts `AuthEventType | str`: the StrEnum is preferred for
    new emit sites; bare strings are kept on the API surface for read-side
    paths that pass through the value from the DB row (where the column is
    TEXT and may contain an event type added in a future schema revision).
    """
    detail = detail or {}
    _check_for_leaks(detail)
    payload = json.dumps(detail, separators=(",", ":"))
    return await pool_or_conn.fetchval(
        "INSERT INTO qiita.auth_events"
        "  (event_type, principal_idx, actor_principal_idx, detail)"
        " VALUES ($1, $2, $3, $4::jsonb) RETURNING event_idx",
        str(event_type),
        principal_idx,
        actor_principal_idx,
        payload,
    )


__all__: Iterable[str] = ("record_event", "sha256_hex")
