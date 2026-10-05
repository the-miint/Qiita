"""qiita-admin CLI — role changes: `set-system-role` (direct DB, bootstrap)
and `principal set-role` (HTTP, audited).
"""

import argparse
import asyncio
import os
import sys

import asyncpg
from qiita_common.api_paths import (
    PATH_ADMIN_PREFIX,
    PATH_ADMIN_PRINCIPAL_LOOKUP_BY_EMAIL,
    PATH_ADMIN_PRINCIPAL_SYSTEM_ROLE,
)
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, SystemRole

from .. import _common
from ._helpers import _DB_CONNECT_TIMEOUT_SECONDS

# Derived from SystemRole so the role list isn't repeated anywhere in this
# file — adding `SystemRole.X` widens validation, error message, and `--help`
# automatically.
_VALID_ROLE_VALUES = tuple(r.value for r in SystemRole)

# ---------------------------------------------------------------------------
# Bootstrap subcommand: set-system-role (direct DB)
# ---------------------------------------------------------------------------


async def _set_system_role(database_url: str, email: str, role: str) -> int:
    """Update the principal's system_role by email lookup.

    Returns the principal_idx that was updated. Refuses to operate on
    idx=1 (the system principal). Raises with a clear message if the
    email is not found (the operator probably hasn't logged in via OIDC
    yet, which is what creates the principal+user pair).
    """
    if role not in _VALID_ROLE_VALUES:
        raise ValueError(f"role must be one of {' / '.join(_VALID_ROLE_VALUES)} (got {role!r})")
    try:
        conn = await asyncpg.connect(database_url, timeout=_DB_CONNECT_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 — show full reason, including OS errors
        raise RuntimeError(
            f"could not connect to DATABASE_URL: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        idx = await conn.fetchval(
            "SELECT u.principal_idx FROM qiita.user u WHERE u.email = $1",
            email,
        )
        if idx is None:
            raise RuntimeError(
                f"no user with email {email!r} — has this user logged in"
                " via OIDC at least once? First login creates the principal+user"
                " rows; only then can their role be set."
            )
        if idx == SYSTEM_PRINCIPAL_IDX:
            raise RuntimeError(
                f"refusing to modify the system principal (idx={SYSTEM_PRINCIPAL_IDX})"
            )
        await conn.execute(
            "UPDATE qiita.principal SET system_role = $1 WHERE idx = $2",
            role,
            idx,
        )
        return idx
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# HTTP subcommand: principal set-role
# ---------------------------------------------------------------------------


def _nonblank_reason(value: str) -> str:
    """argparse `type=` for --reason: the audit event must say why."""
    if not value.strip():
        raise argparse.ArgumentTypeError("must not be blank")
    return value


def _principal_set_role(base_url: str, token: str, email: str, role: str, reason: str) -> dict:
    """Resolve `email` to its principal, then PATCH its system_role through the
    admin route, which records the change (with `reason`) in the audit log.

    Refuses (RuntimeError) a disabled or retired principal, and sends nothing
    when the principal already holds `role`, so no `from == to` event is written.
    """
    found = _common.call(
        "POST",
        base_url,
        token,
        f"{PATH_ADMIN_PREFIX}{PATH_ADMIN_PRINCIPAL_LOOKUP_BY_EMAIL}",
        json={"email": email},
    )
    principal_idx = found["principal_idx"]
    for flag in ("disabled", "retired"):
        if found[flag]:
            raise RuntimeError(f"{email} is {flag}; refusing to change its role")
    result = {"principal_idx": principal_idx, "email": email, "from": found["system_role"]}
    if found["system_role"] == role:
        return {**result, "to": role, "changed": False}
    _common._request(
        "PATCH",
        base_url,
        token,
        f"{PATH_ADMIN_PREFIX}{PATH_ADMIN_PRINCIPAL_SYSTEM_ROLE.format(principal_idx=principal_idx)}",
        json={"system_role": role, "reason": reason},
    )
    return {**result, "to": role, "changed": True}


# ---------------------------------------------------------------------------
# Subcommand handlers (registered via parser.set_defaults(handler=...))
# ---------------------------------------------------------------------------


def _handle_set_system_role(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("error: DATABASE_URL not set", file=sys.stderr)
        return 2
    try:
        idx = asyncio.run(_set_system_role(database_url, args.email, args.role))
    except (RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"updated principal idx={idx} system_role={args.role}")
    return 0


def _handle_principal_set_role(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    try:
        return _common.run_http_subcommand(
            lambda t: _principal_set_role(args.base_url, t, args.email, args.role, args.reason)
        )
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
