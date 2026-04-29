#!/usr/bin/env python
"""Decode and verify a JWT against the configured AuthRocket realm.

Reads `Settings.from_env()` (so `AUTHROCKET_*` env vars must be set), then
runs the same `JwtVerifier.verify` path the production resolver uses. Prints
the decoded `OIDCIdentity` on success, or exits non-zero with a clear error
on any verification failure.

This is deploy-time tooling, not admin tooling — used in `first-deploy.md`
to sanity-check the AuthRocket realm config before bringing the control
plane up. Not exposed as a `qiita-admin` subcommand.

Usage:
    AUTHROCKET_ISSUER=... AUTHROCKET_AUDIENCE=... \\
        uv run python scripts/verify_jwt.py "$JWT"
"""

import sys


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: verify_jwt.py <token>", file=sys.stderr)
        return 2
    token = argv[1]

    try:
        from qiita_control_plane.auth.oidc import (
            AuthRocketVerifier,
            InvalidJwt,
        )
        from qiita_control_plane.config import Settings
    except ImportError as exc:
        print(f"import error: {exc}", file=sys.stderr)
        return 1

    try:
        settings = Settings.from_env()
        verifier = AuthRocketVerifier.from_settings(settings)
    except RuntimeError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 1

    try:
        identity = verifier.verify(token)
    except InvalidJwt as exc:
        print(f"verification failed: {exc}", file=sys.stderr)
        return 1

    print(f"issuer:    {identity.issuer}")
    print(f"subject:   {identity.subject}")
    print(f"email:     {identity.email}")
    print(f"auth_time: {identity.auth_time}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
