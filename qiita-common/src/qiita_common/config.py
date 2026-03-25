"""Shared config patterns: env var loading utilities."""

import os


def require_env(name: str) -> str:
    """Read a required environment variable, raising clearly if absent."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name!r} is not set")
    return value
