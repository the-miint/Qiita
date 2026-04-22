"""Shared config patterns: env var loading utilities."""

import os


def require_env(name: str) -> str:
    """Read a required environment variable, raising clearly if absent or empty."""
    value = os.environ.get(name)
    if value is None:
        raise RuntimeError(f"Required environment variable {name!r} is not set")
    if value == "":
        raise RuntimeError(f"Required environment variable {name!r} is set but empty")
    return value
