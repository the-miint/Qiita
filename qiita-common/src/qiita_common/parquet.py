"""Shared Parquet helpers used across qiita services."""

from pathlib import Path


def validate_parquet_path(path: Path) -> str:
    """Reject paths with characters that can't be safely string-interpolated
    into a DuckDB COPY statement. The COPY target is a SQL string literal
    so we can't bind it as a parameter; sanitise instead."""
    text = str(path)
    if "'" in text or "\\" in text or any(ord(c) < 0x20 for c in text):
        raise ValueError(f"Output path contains unsafe characters: {text}")
    return text
