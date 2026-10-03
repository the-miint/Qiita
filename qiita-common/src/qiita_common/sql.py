"""Rendering of SQL fragments that cannot be supplied as bound parameters."""

from __future__ import annotations

from pathlib import Path


def sql_string_literal(value: str | Path) -> str:
    """Render `value` as a complete, quoted DuckDB string literal.

    DuckDB binds row values but not identifiers, file paths, or DDL operands, so
    those have to be written into the statement text. The surrounding quotes are
    part of the result: callers interpolate what comes back as-is and never add
    quotes of their own.
    """
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"
