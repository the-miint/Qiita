"""Rendering of SQL fragments that cannot be supplied as bound parameters."""

from __future__ import annotations

from pathlib import Path


def sql_string_literal(value: str | Path) -> str:
    """Render `value` as a complete, quoted DuckDB string literal.

    The surrounding quotes are part of the result: callers interpolate what
    comes back as-is and never add quotes of their own.
    """
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def cast_projection(
    table: str, columns: tuple[tuple[str, str], ...], sources: dict[str, str]
) -> str:
    """The SELECT list for `table`'s staging Parquet: every column of `columns`, in
    order, as `CAST(<sources[column]> AS <type>) AS <column>`.

    `sources` maps each column to the SQL that fills it (a tool-output reference, a
    run scalar, or the literal `NULL`) and must name exactly these columns: a missing
    one would write a narrower Parquet than the lake table and a stray one would be
    dropped in silence, and both would surface only as a register-files rejection at
    the end of the run. Every column is CAST, so a write whose sources are all `NULL`
    still carries the lake types; a bare `NULL` would take an ambiguous type.

    Each source is SQL pasted in as is, so it must be built from constants and
    integers, never from text a caller or a file supplied.
    """
    names = [name for name, _ in columns]
    if set(sources) != set(names):
        raise KeyError(
            f"{table} needs exactly {sorted(names)}; "
            f"missing {sorted(set(names) - set(sources))}, "
            f"unexpected {sorted(set(sources) - set(names))}"
        )
    return ", ".join(f"CAST({sources[name]} AS {sqltype}) AS {name}" for name, sqltype in columns)
