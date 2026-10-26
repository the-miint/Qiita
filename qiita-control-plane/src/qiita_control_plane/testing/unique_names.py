"""Unique-value generators for test data.

Each helper returns a fresh, collision-resistant string for a specific
column.
"""

import secrets


def _unique_with_hex(prefix: str, sep: str) -> str:
    return f"{prefix}{sep}{secrets.token_hex(4)}"


def unique_field_name(prefix: str = "owner_biosample_id") -> str:
    """Return prefix + '_' + 8 hex chars; collision-resistant across re-runs."""
    return _unique_with_hex(prefix, "_")


def unique_accession(prefix: str = "BS") -> str:
    """Return prefix + '-' + 8 hex chars; not a valid INSDC accession, so use
    `unique_ena_accession` wherever an accession is validated (e.g. study accessions)."""
    return _unique_with_hex(prefix, "-")


def unique_ena_accession(prefix: str) -> str:
    """Return prefix + 9 random digits, matching the INSDC accession shape."""
    return f"{prefix}{secrets.randbelow(10**9):09d}"


def unique_matrix_tube_id() -> str:
    """Return a 10-digit string; the leading digit is forced to 0 so each
    generated id exercises the column's leading-zero-preservation contract.
    Matches the digits-only CHECK on qiita.biosample.matrix_tube_id."""
    # 9 random digits with one zero-padded leading digit gives 10 total.
    return f"0{secrets.randbelow(10**9):09d}"
