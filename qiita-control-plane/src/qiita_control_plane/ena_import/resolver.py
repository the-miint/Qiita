"""Shared ENA/SRA metadata-resolver helpers.

`MiintEnaResolver` (miint `read_ena` / `read_ena_attributes`) is the resolver;
the error type below is shared with it. Given a validated study accession it
resolves the header, the runs (one row per run, joined to its sample), and the
per-sample attributes as typed `qiita_common.models.ena` models — never a raw
dict, never an empty result for an accession that fails to resolve.
"""

from __future__ import annotations


class EnaAccessionNotFoundError(RuntimeError):
    """A well-formed, validated accession resolved to zero rows from ENA. Raised
    rather than returning an empty list so an operator sees a clear "not found"
    reason instead of a silent no-op import."""
