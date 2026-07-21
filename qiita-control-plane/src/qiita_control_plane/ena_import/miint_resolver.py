"""`MiintEnaResolver` — the default `EnaResolver` implementation (T01-2, D2).

Drives a DuckDB session with the miint extension loaded
(`qiita_control_plane.miint.connect_with_miint`) and calls `read_ena`
(study header + runs) and `read_ena_attributes` (per-sample attributes). See
`duckdb-miint/docs/insdc_ena.md` for the underlying table functions and
`duckdb-miint/src/ena_parser.cpp::DefaultFields` for the exact `read_ena`
column set this resolver relies on.

The three `_query_ena_*` functions below are the connect_with_miint()-
touching seam: each opens its own connection, runs one query, and returns
`(columns, rows)`. They are module-level so unit tests monkeypatch them by
fully-qualified name instead of a live DuckDB+miint session — mirrors
`qiita_control_plane.runner._stream_masked_reads_to_fastq`
(`tests/test_read_ingest_resolvers.py`), the established pattern for testing
connect_with_miint()-touching code."""

from __future__ import annotations

import duckdb
from qiita_common.models.ena import EnaRunRecord, EnaSampleAttributes, EnaStudyHeader

from qiita_control_plane.miint import connect_with_miint

from .accession import validate_study_accession
from .resolver import EnaAccessionNotFoundError, EnaResolver, pivot_sample_attributes

# Explicit fields for read_run keep the mapping tight: this resolver only
# needs the columns EnaRunRecord models, not read_ena's full default set
# (which also carries sample-descriptive fields like scientific_name/
# collection_date — out of scope for T01-2's runs+samples contract).
_RUN_FIELDS = (
    "run_accession,experiment_accession,sample_accession,study_accession,"
    "library_layout,library_strategy,library_source,library_selection,"
    "instrument_platform,"
    "fastq_ftp,fastq_aspera,fastq_bytes,fastq_md5,read_count,base_count"
)


def _open_ena_connection() -> duckdb.DuckDBPyConnection:
    """`connect_with_miint()` plus an explicit `httpfs` install+load.

    `read_ena`/`read_ena_attributes` need `httpfs` for their outbound ENA
    Portal/Browser API calls; `duckdb-miint/docs/insdc_ena.md` states it is
    "automatically loaded", but that isn't reliably true under
    `connect_with_miint()`'s config (`allow_unsigned_extensions` plus a
    private `extension_directory`) — confirmed empirically (T01-2 live
    system test + manual runs against a real ENA study): the query fails
    with a bare DuckDB `'https' scheme is not supported` error instead of
    silently degrading. Rather than depend on DuckDB's own autoload, install
    + load `httpfs` explicitly here, exactly like `connect_with_miint()`
    does for `miint` itself. `INSTALL` is a no-op on a warm cache; `LOAD` is
    per-connection and always needed. Scoped to this ENA-network-dependent
    module rather than `connect_with_miint()` itself, which other (local,
    non-network) miint call sites also share."""
    con = connect_with_miint()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    return con


def _query_ena_study_header(accession: str) -> tuple[list[str], list[tuple]]:
    """`read_ena(accession, result='study')` — one row, the study header."""
    with _open_ena_connection() as con:
        rel = con.execute(
            "SELECT * FROM read_ena($accession, result='study')", {"accession": accession}
        )
        return [d[0] for d in rel.description], rel.fetchall()


def _query_ena_runs(accession: str) -> tuple[list[str], list[tuple]]:
    """`read_ena(accession)` (default `result='read_run'`) — one row per run
    under the study, restricted to `_RUN_FIELDS`."""
    with _open_ena_connection() as con:
        rel = con.execute(
            "SELECT * FROM read_ena($accession, fields=$fields)",
            {"accession": accession, "fields": _RUN_FIELDS},
        )
        return [d[0] for d in rel.description], rel.fetchall()


def _query_ena_sample_attributes(accession: str) -> tuple[list[str], list[tuple]]:
    """`read_ena_attributes(accession)` — one (sample_accession, tag, value)
    row per submitter-defined attribute, across every sample under the
    study (miint resolves the study accession to its samples internally)."""
    with _open_ena_connection() as con:
        rel = con.execute("SELECT * FROM read_ena_attributes($accession)", {"accession": accession})
        return [d[0] for d in rel.description], rel.fetchall()


class MiintEnaResolver(EnaResolver):
    """Default `EnaResolver` — miint `read_ena` / `read_ena_attributes`."""

    def resolve_study_header(self, accession: str) -> EnaStudyHeader:
        accession = validate_study_accession(accession)
        columns, rows = _query_ena_study_header(accession)
        if not rows:
            raise EnaAccessionNotFoundError(f"no ENA study found for accession {accession!r}")
        return EnaStudyHeader(**dict(zip(columns, rows[0], strict=True)))

    def resolve_runs(self, accession: str) -> list[EnaRunRecord]:
        accession = validate_study_accession(accession)
        columns, rows = _query_ena_runs(accession)
        if not rows:
            raise EnaAccessionNotFoundError(f"no ENA runs found for study {accession!r}")
        return [EnaRunRecord(**dict(zip(columns, row, strict=True))) for row in rows]

    def resolve_sample_attributes(self, accession: str) -> list[EnaSampleAttributes]:
        accession = validate_study_accession(accession)
        columns, rows = _query_ena_sample_attributes(accession)
        if not rows:
            raise EnaAccessionNotFoundError(
                f"no ENA sample attributes found for study {accession!r}"
            )
        return pivot_sample_attributes(columns, rows)
