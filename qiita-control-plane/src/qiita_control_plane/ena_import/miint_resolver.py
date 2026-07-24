"""`MiintEnaResolver` — the ENA metadata resolver.

Drives a DuckDB session with the miint extension loaded and calls `read_ena`
(study header + runs) and `read_ena_attributes` (per-sample attributes). See
`duckdb-miint/docs/insdc_ena.md` for the table functions.

The three `_query_ena_*` functions are the `connect_with_miint()`-touching seam: each
opens its own connection, runs one query, returns `(columns, rows)`. They are
module-level so unit tests can monkeypatch them by name instead of needing a live
DuckDB+miint session (mirrors `runner._stream_masked_reads_to_fastq`)."""

from __future__ import annotations

import duckdb
from qiita_common.models.ena import EnaRunRecord, EnaSampleAttributes, EnaStudyHeader

from qiita_control_plane.miint import connect_with_miint_staged

from .accession import validate_study_accession
from .resolver import EnaAccessionNotFoundError, pivot_sample_attributes

# The only ENA metadata resolver backend. Kept as a named constant so the batch
# route/driver validate the request's `backend` against one source of truth.
BACKEND_MIINT = "miint"

# Explicit fields for read_run: only the columns EnaRunRecord models, not read_ena's
# full default set (which also carries sample-descriptive fields out of scope here).
_RUN_FIELDS = (
    "run_accession,experiment_accession,sample_accession,study_accession,"
    "library_layout,library_strategy,library_source,library_selection,"
    "instrument_platform,"
    "fastq_ftp,fastq_aspera,fastq_bytes,fastq_md5,read_count,base_count"
)


def _open_ena_connection() -> duckdb.DuckDBPyConnection:
    """`connect_with_miint_staged()` plus an explicit `LOAD httpfs`.

    Service-side: `MiintEnaResolver` runs inside the CP service (the
    `POST /ena-import-batch` background task, `batch._process_one_study`), so both
    extensions are LOAD-only. `qiita-api`'s `$HOME` is `/dev/null`, so any INSTALL
    dies resolving `~/.duckdb/extensions` — the rule `qiita_control_plane.miint`
    spells out. Use the staged (LOAD-only) helper, never the client INSTALL one.

    miint AND httpfs are both pre-staged into `MIINT_EXTENSION_DIRECTORY` by the
    deploy (`stage_miint_extension` INSTALLs httpfs into the same directory), so
    `LOAD httpfs` resolves from there with no mirror round-trip or writable
    `$HOME`. This is the CP counterpart to the orchestrator's `open_miint_ena_conn`
    — the LOAD-only, both-extensions helper the `ingest_ena_reads` job uses.

    The explicit `LOAD httpfs` is required because `read_ena`/`read_ena_attributes`
    need `httpfs` for their outbound ENA API calls and it does NOT reliably autoload
    under the miint connect config (`allow_unsigned_extensions` + a private
    `extension_directory`): the query fails with `'https' scheme is not supported`
    rather than degrading."""
    con = connect_with_miint_staged()
    con.execute("LOAD httpfs;")
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


class MiintEnaResolver:
    """The ENA metadata resolver — miint `read_ena` / `read_ena_attributes`."""

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
            # Unlike resolve_study_header/resolve_runs, 0 rows here is NOT "nothing
            # resolved" -- a real ENA/DDBJ sample can carry zero <SAMPLE_ATTRIBUTE>
            # elements (e.g. DDBJ study PRJDB40364's SAMD01818724), and resolve_runs
            # already proved these samples real. Return [] rather than raise;
            # registration.register_ena_study treats a missing sample as empty.
            return []
        return pivot_sample_attributes(columns, rows)
