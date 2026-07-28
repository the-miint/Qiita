"""`MiintEnaResolver` — the ENA metadata resolver.

Drives a DuckDB session with the miint extension loaded and calls `read_ena`
(study header + runs) and `read_ena_attributes` (per-sample attributes). See
`duckdb-miint/docs/insdc_ena.md` for the table functions.

The three `_query_ena_*` functions are the `connect_with_miint()`-touching seam: each
opens its own connection, runs one query, returns `(columns, rows)`. They are
module-level so unit tests can monkeypatch them by name instead of needing a live
DuckDB+miint session (mirrors `runner._stream_masked_reads_to_fastq`)."""

from __future__ import annotations

from qiita_common.models.ena import EnaRunRecord, EnaSampleAttributes, EnaStudyHeader

from qiita_control_plane.miint import connect_with_miint_staged

from .accession import validate_study_accession
from .resolver import EnaAccessionNotFoundError

# Explicit fields for read_run: only the columns EnaRunRecord models, not read_ena's
# full default set (which also carries sample-descriptive fields out of scope here).
_RUN_FIELDS = (
    "run_accession,experiment_accession,sample_accession,study_accession,"
    "library_layout,library_strategy,library_source,library_selection,"
    "instrument_platform,"
    "fastq_ftp,fastq_aspera,fastq_bytes,fastq_md5,read_count,base_count"
)


def _query_ena_study_header(accession: str) -> tuple[list[str], list[tuple]]:
    """`read_ena(accession, result='study')` — one row, the study header."""
    with connect_with_miint_staged() as con:
        rel = con.execute(
            "SELECT * FROM read_ena($accession, result='study')", {"accession": accession}
        )
        return [d[0] for d in rel.description], rel.fetchall()


def _query_ena_runs(accession: str) -> tuple[list[str], list[tuple]]:
    """`read_ena(accession)` (default `result='read_run'`) — one row per run
    under the study, restricted to `_RUN_FIELDS`."""
    with connect_with_miint_staged() as con:
        rel = con.execute(
            "SELECT * FROM read_ena($accession, fields=$fields)",
            {"accession": accession, "fields": _RUN_FIELDS},
        )
        return [d[0] for d in rel.description], rel.fetchall()


def _query_ena_sample_attributes(accession: str) -> list[tuple[str, dict[str, str]]]:
    """`read_ena_attributes(accession)` — one `(sample_accession, attributes)`
    row per sample under the study, the narrow `(sample_accession, tag, value)`
    rows grouped into a MAP by DuckDB rather than in Python. Sends one row per
    sample instead of one per attribute, and DuckDB hands the MAP back as a
    plain `dict`."""
    with connect_with_miint_staged() as con:
        return con.execute(
            "SELECT sample_accession,"
            "       map_from_entries(list(struct_pack(k := tag, v := value))) AS attributes"
            " FROM read_ena_attributes($accession)"
            " GROUP BY sample_accession"
            " ORDER BY sample_accession",
            {"accession": accession},
        ).fetchall()


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
        rows = _query_ena_sample_attributes(accession)
        if not rows:
            # Unlike resolve_study_header/resolve_runs, 0 rows here is NOT "nothing
            # resolved" -- a real ENA/DDBJ sample can carry zero <SAMPLE_ATTRIBUTE>
            # elements (e.g. DDBJ study PRJDB40364's SAMD01818724), and resolve_runs
            # already proved these samples real. Return [] rather than raise;
            # registration.register_ena_study treats a missing sample as empty.
            return []
        return [
            EnaSampleAttributes(sample_accession=sample_accession, attributes=attributes)
            for sample_accession, attributes in rows
        ]
