"""`MiintEnaResolver` — the ENA metadata resolver.

Drives a DuckDB session with the miint extension loaded and calls `read_ena`
(study header + runs) and `read_ena_attributes` (per-sample attributes). See
`duckdb-miint/docs/insdc_ena.md` for the table functions.

The three `_query_ena_*` functions are the `connect_with_miint_staged()`-touching seam:
each opens its own connection and runs one query. The study and run queries return
`(columns, rows)`; the attribute query returns `(sample_accession, attributes)` rows. They
are module-level so unit tests can monkeypatch them by name instead of needing a live
DuckDB+miint session (mirrors `runner._stream_masked_reads_to_fastq`)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from time import monotonic

import duckdb
from qiita_common.ena_accession import (
    EnaAccessionKind,
    InvalidEnaAccessionError,
    detect_accession_kind,
    validate_study_accession,
)
from qiita_common.http_status import is_transient_error_text, is_transient_http_status
from qiita_common.models.ena import EnaRunRecord, EnaSampleAttributes, EnaStudyHeader

from qiita_control_plane.miint import connect_with_miint_staged

from .resolver import EnaAccessionNotFoundError

# Requested fields mirror the model exactly, so a field added to one is added to both.
_RUN_FIELDS = ",".join(EnaRunRecord.model_fields)
_STUDY_FIELDS = ",".join(EnaStudyHeader.model_fields)

_log = logging.getLogger(__name__)

# Sized to outlast a multi-hour ENA Portal outage; the deadline is per call, not per item.
_RESOLVE_DEADLINE_S = 6 * 3600
_RESOLVE_BACKOFF_BASE_S = 30.0
_RESOLVE_BACKOFF_CAP_S = 300.0


def _ena_outage(exc: duckdb.IOException) -> str | None:
    """What a retry could outlast in `exc` (a transient or zero HTTP status, or a connection
    failure miint reports without one), or None when it is permanent."""
    if isinstance(exc, duckdb.HTTPException):
        status = getattr(exc, "status_code", None)
        if status is not None and (status == 0 or is_transient_http_status(status)):
            return f"HTTP {status}"
        return None
    return "a connection failure" if is_transient_error_text(str(exc)) else None


async def call_with_ena_retry[T](resolve: Callable[..., T], *args: object) -> T:
    """Run the blocking `resolve(*args)` in a thread, retrying an ENA outage with capped
    backoff until the deadline, then raising `RuntimeError`. Anything else propagates."""
    start = monotonic()
    delay = _RESOLVE_BACKOFF_BASE_S
    attempt = 0
    while True:
        attempt += 1
        try:
            return await asyncio.to_thread(resolve, *args)
        except duckdb.IOException as exc:
            outage = _ena_outage(exc)
            if outage is None:
                raise
            elapsed = monotonic() - start
            if elapsed >= _RESOLVE_DEADLINE_S:
                raise RuntimeError(
                    f"ENA unavailable: {outage} for {elapsed:.0f}s, giving up: {exc}"
                ) from exc
            _log.warning(
                "ENA unavailable to %s: %s (attempt %d, %.0fs elapsed); retrying",
                resolve.__name__,
                outage,
                attempt,
                elapsed,
            )
            await asyncio.sleep(delay)
            delay = min(_RESOLVE_BACKOFF_CAP_S, delay * 2)


def _query_ena_study_header(accession: str) -> tuple[list[str], list[tuple]]:
    """`read_ena(accession, result='study', fields=...)` — one row, the study
    header, restricted to `_STUDY_FIELDS`."""
    with connect_with_miint_staged() as con:
        rel = con.execute(
            "SELECT * FROM read_ena($accession, result='study', fields=$fields)",
            {"accession": accession, "fields": _STUDY_FIELDS},
        )
        return [d[0] for d in rel.description], rel.fetchall()


def _query_ena_runs(accession: str | list[str]) -> tuple[list[str], list[tuple]]:
    """`read_ena(accession)` (default `result='read_run'`) — one row per run
    under a study, or per listed run, restricted to `_RUN_FIELDS`."""
    with connect_with_miint_staged() as con:
        rel = con.execute(
            "SELECT * FROM read_ena($accession, fields=$fields)",
            {"accession": accession, "fields": _RUN_FIELDS},
        )
        return [d[0] for d in rel.description], rel.fetchall()


def _query_ena_sample_attributes(
    accession: str | list[str],
) -> list[tuple[str, dict[str, list[str]]]]:
    """One `(sample_accession, attributes)` row per sample, mapping each tag to its distinct
    non-NULL values sorted, for a study or a list of samples. Entries are tag-ordered so a
    collision of normalised tags resolves the same way every run."""
    with connect_with_miint_staged() as con:
        return con.execute(
            "SELECT sample_accession,"
            "       map_from_entries(list(struct_pack(k := tag, v := vals) ORDER BY tag))"
            "         AS attributes"
            " FROM ("
            "   SELECT sample_accession, tag, list(DISTINCT value ORDER BY value) AS vals"
            "   FROM read_ena_attributes($accession)"
            "   WHERE value IS NOT NULL"
            "   GROUP BY sample_accession, tag)"
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
            raise EnaAccessionNotFoundError(
                f"no public ENA study found for {accession!r}"
                " (nonexistent, or not yet/no longer public)"
            )
        return EnaStudyHeader(**dict(zip(columns, rows[0], strict=True)))

    def resolve_ena_runs(self, accession: str) -> list[EnaRunRecord]:
        accession = validate_study_accession(accession)
        columns, rows = _query_ena_runs(accession)
        if not rows:
            raise EnaAccessionNotFoundError(
                f"no public ENA runs found for study {accession!r}"
                " (nonexistent, or not yet/no longer public)"
            )
        return [EnaRunRecord(**dict(zip(columns, row, strict=True))) for row in rows]

    def resolve_sample_attributes(self, accession: str) -> list[EnaSampleAttributes]:
        accession = validate_study_accession(accession)
        rows = _query_ena_sample_attributes(accession)
        if not rows:
            # Unlike resolve_study_header/resolve_ena_runs, 0 rows here is NOT "nothing
            # resolved" -- a real ENA/DDBJ sample can carry zero <SAMPLE_ATTRIBUTE>
            # elements (e.g. DDBJ study PRJDB40364's SAMD01818724), and resolve_ena_runs
            # already proved these samples real. Return [] rather than raise;
            # registration.register_ena_study treats a missing sample as empty.
            return []
        return [
            EnaSampleAttributes(sample_accession=sample_accession, attributes=attributes)
            for sample_accession, attributes in rows
        ]

    def resolve_runs(self, run_accessions: list[str]) -> list[EnaRunRecord]:
        """The listed runs, in one query. Returns only the runs ENA returned; raises
        `EnaAccessionNotFoundError` only when it returned none."""
        _require_kind(run_accessions, EnaAccessionKind.RUN)
        columns, rows = _query_ena_runs(run_accessions)
        if not rows:
            raise EnaAccessionNotFoundError(
                f"no public ENA runs found for {run_accessions!r}"
                " (nonexistent, or not yet/no longer public)"
            )
        return [EnaRunRecord.model_validate(dict(zip(columns, row, strict=True))) for row in rows]

    def resolve_sample_attributes_for_samples(
        self, sample_accessions: list[str]
    ) -> list[EnaSampleAttributes]:
        _require_kind(sample_accessions, EnaAccessionKind.SAMPLE)
        return [
            EnaSampleAttributes(sample_accession=sample_accession, attributes=attributes)
            for sample_accession, attributes in _query_ena_sample_attributes(sample_accessions)
        ]


def _require_kind(accessions: list[str], kind: EnaAccessionKind) -> None:
    for accession in accessions:
        if detect_accession_kind(accession) is not kind:
            raise InvalidEnaAccessionError(f"'{accession}' is not a {kind.value} accession")
