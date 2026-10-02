"""The miint ENA (Webin V2) submission catalog — this package's one I/O boundary.

Opens a DuckDB session with a Webin secret registered and the fixed-schema `ena`
catalog attached, and tears both down again. Every statement this boundary needs
lives here; it touches no Qiita database and knows nothing of Qiita rows.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import NamedTuple

import duckdb
from pydantic import BaseModel
from qiita_common.sql import sql_string_literal

from qiita_control_plane.miint import connect_with_miint_staged

from .mapping import EnaProjectRow, EnaSampleRow

# Fixed names, never caller input, so they interpolate into SQL unescaped.
_SECRET_NAME = "qiita_ena_submission"
_CATALOG_ALIAS = "ena"

# miint's ENA endpoint is an environment selector, not a URL. Both CREATE SECRET
# and ATTACH reject anything outside this set; checking it here fails before a
# connection is opened and raises this module's own error type rather than a
# DuckDB binder error.
ENA_ENDPOINTS = frozenset({"test", "production"})

# Per-session DuckDB bounds. A connection left at DuckDB's defaults takes every
# core and roughly 80% of system RAM, which inside the process that also serves
# the REST API is a ceiling worth naming rather than inheriting. These are
# conservative bounds, not measurements: a session carries one project row and
# some per-sample attribute maps, so it is sized for metadata rather than for
# anything that streams sequence data, and several sessions may run at once.
_SESSION_THREADS = 2
_SESSION_MEMORY_LIMIT = "1GB"

# A qualified name elides the catalog's one schema, so the leading segment of
# every table below is the ATTACH alias itself.

# miint's record of what it sent and what came back; the one enrichment source
# for a rejected insert.
_SUBMISSION_LOG_TABLE = f"{_CATALOG_ALIAS}.submission_log"

# The submitter-chosen name ENA keys an object on; every spec names it.
_ALIAS_COLUMN = "alias"


class EnaSubmissionError(RuntimeError):
    """This package's base error: a submission session could not be configured
    or opened, or an insert was rejected for a reason not distinguished below."""


class EnaObjectKind(StrEnum):
    """The ENA objects this package submits.

    Backed by no Postgres type: these values never reach Qiita's database. They
    are the nouns that appear in this package's error messages.
    """

    PROJECT = "project"
    SAMPLE = "sample"


class EnaExistingAccession(NamedTuple):
    """An accession ENA's receipt reports against an alias already deposited.

    `alias` is None where the receipt names an accession without the alias it
    belongs to, which leaves the accession unattributable rather than absent.
    """

    alias: str | None
    accession: str


# same-pattern-ok: one of three sibling failure types, each carrying a different
# payload a caller acts on differently; parameterizing erases that distinction.
class EnaAlreadySubmittedError(EnaSubmissionError):
    """ENA's receipt rejected the insert because an alias is already deposited
    under this submission account.

    Carries every accession the receipt names, since one envelope can collide on
    several of its rows. Which of the object's two accessions each is differs by
    object, so recovering the other means looking the object up rather than
    assuming a namespace.
    """

    def __init__(
        self, object_kind: EnaObjectKind, existing: tuple[EnaExistingAccession, ...]
    ) -> None:
        self.object_kind = object_kind
        self.existing = existing
        named = ", ".join(
            accession if alias is None else f"{alias}={accession}" for alias, accession in existing
        )
        super().__init__(f"ENA {object_kind} already submitted; existing accession {named}")


# same-pattern-ok: sibling of the above, carrying the colliding aliases.
class EnaAliasCheckBlockedError(EnaSubmissionError):
    """miint's own alias check refused the insert before any envelope reached
    ENA.

    Carries the colliding aliases, which is all this path reports.
    """

    def __init__(self, object_kind: EnaObjectKind, aliases: tuple[str, ...]) -> None:
        self.object_kind = object_kind
        self.aliases = aliases
        super().__init__(
            f"ENA {object_kind} submission blocked: {', '.join(aliases)} already registered"
            " under this submission account. This check reports no accession, so none can be"
            " recovered from the failure."
        )


# same-pattern-ok: sibling of the above, carrying the validator's detail.
class EnaChecklistValidationError(EnaSubmissionError):
    """miint's checklist validator refused the insert before any envelope
    reached ENA.

    Carries the validator's per-attribute detail, aggregated across the whole
    insert: one row missing a mandatory attribute, or carrying one its checklist
    does not define, fails the envelope.
    """

    def __init__(self, object_kind: EnaObjectKind, detail: str) -> None:
        self.object_kind = object_kind
        self.detail = detail
        super().__init__(f"ENA {object_kind} checklist validation failed: {detail}")


# ENA's own receipt names an accession and leaves a submission_log row behind;
# miint's client-side refusals fire before the POST and leave neither.
_ALREADY_SUBMITTED_RE = re.compile(
    r'(?:alias\s+"([^"]+)"\s+)?already exists in the submission account with accession:\s*"([^"]+)"'
)
_ALIAS_CHECK_BLOCKED_RE = re.compile(r"alias(?:es)? already exists? in submission account:\s*(.+)")
_QUOTED_ALIAS_RE = re.compile(r"'([^']*)'")
_CHECKLIST_VALIDATION_RE = re.compile(r"checklist validation failed:\s*(.*)", re.DOTALL)


@dataclass(frozen=True)
class EnaObjectSpec:
    """One ENA object's insert surface: where its rows go, what a submission
    carries, and what ENA assigns back.

    `columns` is the statement of what a submission carries — a column absent
    from it is a column never sent, not a column sent as NULL.
    """

    object_kind: EnaObjectKind
    table: str
    columns: tuple[str, ...]
    returning: tuple[str, ...]

    @property
    def returning_batch(self) -> tuple[str, ...]:
        """`returning` led by the alias, for a multi-row insert."""
        return (_ALIAS_COLUMN, *self.returning)


_PROJECT_SPEC = EnaObjectSpec(
    object_kind=EnaObjectKind.PROJECT,
    table=f"{_CATALOG_ALIAS}.projects",
    columns=(_ALIAS_COLUMN, "title", "description", "project_type"),
    returning=("prjeb_accession", "erp_accession"),
)

# `scientific_name` is absent rather than bound as NULL: ENA derives it from the
# taxon id, and Qiita's seeded label risks a rejection wherever the two drifted.
_SAMPLE_SPEC = EnaObjectSpec(
    object_kind=EnaObjectKind.SAMPLE,
    table=f"{_CATALOG_ALIAS}.samples",
    columns=(_ALIAS_COLUMN, "taxon_id", "checklist", "attributes", "attribute_units"),
    returning=("ers_accession", "samea_accession"),
)


def _open_ena_submission_session(
    *,
    webin_user: str,
    webin_password_file: Path,
    endpoint: str,
    dry_run: bool,
) -> duckdb.DuckDBPyConnection:
    """Open a session with the Webin secret registered and the catalog attached.

    Under `dry_run` every insert on the returned session is validated against
    ENA instead of submitted. The caller owns the connection and must close it.
    """
    con = connect_with_miint_staged()
    try:
        con.execute(f"SET threads = {_SESSION_THREADS}")
        con.execute(f"SET memory_limit = {sql_string_literal(_SESSION_MEMORY_LIMIT)}")
        con.execute(
            f"CREATE SECRET {_SECRET_NAME} ("
            " TYPE ENA,"
            f" USER {sql_string_literal(webin_user)},"
            f" PASSWORD_FILE {sql_string_literal(webin_password_file)},"
            f" ENDPOINT {sql_string_literal(endpoint)})"
        )
        con.execute(
            f"ATTACH {sql_string_literal(endpoint)} AS {_CATALOG_ALIAS}"
            f" (TYPE ENA, SECRET {_SECRET_NAME})"
        )
        if dry_run:
            con.execute("SET miint_ena_validate_only = true")
    except Exception:
        con.close()
        raise
    return con


def _bind_values(spec: EnaObjectSpec, row: BaseModel) -> tuple[object, ...]:
    """The bind values for one row, ordered to the spec's columns.

    Field names equal column names, so selecting the spec's columns out of the
    dumped model is the whole mapping, with no second ordering to drift from it.
    """
    dumped = row.model_dump()
    values = tuple(dumped[column] for column in spec.columns)
    return values


def _read_log_scalar(
    con: duckdb.DuckDBPyConnection, sql: str, params: list[object] | None = None
) -> object | None:
    """The first column of the one row `sql` selects from the submission log,
    or None when nothing comes back.

    Best-effort by contract: reading the log only ever enriches an error that
    has already been raised, so a failure here must never mask it and every
    DuckDB error becomes None.
    """
    try:
        cursor = con.execute(sql, params or [])
        row = cursor.fetchone()
    except duckdb.Error:
        return None
    return None if row is None else row[0]


def _submission_log_high_water(con: duckdb.DuckDBPyConnection) -> object | None:
    """The newest `ena.submission_log` timestamp, or None when the log holds
    nothing or cannot be read."""
    high_water = _read_log_scalar(con, f"SELECT max(submitted_at) FROM {_SUBMISSION_LOG_TABLE}")
    return high_water


def _last_submission_error(con: duckdb.DuckDBPyConnection, after: object | None) -> str | None:
    """The newest failed submission's error detail, considering only rows newer
    than `after`; None when the log names no such row or cannot be read.

    An `after` of None accepts any row, which is what an unreadable or empty log
    leaves.
    """
    sql = f"SELECT error_messages FROM {_SUBMISSION_LOG_TABLE} WHERE NOT success"
    params: list[object] = []
    if after is not None:
        # One session submits more than once, so an earlier insert's failed row
        # is already here.
        sql += " AND submitted_at > ?"
        params.append(after)
    sql += " ORDER BY submitted_at DESC LIMIT 1"
    detail = _read_log_scalar(con, sql, params)
    if detail is None:
        return None
    if isinstance(detail, list):
        return "; ".join(str(message) for message in detail) or None
    return str(detail) or None


def _classify_submission_failure(spec: EnaObjectSpec, message: str) -> EnaSubmissionError | None:
    """The error the failure text names, or None when it matches none of the
    three distinguishable shapes.

    One message can name several already-deposited aliases, so that shape is
    collected rather than taken from the first match.
    """
    existing = tuple(
        EnaExistingAccession(match.group(1), match.group(2))
        for match in _ALREADY_SUBMITTED_RE.finditer(message)
    )
    if existing:
        return EnaAlreadySubmittedError(spec.object_kind, existing)
    alias_check = _ALIAS_CHECK_BLOCKED_RE.search(message)
    if alias_check is not None:
        reported = alias_check.group(1).strip()
        aliases = tuple(_QUOTED_ALIAS_RE.findall(reported))
        # The sentence matched but named nothing extractable, so the payload
        # this type exists to carry is absent; say that rather than raise it empty.
        if not aliases:
            return EnaSubmissionError(
                f"ENA {spec.object_kind} submission blocked, but no alias could be read"
                f" from the report: {reported}"
            )
        return EnaAliasCheckBlockedError(spec.object_kind, aliases)
    checklist = _CHECKLIST_VALIDATION_RE.search(message)
    if checklist is not None:
        return EnaChecklistValidationError(spec.object_kind, checklist.group(1).strip())
    return None


def _insert_returning_rows(
    con: duckdb.DuckDBPyConnection,
    spec: EnaObjectSpec,
    rows_values: list[tuple[object, ...]],
    returning: tuple[str, ...],
) -> list[tuple]:
    """Insert every row in one statement and return its RETURNING tuples.

    Empty when none come back, which is what a validate-only session yields. A
    multi-row insert is one ENA submission: the envelope is accepted or rejected
    whole, so chunking a long list and recovering from a rejection are both the
    caller's to do.
    """
    # The table and column names are the spec's own closed literals and the
    # caller's `returning` is one of its tuples; only row values bind.
    row_placeholder = "(" + ", ".join("?" for _ in spec.columns) + ")"
    values_clause = ", ".join(row_placeholder for _ in rows_values)
    sql = (
        f"INSERT INTO {spec.table} ({', '.join(spec.columns)})"
        f" VALUES {values_clause}"
        f" RETURNING {', '.join(returning)}"
    )
    params = [value for row_values in rows_values for value in row_values]
    # Bound the log read to rows this statement could have written, before it
    # writes one.
    high_water = _submission_log_high_water(con)
    try:
        cursor = con.execute(sql, params)
        returned_rows = cursor.fetchall()
        return returned_rows
    except duckdb.Error as exc:
        # Classified apart so a pattern matched in one captures only that text:
        # ENA's rejection reaches us through the log, miint's through the error.
        detail = _last_submission_error(con, high_water)
        classified = None
        for message in (detail, str(exc)):
            if not message:
                continue
            classified = _classify_submission_failure(spec, message)
            if classified is not None:
                break
        if classified is not None:
            raise classified from exc
        raise EnaSubmissionError(
            f"ENA {spec.object_kind} submission failed: {detail or exc}"
        ) from exc


def _insert_one_returning(
    con: duckdb.DuckDBPyConnection,
    spec: EnaObjectSpec,
    values: tuple[object, ...],
    returning: tuple[str, ...],
) -> tuple | None:
    """Insert a single row and return its RETURNING tuple, or None when none
    comes back."""
    returned_rows = _insert_returning_rows(con, spec, [values], returning)
    return returned_rows[0] if returned_rows else None


class EnaSubmissionCatalog:
    """One prepared ENA submission session, held for the life of a `with` block.

    A DuckDB connection cannot be used from more than one thread, so a caller
    submitting in parallel gives each worker its own catalog; instances share no
    state and may be opened concurrently. Parameters are validated before any
    connection is opened.
    """

    def __init__(
        self,
        *,
        webin_user: str,
        webin_password_file: Path,
        endpoint: str,
        dry_run: bool = False,
    ) -> None:
        if not webin_user.strip():
            raise EnaSubmissionError("webin_user is blank; cannot register a Webin secret")
        if endpoint not in ENA_ENDPOINTS:
            raise EnaSubmissionError(
                f"endpoint must be one of {sorted(ENA_ENDPOINTS)}; got {endpoint!r}"
            )
        if not webin_password_file.is_file():
            raise EnaSubmissionError(f"webin password file not found: {webin_password_file}")
        self._webin_user = webin_user
        self._webin_password_file = webin_password_file
        self._endpoint = endpoint
        self._dry_run = dry_run
        self._con: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> EnaSubmissionCatalog:
        self._con = _open_ena_submission_session(
            webin_user=self._webin_user,
            webin_password_file=self._webin_password_file,
            endpoint=self._endpoint,
            dry_run=self._dry_run,
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        con = self._con
        self._con = None
        if con is None:
            return
        # Close even if the detach fails, or a failed teardown leaks the connection.
        try:
            con.execute(f"DETACH {_CATALOG_ALIAS}")
        finally:
            con.close()

    def _require_connection(self) -> duckdb.DuckDBPyConnection:
        """The open session. Submitting outside the `with` block is a caller
        error, not a reason to open a second session."""
        if self._con is None:
            raise EnaSubmissionError(
                "submission catalog is not open; enter it as a context manager before submitting"
            )
        return self._con

    def submit_project(self, row: EnaProjectRow) -> tuple[str, str] | None:
        """Register one `ena.projects` row and return its (prjeb_accession,
        erp_accession), or None under dry-run, where validation assigns none."""
        con = self._require_connection()
        values = _bind_values(_PROJECT_SPEC, row)
        assigned = _insert_one_returning(con, _PROJECT_SPEC, values, _PROJECT_SPEC.returning)
        return assigned

    def submit_samples(self, rows: list[EnaSampleRow]) -> list[tuple[str, str] | None]:
        """Register every `ena.samples` row as one submission and return each
        row's (ers_accession, samea_accession) in input order, or None per row
        under dry-run.

        Every alias must be distinct. `_insert_returning_rows` carries the
        envelope's all-or-nothing contract.
        """
        con = self._require_connection()
        # An INSERT with no VALUES group is not valid SQL, so an empty list is
        # a no-op rather than a statement.
        if not rows:
            return []
        # Two rows sharing an alias collapse into one keyed result, which would
        # hand both the same accessions instead of failing.
        aliases = [row.alias for row in rows]
        if len(set(aliases)) != len(aliases):
            raise EnaSubmissionError(
                "ENA sample submission needs a distinct alias per row; the list repeats one"
            )
        rows_values = [_bind_values(_SAMPLE_SPEC, row) for row in rows]
        returned_rows = _insert_returning_rows(
            con, _SAMPLE_SPEC, rows_values, _SAMPLE_SPEC.returning_batch
        )
        # Key by the alias each row came back with: the return order is not
        # promised, and an empty result leaves every row's accessions None.
        by_alias = {returned[0]: tuple(returned[1:]) for returned in returned_rows}
        assigned = [by_alias.get(row.alias) for row in rows]
        return assigned
