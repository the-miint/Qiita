"""Action-layer entry points for the terminology subsystem: status
transition, manifest parsing and checksum verification, and
staged-release import."""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import replace
from pathlib import Path

import asyncpg
from qiita_common.models import (
    VALID_TERMINOLOGY_STATUS_TRANSITIONS,
    TerminologyManifest,
    TerminologyResponse,
    TerminologyStatus,
    TerminologyTermObsoletionKind,
)

from .repositories.terminology import (
    ParsedTerm,
    TerminologyImportAnomaly,
    TerminologyImportResult,
    fetch_terminology,
    import_terminology_release,
    update_terminology_status,
)

# Names of the manifest and the two tab-separated tables a staged release
# carries.
MANIFEST_FILENAME = "manifest.json"
TERMS_TSV_FILENAME = "terms.tsv"
CLOSURE_TSV_FILENAME = "closure.tsv"

# The columns each release table holds. Every column is named once here and
# referenced by name wherever a table is written or read, so the header a
# writer emits and the keys a parser looks up cannot drift apart.
_TERMS_COLUMN_TERM_ID = "term_id"
_TERMS_COLUMN_LABEL = "label"
_TERMS_COLUMN_ALTERNATE_LABEL = "alternate_label"
_TERMS_COLUMN_IS_OBSOLETE = "is_obsolete"
_TERMS_COLUMN_REPLACED_BY_TERM_ID = "replaced_by_term_id"
_TERMS_COLUMN_OBSOLETION_KIND = "obsoletion_kind"
TERMS_TSV_COLUMNS = (
    _TERMS_COLUMN_TERM_ID,
    _TERMS_COLUMN_LABEL,
    _TERMS_COLUMN_ALTERNATE_LABEL,
    _TERMS_COLUMN_IS_OBSOLETE,
    _TERMS_COLUMN_REPLACED_BY_TERM_ID,
    _TERMS_COLUMN_OBSOLETION_KIND,
)
_CLOSURE_COLUMN_ANCESTOR_TERM_ID = "ancestor_term_id"
_CLOSURE_COLUMN_DESCENDANT_TERM_ID = "descendant_term_id"
_CLOSURE_COLUMN_DISTANCE = "distance"
CLOSURE_TSV_COLUMNS = (
    _CLOSURE_COLUMN_ANCESTOR_TERM_ID,
    _CLOSURE_COLUMN_DESCENDANT_TERM_ID,
    _CLOSURE_COLUMN_DISTANCE,
)

# How a boolean cell is spelled in a release table, by the writer and by the
# parser that has to accept exactly what the writer emitted. Deliberately not
# shared with the OWL extractor's owl:deprecated spelling, which is a separate
# source's contract that merely happens to coincide.
_TSV_TRUE = "true"
_TSV_FALSE = "false"


class TerminologyNotFound(Exception):
    """Raised when the terminology_idx doesn't exist."""


# same-pattern-ok: parallel to actions.reference.IllegalStatusTransition; N=2
# cases differing only in the target enum type. Parameterization deferred
# until a third resource (N=3) gains status-transition machinery.
class IllegalStatusTransition(Exception):
    """Raised when the current status can't transition to the target."""

    def __init__(self, *, current: str | None, target: TerminologyStatus) -> None:
        super().__init__(f"Cannot transition from {current!r} to {target!r}")
        self.current = current
        self.target = target


async def transition_terminology_status(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    terminology_idx: int,
    target: TerminologyStatus,
) -> TerminologyResponse:
    """Atomically transition a terminology's status, validated against
    VALID_TERMINOLOGY_STATUS_TRANSITIONS.

    Raises TerminologyNotFound if the row doesn't exist;
    IllegalStatusTransition if no source status maps to `target`, or if
    the row is in a state that cannot reach `target`.
    """
    # Derive from VALID_TERMINOLOGY_STATUS_TRANSITIONS the set of source
    # states that can reach `target`. An empty list means no source can,
    # raise error rather than letting the UPDATE silently match zero rows.
    valid_sources = [
        str(src)
        for src, targets in VALID_TERMINOLOGY_STATUS_TRANSITIONS.items()
        if target in targets
    ]
    if not valid_sources:
        raise IllegalStatusTransition(current=None, target=target)

    row = await update_terminology_status(pool_or_conn, terminology_idx, target, valid_sources)
    if row is not None:
        return TerminologyResponse(**dict(row))

    # UPDATE didn't match. Distinguish "row absent" from "row present
    # but in a state that can't reach target" via a follow-up read.
    existing = await fetch_terminology(pool_or_conn, terminology_idx)
    if existing is None:
        raise TerminologyNotFound(terminology_idx)
    raise IllegalStatusTransition(current=existing["status"], target=target)


def load_manifest(source_dir: Path) -> TerminologyManifest:
    """Read and validate `<source_dir>/manifest.json`.

    Raises FileNotFoundError if manifest.json is missing; raises
    pydantic.ValidationError if its content does not match
    TerminologyManifest.
    """
    manifest_path = source_dir / MANIFEST_FILENAME
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    payload = json.loads(manifest_path.read_text())
    return TerminologyManifest.model_validate(payload)


def write_manifest(source_dir: Path, manifest: TerminologyManifest) -> None:
    """Write `manifest` to `<source_dir>/manifest.json`, overwriting any
    manifest already there."""
    manifest_path = source_dir / MANIFEST_FILENAME
    manifest_path.write_text(manifest.model_dump_json(indent=2) + "\n")


def sha256_of_file(path: Path) -> str:
    """Return the lowercase hex SHA-256 of the bytes at `path`.

    Raises FileNotFoundError if `path` does not exist.
    """
    # Stream the file through the hasher in 1 MiB chunks; a release table
    # can be large and reading it whole just to hash it is wasteful.
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_manifest_checksums(source_dir: Path, manifest: TerminologyManifest) -> None:
    """Check both release tables the manifest declares against the digests
    it carries for them.

    Raises FileNotFoundError if a declared table is missing; raises
    ValueError on digest mismatch, naming the table.
    """
    for declared in (manifest.terms, manifest.closure):
        table_path = source_dir / declared.path
        actual = sha256_of_file(table_path)
        if actual != declared.sha256:
            raise ValueError(
                f"sha256 mismatch for {declared.path}: "
                f"manifest declares {declared.sha256!r}, computed {actual!r}"
            )


def _parse_terms_tsv(path: Path) -> list[ParsedTerm]:
    """Parse a tab-separated terms table at `path` into a list of
    ParsedTerm. An empty alternate_label / replaced_by_term_id /
    obsoletion_kind cell becomes None; is_obsolete must be 'true' or
    'false' case-insensitively.

    Raises ValueError when the header lacks a declared column, when
    is_obsolete holds an uninterpretable value, or when obsoletion_kind
    is unrecognized; the latter two name the offending row.
    """
    rows: list[ParsedTerm] = []
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")

        # Check the header up front, so a table written against an earlier
        # column set is refused rather than silently parsing every column it
        # lacks as absent.
        present_columns = reader.fieldnames or ()
        missing_columns = [c for c in TERMS_TSV_COLUMNS if c not in present_columns]
        if missing_columns:
            raise ValueError(
                f"terms table at {path} is missing column(s) {missing_columns};"
                f" expected {list(TERMS_TSV_COLUMNS)}"
            )

        for row in reader:
            term_id = row[_TERMS_COLUMN_TERM_ID]
            replaced_by = row[_TERMS_COLUMN_REPLACED_BY_TERM_ID] or None

            # An absent second name is spelled NULL in the database, so a
            # blank or whitespace-only cell has to arrive as None.
            alternate_label = row[_TERMS_COLUMN_ALTERNATE_LABEL].strip() or None

            # Reject typo'd is_obsolete values explicitly so they surface
            # at parse time rather than silently coercing to False.
            is_obsolete_text = row[_TERMS_COLUMN_IS_OBSOLETE].lower()
            if is_obsolete_text not in (_TSV_TRUE, _TSV_FALSE):
                raise ValueError(
                    f"invalid {_TERMS_COLUMN_IS_OBSOLETE} value"
                    f" {row[_TERMS_COLUMN_IS_OBSOLETE]!r} for term_id {term_id!r};"
                    f" expected {_TSV_TRUE!r} or {_TSV_FALSE!r}"
                )

            # Wrap the enum cast so the offending row is named in the error.
            kind_text = row[_TERMS_COLUMN_OBSOLETION_KIND] or None
            try:
                obsoletion_kind = TerminologyTermObsoletionKind(kind_text) if kind_text else None
            except ValueError as exc:
                raise ValueError(
                    f"invalid {_TERMS_COLUMN_OBSOLETION_KIND} {kind_text!r} for term_id {term_id!r}"
                ) from exc

            rows.append(
                ParsedTerm(
                    term_id=term_id,
                    label=row[_TERMS_COLUMN_LABEL],
                    alternate_label=alternate_label,
                    is_obsolete=is_obsolete_text == _TSV_TRUE,
                    replaced_by_term_id=replaced_by,
                    obsoletion_kind=obsoletion_kind,
                )
            )
    return rows


def write_terms_tsv(path: Path, terms: list[ParsedTerm]) -> None:
    """Write `terms` as the tab-separated terms table at `path`, headed by
    TERMS_TSV_COLUMNS. An alternate_label, replaced_by_term_id, or
    obsoletion_kind of None becomes an empty cell."""
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(TERMS_TSV_COLUMNS)

        # Cells are written positionally, so this list stays in
        # TERMS_TSV_COLUMNS order.
        for term in terms:
            writer.writerow(
                [
                    term.term_id,
                    term.label,
                    term.alternate_label or "",
                    _TSV_TRUE if term.is_obsolete else _TSV_FALSE,
                    term.replaced_by_term_id or "",
                    str(term.obsoletion_kind) if term.obsoletion_kind is not None else "",
                ]
            )


def write_closure_tsv_stub(path: Path) -> None:
    """Write a closure table at `path` holding only its CLOSURE_TSV_COLUMNS
    header. A closure table with no data rows leaves the terminology's
    closure empty, so term resolution works while subsumption queries have
    nothing to answer from."""
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(CLOSURE_TSV_COLUMNS)


def _parse_closure_tsv(path: Path) -> list[tuple[str, str, int]]:
    """Parse a tab-separated closure table at `path` into a list of
    (ancestor_term_id, descendant_term_id, distance) tuples."""
    rows: list[tuple[str, str, int]] = []
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            rows.append(
                (
                    row[_CLOSURE_COLUMN_ANCESTOR_TERM_ID],
                    row[_CLOSURE_COLUMN_DESCENDANT_TERM_ID],
                    int(row[_CLOSURE_COLUMN_DISTANCE]),
                )
            )
    return rows


def _check_misaligned_replaced_by(parsed_terms: list[ParsedTerm]) -> None:
    """Enforce that only an obsolete term carries a replacement pointer,
    naming the offending rows. Always raises on detection; never
    tolerated, because a non-obsolete row carrying a replacement pointer
    is malformed source data rather than a recoverable anomaly."""
    misaligned = [
        (term.term_id, term.replaced_by_term_id)
        for term in parsed_terms
        if not term.is_obsolete and term.replaced_by_term_id is not None
    ]
    if misaligned:
        raise TerminologyImportAnomaly(misaligned_replaced_by=misaligned)


def _find_unresolved_replaced_by(parsed_terms: list[ParsedTerm]) -> list[tuple[str, str]]:
    """Return (term_id, attempted_target) pairs where an obsolete term
    names a replaced_by_term_id absent from the same batch. Returns the
    empty list when every pointer resolves in-batch."""
    known_term_ids = {term.term_id for term in parsed_terms}
    return [
        (term.term_id, term.replaced_by_term_id)
        for term in parsed_terms
        if term.is_obsolete
        and term.replaced_by_term_id is not None
        and term.replaced_by_term_id not in known_term_ids
    ]


async def import_terminology(
    pool: asyncpg.Pool,
    source_dir: Path,
    *,
    tolerate_anomalies: bool = False,
) -> TerminologyImportResult:
    """Parse one staged terminology release from `source_dir` and apply
    it to the DB in a single transaction.

    Expects `manifest.json`, `terms.tsv`, and `closure.tsv` under
    `source_dir`.

    With `tolerate_anomalies=False` (default), raises
    TerminologyImportAnomaly when the release silently drops term_ids
    already in the database, or when an obsolete row's
    replaced_by_term_id is not present in the same batch.

    With `tolerate_anomalies=True`, those two anomaly kinds are absorbed:
    silent drops are auto-obsoleted (obsoletion_kind=silently_dropped,
    label carried forward), and unresolved replaced_by_term_id values
    are NULLed on the affected rows with a notes line recording the
    attempted CURIE. A misaligned replaced_by_term_id (non-obsolete row
    carrying a pointer) always raises — it is malformed source data
    rather than a tolerable anomaly.
    """

    # Verify before parsing, so a release table that does not match the
    # manifest is refused without any of its content being read.
    manifest = load_manifest(source_dir)
    verify_manifest_checksums(source_dir, manifest)

    parsed_terms = _parse_terms_tsv(source_dir / TERMS_TSV_FILENAME)
    parsed_closure = _parse_closure_tsv(source_dir / CLOSURE_TSV_FILENAME)

    # Misalignment is always fatal; it indicates malformed source data.
    _check_misaligned_replaced_by(parsed_terms)

    # Validate outside the transaction so an unresolved replaced_by_term_id
    # in fail mode leaves the DB untouched (no row is created).
    unresolved_replaced_by_pairs = _find_unresolved_replaced_by(parsed_terms)
    if unresolved_replaced_by_pairs and not tolerate_anomalies:
        raise TerminologyImportAnomaly(unresolved_replaced_by=unresolved_replaced_by_pairs)

    # Tolerate mode: NULL the unresolved replaced_by_term_id on the
    # affected ParsedTerm rows so _resolve_replaced_by skips them; the
    # composer appends a notes line per pair after the upsert.
    if unresolved_replaced_by_pairs:
        unresolved_term_ids = {pair[0] for pair in unresolved_replaced_by_pairs}
        parsed_terms = [
            replace(term, replaced_by_term_id=None) if term.term_id in unresolved_term_ids else term
            for term in parsed_terms
        ]

    async with pool.acquire() as conn, conn.transaction():
        return await import_terminology_release(
            conn,
            name=manifest.name,
            version=manifest.version,
            parsed_terms=parsed_terms,
            parsed_closure=parsed_closure,
            tolerate_anomalies=tolerate_anomalies,
            unresolved_replaced_by_pairs=unresolved_replaced_by_pairs,
        )
