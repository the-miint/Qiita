"""Action layer for the terminology subsystem: reads a staged release, holds
it to what its manifest declares, and applies it to the database."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from pathlib import Path

import asyncpg
from qiita_common.models import (
    TerminologyManifest,
    TerminologyTermObsoletionKind,
)

from .repositories.terminology import (
    ParsedTerm,
    TerminologyImportAnomaly,
    TerminologyImportResult,
    format_offenders,
    import_terminology_release,
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

# How each release table is named in an error about it.
_TERMS_SOURCE_NAME = "terms table"
_CLOSURE_SOURCE_NAME = "closure table"

# How a boolean cell is spelled in a release table, by the writer and by the
# parser that has to accept exactly what the writer emitted. Deliberately not
# shared with the OWL extractor's owl:deprecated spelling, which is a separate
# source's contract that merely happens to coincide.
_TSV_TRUE = "true"
_TSV_FALSE = "false"

# What one row of a release table is keyed by, for the tables whose rows the
# database holds unique: a term id, or an ancestor/descendant pair.
type ReleaseTableKey = str | tuple[str, str]


class TerminologyNotFound(Exception):
    """Raised when the terminology_idx doesn't exist."""


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


def check_tsv_columns(
    present_columns: Sequence[str] | None,
    expected_columns: tuple[str, ...],
    *,
    source_name: str,
    path: Path,
) -> None:
    """Reject a tab-separated source whose header lacks a column the reader
    looks up, naming every absent column.

    `present_columns` is the header as read, None when the source carries none.
    `source_name` names the source in the error. Checking up front is what
    keeps a source written against another column set from being read row by
    row against keys it does not have.
    """
    missing_columns = [c for c in expected_columns if c not in (present_columns or ())]
    if missing_columns:
        raise ValueError(
            f"{source_name} at {path} is missing column(s) {missing_columns};"
            f" expected {list(expected_columns)}"
        )


def _required_cell(
    row: Mapping[str, str | None],
    column: str,
    *,
    path: Path,
    source_name: str,
) -> str:
    """Return a cell the parser reads unconditionally, rejecting a row that
    stops before reaching it.

    A row carrying fewer cells than the header leaves the remainder absent
    rather than empty, so the absence is reported as the row being malformed
    instead of surfacing from whatever the value is asked to do next.
    `column`, `source_name`, and `path` name the offending cell in the error.
    """
    raw_value = row[column]
    if raw_value is None:
        raise ValueError(f"{source_name} at {path} carries a row with no {column}")
    return raw_value


def _stripped_key(
    row: Mapping[str, str | None],
    column: str,
    *,
    path: Path,
    source_name: str,
) -> str:
    """Strip a cell holding a term id and reject one naming nothing.

    The index over a term id is a plain btree, so a padded variant would land
    as a value distinct from its unpadded twin — and the rows that reference it
    spell it unpadded.
    """
    cell = _required_cell(row, column, path=path, source_name=source_name)
    key = cell.strip()
    if not key:
        raise ValueError(f"{source_name} at {path} carries a row with an empty {column}")
    return key


def _check_no_duplicate_keys(
    keys: Iterable[ReleaseTableKey],
    *,
    path: Path,
    source_name: str,
    key_name: str,
) -> None:
    """Reject a release table whose rows collide on the key the database holds
    unique, naming the colliding values — a capped sample, with the total, once
    there are more than the cap.

    `keys` carries one key per row; `source_name` and `key_name` name the
    source and its key in the error.
    """
    key_counts = Counter(keys)
    duplicated_keys = sorted(key for key, count in key_counts.items() if count > 1)
    if duplicated_keys:
        raise ValueError(
            f"{source_name} at {path} carries duplicate {key_name}(s)"
            f" {format_offenders(duplicated_keys)}"
        )


def _parse_terms_tsv(path: Path) -> list[ParsedTerm]:
    """Parse a tab-separated terms table at `path` into a list of
    ParsedTerm. An empty alternate_label / replaced_by_term_id /
    obsoletion_kind cell becomes None; is_obsolete must be 'true' or
    'false' case-insensitively.

    Raises ValueError when the header lacks a declared column, when a row stops
    before a cell that is always read, when a term id cell names nothing, when
    is_obsolete holds an uninterpretable value, when obsoletion_kind is
    unrecognized, or when a term id occupies more than one row.
    """
    rows: list[ParsedTerm] = []
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")

        # Check the header up front, so a table written against an earlier
        # column set is refused rather than silently parsing every column it
        # lacks as absent.
        check_tsv_columns(
            reader.fieldnames,
            TERMS_TSV_COLUMNS,
            source_name=_TERMS_SOURCE_NAME,
            path=path,
        )

        # Fixed for every row of this table, so the per-cell calls below name
        # only what varies between them.
        cell_kwargs = {"path": path, "source_name": _TERMS_SOURCE_NAME}

        for row in reader:
            term_id = _stripped_key(row, _TERMS_COLUMN_TERM_ID, **cell_kwargs)
            replaced_by = row[_TERMS_COLUMN_REPLACED_BY_TERM_ID] or None

            # Both names arrive stripped for the same reason the term id does. A
            # cell holding nothing but whitespace is the source naming nothing,
            # which the database spells NULL — distinct from naming the empty
            # string, so a name may end up absent where a key may not.
            label_cell = _required_cell(row, _TERMS_COLUMN_LABEL, **cell_kwargs)
            alternate_label_cell = _required_cell(row, _TERMS_COLUMN_ALTERNATE_LABEL, **cell_kwargs)
            label = label_cell.strip() or None
            alternate_label = alternate_label_cell.strip() or None

            # Reject typo'd is_obsolete values explicitly so they surface
            # at parse time rather than silently coercing to False.
            is_obsolete_cell = _required_cell(row, _TERMS_COLUMN_IS_OBSOLETE, **cell_kwargs)
            is_obsolete_text = is_obsolete_cell.lower()
            if is_obsolete_text not in (_TSV_TRUE, _TSV_FALSE):
                raise ValueError(
                    f"invalid {_TERMS_COLUMN_IS_OBSOLETE} value"
                    f" {is_obsolete_cell!r} for term_id {term_id!r};"
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

            # The database ties the two columns together: an obsolete term must
            # record why it is obsolete, and a live one must not carry a reason.
            # Rejecting here names the row, rather than leaving the CHECK to
            # fire part-way through the load.
            is_obsolete = is_obsolete_text == _TSV_TRUE
            if is_obsolete and obsoletion_kind is None:
                raise ValueError(
                    f"{_TERMS_COLUMN_IS_OBSOLETE} is {_TSV_TRUE!r} with no"
                    f" {_TERMS_COLUMN_OBSOLETION_KIND} for term_id {term_id!r}"
                )
            if not is_obsolete and obsoletion_kind is not None:
                raise ValueError(
                    f"{_TERMS_COLUMN_OBSOLETION_KIND} {kind_text!r} on a"
                    f" {_TSV_FALSE!r} {_TERMS_COLUMN_IS_OBSOLETE} row"
                    f" for term_id {term_id!r}"
                )

            rows.append(
                ParsedTerm(
                    term_id=term_id,
                    label=label,
                    alternate_label=alternate_label,
                    is_obsolete=is_obsolete,
                    replaced_by_term_id=replaced_by,
                    obsoletion_kind=obsoletion_kind,
                )
            )

    _check_no_duplicate_keys(
        (term.term_id for term in rows),
        path=path,
        source_name=_TERMS_SOURCE_NAME,
        key_name="term_id",
    )
    return rows


def write_terms_tsv(path: Path, terms: list[ParsedTerm]) -> None:
    """Write `terms` as the tab-separated terms table at `path`, headed by
    TERMS_TSV_COLUMNS. A label, alternate_label, replaced_by_term_id, or
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
                    term.label if term.label is not None else "",
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
    (ancestor_term_id, descendant_term_id, distance) tuples.

    Raises ValueError when the header lacks a declared column, when an endpoint
    cell names nothing, when a distance is absent, unparseable, or negative, or
    when an ancestor/descendant pair occupies more than one row.
    """
    rows: list[tuple[str, str, int]] = []
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        check_tsv_columns(
            reader.fieldnames,
            CLOSURE_TSV_COLUMNS,
            source_name=_CLOSURE_SOURCE_NAME,
            path=path,
        )

        cell_kwargs = {"path": path, "source_name": _CLOSURE_SOURCE_NAME}

        for row in reader:
            ancestor_term_id = _stripped_key(row, _CLOSURE_COLUMN_ANCESTOR_TERM_ID, **cell_kwargs)
            descendant_term_id = _stripped_key(
                row, _CLOSURE_COLUMN_DESCENDANT_TERM_ID, **cell_kwargs
            )
            pair = (ancestor_term_id, descendant_term_id)

            # A row that stops early leaves the cell absent rather than empty,
            # so the cast is wrapped to name the pair instead of failing on the
            # absence itself.
            distance_text = row[_CLOSURE_COLUMN_DISTANCE]
            try:
                distance = int(distance_text)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"invalid {_CLOSURE_COLUMN_DISTANCE} {distance_text!r} for {pair}"
                ) from exc
            if distance < 0:
                raise ValueError(f"negative {_CLOSURE_COLUMN_DISTANCE} {distance} for {pair}")

            rows.append((ancestor_term_id, descendant_term_id, distance))

    # The database holds the ancestor/descendant pair unique without regard to
    # distance, so two rows disagreeing on distance collide like an exact repeat.
    _check_no_duplicate_keys(
        ((ancestor, descendant) for ancestor, descendant, _ in rows),
        path=path,
        source_name=_CLOSURE_SOURCE_NAME,
        key_name="ancestor/descendant pair",
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


def _find_unresolved_replaced_by(
    parsed_terms: list[ParsedTerm],
    known_term_ids: set[str],
) -> list[tuple[str, str]]:
    """Return (term_id, attempted_target) pairs where an obsolete term
    names a replaced_by_term_id absent from the same batch. Returns the
    empty list when every pointer resolves in-batch.

    Currently we only support replacements within the same terminology, so
    a target in another vocabulary is unresolved."""
    unresolved_pairs = [
        (term.term_id, term.replaced_by_term_id)
        for term in parsed_terms
        if term.is_obsolete
        and term.replaced_by_term_id is not None
        and term.replaced_by_term_id not in known_term_ids
    ]
    return unresolved_pairs


def _find_unresolved_closure_endpoints(
    parsed_closure: list[tuple[str, str, int]],
    known_term_ids: set[str],
) -> list[tuple[str, str]]:
    """Return the (ancestor, descendant) pairs of closure rows naming a term
    the release does not define. Returns the empty list when every endpoint
    resolves in-batch.

    A closure relates terms of one terminology — the database ties both
    endpoints to a single terminology_idx — so an endpoint the release does
    not define can never be stored.
    """
    unresolved_pairs = [
        (ancestor, descendant)
        for ancestor, descendant, _ in parsed_closure
        if ancestor not in known_term_ids or descendant not in known_term_ids
    ]
    return unresolved_pairs


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
    already in the database, or when it references a term it does not
    define — an obsolete row's replaced_by_term_id, or either endpoint of
    a closure row.

    With `tolerate_anomalies=True`, those anomaly kinds are absorbed:
    silent drops are auto-obsoleted (obsoletion_kind=silently_dropped,
    label carried forward), unresolved replaced_by_term_id values are
    NULLed on the affected rows with a notes line recording the attempted
    CURIE, and a closure row naming an undefined endpoint is dropped, so
    the reported closure count covers only the rows that resolved. A
    misaligned replaced_by_term_id (non-obsolete row carrying a pointer)
    always raises — it is malformed source data rather than a tolerable
    anomaly.
    """

    # Verify before parsing, so a release table that does not match the
    # manifest is refused without any of its content being read.
    manifest = load_manifest(source_dir)
    verify_manifest_checksums(source_dir, manifest)

    # Read each table from the path the manifest declares, which is the path
    # its digest was checked against.
    parsed_terms = _parse_terms_tsv(source_dir / manifest.terms.path)
    parsed_closure = _parse_closure_tsv(source_dir / manifest.closure.path)

    # Misalignment is always fatal; it indicates malformed source data.
    _check_misaligned_replaced_by(parsed_terms)

    # One set of defined ids serves both checks below; rebuilding it per check
    # is a second pass over a batch that runs to millions of terms.
    known_term_ids = {term.term_id for term in parsed_terms}

    # A release is authoritative and self-contained, so an id it references but
    # does not define is a dangling reference rather than a lookup to widen.
    # Checked before the transaction so a rejection writes nothing, and both
    # kinds are collected so one read of the error names everything unresolved.
    unresolved_replaced_by_pairs = _find_unresolved_replaced_by(parsed_terms, known_term_ids)
    unresolved_closure_endpoints = _find_unresolved_closure_endpoints(
        parsed_closure, known_term_ids
    )
    if not tolerate_anomalies and (unresolved_replaced_by_pairs or unresolved_closure_endpoints):
        raise TerminologyImportAnomaly(
            unresolved_replaced_by=unresolved_replaced_by_pairs,
            unresolved_closure_endpoints=unresolved_closure_endpoints,
        )

    # Tolerate mode: NULL the unresolved replaced_by_term_id on the affected
    # ParsedTerm rows, so no pointer the release cannot resolve is stored; the
    # attempted target is recorded as a note on the row instead.
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
