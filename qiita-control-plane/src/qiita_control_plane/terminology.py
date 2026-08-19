"""Action layer for the terminology subsystem: reads a staged release, checks
it against its manifest, and applies it to the database."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from pathlib import Path

import asyncpg
import pyarrow as pa
from qiita_common.models import (
    TerminologyManifest,
    TerminologyTermObsoletionKind,
)

from .miint import duckdb_connect
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

# The columns that each release table holds, in the order they are written.
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

# How an error names each release table.
_TERMS_SOURCE_NAME = "terms table"
_CLOSURE_SOURCE_NAME = "closure table"

# How the writer spells a boolean cell, and exactly what the parser accepts.
# Deliberately not shared with the OWL source's own boolean spelling, which is
# a separate contract that merely happens to coincide.
_TSV_TRUE = "true"
_TSV_FALSE = "false"

# How a table is written: the name the rows register under, and the statement
# that copies them out. Pinning the terminator to LF here spares each table
# remembering it, and binding the destination spares escaping a path that
# holds a quote.
_WRITE_RELATION_NAME = "release_table"
_COPY_TSV_SQL = (
    f"COPY {_WRITE_RELATION_NAME} TO ? (FORMAT CSV, DELIMITER '\t', HEADER true, NEW_LINE '\n')"
)

# How a table is read. Every column is VARCHAR so the parser, not a guessed
# type, interprets each cell; a first pass reads the header as data, which is
# the only way to see it.
_READ_TSV_SQL = (
    "SELECT * FROM read_csv(?, delim='\t', header={header}, columns={columns},"
    " auto_detect=false, quote='\"', escape='\"')"
)
_READ_HEADER_SUFFIX = " LIMIT 1"

# Mirrors qiita.terminology_term: term_id and replaced_by_term_id
# VARCHAR(255), label and alternate_label VARCHAR(500).
MAX_TERM_ID_LENGTH = 255
MAX_TERM_NAME_LENGTH = 500

# What keys one row of a release table, where the database holds rows unique:
# a term id, or an ancestor/descendant pair.
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


def row_location(*, path: Path, source_name: str, row_number: int) -> str:
    """Name the row an error is about: which table, where it lives, and which
    row, counting the first row after the header as row 1.

    A release runs to millions of rows, so an error naming only the column
    leaves an operator nothing to look up.
    """
    return f"{source_name} at {path} row {row_number}"


def stripped_cell(
    row: Mapping[str, str],
    column: str,
    *,
    path: Path,
    source_name: str,
    row_number: int,
    max_length: int | None = None,
) -> str:
    """Return `column`'s cell from `row`, stripped. A whitespace-only cell
    comes back empty, which the caller interprets.

    `row` carries one cell per declared column, so the read already refused a
    short row. `column` and the row location name the offending cell in the
    error. `max_length` bounds the cell, for a value the database stores in a
    fixed-width column.
    """
    value = row[column].strip()

    # Bounding here names the offending row; leaving it to the column would
    # reject the whole release from inside the load, naming nothing.
    if max_length is not None and len(value) > max_length:
        location = row_location(path=path, source_name=source_name, row_number=row_number)
        raise ValueError(
            f"{location} carries a {column} of {len(value)} characters;"
            f" at most {max_length} can be stored"
        )
    return value


def stripped_key(
    row: Mapping[str, str],
    column: str,
    *,
    path: Path,
    source_name: str,
    row_number: int,
    max_length: int | None = None,
) -> str:
    """Return a cell holding a term id, rejecting an empty one.

    The database keys or matches rows by term id, so it cannot store an empty
    one — though it can store a name the source leaves out.
    """
    key = stripped_cell(
        row,
        column,
        path=path,
        source_name=source_name,
        row_number=row_number,
        max_length=max_length,
    )
    if not key:
        location = row_location(path=path, source_name=source_name, row_number=row_number)
        raise ValueError(f"{location} carries an empty {column}")
    return key


def _check_no_duplicate_keys(
    keys: Iterable[ReleaseTableKey],
    *,
    path: Path,
    source_name: str,
    key_name: str,
) -> None:
    """Reject a release table whose rows collide on the key the database holds
    unique, naming the colliding values — a capped sample plus the total once
    they exceed the cap.

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


def _columns_spec(columns: tuple[str, ...]) -> str:
    """Render `columns` as the column set a read binds."""
    declared = []
    for column in columns:
        escaped = column.replace("'", "''")
        declared.append(f"'{escaped}': 'VARCHAR'")
    return "{" + ", ".join(declared) + "}"


def read_tsv(
    path: Path,
    columns: tuple[str, ...],
    *,
    source_name: str,
) -> list[dict[str, str]]:
    """Read the tab-separated table at `path` into one `columns`-keyed dict per
    row. An empty cell becomes the empty string; `source_name` identifies the
    table in header errors.

    Raises duckdb.Error when DuckDB cannot read the file, or when a row holds
    the wrong number of cells — the error names the row and both counts.
    Raises ValueError when `path` is not a file, or when the header is missing
    or lists `columns` in another order.
    """
    # DuckDB reads a directory as a glob of the tables under it, never the
    # named table; refusing it names the source instead.
    if not path.is_file():
        raise ValueError(f"{source_name} at {path} is not a file")

    spec = _columns_spec(columns)
    header_sql = _READ_TSV_SQL.format(header="false", columns=spec) + _READ_HEADER_SUFFIX
    read_sql = _READ_TSV_SQL.format(header="true", columns=spec)

    conn = duckdb_connect()
    try:
        header_cursor = conn.execute(header_sql, [str(path)])
        header_rows = header_cursor.fetchall()
        _check_header(header_rows, columns, source_name=source_name, path=path)
        row_cursor = conn.execute(read_sql, [str(path)])
        read_rows = row_cursor.fetchall()
    finally:
        conn.close()

    # A NULL cell is an empty one: every row holds every declared column,
    # because the read itself refuses a short row.
    rows = [
        dict(zip(columns, [cell if cell is not None else "" for cell in read_row], strict=True))
        for read_row in read_rows
    ]
    return rows


def _check_header(
    header_rows: list[tuple[str | None, ...]],
    columns: tuple[str, ...],
    *,
    source_name: str,
    path: Path,
) -> None:
    """Reject a table whose first row does not hold `columns` in order.

    `header_rows` holds the first row read as data, empty when the table has no
    rows at all. Order matters, not just membership: the declared column set
    maps positionally, so a header listing the same columns in another order
    would load every cell under the wrong one and still read as valid.
    """
    if not header_rows:
        raise ValueError(f"{source_name} at {path} carries no header; expected {list(columns)}")
    present_columns = tuple(cell if cell is not None else "" for cell in header_rows[0])
    if present_columns != columns:
        raise ValueError(
            f"{source_name} at {path} is headed by {list(present_columns)};"
            f" expected {list(columns)} in that order"
        )


def _parse_terms_tsv(path: Path) -> list[ParsedTerm]:
    """Parse a tab-separated terms table at `path` into a list of ParsedTerm,
    which settles what an empty cell means per column. is_obsolete must be
    'true' or 'false' case-insensitively.

    Raises ValueError when the header does not hold the declared columns in
    order, when a term id cell is empty or longer than the database stores,
    when is_obsolete holds an uninterpretable value, when obsoletion_kind is
    unrecognized, or when a term id occupies more than one row; raises
    duckdb.Error when a row does not carry one cell per column.
    """
    rows: list[ParsedTerm] = []
    read_rows = read_tsv(path, TERMS_TSV_COLUMNS, source_name=_TERMS_SOURCE_NAME)
    for row_number, row in enumerate(read_rows, start=1):
        # What every error about this row names.
        cell_kwargs = {
            "path": path,
            "source_name": _TERMS_SOURCE_NAME,
            "row_number": row_number,
        }

        term_id = stripped_key(
            row, _TERMS_COLUMN_TERM_ID, **cell_kwargs, max_length=MAX_TERM_ID_LENGTH
        )
        label = stripped_cell(
            row, _TERMS_COLUMN_LABEL, **cell_kwargs, max_length=MAX_TERM_NAME_LENGTH
        )
        alternate_label = stripped_cell(
            row, _TERMS_COLUMN_ALTERNATE_LABEL, **cell_kwargs, max_length=MAX_TERM_NAME_LENGTH
        )

        # Accept only the two spellings the writer emits, so anything else
        # is refused at parse time.
        is_obsolete_cell = stripped_cell(row, _TERMS_COLUMN_IS_OBSOLETE, **cell_kwargs)
        is_obsolete_text = is_obsolete_cell.lower()
        if is_obsolete_text not in (_TSV_TRUE, _TSV_FALSE):
            raise ValueError(
                f"{row_location(**cell_kwargs)} carries an invalid"
                f" {_TERMS_COLUMN_IS_OBSOLETE} value {is_obsolete_cell!r}"
                f" for term_id {term_id!r};"
                f" expected {_TSV_TRUE!r} or {_TSV_FALSE!r}"
            )

        # Read in declared column order, so a short row fails on the first
        # cell it cannot reach.
        replaced_by = stripped_cell(
            row,
            _TERMS_COLUMN_REPLACED_BY_TERM_ID,
            **cell_kwargs,
            max_length=MAX_TERM_ID_LENGTH,
        )

        # Wrap the enum cast so the error names the offending row.
        kind_text = stripped_cell(row, _TERMS_COLUMN_OBSOLETION_KIND, **cell_kwargs) or None
        try:
            obsoletion_kind = TerminologyTermObsoletionKind(kind_text) if kind_text else None
        except ValueError as exc:
            raise ValueError(
                f"{row_location(**cell_kwargs)} carries an invalid"
                f" {_TERMS_COLUMN_OBSOLETION_KIND} {kind_text!r}"
                f" for term_id {term_id!r}"
            ) from exc

        # An obsolete term must record why, and a live one must not carry a
        # reason; rejecting here names the offending row.
        is_obsolete = is_obsolete_text == _TSV_TRUE
        if is_obsolete and obsoletion_kind is None:
            raise ValueError(
                f"{row_location(**cell_kwargs)} carries"
                f" {_TERMS_COLUMN_IS_OBSOLETE} {_TSV_TRUE!r} with no"
                f" {_TERMS_COLUMN_OBSOLETION_KIND} for term_id {term_id!r}"
            )
        if not is_obsolete and obsoletion_kind is not None:
            raise ValueError(
                f"{row_location(**cell_kwargs)} carries"
                f" {_TERMS_COLUMN_OBSOLETION_KIND} {kind_text!r} on a"
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


def write_tsv(
    path: Path,
    columns: tuple[str, ...],
    rows: Sequence[Sequence[str | None]] = (),
) -> None:
    """Write `rows` as the tab-separated table at `path`, headed by `columns`.

    Cells are positional, so every row holds one per column in `columns` order,
    and a None cell reaches the file empty. Passing no rows writes the header
    alone.

    Raises ValueError when a row does not carry one cell per column, and
    duckdb.Error when DuckDB cannot write the file.
    """
    # Arrow rather than an INSERT per row, which crosses into DuckDB once per
    # row and is prohibitively slow at a few million terms.
    cells_by_column = list(zip(*rows)) if rows else [()] * len(columns)
    arrays = {
        column: pa.array(cells, type=pa.string())
        for column, cells in zip(columns, cells_by_column, strict=True)
    }
    relation = pa.table(arrays)

    # None reaches the file empty only while it stays NULL: DuckDB quotes an
    # empty string, which would read back as a cell holding "".
    conn = duckdb_connect()
    try:
        conn.register(_WRITE_RELATION_NAME, relation)
        conn.execute(_COPY_TSV_SQL, [str(path)])
    finally:
        conn.close()


def write_terms_tsv(path: Path, terms: list[ParsedTerm]) -> None:
    """Write `terms` as the tab-separated terms table at `path`, headed by
    TERMS_TSV_COLUMNS. A label, alternate_label, replaced_by_term_id, or
    obsoletion_kind of None becomes an empty cell."""
    # write_tsv places cells positionally, so this list stays in
    # TERMS_TSV_COLUMNS order.
    rows = [
        (
            term.term_id,
            term.label,
            term.alternate_label,
            _TSV_TRUE if term.is_obsolete else _TSV_FALSE,
            term.replaced_by_term_id,
            str(term.obsoletion_kind) if term.obsoletion_kind is not None else None,
        )
        for term in terms
    ]
    write_tsv(path, TERMS_TSV_COLUMNS, rows)


def write_closure_tsv_stub(path: Path) -> None:
    """Write a closure table at `path` holding only its CLOSURE_TSV_COLUMNS
    header. A closure table with no data rows leaves the terminology's
    closure empty, so term resolution works while subsumption queries have
    nothing to answer from."""
    write_tsv(path, CLOSURE_TSV_COLUMNS)


def _parse_closure_tsv(path: Path) -> list[tuple[str, str, int]]:
    """Parse a tab-separated closure table at `path` into a list of
    (ancestor_term_id, descendant_term_id, distance) tuples.

    Raises ValueError when the header does not spell the declared columns in
    order, when an endpoint cell is empty, when a distance is unparseable or
    negative, or when an ancestor/descendant pair occupies more than one row;
    raises duckdb.Error when a row does not carry one cell per column.
    """
    rows: list[tuple[str, str, int]] = []
    read_rows = read_tsv(path, CLOSURE_TSV_COLUMNS, source_name=_CLOSURE_SOURCE_NAME)
    for row_number, row in enumerate(read_rows, start=1):
        cell_kwargs = {
            "path": path,
            "source_name": _CLOSURE_SOURCE_NAME,
            "row_number": row_number,
        }

        ancestor_term_id = stripped_key(row, _CLOSURE_COLUMN_ANCESTOR_TERM_ID, **cell_kwargs)
        descendant_term_id = stripped_key(row, _CLOSURE_COLUMN_DESCENDANT_TERM_ID, **cell_kwargs)
        pair = (ancestor_term_id, descendant_term_id)

        # Wrap the cast so the error names the offending row.
        distance_text = stripped_cell(row, _CLOSURE_COLUMN_DISTANCE, **cell_kwargs)
        try:
            distance = int(distance_text)
        except ValueError as exc:
            raise ValueError(
                f"{row_location(**cell_kwargs)} carries an invalid"
                f" {_CLOSURE_COLUMN_DISTANCE} {distance_text!r} for {pair}"
            ) from exc
        if distance < 0:
            raise ValueError(
                f"{row_location(**cell_kwargs)} carries a negative"
                f" {_CLOSURE_COLUMN_DISTANCE} {distance} for {pair}"
            )

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
    naming the offending rows. Always raises, even in tolerate mode: a
    non-obsolete row carrying a pointer is malformed source data rather than a
    recoverable anomaly."""
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

    With `tolerate_anomalies=True`, the load absorbs those anomaly kinds: it
    auto-obsoletes silent drops (obsoletion_kind=silently_dropped, label
    carried forward), NULLs unresolved replaced_by_term_id values on the
    affected rows with a notes line recording the attempted CURIE, and drops a
    closure row naming an undefined endpoint, so the reported closure count
    covers only the rows that resolved. A misaligned replaced_by_term_id
    (non-obsolete row carrying a pointer) always raises — it is malformed
    source data rather than a tolerable anomaly.
    """

    # Verify before parsing, so a table contradicting the manifest is refused
    # before anything reads its content.
    manifest = load_manifest(source_dir)
    verify_manifest_checksums(source_dir, manifest)

    # Read each table from the path the manifest declares, the same path the
    # digest check covered.
    parsed_terms = _parse_terms_tsv(source_dir / manifest.terms.path)
    parsed_closure = _parse_closure_tsv(source_dir / manifest.closure.path)

    # Misalignment is always fatal; it indicates malformed source data.
    _check_misaligned_replaced_by(parsed_terms)

    # One set of defined ids serves both checks below; rebuilding it per check
    # walks a batch of millions of terms twice.
    known_term_ids = {term.term_id for term in parsed_terms}

    # A release is authoritative and self-contained, so an id it references but
    # does not define dangles rather than widening a lookup. Checking before the
    # transaction means a rejection writes nothing, and collecting both kinds
    # lets one error name everything unresolved.
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
    # ParsedTerm rows, so nothing stores a pointer the release cannot resolve;
    # a note on the row records the attempted target instead.
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
