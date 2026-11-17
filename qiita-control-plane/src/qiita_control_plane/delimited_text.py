"""Reading and writing tab-separated tables, every cell as text. A read checks
the header and refuses a row with the wrong number of cells."""

import csv
import stat
from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa
from qiita_common.models import names_colliding_on_strip

from .miint import duckdb_connect

# How a table is read. Every column is VARCHAR so the parser, not a guessed
# type, interprets each cell; a first pass reads the header as data, which is
# the only way to see it.
_READ_TSV_SQL = (
    "SELECT * FROM read_csv(?, delim='\t', header={header}, columns={columns},"
    " auto_detect=false, quote='\"', escape='\"')"
)
_READ_HEADER_SUFFIX = " LIMIT 1"

# write_tsv hands the rows to DuckDB as a table of this name, then writes that
# table to the file with this COPY. NEW_LINE fixes the line ending to LF, and the
# file path is passed as a bound parameter so a path holding a quote needs no
# escaping.
_WRITE_RELATION_NAME = "written_rows"
_COPY_TSV_SQL = (
    f"COPY {_WRITE_RELATION_NAME} TO ? (FORMAT CSV, DELIMITER '\t', HEADER true, NEW_LINE '\n')"
)


def _require_file(path: Path, *, source_name: str) -> None:
    """Refuse a `path` that is not an existing regular file, naming the source.
    An error examining the path other than its absence, such as a permission
    error, propagates as itself."""
    # Refusing here names the source, where DuckDB reports only that no file
    # matches the path.
    try:
        file_status = path.stat()
        is_regular_file = stat.S_ISREG(file_status.st_mode)
    except FileNotFoundError:
        is_regular_file = False
    if not is_regular_file:
        raise ValueError(f"{source_name} at {path} is not a file")


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
    or lists `columns` in another order; OSError when `path` cannot be examined.
    """
    _require_file(path, source_name=source_name)

    # `columns` may come from a user's header line; _columns_spec escapes them.
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


def read_headed_tsv(path: Path, *, source_name: str) -> list[dict[str, str]]:
    """Read the tab-separated table at `path`, whose first line names its
    columns, into one dict per row keyed on those names stripped of outer
    whitespace. Raises ValueError when the file is empty, when its header line is
    not UTF-8 text, or when a name is blank or repeats another once stripped;
    OSError when the file cannot be opened; otherwise raises as `read_tsv`, whose
    duckdb.Error covers a later byte that is not UTF-8.
    """
    _require_file(path, source_name=source_name)

    # Read the names as written, because read_tsv compares them with the header.
    # utf-8-sig drops a leading byte-order mark, as DuckDB does. csv.reader
    # unquotes a quoted name as read_tsv's DuckDB pass does, so both see one
    # set of names; DuckDB cannot supply the names itself, because its
    # detection takes a later, wider row as the header without any error.
    try:
        with path.open(encoding="utf-8-sig", newline="") as handle:
            first_row = next(csv.reader(handle, delimiter="\t"), None)
    except UnicodeDecodeError as exc:
        raise ValueError(f"{source_name} at {path} is not UTF-8 text") from exc
    if not first_row:
        raise ValueError(f"{source_name} at {path} is empty; expected a header line")
    names = tuple(first_row)

    # Refuse a name that is blank, or that would land on another's key once stripped.
    blank_positions = [position for position, name in enumerate(names, start=1) if not name.strip()]
    if blank_positions:
        raise ValueError(
            f"{source_name} at {path} has a blank column name at position(s) {blank_positions}"
        )
    collisions = names_colliding_on_strip(names)
    if collisions:
        raise ValueError(
            f"{source_name} at {path} names column(s) {collisions} more than once"
            " (ignoring surrounding whitespace)"
        )

    read_rows = read_tsv(path, names, source_name=source_name)
    stripped_rows = [{name.strip(): cell for name, cell in row.items()} for row in read_rows]
    return stripped_rows


def write_tsv(
    path: Path,
    columns: tuple[str, ...],
    rows: Sequence[Sequence[str | None]] = (),
) -> None:
    """Write `rows` as the tab-separated table at `path`, headed by `columns`.

    Cells are positional, so every row holds one per column in `columns` order,
    and a None cell reaches the file empty. Passing no rows writes the header
    alone.

    Raises ValueError when a column name repeats, ignoring case, or a row does
    not carry one cell per column, and duckdb.Error when DuckDB cannot write the
    file.
    """
    # Refuse any repeated column name, ignoring case: an exact repeat would replace
    # the earlier column, and DuckDB renames a repeat that differs only in case.
    folded_columns = [column.casefold() for column in columns]
    repeated_columns = sorted(
        {column for column in columns if folded_columns.count(column.casefold()) > 1}
    )
    if repeated_columns:
        raise ValueError(f"column name(s) {repeated_columns} repeat")

    # Refuse a row of the wrong width, which zip would otherwise truncate.
    for row in rows:
        if len(row) != len(columns):
            raise ValueError(f"row {list(row)} holds {len(row)} cells for {len(columns)} columns")

    # Arrow rather than an INSERT per row, which crosses into DuckDB once per row.
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
