"""Read a jplace of any size through miint's `read_jplace`, refusing what it would truncate.

Two properties of `read_jplace` shape this module; the `read_jplace` entry in
`docs/duckdb-miint.md` describes both:

* It cannot read a file over 1 GB (duckdb-miint#400), so `load_jplace` splits the
  `placements` array across part files under `_PART_MAX_BYTES` and reads them one call
  at a time.
* It keeps one name and one location per placement and reads the location row by
  position, ignoring `fields` (duckdb-miint#407), so the split refuses any placement or
  `fields` list it would load as something other than what the file says.

The split is JSON framing only: each placement is located with
`json.JSONDecoder.raw_decode`, checked, and its source text copied through unchanged, so
miint still does the parsing. A part carries only the `placements` array — `read_jplace`
reads nothing else — so it is not a jplace by the spec, and it never leaves the job's
temp directory. The jplace's own `tree` is kept too, from the same pass rather than
`read_jplace_newick` (duckdb-miint#413, in the same `docs/duckdb-miint.md` entry): its
`{N}` decorations are what the placements' `edge_num`s name.

This is a **private helper**, not a dispatchable native job: it exports neither `Inputs`
nor `execute`, and its leading underscore exempts it from the boot-time job scan.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO, TextIO

import duckdb

# The location-row layout `read_jplace` assumes: it takes edge_num from `p[0][0]`,
# likelihood from `p[0][1]`, and so on, whatever the file's `fields` say.
EXPECTED_FIELDS = ("edge_num", "likelihood", "like_weight_ratio", "distal_length", "pendant_length")

# Upper bound on one part file. The macro's 1e9-byte object limit (duckdb-miint#400) is
# the hard ceiling, but time sets the size: one `read_jplace` call grows much faster
# than linearly in the placements it reads. Measured on Greengenes2 2024.09's placement
# file (DuckDB 2.0.0, miint e88619bf00, 8 threads), a first part of
#    16 MiB (173K placements) read in 0.6 s,     64 MiB (699K) in 5.9 s,
#   128 MiB (1.4M) in 45 s,                     256 MiB (2.8M) in 214 s,
# and the whole 2.26 GB file, split at 16 MiB, loaded in 115 s at 2.4 GB peak RSS.
# The growth is the macro's: plain `read_json` with `columns={'placements': 'JSON[]'}`
# read the same 256 MiB part in 6.9 s. Neither reads the whole file in one call: that
# `read_json` ran out of memory at a 24 GB limit.
_PART_MAX_BYTES = 16 * 1024 * 1024

# Characters pulled from the source per read, at least. A GG2 placement is about 100
# bytes as released and about 200 once its name is its sequence, so this only sets how
# often the buffer is refilled; a value longer than the buffer (a tree string) doubles
# it per refill, so the copying stays linear.
_READ_CHARS = 16 * 1024 * 1024

_PART_PREFIX = b'{"placements": ['
_PART_SUFFIX = b"]}"

_DECODER = json.JSONDecoder()
# What `raw_decode` leaves unread of a valid number cut short by the buffer's end. It
# takes the longest number it can, so the rest is nothing ("12" of "123"), or a "." or
# an exponent it could not finish ("3" of "3.5", "1" of "1e+5"). Anything else after a
# number is malformed, and is not read further looking for more.
_CUT_NUMBER_TAILS = frozenset({"", ".", "e", "E", "e+", "e-", "E+", "E-"})


class _JsonStream:
    """A cursor over one JSON document, decoding one value at a time.

    `raw_decode` needs a whole value in memory but not the whole document, so the
    buffer holds the unread tail of what has been read, and grows whenever a value runs
    past its end."""

    def __init__(self, fh: TextIO, source: Path) -> None:
        self._fh = fh
        self._source = source
        self._buf = ""
        self._pos = 0
        self._eof = False

    def _fill(self) -> bool:
        if self._eof:
            return False
        more = self._fh.read(max(_READ_CHARS, len(self._buf) - self._pos))
        if not more:
            self._eof = True
            return False
        self._buf = self._buf[self._pos :] + more
        self._pos = 0
        return True

    def peek(self) -> str:
        """The next non-whitespace character, without consuming it; "" at end of file."""
        while True:
            buf, pos = self._buf, self._pos
            while pos < len(buf) and buf[pos] in " \t\r\n":
                pos += 1
            self._pos = pos
            if pos < len(buf):
                return buf[pos]
            if not self._fill():
                return ""

    def expect(self, char: str) -> None:
        found = self.peek()
        if found != char:
            raise ValueError(
                f"{self._source}: expected {char!r} in the jplace's JSON, found {found or 'EOF'!r}"
            )
        self._pos += 1

    def value(self, *, max_chars: int | None = None) -> tuple[object, str]:
        """Decode the next value and return it with its exact source text.

        `max_chars` bounds how far a value that will not decode is read before it is
        called malformed; without it, a broken value is read to end of file first."""
        self.peek()
        while True:
            try:
                obj, end = _DECODER.raw_decode(self._buf, self._pos)
            except json.JSONDecodeError as e:
                within = max_chars is None or len(self._buf) - self._pos <= max_chars
                if within and self._fill():
                    continue
                raise ValueError(f"{self._source}: malformed jplace JSON: {e}") from e
            # A number is the one value that decodes when cut short (`_CUT_NUMBER_TAILS`);
            # a cut object, array or string fails to decode instead.
            if (
                isinstance(obj, int | float)
                and not isinstance(obj, bool)
                and len(self._buf) - end <= 2
                and self._buf[end:] in _CUT_NUMBER_TAILS
                and self._fill()
            ):
                continue
            text = self._buf[self._pos : end]
            self._pos = end
            return obj, text


def _check_fields(fields: object, source: Path) -> None:
    if not isinstance(fields, list) or tuple(fields) != EXPECTED_FIELDS:
        raise ValueError(
            f"{source}: jplace fields are {fields!r}; read_jplace reads each location row by "
            f"position as {list(EXPECTED_FIELDS)} and ignores 'fields' (duckdb-miint#407), "
            "so any other list would load values into the wrong columns or drop some"
        )


def _check_placement(obj: object, source: Path, index: int) -> None:
    """Refuse a placement `read_jplace` would load as anything other than what it says."""

    def refuse(why: str) -> ValueError:
        return ValueError(
            f"{source}: placement {index} {why}, which read_jplace would not load as written "
            f"(it reads one name and one location per placement, duckdb-miint#407): "
            f"{json.dumps(obj)[:200]}"
        )

    if not isinstance(obj, dict):
        raise refuse("is not a JSON object")
    if not obj.keys() <= {"p", "n", "nm"}:
        raise refuse(f"carries keys {sorted(obj.keys() - {'p', 'n', 'nm'})}")
    if ("n" in obj) == ("nm" in obj):
        raise refuse("does not carry exactly one of 'n' and 'nm'")
    if "n" in obj:
        names = obj["n"]
        if not (isinstance(names, list) and len(names) == 1 and isinstance(names[0], str)):
            raise refuse("does not name exactly one fragment in 'n'")
    else:
        names = obj["nm"]
        if not (
            isinstance(names, list)
            and len(names) == 1
            and isinstance(names[0], list)
            and len(names[0]) == 2
            and isinstance(names[0][0], str)
            and type(names[0][1]) is int
            and names[0][1] == 1
        ):
            raise refuse("does not name exactly one fragment, of integer multiplicity 1, in 'nm'")
    locations = obj.get("p")
    if not (isinstance(locations, list) and len(locations) == 1):
        raise refuse("does not carry exactly one location in 'p'")
    row = locations[0]
    if not (isinstance(row, list) and len(row) == len(EXPECTED_FIELDS)):
        raise refuse(f"has a location row that is not {len(EXPECTED_FIELDS)} values")
    edge_num = row[0]
    if not isinstance(edge_num, int) or isinstance(edge_num, bool):
        raise refuse(f"has an edge_num that is not an integer ({edge_num!r})")


class _PartWriter:
    """Write placements into numbered part files, none over `max_bytes`."""

    def __init__(self, out_dir: Path, max_bytes: int) -> None:
        self._out_dir = out_dir
        self._max_bytes = max_bytes
        self._parts = 0
        self._fh: BinaryIO | None = None
        self._path: Path | None = None
        self._size = 0
        self._placements = 0

    def add(self, text: str) -> tuple[Path, int] | None:
        """Append one placement; return the part it closed, if it closed one."""
        data = text.encode()
        if len(_PART_PREFIX) + len(data) + len(_PART_SUFFIX) > self._max_bytes:
            raise ValueError(
                f"one placement is {len(data)} bytes, more than a part may hold "
                f"({self._max_bytes} bytes)"
            )
        closed = None
        if self._fh is not None and (
            self._size + 1 + len(data) + len(_PART_SUFFIX) > self._max_bytes
        ):
            closed = self.close()
        if self._fh is None:
            self._path = self._out_dir / f"part-{self._parts:05d}.json"
            self._parts += 1
            self._fh = self._path.open("wb")
            self._fh.write(_PART_PREFIX)
            self._size = len(_PART_PREFIX)
            self._placements = 0
        if self._placements:
            self._fh.write(b",")
            self._size += 1
        self._fh.write(data)
        self._size += len(data)
        self._placements += 1
        return closed

    def close(self) -> tuple[Path, int] | None:
        """Finish the open part, if any; return its path and placement count."""
        if self._fh is None:
            return None
        self._fh.write(_PART_SUFFIX)
        self._fh.close()
        self._fh = None
        assert self._path is not None
        return self._path, self._placements


def split_jplace(
    source: Path, out_dir: Path, *, part_max_bytes: int, tree_out: Path
) -> Iterator[tuple[Path, int]]:
    """Yield `(part_path, placement_count)` for parts that together carry every
    placement in `source`, in file order, and write the jplace's `tree` string to
    `tree_out` once the whole file has been read.

    A part is complete when yielded, and the caller may delete it before asking for the
    next, so at most two parts are on disk at a time: the one yielded, and the next one,
    which already holds the placement that did not fit in the yielded one. Every
    placement is checked as it
    passes. `fields` and `tree` may come before or after `placements` — pplacer writes
    `fields` after — so a wrong or missing one can raise after parts have been yielded:
    nothing a caller builds from the parts is final until the generator is exhausted. A
    jplace with an empty `placements` array yields nothing.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    writer = _PartWriter(out_dir, part_max_bytes)
    keys: set[str] = set()
    tree: object = None
    try:
        with source.open(encoding="utf-8") as fh:
            stream = _JsonStream(fh, source)
            stream.expect("{")
            while stream.peek() != "}":
                if keys:
                    stream.expect(",")
                key, _ = stream.value()
                if not isinstance(key, str):
                    raise ValueError(f"{source}: a jplace key is not a string: {key!r}")
                if key in keys:
                    raise ValueError(f"{source}: the jplace repeats the key {key!r}")
                keys.add(key)
                stream.expect(":")
                if key != "placements":
                    value, _ = stream.value()
                    if key == "fields":
                        _check_fields(value, source)
                    elif key == "tree":
                        tree = value
                    continue
                stream.expect("[")
                index = 0
                while stream.peek() != "]":
                    if index:
                        stream.expect(",")
                    obj, text = stream.value(max_chars=part_max_bytes)
                    _check_placement(obj, source, index)
                    closed = writer.add(text)
                    if closed is not None:
                        yield closed
                    index += 1
                stream.expect("]")
            stream.expect("}")
            if stream.peek():
                raise ValueError(f"{source}: unexpected content after the jplace's JSON object")
        for required in ("placements", "fields", "tree"):
            if required not in keys:
                raise ValueError(f"{source}: the jplace has no {required!r}")
        if not isinstance(tree, str):
            raise ValueError(f"{source}: the jplace's 'tree' is not a Newick string")
        # Taken from this pass rather than miint's `read_jplace_newick`
        # (duckdb-miint#413; its bullet in docs/duckdb-miint.md says why).
        tree_out.write_text(tree, encoding="utf-8")
        last = writer.close()
        if last is not None:
            yield last
    finally:
        writer.close()


def load_jplace(
    conn: duckdb.DuckDBPyConnection,
    jplace_path: Path,
    *,
    table: str,
    tree_table: str,
    work_dir: Path,
) -> None:
    """Read every placement in `jplace_path` into a new TEMP TABLE `table`, and the
    jplace's own tree into a new TEMP TABLE `tree_table` (`read_newick`'s columns),
    refusing a tree with an edge that has no edge number.

    `table` has `read_jplace`'s columns minus `filepath`, one row per placement in the
    file: the split refuses every shape `read_jplace` would collapse, and each part is
    checked to come back with as many rows as it was written with, since a part that
    loads fewer is exactly the silent loss this module exists to rule out.
    """
    for name in (table, tree_table):
        if not name.isidentifier():
            raise ValueError(f"not a table name: {name!r}")
    tree_path = work_dir / "jplace_tree.nwk"
    conn.execute(
        f"CREATE TEMP TABLE {table} ("
        "  fragment VARCHAR, edge_num INTEGER, likelihood DOUBLE,"
        "  like_weight_ratio DOUBLE, distal_length DOUBLE, pendant_length DOUBLE)"
    )
    for part, written in split_jplace(
        jplace_path, work_dir, part_max_bytes=_PART_MAX_BYTES, tree_out=tree_path
    ):
        (inserted,) = conn.execute(
            f"INSERT INTO {table} "
            "SELECT fragment, edge_num, likelihood, like_weight_ratio,"
            "  distal_length, pendant_length "
            "FROM read_jplace(?)",
            [str(part)],
        ).fetchone()
        if inserted != written:
            raise ValueError(
                f"{part.name}: read_jplace returned {inserted} rows for {written} placements"
            )
        part.unlink()
    conn.execute(
        f"CREATE TEMP TABLE {tree_table} AS SELECT * FROM read_newick(?)", [str(tree_path)]
    )
    tree_path.unlink()
    # Every edge must carry its `{N}`, or a placement on it could not name it.
    # `read_newick` reads an edge number written `[N]`, as some jplace writers do, as a
    # comment (duckdb-miint#419), so such a tree arrives here with none.
    unnumbered, edges = conn.execute(
        f"SELECT count(*) FILTER (WHERE edge_id IS NULL), count(*) FROM {tree_table}"
        " WHERE parent_index IS NOT NULL"
    ).fetchone()
    if unnumbered:
        raise ValueError(
            f"{jplace_path}: {unnumbered} of the {edges} edges in the jplace's tree carry no "
            "{N} edge number, so the tree is refused; number every edge {N}. An edge number "
            "written [N] does not count: read_newick takes it as a comment (duckdb-miint#419)"
        )
