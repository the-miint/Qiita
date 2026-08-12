"""qiita user CLI — the client-side feature table.

The analytic itself is `qiita_common.feature_table`, shared with the server-side
compute job; this module is the half that only a client has: fetching the two
identifier maps over REST, staging every input into the user's own DuckDB,
relabelling the result to public handles, and writing the bundle — the table plus
the map needed to read it — as one all-or-nothing commit.

Nothing here reaches the database. The three data inputs arrive as Flight streams
(the alignment slice and the reference lengths) or as REST reads (the genome map),
and the public handles come from a mint route — all of them with the caller's own
token, on the caller's own machine.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from qiita_common import feature_table as ft
from qiita_common.api_paths import (
    PATH_EXPORTED_IDENTIFIER_PREFIX,
    PATH_EXPORTED_IDENTIFIER_ROOT,
    PATH_REFERENCE_GENOME_MAP,
    PATH_REFERENCE_PREFIX,
)
from qiita_common.models import ExportedIdentifierRequest

from .. import _common

# The registered arrow relations each REST response is staged through. Local to
# this module: the shared builders read them once to CREATE the contract's
# relations, and nothing outside sees them (they are unregistered immediately —
# see `_stage_genome_map`).
_GENOME_MAP_SOURCE = "genome_map_response"
_MINT_SOURCE = "exported_identifier_response"


def _fetch_genome_map(base_url: str, token: str, *, reference_idx: int) -> list[dict[str, Any]]:
    """GET the whole reference's `feature_idx → genome` map: one entry per (feature,
    genome) pair with the genome's `source` / `source_id`.

    **A refusal propagates.** Over its hard cap the route 413s, naming the real size,
    rather than truncating — and there is nothing to fall back to here: a lookup table
    silently missing rows produces a WRONG feature table rather than a short one, and
    the route serves the map whole or not at all. So the `HTTPStatusError` travels up
    to whoever can show the user the size and stop.
    """
    path = f"{PATH_REFERENCE_PREFIX}{PATH_REFERENCE_GENOME_MAP.format(reference_idx=reference_idx)}"
    return _common.call("GET", base_url, token, path)["entries"]


def _mint_exported_identifiers(
    base_url: str, token: str, *, alignment_idx: int, prep_sample_idx: list[int]
) -> list[dict[str, Any]]:
    """Mint (or recover) the public `export_id` of each processed sample in the cohort.

    Idempotent server-side, so a retried build names its samples the same way. The body
    goes through the route's own request model, which validates the cohort's shape and
    cap locally rather than after a round trip.
    """
    body = ExportedIdentifierRequest(
        alignment_idx=alignment_idx, prep_sample_idx=prep_sample_idx
    ).model_dump()
    path = f"{PATH_EXPORTED_IDENTIFIER_PREFIX}{PATH_EXPORTED_IDENTIFIER_ROOT}"
    return _common.call("POST", base_url, token, path, json=body)["identifiers"]


def _stage_genome_map(con, entries: list[dict[str, Any]]) -> None:
    """Stage the genome-map response into both relations it feeds — the roll-up key and
    the public label — from the one response, so they cannot disagree about which
    genomes exist.

    The arrow schema is written out rather than inferred: miint's functions take native
    BIGINT id columns, and an empty map (a 16S reference has no genome-bearing features)
    would otherwise stage NULL-typed columns that fail the first join.

    The response's `source` is not staged. It exists so a consumer can tell two
    same-`source_id` genomes apart, and `check_relabel_diagnostics` is what acts on
    that — by refusing; a published table names the genome by `source_id` alone.
    """
    import pyarrow as pa  # noqa: PLC0415

    source = pa.table(
        {
            "feature_idx": pa.array([e["feature_idx"] for e in entries], pa.int64()),
            "genome_idx": pa.array([e["genome_idx"] for e in entries], pa.int64()),
            "source_id": pa.array([e["source_id"] for e in entries], pa.string()),
        }
    )
    con.register(_GENOME_MAP_SOURCE, source)
    try:
        for sql in ft.genome_map_relations_sql(_GENOME_MAP_SOURCE):
            con.execute(sql)
    finally:
        # Released as soon as the CREATEs have copied it: at the route's cap this is
        # a quarter-million pairs, and holding the arrow table as well as the two
        # relations doubles that for nothing.
        con.unregister(_GENOME_MAP_SOURCE)


def _stage_exported_identifiers(con, identifiers: list[dict[str, Any]]) -> None:
    """Stage the mint's response into the sample-label relation.

    `prep_sample_idx` and `export_id` only: the accessions ride the response because
    they are already public, but they are not what a table's columns are named with,
    and this relation is joined into an artifact.
    """
    import pyarrow as pa  # noqa: PLC0415

    source = pa.table(
        {
            "prep_sample_idx": pa.array([i["prep_sample_idx"] for i in identifiers], pa.int64()),
            "export_id": pa.array([i["export_id"] for i in identifiers], pa.string()),
        }
    )
    con.register(_MINT_SOURCE, source)
    try:
        con.execute(ft.sample_label_table_sql(_MINT_SOURCE))
    finally:
        con.unregister(_MINT_SOURCE)


def _relabel(con) -> ft.LabelClearance:
    """Attach the public handles to the counts, refusing anything that would publish a
    wrong table, and write `LABELLED_TABLE`. Returns the clearance, whose `rows` is the
    size of the table now written.

    The diagnostics row is passed to the check **by name** — the query's column names
    are the check's parameter names — so a rename on either side is a `TypeError` here
    rather than one count silently arriving as another's argument.
    """
    cursor = con.execute(ft.relabel_diagnostics_sql())
    names = [column[0] for column in cursor.description]
    clearance = ft.check_relabel_diagnostics(**dict(zip(names, cursor.fetchone(), strict=True)))
    con.execute(ft.labelled_table_sql(clearance=clearance))
    return clearance


# The bundle's filenames. Fixed rather than composed, because every handle that would
# distinguish one run from another — the alignment, the cohort — is one of ours, and a
# filename is something a user publishes. Runs are told apart by the directory the
# caller chose.
TABLE_FORMATS = ("parquet", "biom")
_TABLE_STEM = "feature-table"
_IDENTIFIER_MAP_NAME = "exported-identifier.json"

# Written INTO the map as well as printed, because a warning that lives only in a
# terminal is gone the moment the terminal is, and this file's whole hazard is that it
# looks like part of the deliverable.
IDENTIFIER_MAP_NOTE = (
    "This file is the only artifact in this bundle carrying prep_sample_idx, Qiita's"
    " internal sample identifier, beside the public export_id. It is your join key back"
    " to your own records — keep it, and do not publish it or ship it alongside the"
    " feature table."
)


def _bundle_paths(output_dir: Path, fmt: str) -> list[Path]:
    """The bundle's two files, in write order: the table, then the identifier map."""
    if fmt not in TABLE_FORMATS:
        raise ValueError(f"unsupported feature-table format: {fmt!r} (expected {TABLE_FORMATS})")
    return [output_dir / f"{_TABLE_STEM}.{fmt}", output_dir / _IDENTIFIER_MAP_NAME]


def _write_bundle(
    con, *, output_dir: Path, fmt: str, identifiers: list[dict[str, Any]]
) -> list[Path]:
    """Write the bundle — the relabelled table plus the exported-identifier map — and
    return the files written.

    **Both files or neither.** The table names its samples by `export_id` alone, so
    without the map beside it a caller cannot join their own records back to it; half a
    bundle is not a partial result but a useless one.

    **An existing artifact is refused, before anything is written.** The two writers
    disagree about this on their own — the BIOM writer refuses to overwrite, the Parquet
    COPY replaces silently — so without one check here a second run would fail loudly in
    one format and quietly destroy a published file in the other.
    """
    targets = _bundle_paths(output_dir, fmt)
    existing = [p for p in targets if p.exists()]
    if existing:
        # A single survivor is the one shape a crash can leave (see
        # `_common.commit_partials`), and it reads identically to a finished run on
        # disk — so say which case this is rather than let a user guess before
        # deleting something.
        state = (
            "an earlier run did not finish, since a complete bundle is both files"
            if len(existing) < len(targets)
            else "an earlier run wrote a bundle here"
        )
        raise FileExistsError(
            f"refusing to overwrite {', '.join(p.name for p in existing)} in "
            f"{output_dir}: {state}. Write to a different directory, or remove what "
            f"is there if you are finished with it."
        )

    pairs = [(p.with_name(p.name + ".partial"), p) for p in targets]
    for partial, _ in pairs:
        # Our own leftovers from a killed run. The BIOM writer refuses to overwrite ANY
        # existing target, partials included, so clearing them is what keeps a retry's
        # error about the user's files rather than ours.
        partial.unlink(missing_ok=True)
    table_partial, map_partial = (partial for partial, _ in pairs)

    def write() -> None:
        if fmt == "parquet":
            # ROW_GROUP_SIZE_BYTES in PARQUET_OPTS requires this; DuckDB errors at bind
            # time without it (see qiita_common.parquet).
            con.execute("SET preserve_insertion_order=false")
            con.execute(ft.parquet_copy_sql(table_partial))
        else:
            con.execute(ft.biom_copy_sql(table_partial))
        map_partial.write_text(
            json.dumps(
                {
                    "note": IDENTIFIER_MAP_NOTE,
                    "count": len(identifiers),
                    "identifiers": identifiers,
                },
                indent=2,
            )
            + "\n"
        )

    # No `mode`: the two files land side by side and one of them is meant to be
    # published, so a restrictive mode would be wrong for it. The map's hazard is
    # being shipped onward, which is what the note in it addresses — not being
    # readable on the machine of the user whose own data it describes.
    _common.commit_partials(write, pairs)
    return targets
