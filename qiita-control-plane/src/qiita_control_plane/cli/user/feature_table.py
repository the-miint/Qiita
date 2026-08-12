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

import argparse
import base64
import contextlib
import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from qiita_common import feature_table as ft
from qiita_common.api_paths import (
    PATH_ALIGNMENT_COHORT_DOGET,
    PATH_ALIGNMENT_PREFIX,
    PATH_EXPORTED_IDENTIFIER_PREFIX,
    PATH_EXPORTED_IDENTIFIER_ROOT,
    PATH_REFERENCE_DOGET,
    PATH_REFERENCE_GENOME_MAP,
    PATH_REFERENCE_PREFIX,
)
from qiita_common.models import ExportedIdentifierRequest

from .. import _common
from .alignment import _alignment_reference_idx, _fetch_alignment_cohort, _fetch_pool_alignments

# The registered arrow relations each REST response is staged through. Local to
# this module: the shared builders read them once to CREATE the contract's
# relations, and nothing outside sees them (they are unregistered immediately —
# see `_stage_genome_map`).
_GENOME_MAP_SOURCE = "genome_map_response"
_MINT_SOURCE = "exported_identifier_response"

# The relations each Flight stream is registered as, for the duration of the one
# CREATE that drains it (see `_staged_stream`).
_ALIGNMENT_STREAM = "alignment_stream"
_LENGTHS_STREAM = "reference_lengths_stream"

# The data-plane table the per-feature lengths come from. Whole-reference, for the
# reason `ft.genome_lengths_table_sql` gives: the coverage denominator is the FULL
# genome length, so contigs with no alignment in the cohort are needed too.
_SEQUENCES_TABLE = "reference_sequences"


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


def _create_alignment_doget_ticket(
    base_url: str,
    token: str,
    *,
    alignment_idx: int,
    prep_sample_idx: list[int],
    columns: tuple[str, ...],
) -> bytes:
    """Mint the signed DoGet ticket for this alignment slice, returning the decoded
    ticket bytes.

    The human-callable mint: the caller names the alignment and the cohort, and the
    route authorizes every sample per-study before signing — all-or-nothing, never
    narrowed, because coverage filtering makes a table cohort-dependent.

    `columns` is required by that route rather than optional: the alignment surface has
    no safe server-side default projection (`cigar` is most of a row), so an omitted
    one becomes a Flight error at stream time instead of a 422 here.
    """
    sub_path = PATH_ALIGNMENT_COHORT_DOGET.format(alignment_idx=alignment_idx)
    path = f"{PATH_ALIGNMENT_PREFIX}{sub_path}"
    resp = _common.call(
        "POST",
        base_url,
        token,
        path,
        json={"prep_sample_idx": prep_sample_idx, "columns": list(columns)},
    )
    return base64.b64decode(resp["ticket"])


def _create_lengths_doget_ticket(base_url: str, token: str, *, reference_idx: int) -> bytes:
    """Mint a WHOLE-reference `reference_sequences` DoGet ticket — every contig's
    length, including contigs nothing aligned to. `feature_idx` is omitted rather than
    passed empty, which is how that route spells whole-reference.
    """
    path = f"{PATH_REFERENCE_PREFIX}{PATH_REFERENCE_DOGET.format(reference_idx=reference_idx)}"
    resp = _common.call("POST", base_url, token, path, json={"table": _SEQUENCES_TABLE})
    return base64.b64decode(resp["ticket"])


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


@contextlib.contextmanager
def _staged_stream(con, flight_client, ticket: bytes, *, relation: str) -> Iterator[str]:
    """Register a DoGet stream on `con` as `relation` for the block's duration.

    DuckDB pulls the Arrow stream lazily as the query scans `relation`, so the rows are
    never buffered in Python — the reason each caller below runs exactly one
    materializing CREATE inside the block and lets the relation go.

    **Scanning this relation logs one Arrow "input buffer was poorly aligned" warning per
    projected column (apache/arrow#37195), and no `ensure_alignment` read option removes
    it here** — measured on the end-to-end test at 12 warnings with none, 12 with
    `At64Byte`, 15 with `DataTypeSpecific`. So none is passed: an option that measurably
    does not help is worse than no option, because the next reader believes it.
    """
    import pyarrow.flight as flight  # noqa: PLC0415

    reader = flight_client.do_get(flight.Ticket(ticket)).to_reader()
    con.register(relation, reader)
    try:
        yield relation
    finally:
        con.unregister(relation)


def _stage_alignment(con, flight_client, ticket: bytes, *, gate: ft.AlignmentGate | None) -> None:
    """Stream the alignment slice into `ALIGNMENT_TABLE`, applying `gate` if there is
    one. Everything downstream reads that one relation either way, so a gate cannot be
    half-applied.

    The gated path materializes the UNGATED slice first: the checks below have to see
    the rows the gate would drop, and a Flight stream cannot be scanned twice. The
    diagnostics therefore run after the stream is released, off the materialized copy.

    The diagnostics row is passed to the check **by name** — the query's column names
    are the check's parameter names — so a rename on either side is a `TypeError` here
    rather than one count silently arriving as another's argument.
    """
    with _staged_stream(con, flight_client, ticket, relation=_ALIGNMENT_STREAM) as source:
        con.execute(
            ft.alignment_table_sql(source)
            if gate is None
            else ft.streamed_alignment_table_sql(source, gate=gate)
        )
    if gate is None:
        return
    cursor = con.execute(ft.gate_diagnostics_sql(gate))
    names = [column[0] for column in cursor.description]
    clearance = ft.check_gate_diagnostics(gate, **dict(zip(names, cursor.fetchone(), strict=True)))
    # The clearance carries the rest of the protocol in order — apply the gate, then
    # release the streamed copy that holds `cigar`.
    for sql, parameters in clearance.statements:
        con.execute(sql, parameters)


def _stage_lengths(con, flight_client, ticket: bytes) -> None:
    """Stream the reference's per-feature lengths and roll them up to the per-genome
    denominators the coverage filter divides by. Requires `MAP_TABLE`."""
    with _staged_stream(con, flight_client, ticket, relation=_LENGTHS_STREAM) as source:
        con.execute(ft.genome_lengths_table_sql(source))


def _build_ogu_output(con, *, scope: ft.CoverageScope, coverage_threshold: float) -> None:
    """Run the analytic: the coverage filter at `scope`, then woltka, landing in
    `OGU_OUTPUT_TABLE`. Requires `ALIGNMENT_TABLE` and `MAP_TABLE`, plus
    `GENOME_LENGTHS_TABLE` when the threshold filters anything.

    The server-side job's `_write_ogu_table` is the same sequence over the same
    builders, differing only in being pooled-only and COPYing straight out; the shared
    module is what keeps the two from disagreeing about the analytic itself.
    """
    survivor_scope = scope if ft.coverage_filter_applies(coverage_threshold) else None
    if survivor_scope is not None:
        con.execute(ft.coverage_alignments_view_sql())
        con.execute(ft.survivor_table_sql(survivor_scope), [coverage_threshold])
    con.execute(ft.ogu_input_table_sql(survivor_scope=survivor_scope))
    # woltka rejects an all-NULL sample_id source, so an empty input short-circuits —
    # into the same relation, so the empty cohort travels the same relabel and writer.
    rows = con.execute(f"SELECT count(*) FROM {ft.OGU_INPUT_TABLE}").fetchone()[0]
    con.execute(ft.ogu_output_table_sql(populated=bool(rows)))


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


# The two table formats, and the one to reach for by default. They carry the same
# numbers — the relabelled table is `(sample_id, feature_id, value)` either way — so
# this is a choice of who reads the file next, never of what it contains: Parquet is
# what the rest of this system and every dataframe tool read, BIOM is for the
# microbiome tools that require it. One or the other, never both.
TABLE_FORMATS = ("parquet", "biom")
DEFAULT_TABLE_FORMAT = "parquet"

# **The caller names the table**; the identifier map is derived from that name rather
# than fixed, so the pair stays visibly together and two runs can share a directory.
# What must not happen is qiita COMPOSING a name out of an alignment or a cohort —
# those are our identifiers and a filename is something a user publishes. A name the
# caller chose is theirs.
_IDENTIFIER_MAP_SUFFIX = ".exported-identifier.json"

# Written INTO the map as well as printed, because a warning that lives only in a
# terminal is gone the moment the terminal is, and this file's whole hazard is that it
# looks like part of the deliverable.
IDENTIFIER_MAP_NOTE = (
    "This file is the only artifact in this bundle carrying prep_sample_idx, Qiita's"
    " internal sample identifier, beside the public export_id. It is your join key back"
    " to your own records — keep it, and do not publish it or ship it alongside the"
    " feature table."
)


def _bundle_paths(table_path: Path, fmt: str) -> list[Path]:
    """The two files `table_path` implies, in write order: the table itself, at exactly
    the name the caller gave, and the identifier map beside it.

    Refuses a `fmt` the writers do not have, and a `table_path` whose extension names
    the OTHER format — a `.biom` holding Parquet bytes is a file that lies about itself
    to everyone downstream, and it is the caller's name, so we cannot quietly rewrite
    it.
    """
    if fmt not in TABLE_FORMATS:
        raise ValueError(f"unsupported feature-table format: {fmt!r} (expected {TABLE_FORMATS})")
    suffix = table_path.suffix.lstrip(".").lower()
    if suffix in TABLE_FORMATS and suffix != fmt:
        raise ValueError(
            f"{table_path.name} is named for {suffix} but the requested format is {fmt}; "
            f"the two formats hold the same numbers, so pick either — a file whose "
            f"extension contradicts its contents misleads every reader of it."
        )
    return [table_path, table_path.with_name(table_path.stem + _IDENTIFIER_MAP_SUFFIX)]


def _bundle_targets(table_path: Path, fmt: str) -> list[Path]:
    """The two paths a bundle would occupy, checked to be writable and unoccupied.

    **An existing artifact is refused, before anything is written.** The two writers
    disagree about this on their own — the BIOM writer refuses to overwrite, the Parquet
    COPY replaces silently — so without one check here a second run would fail loudly in
    one format and quietly destroy a published file in the other.

    Separate from the write so the handler can run it FIRST: the recipe streams a
    cohort's alignment and a whole reference before it has anything to write, and
    discovering the collision after all that wastes the run. `_write_bundle` calls it
    again at the write itself, which is the check that actually guards the files.
    """
    targets = _bundle_paths(table_path, fmt)
    parent = table_path.parent
    if not parent.is_dir():
        # Caught here rather than left to the writers: DuckDB's Parquet COPY and HDF5
        # report a missing directory differently, and one of them badly.
        raise FileNotFoundError(f"no such directory to write into: {parent}")

    existing = [p for p in targets if p.exists()]
    if existing:
        # A single survivor is the one shape a crash can leave (see
        # `_common.commit_partials`), and it reads identically to a finished run on
        # disk — so say which case this is rather than let a user guess before
        # deleting something.
        state = (
            "an earlier run did not finish, since a complete bundle is both files"
            if len(existing) < len(targets)
            else "an earlier run wrote this bundle"
        )
        raise FileExistsError(
            f"refusing to overwrite {', '.join(str(p) for p in existing)}: {state}. "
            f"Choose another name, or remove what is there if you are finished with it."
        )
    return targets


def _write_bundle(
    con, *, table_path: Path, fmt: str, identifiers: list[dict[str, Any]]
) -> list[Path]:
    """Write the bundle — the relabelled table at `table_path`, plus the
    exported-identifier map beside it — and return the files written.

    **Both files or neither**, and that is the table plus its map, not two copies of the
    table: the two formats are the same numbers, so a run writes one of them. The map is
    the other half because the table names its samples by `export_id` alone, so without
    it a caller cannot join their own records back to the table; half a bundle is not a
    partial result but a useless one.
    """
    targets = _bundle_targets(table_path, fmt)
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


# ---------------------------------------------------------------------------
# `qiita feature-table build` — the whole recipe, from an alignment to a file
# ---------------------------------------------------------------------------


def _gate_from_args(args: argparse.Namespace) -> ft.AlignmentGate | None:
    """The gate the flags describe, or None when neither threshold was given.

    **Mates are pooled unless `--unpaired-gate` says otherwise**, because pooling is
    also correct for single-end rows (their partition is one row) while the per-row form
    over paired data judges a placement's mates independently and orphans one of them.
    Getting it wrong in the cheap direction is a silent correctness loss, so the default
    is the safe one — and `check_gate_diagnostics` refuses the unsafe combination
    anyway, naming the unpaired form as the way out of the case it cannot pool.
    """
    if args.min_identity is None and args.min_query_coverage is None:
        if args.unpaired_gate:
            raise ValueError(
                "--unpaired-gate only says how to judge a gate, so it does nothing "
                "without --min-identity or --min-query-coverage."
            )
        return None
    return ft.AlignmentGate(
        min_identity=args.min_identity,
        min_query_coverage=args.min_query_coverage,
        paired=not args.unpaired_gate,
    )


def _resolve_cohort(base_url: str, token: str, args: argparse.Namespace) -> list[int]:
    """The cohort to build over: the samples named on the command line, or the pool's
    own mintable set for this alignment.

    An empty resolved cohort is refused here. It is a legitimate answer from that route
    — "nothing here you may mint" — but it is not a table: both mints reject an empty
    cohort, so left alone this becomes a 422 naming a request body the user never wrote.
    """
    if args.prep_sample_idx is not None:
        return list(args.prep_sample_idx)
    body = _fetch_alignment_cohort(
        base_url,
        token,
        sequencing_run_idx=args.sequencing_run_idx,
        sequenced_pool_idx=args.sequenced_pool_idx,
        alignment_idx=args.alignment_idx,
    )
    cohort = body["prep_sample_idx"]
    if not cohort:
        raise ValueError(
            f"no prep_sample of sequenced_pool {args.sequenced_pool_idx} is both readable "
            f"by you and completed for alignment {args.alignment_idx}, so there is no "
            f"cohort to build a table over. `qiita alignment list` shows each alignment's "
            f"completed count."
        )
    return cohort


def _run_build(args: argparse.Namespace, token: str, con) -> tuple[list[Path], int]:
    """The recipe, on an open connection: discover, stage, compute, relabel, write.
    Returns the files written and the published table's row count.

    Ordered so that everything cheap and refusable happens first: the gate needs nothing
    but the flags, so it is built before any round trip at all, and a wrong
    `--alignment-idx`, an empty cohort, or an over-cap genome map each stop the run before
    a byte of alignment data moves (an occupied output is refused earlier still, by the
    handler).

    The Flight client lives only for the two streams and the tickets that authorize them:
    each stream is drained by the one CREATE that materializes it, so nothing holds a gRPC
    connection open through the compute, the relabel, or the write.
    """
    import pyarrow.flight as flight  # noqa: PLC0415

    gate = _gate_from_args(args)
    reference_idx = _alignment_reference_idx(
        _fetch_pool_alignments(
            args.base_url,
            token,
            sequencing_run_idx=args.sequencing_run_idx,
            sequenced_pool_idx=args.sequenced_pool_idx,
        ),
        alignment_idx=args.alignment_idx,
    )
    cohort = _resolve_cohort(args.base_url, token, args)

    _stage_genome_map(con, _fetch_genome_map(args.base_url, token, reference_idx=reference_idx))
    identifiers = _mint_exported_identifiers(
        args.base_url, token, alignment_idx=args.alignment_idx, prep_sample_idx=cohort
    )
    _stage_exported_identifiers(con, identifiers)

    with contextlib.closing(flight.FlightClient(args.data_plane_url)) as flight_client:
        _stage_alignment(
            con,
            flight_client,
            _create_alignment_doget_ticket(
                args.base_url,
                token,
                alignment_idx=args.alignment_idx,
                prep_sample_idx=cohort,
                columns=ft.gate_alignment_columns(gate),
            ),
            gate=gate,
        )
        if ft.coverage_filter_applies(args.coverage_threshold):
            _stage_lengths(
                con,
                flight_client,
                _create_lengths_doget_ticket(args.base_url, token, reference_idx=reference_idx),
            )
    _build_ogu_output(
        con,
        scope=ft.CoverageScope(args.coverage_scope),
        coverage_threshold=args.coverage_threshold,
    )
    clearance = _relabel(con)
    written = _write_bundle(con, table_path=args.output, fmt=args.format, identifiers=identifiers)
    return written, clearance.rows


def _handle_feature_table_build(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Entry point for `qiita feature-table build`.

    Composes the analytic-export routes into a genome-keyed feature table on the
    caller's own machine: two REST maps, two Flight streams, the coverage filter and
    the optional CIGAR gate, then the relabel to public handles and the bundle. Nothing
    here touches a database, and nothing is computed server-side.

    Fails loudly (exit 1) on the first refusal, and every refusal in this recipe exists
    because the alternative is a published table that looks right — a map fetched for
    the wrong reference, a gate that silently dropped every row, two genomes merged
    under one handle. `connect_with_miint()` is the client-side INSTALL path; the
    connection is per-attempt, since a staging CREATE that failed part-way leaves
    relations behind that a retry would collide with.
    """
    import pyarrow.flight as flight  # noqa: PLC0415

    from ...miint import connect_with_miint  # noqa: PLC0415

    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    con = None
    try:
        # Before anything else: a name already occupied, or a directory that is not
        # there, is not worth streaming a cohort and a whole reference to discover.
        _bundle_targets(args.output, args.format)
        con = connect_with_miint()
        written, rows = _run_build(args, token, con)
    except _common.httpx.HTTPStatusError as exc:
        print(f"http error {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
        return 1
    except _common.httpx.RequestError as exc:
        print(
            f"error: could not reach the control plane: {exc!r}. Check --base-url /"
            f" $QIITA_CONTROL_PLANE_URL.",
            file=sys.stderr,
        )
        return 1
    except flight.FlightError as exc:
        print(f"flight error: {exc}", file=sys.stderr)
        return 1
    except (ValueError, OSError) as exc:
        # Every refusal the recipe makes, plus the bundle's own file checks
        # (FileExistsError / FileNotFoundError are OSErrors). Each carries the message
        # that says what to do about it, so print it and stop.
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        if con is not None:
            con.close()

    table_path, map_path = written
    print(f"wrote {rows} row(s) to {table_path} ({args.format})")
    print(f"and the map needed to read it: {map_path}")
    print(IDENTIFIER_MAP_NOTE)
    return 0
