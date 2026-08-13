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
from collections.abc import Callable, Iterator
from importlib import metadata
from pathlib import Path
from typing import Any, NamedTuple

from qiita_common import feature_table as ft
from qiita_common.api_paths import (
    PATH_ALIGNMENT_COHORT_DOGET,
    PATH_ALIGNMENT_PREFIX,
    PATH_EXPORTED_FEATURE_PREFIX,
    PATH_EXPORTED_FEATURE_ROOT,
    PATH_EXPORTED_IDENTIFIER_PREFIX,
    PATH_EXPORTED_IDENTIFIER_ROOT,
    PATH_EXPORTED_PROCESSING_PREFIX,
    PATH_EXPORTED_PROCESSING_ROOT,
    PATH_REFERENCE_BY_IDX,
    PATH_REFERENCE_DOGET,
    PATH_REFERENCE_EXCLUSION_BY_IDX,
    PATH_REFERENCE_GENOME_MAP,
    PATH_REFERENCE_PREFIX,
)
from qiita_common.duckdb_miint import require_miint_function
from qiita_common.models import (
    MAX_EXPORTED_FEATURE_ENTITIES,
    ExportedFeatureRequest,
    ExportedIdentifierRequest,
    ExportedProcessingRequest,
)
from qiita_common.taxonomy import TAXONOMY_SOURCE_TABLE

from .. import _common
from .alignment import (
    _alignment_reference_idx,
    _alignment_summary,
    _fetch_alignment_cohort,
    _fetch_pool_alignments,
)

# The registered arrow relations each REST response is staged through. Local to
# this module: the shared builders read them once to CREATE the contract's
# relations, and nothing outside sees them (they are unregistered immediately —
# see `_stage_genome_map`).
_GENOME_MAP_SOURCE = "genome_map_response"
_MINT_SOURCE = "exported_identifier_response"
_EXPORTED_FEATURE_SOURCE = "exported_feature_response"
_EXCLUSION_SOURCE = "reference_exclusion_response"

# The relations each Flight stream is registered as, for the duration of the one
# CREATE that drains it (see `_staged_stream`).
_ALIGNMENT_STREAM = "alignment_stream"
_LENGTHS_STREAM = "reference_lengths_stream"
_TAXONOMY_STREAM = "reference_taxonomy_stream"
_PHYLOGENY_STREAM = "reference_phylogeny_stream"

# The data-plane table the per-feature lengths come from. Whole-reference, for the
# reason `ft.genome_lengths_table_sql` gives: the coverage denominator is the FULL
# genome length, so contigs with no alignment in the cohort are needed too.
_SEQUENCES_TABLE = "reference_sequences"

# The exclusion-aware VIEW, never the base table. `qiita_common.taxonomy` carries why,
# beside the reduction that depends on it.
_TAXONOMY_TABLE = TAXONOMY_SOURCE_TABLE

# The tree, which has NO exclusion-aware view on purpose: a row-wise anti-join would
# orphan the internal parents of an excluded tip and malform the tree, so the contract is
# that a consumer shears to its own keep-set instead — which is what this recipe does.
_PHYLOGENY_TABLE = "reference_phylogeny"


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


def _fetch_reference_exclusion(
    base_url: str, token: str, *, reference_idx: int
) -> list[dict[str, Any]]:
    """GET the curated blocklist as it applies to this reference: one entry per blocked
    feature that the reference actually holds.

    Only the tree needs it — see `ft.BLOCKED_FEATURE_TABLE`. Uncapped, and deliberately
    not treated as if it were: a blocklist is hand-curated, so it is small by
    construction, and the route already scopes it to one reference.
    """
    path = (
        f"{PATH_REFERENCE_PREFIX}"
        f"{PATH_REFERENCE_EXCLUSION_BY_IDX.format(reference_idx=reference_idx)}"
    )
    return _common.call("GET", base_url, token, path)


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


def _mint_exported_features(
    base_url: str, token: str, *, genome_idx: list[int]
) -> list[dict[str, Any]]:
    """Mint (or recover) the public handle of each genome the table will publish.

    **Batched, because the reference is not the unit here.** The route caps one request
    at `MAX_EXPORTED_FEATURE_ENTITIES`, and a whole-reference genome set runs well past
    it — GG2's backbone alone is a six-figure count. Minting is idempotent server-side,
    which is what makes splitting one logical mint across N requests safe: a batch that
    fails can be retried, and a batch already minted returns the handles it minted
    before.

    Sorted so the same genome set always produces the same request bytes, and so a
    failure names a batch a reader can locate rather than an arbitrary slice.
    """
    ordered = sorted(genome_idx)
    entries: list[dict[str, Any]] = []
    path = f"{PATH_EXPORTED_FEATURE_PREFIX}{PATH_EXPORTED_FEATURE_ROOT}"
    for start in range(0, len(ordered), MAX_EXPORTED_FEATURE_ENTITIES):
        batch = ordered[start : start + MAX_EXPORTED_FEATURE_ENTITIES]
        body = ExportedFeatureRequest(genome_idx=batch).model_dump()
        entries.extend(_common.call("POST", base_url, token, path, json=body)["identifiers"])
    return entries


def _mint_exported_processing(
    base_url: str, token: str, *, alignment_idx: int, prep_sample_idx: list[int]
) -> str:
    """Mint (or recover) the public handle for the processing this table was built from —
    what the manifest cites instead of an `alignment_idx`.

    The cohort authorizes and is not part of the handle, so two callers publishing
    different cohorts of one alignment cite the same handle; that is what lets a reader
    see two bundles share a processing.
    """
    body = ExportedProcessingRequest(
        alignment_idx=alignment_idx, prep_sample_idx=prep_sample_idx
    ).model_dump()
    path = f"{PATH_EXPORTED_PROCESSING_PREFIX}{PATH_EXPORTED_PROCESSING_ROOT}"
    return _common.call("POST", base_url, token, path, json=body)["export_processing_id"]


def _fetch_reference(base_url: str, token: str, *, reference_idx: int) -> dict[str, Any]:
    """The reference row, for the name and version a manifest names it by.

    A `reference_idx` is ours and means nothing outside this system; `(name, version)` is
    what somebody reproducing the table would go looking for. Resolved here rather than
    carried through, since the recipe already knows the idx and nothing else needs it.
    """
    path = f"{PATH_REFERENCE_PREFIX}{PATH_REFERENCE_BY_IDX.format(reference_idx=reference_idx)}"
    return _common.call("GET", base_url, token, path)


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


def _create_reference_doget_ticket(
    base_url: str, token: str, *, reference_idx: int, table: str
) -> bytes:
    """Mint a WHOLE-reference DoGet ticket for one of the reference's lake tables.

    Whole-reference in both uses: `feature_idx` is omitted rather than passed empty,
    which is how that route spells it. For lengths that is required — the coverage
    denominator is the full genome, so contigs nothing aligned to are needed too; for
    taxonomy it is simply cheaper than naming a reference's worth of features in a
    request body.

    Parameterized by `table` rather than one helper per table: the route already signs
    every reference table this recipe reads, and the two calls differ in nothing else.
    """
    path = f"{PATH_REFERENCE_PREFIX}{PATH_REFERENCE_DOGET.format(reference_idx=reference_idx)}"
    resp = _common.call("POST", base_url, token, path, json={"table": table})
    return base64.b64decode(resp["ticket"])


def _stage_response(
    con, entries: list[dict[str, Any]], *, relation: str, columns: list, table_sql
) -> None:
    """Stage a REST response into the relation `table_sql` builds from it.

    **The arrow schema is written out rather than inferred**, and it is what selects the
    columns: miint's functions take native BIGINT id columns, and an empty response — a
    16S reference has no genome-bearing features, an unblocked reference no exclusions —
    would otherwise stage NULL-typed columns that fail the first join. `from_pylist` with
    an explicit schema also reads the entries once, where a column-at-a-time
    comprehension walks a quarter-million dicts once per column, and drops the columns
    nobody staged for free.

    Each caller below says which columns those are and why the rest are left behind; that
    choice is the only thing they differ in.
    """
    import pyarrow as pa  # noqa: PLC0415

    source = pa.Table.from_pylist(entries, schema=pa.schema(columns))
    with _registered(con, relation, source):
        con.execute(table_sql(relation))


def _stage_genome_map(con, entries: list[dict[str, Any]]) -> None:
    """Stage the genome-map response into the roll-up key, `MAP_TABLE`.

    Neither `source` nor `source_id` is staged. The map's job here is the roll-up key;
    what a published row is NAMED comes from the exported-feature mint, which is the
    only authority on whether a genome's accession is unique in the published namespace.
    """
    import pyarrow as pa  # noqa: PLC0415

    _stage_response(
        con,
        entries,
        relation=_GENOME_MAP_SOURCE,
        columns=[("feature_idx", pa.int64()), ("genome_idx", pa.int64())],
        table_sql=ft.map_table_sql,
    )


def _stage_blocked_features(con, entries: list[dict[str, Any]]) -> None:
    """Stage the reference's blocklist into `BLOCKED_FEATURE_TABLE`.

    `feature_idx` only. The listing also reports why each was blocked and by whom, which
    is what makes it readable for a curator and is not something a shear needs.
    """
    import pyarrow as pa  # noqa: PLC0415

    _stage_response(
        con,
        entries,
        relation=_EXCLUSION_SOURCE,
        columns=[("feature_idx", pa.int64())],
        table_sql=ft.blocked_feature_table_sql,
    )


def _stage_exported_features(con, entries: list[dict[str, Any]]) -> None:
    """Stage the exported-feature mint's response into the genome-label relation.

    `genome_idx` and `export_feature_id` only. The response also reports the accession
    each genome *wanted* and whether it won, which is how a caller learns why a label
    is a `QF<n>` — useful to read, and not what a published row is named with.
    """
    import pyarrow as pa  # noqa: PLC0415

    _stage_response(
        con,
        entries,
        relation=_EXPORTED_FEATURE_SOURCE,
        columns=[("genome_idx", pa.int64()), ("export_feature_id", pa.string())],
        table_sql=ft.genome_label_table_sql,
    )


def _stage_exported_identifiers(con, identifiers: list[dict[str, Any]]) -> None:
    """Stage the mint's response into the sample-label relation.

    `prep_sample_idx` and `export_id` only: the accessions ride the response because
    they are already public, but they are not what a table's columns are named with,
    and this relation is joined into an artifact.
    """
    import pyarrow as pa  # noqa: PLC0415

    _stage_response(
        con,
        identifiers,
        relation=_MINT_SOURCE,
        columns=[("prep_sample_idx", pa.int64()), ("export_id", pa.string())],
        table_sql=ft.sample_label_table_sql,
    )


@contextlib.contextmanager
def _registered(con, relation: str, obj) -> Iterator[str]:
    """Register an Arrow object on `con` as `relation` for the block's duration, and
    release it however the block ends.

    One lifecycle for all three staged inputs — `con.register` takes an Arrow table as
    happily as a stream reader. The release matters most for what it frees soonest: at
    the genome-map route's cap the registered table is a quarter-million pairs, and
    holding it alongside the two relations copied out of it doubles that for nothing.
    """
    con.register(relation, obj)
    try:
        yield relation
    finally:
        con.unregister(relation)


@contextlib.contextmanager
def _staged_stream(con, flight_client, ticket: bytes, *, relation: str) -> Iterator[str]:
    """`_registered` over a Flight DoGet stream.

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

    with _registered(con, relation, flight_client.do_get(flight.Ticket(ticket)).to_reader()):
        yield relation


def _stage_from_stream(con, flight_client, ticket: bytes, *, relation: str, table_sql) -> None:
    """Drain a Flight DoGet into the relation `table_sql` builds from it.

    **Every reference-side stream is staged INSIDE the Flight window**, even the two
    whose results cannot be built until long after it closes: none of them depends on
    which genomes survive, and reopening a client later would break the invariant that
    the client lives only for the streams.
    """
    with _staged_stream(con, flight_client, ticket, relation=relation) as source:
        con.execute(table_sql(source))


def _build_taxonomy_sidecar(con) -> ft.TaxonomyClearance:
    """Define the sidecar and refuse one that does not describe the table beside it.

    Runs after the row labels are minted, because it is named from them — the sidecar
    and the table carry the same `feature_id` or they cannot be used together.
    """
    con.execute(ft.taxonomy_sidecar_sql())
    return _cleared(con, ft.taxonomy_diagnostics_sql(), ft.check_taxonomy_diagnostics)


def _shear_tree(con) -> ft.TreeClearance:
    """Shear the reference's tree down to the rows the table publishes, and refuse a tree
    that cannot honestly accompany it.

    Runs after the row labels are minted, because the sheared tree's tips ARE those
    labels — one vocabulary across the table, the sidecar, and the tree.
    """
    for sql in ft.shear_input_statements():
        con.execute(sql)
    clearance = _cleared(con, ft.tree_diagnostics_sql(), ft.check_tree_diagnostics)
    # The clearance carries the rest in order — shear, then release the staged tree.
    for sql in clearance.statements:
        con.execute(sql)
    return clearance


def _cleared(con, diagnostics_sql: str, check, *args):
    """Run a `*_diagnostics_sql` and hand its single row to the matching `check_*`,
    returning the clearance that check produces.

    **The row is passed BY NAME** — the query's column names are the check's parameter
    names — so a rename on either side is a `TypeError` here rather than one count
    silently arriving as another's argument. Both checks in this recipe pair the same
    way, hence one helper: two hand-written `zip`s is two chances to unpack positionally,
    which is exactly the failure the by-name form exists to prevent.
    """
    cursor = con.execute(diagnostics_sql)
    names = [column[0] for column in cursor.description]
    return check(*args, **dict(zip(names, cursor.fetchone(), strict=True)))


def _stage_alignment(con, flight_client, ticket: bytes, *, gate: ft.AlignmentGate | None) -> None:
    """Stream the alignment slice into `ALIGNMENT_TABLE`, applying `gate` if there is
    one. Everything downstream reads that one relation either way, so a gate cannot be
    half-applied.

    The gated path materializes the UNGATED slice first: the checks have to see the rows
    the gate would drop, and a Flight stream cannot be scanned twice. The diagnostics
    therefore run after the stream is released, off the materialized copy.
    """
    with _staged_stream(con, flight_client, ticket, relation=_ALIGNMENT_STREAM) as source:
        con.execute(
            ft.alignment_table_sql(source)
            if gate is None
            else ft.streamed_alignment_table_sql(source, gate=gate)
        )
    if gate is None:
        return
    clearance = _cleared(con, ft.gate_diagnostics_sql(gate), ft.check_gate_diagnostics, gate)
    # The clearance carries the rest of the protocol in order — apply the gate, then
    # release the streamed copy that holds `cigar`.
    for sql, parameters in clearance.statements:
        con.execute(sql, parameters)


def _build_ogu_output(con, *, scope: ft.CoverageScope, coverage_threshold: float) -> None:
    """Run the analytic: the coverage filter at `scope`, then woltka, landing in
    `OGU_OUTPUT_TABLE`. Requires `ALIGNMENT_TABLE` and `MAP_TABLE`, plus
    `GENOME_LENGTHS_TABLE` when the threshold filters anything.

    Both the statement order and the empty-input short-circuit come from the shared
    module, so this driver and the server-side job's `_write_ogu_table` cannot disagree
    about the analytic — only about how the result is written.
    """
    for sql, parameters in ft.ogu_input_statements(
        scope=scope, coverage_threshold=coverage_threshold
    ):
        con.execute(sql, parameters)
    rows = con.execute(ft.ogu_input_count_sql()).fetchone()[0]
    con.execute(ft.ogu_output_table_sql(populated=bool(rows)))
    con.execute(ft.drop_ogu_input_table_sql())


def _published_genome_idxs(con) -> list[int]:
    """The genomes the roll-up actually emitted, ascending.

    Read AFTER the coverage filter, not from the genome map, and that is the whole
    point: a reference's genome set is the wrong unit to mint public handles for. A
    handle is a durable public act, and minting one for every genome in GG2's backbone
    because a table mentioned the reference would leave six figures of permanent
    identifiers for rows nobody published.
    """
    rows = con.execute(
        f"SELECT DISTINCT genome_idx FROM {ft.OGU_OUTPUT_TABLE} ORDER BY genome_idx"
    ).fetchall()
    return [genome_idx for (genome_idx,) in rows]


def _relabel(con) -> ft.LabelClearance:
    """Attach the public handles to the counts, refusing anything that would publish a
    wrong table, and write `LABELLED_RELATION`. Returns the clearance, whose `rows` is the
    size of the table now written.

    Returns the clearance, whose `rows` is the size of the relation now defined.
    """
    clearance = _cleared(con, ft.relabel_diagnostics_sql(), ft.check_relabel_diagnostics)
    con.execute(ft.labelled_relation_sql(clearance=clearance))
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

# Derived the same way, and NOT optional — see `_manifest_payload`.
_MANIFEST_SUFFIX = ".manifest.json"

# What a version the manifest could not resolve reads as. Spelled once so the two places
# that can fall back cannot disagree about the word.
_UNKNOWN_VERSION = "unknown"

MANIFEST_NOTE = (
    "This file records what produced the table beside it. Coverage filtering makes a"
    " feature table a function of the whole cohort it was built over, not only of the"
    " samples in it, so the same processing over a different cohort yields a different"
    " table — which is why this record is part of the bundle rather than optional."
)


class _Companion(NamedTuple):
    """An optional bundle member: the flag that asks for one, the suffix it lands at, and
    the COPY that writes it.

    One record read by both halves — `_bundle_targets` needs the suffixes from the flags
    alone, since it runs before anything has been computed, and `_write_bundle` needs the
    writers — so a member cannot be reserved a path and then not written, and adding one
    is a line here rather than a third positional index at each site.
    """

    name: str
    suffix: str
    copy_sql: Callable[..., str]


# In write order. Each suffix is derived from the caller's name for the reason the
# identifier map's is, and named for what it holds rather than for the reference it came
# from: a reference name is not ours to compose into a filename either.
_COMPANIONS = (
    _Companion("taxonomy", ".taxonomy.parquet", ft.taxonomy_copy_sql),
    _Companion("tree", ".tree.parquet", ft.tree_copy_sql),
)


def _tool_versions(con) -> dict[str, str]:
    """What computed this table, as three versions worth recording.

    The miint build is the one that matters most and is the one nothing else would
    capture: every bioinformatics primitive is compiled INTO the extension, so two
    clients on different builds can produce different numbers from identical input. It is
    read from the catalog rather than assumed, because the client path INSTALLs into a
    cache that never refreshes (see `require_miint_function`) — what is loaded is the only
    honest answer. `install_path` is deliberately not recorded: it is a path on this
    machine, and a published file should not describe the machine that made it.

    The alignment's OWN aligner version is not here and cannot be: it ran server-side,
    under whatever build that host had. `params_hash` is what identifies that processing.

    **An unreadable version is `"unknown"`, never a refusal.** These describe the build,
    not the data: a run whose table, sidecars and digests are all correct must not be
    thrown away because a version string could not be resolved. `PackageNotFoundError` is
    the one that actually happens — a from-source run with no installed distribution —
    and `landing.py` guards the same call the same way for the same reason.
    """
    duckdb_version = con.execute("SELECT version()").fetchone()[0]
    # Both halves matter: no row at all, and a row whose version is NULL. Testing the
    # tuple alone would let `(None,)` through as a JSON null.
    miint_row = con.execute(
        "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'miint'"
    ).fetchone()
    try:
        cli_version = metadata.version("qiita-control-plane")
    except metadata.PackageNotFoundError:
        cli_version = _UNKNOWN_VERSION
    return {
        "duckdb": duckdb_version,
        "miint": miint_row[0] if miint_row and miint_row[0] else _UNKNOWN_VERSION,
        "qiita_cli": cli_version,
    }


def _manifest_payload(
    *,
    args: argparse.Namespace,
    gate: ft.AlignmentGate | None,
    summary: dict[str, Any],
    reference: dict[str, Any],
    export_processing_id: str,
    identifiers: list[dict[str, Any]],
    rows: int,
    files: list[Path],
    tools: dict[str, str],
) -> dict[str, Any]:
    """The bundle's record of what produced the table — the one artifact here that is
    always written, because without it the table is not reproducible.

    **Every identifier in it is public.** The processing is its minted handle and its
    content-derived `params_hash`, never `alignment_idx`; the reference is its name and
    version, never `reference_idx`; the cohort is `export_id`s, never `prep_sample_idx`.
    `params` is not copied verbatim for exactly that reason — the blob carries
    `reference_idx`, `mask_idx` and `shard_ids` — so `aligner` is lifted out of it and the
    rest is represented by the digest, which is what identifies the config anyway.

    `files` are basenames: the bundle's members relative to wherever it lands, since an
    absolute path describes the machine that built it.

    No build timestamp, deliberately. Nothing here would use one, and leaving it out makes
    the manifest a pure function of its inputs — so two bundles built the same way are
    byte-identical and can be diffed to show it.

    **`gate` is passed in, not rebuilt from `args`.** Deriving it a second time would agree
    today — `_gate_from_args` is pure and nothing mutates the namespace — but a record of
    which rows were kept has to be the gate that kept them, and "two derivations that
    happen to agree" is the shape that has already produced defects on this branch.
    """
    return {
        "note": MANIFEST_NOTE,
        "processing": {
            "export_processing_id": export_processing_id,
            "params_hash": summary["params_hash"],
            "aligner": summary.get("params", {}).get("aligner"),
            "reference": {
                "name": reference["name"],
                "version": reference["version"],
                "kind": reference["kind"],
            },
        },
        "table": {
            "format": args.format,
            "rows": rows,
            "coverage_scope": args.coverage_scope,
            "coverage_threshold": args.coverage_threshold,
            "gate": None
            if gate is None
            else {
                "min_identity": gate.min_identity,
                "min_query_coverage": gate.min_query_coverage,
                "mates_pooled": gate.paired,
            },
            # Sorted, so the record does not depend on the order the mint answered in.
            "cohort": sorted(identifier["export_id"] for identifier in identifiers),
        },
        "tools": tools,
        "files": [path.name for path in files],
    }


def _requested_companions(*, taxonomy: bool, tree: bool) -> tuple[_Companion, ...]:
    """The companions these flags ask for, in write order. Spelled out per member rather
    than read off the namespace, so a renamed flag is a signature change here instead of a
    silently absent file."""
    asked = {"taxonomy": taxonomy, "tree": tree}
    return tuple(companion for companion in _COMPANIONS if asked[companion.name])


# Written INTO the map as well as printed, because a warning that lives only in a
# terminal is gone the moment the terminal is, and this file's whole hazard is that it
# looks like part of the deliverable.
IDENTIFIER_MAP_NOTE = (
    "This file is the only artifact in this bundle carrying prep_sample_idx, Qiita's"
    " internal sample identifier, beside the public export_id. It is your join key back"
    " to your own records — keep it, and do not publish it or ship it alongside the"
    " feature table."
)


def _bundle_targets(
    table_path: Path, fmt: str, *, companions: tuple[_Companion, ...] = ()
) -> list[Path]:
    """The paths a bundle occupies, in write order — the table at exactly the name the
    caller gave, the identifier map beside it, and each companion asked for — checked to be
    writable and unoccupied.

    Refuses a `fmt` the writers do not have, and a `table_path` whose extension names
    the OTHER format: a `.biom` holding Parquet bytes lies about itself to everyone
    downstream, and the name is the caller's, so it is not ours to quietly rewrite.

    **An existing artifact is refused, before anything is written.** The two writers
    disagree about this on their own — the BIOM writer refuses to overwrite, the Parquet
    COPY replaces silently — so without one check here a second run would fail loudly in
    one format and quietly destroy a published file in the other.

    Separate from the write so the handler can run it FIRST: the recipe streams a
    cohort's alignment and a whole reference before it has anything to write, and
    discovering the collision after all that wastes the run. `_write_bundle` calls it
    again at the write itself, which is the check that actually guards the files.
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
    targets = [
        table_path,
        table_path.with_name(table_path.stem + _IDENTIFIER_MAP_SUFFIX),
        table_path.with_name(table_path.stem + _MANIFEST_SUFFIX),
        *(table_path.with_name(table_path.stem + c.suffix) for c in companions),
    ]
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
            "an earlier run did not finish, since a complete bundle is all of its files"
            if len(existing) < len(targets)
            else "an earlier run wrote this bundle"
        )
        raise FileExistsError(
            f"refusing to overwrite {', '.join(str(p) for p in existing)}: {state}. "
            f"Choose another name, or remove what is there if you are finished with it."
        )
    return targets


def _write_bundle(
    con,
    *,
    table_path: Path,
    fmt: str,
    identifiers: list[dict[str, Any]],
    manifest: Callable[[list[Path]], dict[str, Any]],
    clearances: dict[str, Any],
) -> list[Path]:
    """Write the bundle — the relabelled table at `table_path`, the exported-identifier map
    and the manifest beside it, and one file per companion in `clearances` — and return the
    files written.

    `clearances` is keyed by companion name, and holding the clearance is what makes a
    member writable: each companion's COPY takes one, so a file cannot be written without
    the check that says it describes the table beside it.

    `manifest` is a function OF the bundle's file list rather than a finished document,
    because the manifest records that list and only this function knows it. Deriving the
    list twice — once to write, once to describe — is how the two come to disagree.

    **All of them or none**, and that is the table plus its companions, not two copies
    of the table: the two formats are the same numbers, so a run writes one of them. The
    map is a companion because the table names its samples by `export_id` alone, so
    without it a caller cannot join their own records back; the manifest because coverage
    filtering makes the table a function of its cohort, so without it the table cannot be
    reproduced; the rest because an artifact naming rows the table does not contain is
    worse than no artifact. Half a bundle is not a partial result but a useless one.
    """
    unknown = set(clearances) - {c.name for c in _COMPANIONS}
    if unknown:
        # Silently writing nothing is the one outcome a bundle must not have: the member
        # would be absent from a bundle whose whole promise is all-of-its-files.
        raise ValueError(f"no bundle companion is named {sorted(unknown)}")
    companions = tuple(c for c in _COMPANIONS if c.name in clearances)
    targets = _bundle_targets(table_path, fmt, companions=companions)
    pairs = [(p.with_name(p.name + ".partial"), p) for p in targets]
    for partial, _ in pairs:
        # Our own leftovers from a killed run. The BIOM writer refuses to overwrite ANY
        # existing target, partials included, so clearing them is what keeps a retry's
        # error about the user's files rather than ours.
        partial.unlink(missing_ok=True)
    table_partial, map_partial, manifest_partial, *companion_partials = (
        partial for partial, _ in pairs
    )

    def write() -> None:
        # ROW_GROUP_SIZE_BYTES in PARQUET_OPTS requires this; DuckDB errors at bind time
        # without it (see qiita_common.parquet). Set once for every Parquet COPY below —
        # the BIOM writer neither needs nor minds it, and the table's format does not
        # decide whether a companion is written.
        con.execute("SET preserve_insertion_order=false")
        if fmt == "parquet":
            con.execute(ft.parquet_copy_sql(table_partial))
        else:
            con.execute(ft.biom_copy_sql(table_partial))
        payload = {
            "note": IDENTIFIER_MAP_NOTE,
            "count": len(identifiers),
            "identifiers": identifiers,
        }
        # Straight into the file: `json.dumps` would build the whole document as a
        # string first, and at cohort scale that document is tens of megabytes.
        #
        # `ensure_ascii=False` with an explicit encoding, because both documents lead with
        # a note meant to be READ: escaped as `\\u2014`, the dashes in it interrupt the one
        # sentence a user opening the file needs. The encoding is named rather than left to
        # the locale's — that is what makes writing those characters safe on a machine
        # whose default is not UTF-8.
        # Built before the loop rather than inside its tuple literal, where it would be
        # called before the first iteration regardless of where it is written — harmless
        # while the callable is pure, and a trap the moment one is not.
        manifest_document = manifest(targets)
        for partial, document in ((map_partial, payload), (manifest_partial, manifest_document)):
            with partial.open("w", encoding="utf-8") as handle:
                json.dump(document, handle, indent=2, ensure_ascii=False)
                handle.write("\n")
        for companion, partial in zip(companions, companion_partials, strict=True):
            con.execute(companion.copy_sql(partial, clearance=clearances[companion.name]))

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


def _run_build(
    args: argparse.Namespace, token: str, con, *, gate: ft.AlignmentGate | None
) -> tuple[list[Path], int]:
    """The recipe, on an open connection: discover, stage, compute, relabel, write.
    Returns the files written and the published table's row count.

    Ordered so that everything cheap and refusable happens first: a wrong
    `--alignment-idx`, an empty cohort, or an over-cap genome map each stop the run before
    a byte of alignment data moves. The two refusals that need nothing but the flags — the
    gate's own coherence and an occupied output — are made by the handler, before this is
    called at all, so neither costs a connection.

    The Flight client lives only for the streams and the tickets that authorize them:
    each stream is drained by the one CREATE that materializes it, so nothing holds a gRPC
    connection open through the compute, the relabel, or the write. `FlightClient` is its
    own context manager, so that lifetime is the `with` block.
    """
    import pyarrow.flight as flight  # noqa: PLC0415

    if args.tree:
        # Before the round trips, not at the shear: `shear_tree` is absent from builds a
        # user's extension cache may still hold, and the raw failure arrives after a whole
        # reference has been streamed.
        require_miint_function(con, "shear_tree", needed_for="--tree")
    # ONE verified summary, read several times: the reference the table is relabelled
    # through, the digest the manifest cites, the aligner it names. Verifying it once and
    # reading it is what keeps those three from being three separate acts of trust.
    summary = _alignment_summary(
        _fetch_pool_alignments(
            args.base_url,
            token,
            sequencing_run_idx=args.sequencing_run_idx,
            sequenced_pool_idx=args.sequenced_pool_idx,
        ),
        alignment_idx=args.alignment_idx,
    )
    reference_idx = _alignment_reference_idx(summary)
    cohort = _resolve_cohort(args.base_url, token, args)

    _stage_genome_map(con, _fetch_genome_map(args.base_url, token, reference_idx=reference_idx))
    identifiers = _mint_exported_identifiers(
        args.base_url, token, alignment_idx=args.alignment_idx, prep_sample_idx=cohort
    )
    _stage_exported_identifiers(con, identifiers)

    with flight.FlightClient(args.data_plane_url) as flight_client:
        # Lengths first, though the alignment is the interesting stream: this one is a
        # whole-reference read that costs a ticket mint and a small aggregate, so a
        # failure in it should not come after a cohort's worth of alignment rows have
        # already crossed the wire. Same order as the server-side job.
        def _reference_stream(*, table: str, relation: str, table_sql) -> None:
            _stage_from_stream(
                con,
                flight_client,
                _create_reference_doget_ticket(
                    args.base_url, token, reference_idx=reference_idx, table=table
                ),
                relation=relation,
                table_sql=table_sql,
            )

        if ft.coverage_filter_applies(args.coverage_threshold):
            _reference_stream(
                table=_SEQUENCES_TABLE,
                relation=_LENGTHS_STREAM,
                table_sql=ft.genome_lengths_table_sql,
            )
        if args.taxonomy:
            _reference_stream(
                table=_TAXONOMY_TABLE,
                relation=_TAXONOMY_STREAM,
                table_sql=ft.taxonomy_table_sql,
            )
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
        if args.tree:
            # LAST in the window, after the stream that can refuse: a whole reference's
            # tree is the largest relation this recipe holds, and it is held from here
            # until the shear — so every megabyte of it staged before a gate that might
            # fail the build is a megabyte spent for nothing.
            #
            # The blocklist is a REST read, fetched here so the tree's two halves arrive
            # together: the stream carries a blocked tip and this is the only thing that
            # says so.
            _stage_blocked_features(
                con, _fetch_reference_exclusion(args.base_url, token, reference_idx=reference_idx)
            )
            _reference_stream(
                table=_PHYLOGENY_TABLE,
                relation=_PHYLOGENY_STREAM,
                table_sql=ft.phylogeny_table_sql,
            )
    coverage = _cleared(con, ft.rollup_coverage_diagnostics_sql(), ft.RollupCoverage)
    if not coverage.complete:
        print(ft.rollup_coverage_warning(coverage), file=sys.stderr)
    _build_ogu_output(
        con,
        scope=ft.CoverageScope(args.coverage_scope),
        coverage_threshold=args.coverage_threshold,
    )
    # The row labels are minted here rather than beside the sample labels above,
    # because only now is the published genome set known — see `_published_genome_idxs`.
    _stage_exported_features(
        con,
        _mint_exported_features(args.base_url, token, genome_idx=_published_genome_idxs(con)),
    )
    clearance = _relabel(con)
    clearances: dict[str, Any] = {}
    if args.taxonomy:
        clearances["taxonomy"] = _build_taxonomy_sidecar(con)
    if args.tree:
        clearances["tree"] = _shear_tree(con)

    # The manifest's two round trips happen here, after everything that could refuse: the
    # processing mint is a WRITE, and minting a public handle for a build that then fails
    # would leave a permanent name for a bundle nobody has.
    export_processing_id = _mint_exported_processing(
        args.base_url, token, alignment_idx=args.alignment_idx, prep_sample_idx=cohort
    )
    reference = _fetch_reference(args.base_url, token, reference_idx=reference_idx)
    tools = _tool_versions(con)
    written = _write_bundle(
        con,
        table_path=args.output,
        fmt=args.format,
        identifiers=identifiers,
        manifest=lambda files: _manifest_payload(
            args=args,
            gate=gate,
            summary=summary,
            reference=reference,
            export_processing_id=export_processing_id,
            identifiers=identifiers,
            rows=clearance.rows,
            files=files,
            tools=tools,
        ),
        clearances=clearances,
    )
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
    import duckdb  # noqa: PLC0415
    import pyarrow.flight as flight  # noqa: PLC0415

    from ...miint import connect_with_miint  # noqa: PLC0415

    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    try:
        # Before anything else, and before the connection: the two refusals that need
        # nothing but the flags. An incoherent gate, a name already occupied, or a
        # directory that is not there is not worth an extension INSTALL to discover —
        # `connect_with_miint` downloads a cold cache from the mirror — let alone a
        # cohort and a whole reference.
        gate = _gate_from_args(args)
        _bundle_targets(
            args.output,
            args.format,
            companions=_requested_companions(taxonomy=args.taxonomy, tree=args.tree),
        )
        with contextlib.closing(connect_with_miint()) as con:
            written, rows = _run_build(args, token, con, gate=gate)
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
    except duckdb.Error as exc:
        # The analytic runs HERE, so duckdb's own failures are the ones a user is least
        # able to place: the recipe's peak is a whole reference's tree, `shear_tree` is
        # single-threaded and allocation-bound, and an out-of-memory or a spill with
        # nowhere to go says nothing about whose machine ran out. Named separately from
        # the refusals below because it is not one — nothing here chose to stop.
        print(f"error: the analytic failed on this machine: {exc}", file=sys.stderr)
        return 1
    except (ValueError, OSError, RuntimeError) as exc:
        # Every refusal the recipe makes, plus the bundle's own file checks
        # (FileExistsError / FileNotFoundError are OSErrors) and the miint capability
        # probe, which raises RuntimeError like the other guards in that module. Each
        # carries the message that says what to do about it, so print it and stop.
        print(f"error: {exc}", file=sys.stderr)
        return 1

    # The three fixed members are named for what they are; the optional ones vary, so
    # they are listed rather than enumerated here.
    table_path, map_path, manifest_path, *companions = written
    print(f"wrote {rows} row(s) to {table_path} ({args.format})")
    print(f"and the map needed to read it: {map_path}")
    print(f"and the record of what produced it: {manifest_path}")
    for companion in companions:
        print(f"and {companion}")
    print(IDENTIFIER_MAP_NOTE)
    return 0
