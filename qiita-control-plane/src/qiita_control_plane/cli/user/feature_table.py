"""qiita user CLI — the client-side feature table.

The analytic itself is `qiita_common.analytic`, shared with the server-side
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
import dataclasses
import functools
import json
import sys
from collections.abc import Callable, Iterator, Sequence
from importlib import metadata
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

from qiita_common import analytic as ft
from qiita_common.api_paths import (
    PATH_ALIGNMENT_COHORT_DOGET,
    PATH_ALIGNMENT_PREFIX,
    PATH_ASSEMBLY_GENOME_MAP_PARQUET,
    PATH_ASSEMBLY_PREFIX,
    PATH_ASSEMBLY_RUN_DOGET,
    PATH_DEFINITION_GROUP_PREFIX,
    PATH_DEFINITION_GROUP_RESOLVE,
    PATH_EXPORTED_FEATURE_PREFIX,
    PATH_EXPORTED_FEATURE_ROOT,
    PATH_EXPORTED_IDENTIFIER_PREFIX,
    PATH_EXPORTED_IDENTIFIER_ROOT,
    PATH_EXPORTED_PROCESSING_PREFIX,
    PATH_EXPORTED_PROCESSING_ROOT,
    PATH_PROCESSING_BY_IDX,
    PATH_PROCESSING_PREFIX,
    PATH_REFERENCE_BY_IDX,
    PATH_REFERENCE_DOGET,
    PATH_REFERENCE_EXCLUSION_BY_IDX,
    PATH_REFERENCE_GENOME_MAP_PARQUET,
    PATH_REFERENCE_PREFIX,
)
from qiita_common.assembly_constants import ASSEMBLED_SEQUENCE_TABLE
from qiita_common.duckdb_relation import registered
from qiita_common.flight_stream import counted_doget
from qiita_common.models import (
    INCLUDE_DEPRECATED_FLAG,
    MAX_EXPORTED_FEATURE_ENTITIES,
    AlignmentDefinitionStatus,
    DefinitionGroupResolveRequest,
    DefinitionGroupResolveResponse,
    DefinitionKind,
    ExportedFeatureRequest,
    ExportedIdentifierRequest,
    ExportedProcessingRequest,
    SampleResolution,
    SampleResolutionReason,
    SampleResolutionState,
    admitted_deprecated_upstream_kinds,
    deprecated_read_refusal,
)
from qiita_common.models._base import MAX_COHORT_PREP_SAMPLE_IDX
from qiita_common.parquet import PARQUET_MEDIA_TYPE, PARQUET_OPTS_TRANSFER, validate_parquet_path
from qiita_common.taxonomy import TAXONOMY_SOURCE_TABLE

if TYPE_CHECKING:
    import pyarrow as pa

from ...feature_table import (
    denovo_alignment_processing_idx,
    denovo_assembly_deprecation_error,
)
from ...run_feature_table import FEATURE_RELATION
from .. import _common
from ._helpers import _UNSET
from .alignment import (
    _alignment_reference_idx,
    _alignment_summary,
    _fetch_alignment_cohort,
    _fetch_pool_alignments,
)

# The most prep_samples one /definition-group/resolve request carries: the route's cap.
_RESOLVE_CHUNK = MAX_COHORT_PREP_SAMPLE_IDX

# The registered arrow relations each REST response is staged through. Local to
# this module: the shared builders read them once to CREATE the contract's
# relations, and nothing outside sees them (they are unregistered immediately —
# see `_stage_genome_map`).
_GENOME_MAP_SOURCE = "genome_map_response"
# The two columns either map stages; provenance rides the wire for callers that
# want it and is dropped here (see `_stage_genome_map`).
_MAP_KEY_COLUMNS = ["feature_idx", "genome_idx"]
_MINT_SOURCE = "exported_identifier_response"
_EXPORTED_FEATURE_SOURCE = "exported_feature_response"
_EXCLUSION_SOURCE = "reference_exclusion_response"
_DENOVO_GENOME_MAP_SOURCE = "denovo_genome_map_response"

# The relations each Flight stream is registered as, for the duration of the one
# CREATE that drains it (see `staged_stream`).
_ALIGNMENT_STREAM = "alignment_stream"
_LENGTHS_STREAM = "reference_lengths_stream"
_DENOVO_ALIGNMENT_STREAM = "denovo_alignment_stream"
_DENOVO_LENGTHS_STREAM = "denovo_lengths_stream"
_TAXONOMY_STREAM = "reference_taxonomy_stream"
_PHYLOGENY_STREAM = "reference_phylogeny_stream"

# One part's prep_samples, and the view of that part's rows of the streamed slice that
# the gate's diagnostics read (see `_gate_clearance`).
_PART_SAMPLES = "gate_part_samples"
_PART_SLICE = "gate_part_slice"

# The data-plane table the per-feature lengths come from. Whole-reference, for the
# reason `ft.genome_lengths_table_sql` gives: the coverage denominator is the FULL
# genome length, so contigs with no alignment in the cohort are needed too.
_SEQUENCES_TABLE = "reference_sequences"

# The exclusion-aware VIEW, never the base table. `qiita_common.taxonomy` carries why,
# beside the reduction that depends on it.
_TAXONOMY_TABLE = TAXONOMY_SOURCE_TABLE

# The tree, which has no exclusion-aware view: a row-wise anti-join would
# orphan the internal parents of an excluded tip and malform the tree, so the contract is
# that a consumer shears to its own keep-set instead — which is what this recipe does.
_PHYLOGENY_TABLE = "reference_phylogeny"


def _fetch_genome_map(base_url: str, token: str, *, reference_idx: int) -> pa.Table:
    """GET the whole reference's `feature_idx → genome` map as an Arrow table: one
    row per (feature, genome) pair with the genome's `source` / `source_id`.

    The Parquet form, because the JSON one caps at `GENOME_MAP_HARD_CAP` — which
    `routes/_helpers.py` records the deploy's references as exceeding — and
    because that form needs no cap at all.
    """
    sub_path = PATH_REFERENCE_GENOME_MAP_PARQUET.format(reference_idx=reference_idx)
    return _fetch_map_parquet(base_url, token, f"{PATH_REFERENCE_PREFIX}{sub_path}")


def _fetch_assembly_genome_map(
    base_url: str, token: str, *, prep_sample_idx: int, processing_idx: int
) -> pa.Table:
    """GET one assembly run's `feature_idx → genome` map, the de novo twin of
    `_fetch_genome_map`.

    **One call per cohort prep_sample**, because the map is run-scoped where the
    reference one is whole-reference. The caller re-attaches the prep_sample the
    response was scoped to — `_stage_denovo_genome_map` says why that key matters.

    Refusals propagate, for the reason its twin's do, and one more of its own: the
    route 422s a run whose memberships are not all genome-minted, because a map
    short by a contig does not read as short — it reads as a genome that covered
    more of a smaller length than it did.
    """
    sub_path = PATH_ASSEMBLY_GENOME_MAP_PARQUET.format(
        prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
    )
    return _fetch_map_parquet(base_url, token, f"{PATH_ASSEMBLY_PREFIX}{sub_path}")


def _fetch_processing(base_url: str, token: str, *, processing_idx: int) -> dict:
    """GET one assembly run's params and lifecycle columns."""
    sub_path = PATH_PROCESSING_BY_IDX.format(processing_idx=processing_idx)
    return _common.call("GET", base_url, token, f"{PATH_PROCESSING_PREFIX}{sub_path}")


def _refuse_deprecated_denovo_run(
    base_url: str, token: str, *, processing_idx: int, admitted: bool = False
) -> None:
    """Refuse a deprecated assembly run as a de novo arm, with the wording that
    `runner/_feature_table.py` uses server-side. The client asks this of each run that
    a prep_sample reads (`_plan_denovo`); the runner, which does not resolve groups,
    asks it only of the named de novo alignment's run.

    `denovo_alignment_processing_idx` states why neither driver may be the only one
    that checks, and `denovo_assembly_deprecation_error` owns the wording so the two
    cannot word it differently. The read is one GET per build, not per cohort sample:
    the assembly run is in the de novo alignment's hashed params, so one value covers
    the whole cohort.

    `admitted` (--include-deprecated or --include-deprecated-upstream) reads a deprecated
    run; the cohort DoGet admits it on the same flags and names it in the ticket
    response, which the manifest records (`_mint_ticket`).
    """
    row = _fetch_processing(base_url, token, processing_idx=processing_idx)
    message = denovo_assembly_deprecation_error(
        processing_idx=processing_idx,
        status=row.get("status"),
        superseded_by=row.get("superseded_by"),
    )
    if message is not None and not admitted:
        raise ValueError(f"{message}. To read it, pass {INCLUDE_DEPRECATED_FLAG}")


def _resolve_definition_group(
    base_url: str,
    token: str,
    *,
    kind: DefinitionKind,
    definition_idx: int,
    prep_sample_idx: list[int],
    as_of_event: str | None = None,
    include_deprecated: bool = False,
    include_deprecated_upstream: bool = False,
) -> DefinitionGroupResolveResponse:
    """POST /definition-group/resolve: the named definition's group and, per prep_sample,
    which member it is processed under or why it is not, under `as_of_event`'s decision
    set when given. The first chunk's `decisions.decision_set` is sent as `as_of_event`
    with every later chunk, so all chunks are resolved under one decision set.

    The cohort is sent in chunks of at most `_RESOLVE_CHUNK` prep_samples, the route's
    cap, so a cohort over the cap resolves. Only the resolve is chunked: each part's
    DoGet and handle mints carry the part's whole cohort, so a part over the cap is
    refused by those routes. Each prep_sample
    resolves on its own, so the chunks' samples are joined; a group that differs
    between chunks (an output-equivalence verdict committed between two requests) is refused rather
    than joined. Each body goes through the route's request model and each answer
    through its response model."""
    samples = sorted(set(prep_sample_idx))
    path = f"{PATH_DEFINITION_GROUP_PREFIX}{PATH_DEFINITION_GROUP_RESOLVE}"
    # An empty cohort is still one request, which the request model refuses.
    chunks = [
        samples[start : start + _RESOLVE_CHUNK] for start in range(0, len(samples), _RESOLVE_CHUNK)
    ] or [samples]
    answers: list[DefinitionGroupResolveResponse] = []
    for chunk in chunks:
        body = DefinitionGroupResolveRequest(
            definition_kind=kind,
            definition_idx=definition_idx,
            prep_sample_idx=chunk,
            as_of_event=answers[0].decisions.decision_set if answers else as_of_event,
            include_deprecated=include_deprecated,
            include_deprecated_upstream=include_deprecated_upstream,
        ).model_dump(mode="json", exclude_none=True, exclude_defaults=True)
        answer = DefinitionGroupResolveResponse.model_validate(
            _common.call("POST", base_url, token, path, json=body)
        )
        if answer.decisions is None:
            raise RuntimeError(f"the resolve of {kind.value} {definition_idx} carries no decisions")
        answers.append(answer)
    groups = {tuple(answer.group) for answer in answers}
    if len(groups) > 1:
        raise RuntimeError(
            f"the group of {kind.value} {definition_idx} changed while its"
            f" {len(samples)} prep_samples were resolved in {len(answers)} requests"
            f" ({sorted(groups)}); an output-equivalence verdict landed meanwhile. Run the"
            " build again."
        )
    return answers[0].model_copy(
        update={"samples": [entry for answer in answers for entry in answer.samples]}
    )


def _pinned(resolution: DefinitionGroupResolveResponse, as_of_event: str | None) -> str | None:
    """The decision set a build's later resolves and DoGets are sent: the one the anchor
    resolved under, or `--as-of-event` when the anchor was read exactly (no resolve)."""
    return as_of_event if resolution.decisions is None else resolution.decisions.decision_set


def _fetch_map_parquet(base_url: str, token: str, path: str) -> pa.Table:
    """The shared half of the two map fetches: read the Parquet body and decode it.

    **A refusal propagates.** There is nothing to fall back to — a lookup table
    silently missing rows produces a WRONG feature table rather than a short one —
    so any non-2xx travels up to whoever can show the user what happened.
    """
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415

    body, content_type = _common.fetch_binary(base_url, token, path, accept=PARQUET_MEDIA_TYPE)
    # Parameters (`; charset=`) are not part of the type, and media types are
    # case-insensitive. A body that is not Parquet fails in `read_table` anyway,
    # but with an Arrow error about magic bytes rather than a statement of what
    # the server actually sent.
    if content_type.split(";")[0].strip().lower() != PARQUET_MEDIA_TYPE:
        raise RuntimeError(f"{path}: expected {PARQUET_MEDIA_TYPE}, server sent {content_type!r}")
    return pq.read_table(pa.BufferReader(body))


def _assembly_genome_map_or_none(
    base_url: str, token: str, *, prep_sample_idx: int, processing_idx: int
) -> pa.Table | None:
    """This sample's run-scoped genome map, or `None` when it assembled nothing.

    **A 404 here is the design's graceful path, not an error.** A sample whose
    assembly produced no contigs has no de novo arm; the combined table degrades to
    plain reference alignment for it, which is the stated behaviour rather than a
    state to refuse. Every other status still propagates — a 422 on an unminted map
    and a 413 over the cap are refusals whose whole point is that they are not
    silently absorbed.

    The submit-time twin of this is the server-side resolver's arm gate, which reads
    `assembly_sample` directly and can tell "assembled nothing" from "has not run
    yet". A client has no such gate, so it takes the 404 at face value — which is
    why the two drivers are not equally strict here, and the server one is the
    stricter.
    """
    try:
        return _fetch_assembly_genome_map(
            base_url, token, prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
        )
    except _common.httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return None
        raise


def _staged_denovo_samples(con) -> list[int]:
    """The cohort samples the staged de novo map actually named, ascending.

    Read from the map rather than from the cohort, so the lengths are fetched for
    exactly the runs that contributed a genome — a sample the map skipped has no
    contigs, and asking the data plane for its assembly would 404 on a run that
    legitimately produced none. Same rule the server-side job applies, for the same
    reason.
    """
    rows = con.execute(
        f"SELECT DISTINCT prep_sample_idx FROM {ft.DENOVO_MAP_TABLE} ORDER BY prep_sample_idx"
    ).fetchall()
    return [prep_sample_idx for (prep_sample_idx,) in rows]


def _fetch_reference_exclusion(
    base_url: str, token: str, *, reference_idx: int
) -> list[dict[str, Any]]:
    """GET the curated blocklist as it applies to this reference: one entry per blocked
    feature that the reference actually holds.

    Only the tree needs it — see `ft.BLOCKED_FEATURE_TABLE`. Uncapped, and not treated
    as if it were: a blocklist is hand-curated, so it is small by
    construction, and the route already scopes it to one reference.
    """
    path = (
        f"{PATH_REFERENCE_PREFIX}"
        f"{PATH_REFERENCE_EXCLUSION_BY_IDX.format(reference_idx=reference_idx)}"
    )
    return _common.call("GET", base_url, token, path)


def _mint_exported_identifiers(
    base_url: str,
    token: str,
    *,
    alignment_idx: int,
    prep_sample_idx: list[int],
    include_deprecated: bool = False,
    include_deprecated_upstream: bool = False,
) -> list[dict[str, Any]]:
    """Mint (or recover) the public `export_id` of each processed sample in the cohort.

    Idempotent server-side, so a retried build names its samples the same way. The body
    goes through the route's own request model, which validates the cohort's shape and
    cap locally rather than after a round trip.
    """
    body = ExportedIdentifierRequest(
        alignment_idx=alignment_idx,
        prep_sample_idx=prep_sample_idx,
        include_deprecated=include_deprecated,
        include_deprecated_upstream=include_deprecated_upstream,
    ).model_dump()
    path = f"{PATH_EXPORTED_IDENTIFIER_PREFIX}{PATH_EXPORTED_IDENTIFIER_ROOT}"
    return _common.call("POST", base_url, token, path, json=body)["identifiers"]


def _mint_exported_features(
    base_url: str, token: str, *, genome_idx: list[int]
) -> list[dict[str, Any]]:
    """Mint (or recover) the public handle of each genome the table will publish.

    **Batched, because the reference is not the unit here.** The route caps one request
    at `MAX_EXPORTED_FEATURE_ENTITIES`, and a whole-reference genome set runs past it —
    GG2's backbone is over the cap on its own. Minting is idempotent server-side,
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
    base_url: str,
    token: str,
    *,
    alignment_idx: int,
    prep_sample_idx: list[int],
    include_deprecated: bool = False,
    include_deprecated_upstream: bool = False,
) -> str:
    """Mint (or recover) the public handle for the processing this table was built from —
    what the manifest cites instead of an `alignment_idx`.

    The cohort authorizes and is not part of the handle, so two callers publishing
    different cohorts of one alignment cite the same handle; that is what lets a reader
    see two bundles share a processing.
    """
    body = ExportedProcessingRequest(
        alignment_idx=alignment_idx,
        prep_sample_idx=prep_sample_idx,
        include_deprecated=include_deprecated,
        include_deprecated_upstream=include_deprecated_upstream,
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
    include_deprecated: bool = False,
    include_deprecated_upstream: bool = False,
    as_of_event: str | None = None,
    upstreams_read: dict[int, list[dict[str, Any]]] | None = None,
    exact: bool = False,
) -> bytes:
    """Mint the signed DoGet ticket for this alignment slice, returning the decoded
    ticket bytes. `as_of_event` is the decision set the route resolves the cohort under.
    The deprecated upstreams the route reports reading are printed on stderr, once per
    alignment, and recorded in `upstreams_read` under the alignment for the manifest;
    `exact` (a deprecated alignment read alone) words the line for that read.

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
        json={
            "prep_sample_idx": prep_sample_idx,
            "columns": list(columns),
            **_common.optional_body_fields(
                include_deprecated=include_deprecated,
                include_deprecated_upstream=include_deprecated_upstream,
                as_of_event=as_of_event,
            ),
        },
    )
    upstreams = resp.get("deprecated_upstreams") or []
    if upstreams and upstreams_read is not None and alignment_idx not in upstreams_read:
        upstreams_read[alignment_idx] = upstreams
        for upstream in upstreams:
            _common.print_deprecated_upstream_read(
                "alignment", alignment_idx, upstream, exact=exact
            )
    return base64.b64decode(resp["ticket"])


def _create_assembly_run_doget_ticket(
    base_url: str, token: str, *, prep_sample_idx: int, processing_idx: int, table: str
) -> bytes:
    """Mint a DoGet ticket for ONE assembly run's contigs, returning the decoded
    ticket bytes.

    The human-callable mint (`assembly:doget`), which authorizes this run's
    prep_sample against the caller's studies before signing — the service-account
    route beside it is the compute job's and is not reachable with a PAT.

    Run-scoped, so a cohort costs N mints where the reference lengths cost one. That
    is the ticket's shape, not a choice here: the pair IS the signed filter.
    """
    sub_path = PATH_ASSEMBLY_RUN_DOGET.format(
        prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
    )
    resp = _common.call(
        "POST", base_url, token, f"{PATH_ASSEMBLY_PREFIX}{sub_path}", json={"table": table}
    )
    return base64.b64decode(resp["ticket"])


def create_reference_doget_ticket(
    base_url: str, token: str, *, reference_idx: int, table: str
) -> bytes:
    """Mint a whole-reference DoGet ticket for one of the reference's lake tables.

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
    columns: miint's functions take native BIGINT id columns, and an empty response — an
    unblocked reference has no exclusions, a cohort no minted handles — would otherwise
    stage NULL-typed columns that fail the first join. `from_pylist` with an explicit
    schema also reads the entries once, where a column-at-a-time comprehension walks them
    once per column, and drops the columns nobody staged for free.

    JSON responses only. The two genome maps arrive as Parquet and stage their Arrow
    table directly, having no dicts to walk.

    Each caller below says which columns those are and why the rest are left behind; that
    choice is the only thing they differ in.
    """
    import pyarrow as pa  # noqa: PLC0415

    source = pa.Table.from_pylist(entries, schema=pa.schema(columns))
    with registered(con, relation, source):
        con.execute(table_sql(relation))


def _stage_genome_map(con, table: pa.Table) -> None:
    """Stage the genome-map Parquet into the roll-up key, `MAP_TABLE`.

    Neither `source` nor `source_id` is staged. The map's job here is the roll-up key;
    what a published row is NAMED comes from the exported-feature mint, which is the
    only authority on whether a genome's accession is unique in the published namespace.

    The projection happens on the Arrow table rather than in the CREATE, so the two
    columns nobody stages are dropped before DuckDB ever sees them. The Parquet
    schema is typed, so an empty map stages BIGINT columns rather than NULL-typed
    ones the first join would reject.
    """
    # Released as soon as it is copied: the genome map is the largest staged input by
    # far, and uncapped, so holding it alongside the relations copied out of it would
    # double that for nothing.
    with registered(con, _GENOME_MAP_SOURCE, table.select(_MAP_KEY_COLUMNS)):
        con.execute(ft.map_table_sql(_GENOME_MAP_SOURCE))


def _stage_denovo_genome_map(con, per_sample: dict[int, pa.Table]) -> None:
    """Stage the cohort's per-run genome maps into `DENOVO_MAP_TABLE`, one relation
    keyed `(prep_sample_idx, feature_idx, genome_idx)`.

    **The prep_sample key is re-attached here**, because the REST route carries it in
    the path rather than in each entry. `AssemblyGenomeMapResponse` states what goes
    wrong without it.

    Neither `source` nor `source_id` is staged, for the reason its reference twin
    gives: the map's job here is the roll-up key.
    """
    import pyarrow as pa  # noqa: PLC0415

    per_run = [
        table.select(_MAP_KEY_COLUMNS).add_column(
            0,
            pa.field("prep_sample_idx", pa.int64()),
            pa.array([prep_sample_idx] * table.num_rows, type=pa.int64()),
        )
        for prep_sample_idx, table in sorted(per_sample.items())
    ]
    # Spelled out rather than derived from the served schema: importing it here
    # would pull a server-side action module into the CLI for three int64 columns.
    # `test_an_empty_genome_map_still_stages_typed_relations` is what catches a
    # drift, since it stages through this branch.
    schema = pa.schema(
        [
            ("prep_sample_idx", pa.int64()),
            ("feature_idx", pa.int64()),
            ("genome_idx", pa.int64()),
        ]
    )
    # `concat_tables` takes no `schema=` (it reads only `promote_options` out of
    # its kwargs and drops the rest silently), and refuses an empty list outright —
    # so the no-run case is the `empty_table()` branch, not an argument. A cohort
    # that assembled nothing still has to stage typed columns.
    combined = pa.concat_tables(per_run) if per_run else schema.empty_table()
    with registered(con, _DENOVO_GENOME_MAP_SOURCE, combined):
        con.execute(ft.denovo_map_table_sql(_DENOVO_GENOME_MAP_SOURCE))


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
def staged_stream(con, flight_client, ticket: bytes, *, relation: str) -> Iterator[str]:
    """`registered` over a Flight DoGet stream.

    DuckDB pulls the Arrow stream lazily as the query scans `relation`, so the rows are
    never buffered in Python — the reason each caller reads the relation exactly once
    inside the block and lets it go.

    **Scanning this relation logs one Arrow "input buffer was poorly aligned" warning per
    projected column (apache/arrow#37195), and no `ensure_alignment` read option removes
    it here** — measured on the end-to-end test at 12 warnings with none, 12 with
    `At64Byte`, 15 with `DataTypeSpecific`. So none is passed: an option that measurably
    does not help is worse than no option, because the next reader believes it.
    """
    with (
        counted_doget(flight_client, ticket, name=relation) as reader,
        registered(con, relation, reader),
    ):
        yield relation


@contextlib.contextmanager
def staged_streams(
    con, flight_client, mints: Sequence[Callable[[], bytes]], *, relation: str
) -> Iterator[str]:
    """`staged_stream` over several DoGet tickets of one surface, registered as ONE
    relation that yields every stream's batches in order.

    This is where a multi-part build's parts are unioned: before anything is staged, so
    the coverage filter and precedence each run once over the whole cohort, through the
    same builders a single-part build uses. Each stream is opened only when the one
    before it is drained, so one gRPC stream is open at a time. A stream whose schema
    differs from the first is refused rather than cast.

    `mints` holds one callable per stream, each returning a freshly signed ticket. A
    ticket is minted just before its DoGet, after the streams before it have drained:
    a signed ticket expires (`auth/tickets.py`), and one minted when the build started
    could expire while an earlier part streams.
    """
    if not mints:
        raise ValueError(f"no DoGet ticket to stage {relation} from")
    if len(mints) == 1:
        with staged_stream(con, flight_client, mints[0](), relation=relation) as source:
            yield source
        return
    import pyarrow as pa  # noqa: PLC0415

    def name(position: int) -> str:
        return f"{relation} (stream {position} of {len(mints)})"

    import duckdb  # noqa: PLC0415

    # Each stream is counted on its own; a later one's check runs as its stream ends,
    # inside the scan, and the first one's when the block returns. DuckDB reports an
    # exception raised inside a scan as its own error (measured on 2.0.0, for a
    # RuntimeError and a ValueError alike: InvalidInputException, the original text
    # inside), which the build's handler reads as the analytic failing; an exception
    # raised here, a later stream's mismatch or schema, is kept and raised in its place.
    raised: list[Exception] = []
    with counted_doget(flight_client, mints[0](), name=name(1)) as first:

        def batches():
            yield from first
            for position, mint in enumerate(mints[1:], start=2):
                try:
                    with counted_doget(flight_client, mint(), name=name(position)) as reader:
                        if not reader.schema.equals(first.schema):
                            raise ValueError(
                                f"{relation}: stream {position} of {len(mints)} has schema "
                                f"{reader.schema} where the first has {first.schema}"
                            )
                        yield from reader
                except Exception as exc:
                    raised.append(exc)
                    raise

        with registered(con, relation, pa.RecordBatchReader.from_batches(first.schema, batches())):
            try:
                yield relation
            except duckdb.Error as exc:
                if raised:
                    raise raised[0] from exc
                raise


def _stage_from_stream(con, flight_client, ticket: bytes, *, relation: str, table_sql) -> None:
    """Drain a Flight DoGet into the relation `table_sql` builds from it.

    **Every reference-side stream is staged INSIDE the Flight window**, even the two
    whose results cannot be built until long after it closes: none of them depends on
    which genomes survive, and reopening a client later would break the invariant that
    the client lives only for the streams.
    """
    with staged_stream(con, flight_client, ticket, relation=relation) as source:
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


def _stage_alignment(
    con,
    flight_client,
    parts: Sequence[_Part],
    *,
    mint: Callable[[_Part], bytes],
    gate: ft.AlignmentGate | None,
) -> None:
    """Stream the alignment slice — one ticket per part, `mint(part)` minted just before
    its stream, unioned by `staged_streams` — into `ALIGNMENT_TABLE`, applying `gate` if
    there is one. Everything downstream reads that one relation either way, so a gate
    cannot be half-applied.

    The gated path materializes the UNGATED slice first: the checks have to see the rows
    the gate would drop, and a Flight stream cannot be scanned twice. The diagnostics
    therefore run after the stream is released, off the materialized copy, once per part
    (`_gate_clearance`).
    """
    mints = [functools.partial(mint, part) for part in parts]
    with staged_streams(con, flight_client, mints, relation=_ALIGNMENT_STREAM) as source:
        con.execute(
            ft.alignment_table_sql(source)
            if gate is None
            else ft.streamed_alignment_table_sql(source, gate=gate)
        )
    if gate is None:
        return
    clearance = _gate_clearance(con, parts, gate=gate)
    # The clearance carries the rest of the protocol in order — apply the gate, then
    # release what the gated path put up, the streamed copy holding `cigar` included.
    for sql, parameters in clearance.statements:
        con.execute(sql, parameters)


def _gate_clearance(con, parts: Sequence[_Part], *, gate: ft.AlignmentGate) -> ft.GateClearance:
    """`check_gate_diagnostics` over each part's rows of the streamed slice.

    Per part, because each part is a different alignment: `check_gate_diagnostics`
    refuses only when no row of the slice it is given can be scored, so over the union
    a part whose CIGARs the gate can score would let the gate drop every row of a part
    whose CIGARs it cannot. A refusal names the part.
    """
    if len(parts) == 1:
        return _cleared(con, ft.gate_diagnostics_sql(gate), ft.check_gate_diagnostics, gate)
    import pyarrow as pa  # noqa: PLC0415

    clearance = None
    for part in parts:
        samples = pa.table({"prep_sample_idx": pa.array(part.samples, type=pa.int64())})
        with registered(con, _PART_SAMPLES, samples):
            con.execute(
                f"CREATE TEMP VIEW {_PART_SLICE} AS SELECT * FROM {ft.STREAMED_ALIGNMENT_TABLE}"
                f" SEMI JOIN {_PART_SAMPLES} USING (prep_sample_idx)"
            )
            try:
                clearance = _cleared(
                    con,
                    ft.gate_diagnostics_sql(gate, source=_PART_SLICE),
                    ft.check_gate_diagnostics,
                    gate,
                )
            except ValueError as exc:
                raise ValueError(
                    f"alignment {part.alignment_idx} ({len(part.samples)} prep_sample(s)"
                    f" of the cohort): {exc}"
                ) from exc
            finally:
                con.execute(f"DROP VIEW {_PART_SLICE}")
    return clearance


def _build_ogu_output(
    con, *, scope: ft.CoverageScope, coverage_threshold: float, combined: bool
) -> None:
    """Run the analytic: the coverage filter at `scope`, then woltka, landing in
    `OGU_OUTPUT_TABLE`. Requires `ALIGNMENT_TABLE` and `MAP_TABLE`, plus
    `GENOME_LENGTHS_TABLE` when the threshold filters anything, and the de novo arm's
    own relations when `combined`.

    Both the statement order and the empty-input short-circuit come from the shared
    module, so this driver and the server-side job's `_write_ogu_table` cannot disagree
    about the analytic — only about how the result is written.
    """
    for sql, parameters in ft.ogu_input_statements(
        scope=scope, coverage_threshold=coverage_threshold, combined=combined
    ):
        con.execute(sql, parameters)
    rows = con.execute(ft.ogu_input_count_sql()).fetchone()[0]
    con.execute(ft.ogu_output_table_sql(populated=bool(rows)))
    con.execute(ft.drop_ogu_input_table_sql())


def _published_genome_idxs(con) -> list[int]:
    """The genomes the roll-up actually emitted, ascending.

    Read AFTER the coverage filter, not from the genome map, and that is the whole
    point: a reference's genome set is the wrong unit to mint public handles for. A
    handle is a durable public act, and minting one for every genome in a reference
    because a table mentioned it would leave permanent identifiers for rows nobody
    published.
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

# The manifest's shape, recorded as its `schema_version`. 2 is `processing.parts` /
# `processing.sample_part` (and the same under `table.denovo`); a manifest with no
# `schema_version` is the earlier shape, with one `processing.export_processing_id`,
# `params_hash` and `aligner`. Incremented whenever a key is renamed, moved or removed.
MANIFEST_SCHEMA_VERSION = 2

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

    `flag_gated` is the default: a member a `--flag` asks for. The amplicon `sequences`
    member is not — it is always written by its own path — so it is excluded from the
    flag lookup (`_requested_companions`) rather than masked out of it.
    """

    name: str
    suffix: str
    copy_sql: Callable[..., str]
    flag_gated: bool = True


def _amplicon_sequences_copy_sql(path: Path, *, clearance: object = None) -> str:
    """COPY the handle -> (sequence, md5) mapping to Parquet: each published feature's
    `QF`, its ASV sequence, and the reproducible-from-sequence md5. SEMI JOIN to the
    published set (not `IN`) and unordered, like `write.taxonomy_copy_sql`: the reader
    joins on the id. `clearance` is accepted for the uniform companion dispatch; this
    companion has no relabel gate to clear."""
    del clearance
    return (
        f"COPY (SELECT f.feature AS feature_id, f.sequence, f.length, f.md5"
        f" FROM {FEATURE_RELATION} f"
        f" SEMI JOIN {ft.LABELLED_RELATION} l ON l.feature_id = f.feature)"
        f" TO '{validate_parquet_path(path)}' ({PARQUET_OPTS_TRANSFER})"
    )


_COMPANIONS = (
    _Companion("taxonomy", ".taxonomy.parquet", ft.taxonomy_copy_sql),
    _Companion("tree", ".tree.parquet", ft.tree_copy_sql),
    # The amplicon table's (`qiita feature-table runs`), not flag-gated.
    _Companion("sequences", ".sequences.parquet", _amplicon_sequences_copy_sql, flag_gated=False),
)


def _tool_versions(con) -> dict[str, str]:
    """What computed this table, as three versions worth recording.

    The miint build is the one that matters most and is the one nothing else would
    capture: every bioinformatics primitive is compiled INTO the extension, so two
    clients on different builds can produce different numbers from identical input. It is
    read from the catalog rather than assumed, because the client path INSTALLs into a
    cache that never refreshes (see `miint_install_sql`) — what is loaded is the only
    honest answer. `install_path` is not recorded: it is a path on this
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


def _manifest_gate(gate: ft.AlignmentGate | None) -> dict[str, Any] | None:
    """What the manifest records about the gate: the thresholds of the axis it scored,
    under that axis's own key names, so a reader can tell which one was applied.
    """
    if gate is None:
        return None
    if gate.circular:
        return {
            "circular_min_coverage": gate.circular_min_coverage,
            "circular_min_identity": gate.circular_min_identity,
        }
    return {
        "min_identity": gate.min_identity,
        "min_query_coverage": gate.min_query_coverage,
        "mates_pooled": gate.paired,
    }


def _manifest_parts(
    parts: Sequence[_Part],
    handles: dict[int, str],
    identifiers: list[dict[str, Any]],
    upstreams_read: dict[int, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """One arm's processing record: `parts`, one entry per part (its minted handle, its
    verified `params_hash`, its aligner, and `deprecated` with the reason for a
    deprecated alignment read under --include-deprecated, and `deprecated_upstreams`,
    each deprecated upstream the cohort DoGet read it past by kind, `params_hash` and
    reason), and `sample_part`, each
    prep_sample's `export_id` mapped to the handle of the part it was read from.

    Parts are in `alignment_idx` order, so two builds over the same inputs write the
    same bytes; the order is all of that id the manifest keeps. `sample_part` is sorted
    by `export_id` for the same reason.
    """
    export_id_of = {entry["prep_sample_idx"]: entry["export_id"] for entry in identifiers}
    return {
        "parts": [
            {
                "export_processing_id": handles[part.alignment_idx],
                "params_hash": part.summary["params_hash"],
                "aligner": part.summary.get("params", {}).get("aligner"),
                **(
                    {"deprecated": True, "deprecation_reason": part.summary["deprecation_reason"]}
                    if _is_deprecated(part.summary)
                    else {}
                ),
                **(
                    {
                        "deprecated_upstreams": [
                            {
                                "kind": u["definition_kind"],
                                "params_hash": u["params_hash"],
                                "deprecation_reason": u.get("deprecation_reason"),
                            }
                            for u in upstreams_read[part.alignment_idx]
                        ]
                    }
                    if upstreams_read and part.alignment_idx in upstreams_read
                    else {}
                ),
            }
            for part in parts
        ],
        "sample_part": dict(
            sorted(
                (export_id_of[sample], handles[part.alignment_idx])
                for part in parts
                for sample in part.samples
            )
        ),
    }


def _manifest_denovo(
    *,
    requested: bool,
    record: dict[str, Any] | None,
    left_out: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """The de novo arm's record, or None when the build never asked for one.

    **`applied` is separate from the ask because the two come apart**: a caller can name
    a de novo alignment none of whose cohort has an arm, and the build then degrades to
    reference-only. Without this block a combined request and a reference-only request
    write byte-identical manifests.

    `record` is `_manifest_parts` over the de novo parts, or None when no prep_sample had an
    arm. Its `sample_part` names only the prep_samples with an arm, so their count is
    `prep_samples_with_a_de_novo_arm`. `left_out` (`_listed_left_out`), present only
    for a deprecated de novo alignment read under --include-deprecated, lists the
    cohort's prep_samples that read left reference-only although they have a run on it.
    """
    if not requested:
        return None
    record = record or {"parts": [], "sample_part": {}}
    return {
        "applied": bool(record["parts"]),
        **record,
        "prep_samples_with_a_de_novo_arm": len(record["sample_part"]),
        **({"left_out": left_out} if left_out is not None else {}),
    }


def _manifest_payload(
    *,
    args: argparse.Namespace,
    gate: ft.AlignmentGate | None,
    reference: dict[str, Any],
    processing: dict[str, Any],
    identifiers: list[dict[str, Any]],
    rows: int,
    files: list[Path],
    tools: dict[str, str],
    denovo: dict[str, Any] | None = None,
    left_out: list[dict[str, Any]] | None = None,
    decisions: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The bundle's record of what produced the table — the one artifact here that is
    always written, because without it the table is not reproducible.

    **Every identifier in it is public.** Each part's processing is its minted handle
    and its content-derived `params_hash`, never `alignment_idx`; the reference is its
    name and version, never `reference_idx`; the cohort is `export_id`s, never
    `prep_sample_idx`. `params` is not copied verbatim for exactly that reason — the blob
    carries `reference_idx`, `mask_idx` and `shard_ids` — so `aligner` is lifted out of it
    and the rest is represented by the digest, which is what identifies the config anyway.
    `processing` is `_manifest_parts` over the reference arm's parts: one part per
    alignment that the cohort's prep_samples resolved to, and the map from each
    prep_sample to its part. `table.left_out` is the default cohort's prep_samples that the
    table leaves out, as `_left_out_record`s: from the group resolution
    (`_left_out_identifiers`, named by `export_id`), and from the pool cohort route's
    `left_out` under --include-deprecated or --include-deprecated-upstream
    (`_listed_left_out`, named by biosample handles); empty for a named cohort, which is
    refused instead.

    `files` are basenames: the bundle's members relative to wherever it lands, since an
    absolute path describes the machine that built it.

    No build timestamp. Nothing here would use one, and leaving it out makes
    the manifest a pure function of its inputs — so two bundles built the same way are
    byte-identical and can be diffed to show it.

    **`gate` is passed in, not rebuilt from `args`.** Deriving it a second time would agree
    today — `_gate_from_args` is pure and nothing mutates the namespace — but a record of
    which rows were kept has to be the gate that kept them, and "two derivations that
    happen to agree" is the shape that has already produced defects on this branch.

    `decisions` is the reference arm's `DecisionProvenance`, the output-equivalence verdicts its
    group was resolved under (the de novo arm is resolved under the same decision set);
    None when the reference arm was read exactly, without a group.
    """
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "note": MANIFEST_NOTE,
        "decisions": decisions,
        "processing": {
            "reference": {
                "name": reference["name"],
                "version": reference["version"],
                "kind": reference["kind"],
            },
            **processing,
        },
        "table": {
            "format": args.format,
            "rows": rows,
            "coverage_scope": args.coverage_scope,
            "coverage_threshold": args.coverage_threshold,
            "gate": _manifest_gate(gate),
            # Present only for a build that ASKED for a second arm — see
            # `_manifest_denovo` for why `applied` is a separate key from the ask.
            **({"denovo": denovo} if denovo is not None else {}),
            # Sorted, so the record does not depend on the order the mint answered in.
            "cohort": sorted(identifier["export_id"] for identifier in identifiers),
            "left_out": sorted(left_out or [], key=_left_out_sort_key),
        },
        "tools": tools,
        "files": [path.name for path in files],
    }


def _requested_companions(*, taxonomy: bool, tree: bool) -> tuple[_Companion, ...]:
    """The flag-gated companions these flags ask for, in write order. Spelled out per
    member rather than read off the namespace, so a renamed flag is a signature change
    here instead of a silently absent file."""
    asked = {"taxonomy": taxonomy, "tree": tree}
    return tuple(c for c in _COMPANIONS if c.flag_gated and asked[c.name])


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
    map_note: str = IDENTIFIER_MAP_NOTE,
) -> list[Path]:
    """Write the bundle — the relabelled table at `table_path`, the exported-identifier map
    (led by `map_note`) and the manifest beside it, and one file per companion in
    `clearances` — and return the files written.

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
        # ROW_GROUP_SIZE_BYTES in PARQUET_OPTS_TRANSFER requires this; DuckDB errors at bind time
        # without it (see qiita_common.parquet). Set once for every Parquet COPY below —
        # the BIOM writer neither needs nor minds it, and the table's format does not
        # decide whether a companion is written.
        con.execute("SET preserve_insertion_order=false")
        if fmt == "parquet":
            con.execute(ft.parquet_copy_sql(table_partial))
        else:
            con.execute(ft.biom_copy_sql(table_partial))
        payload = {
            "note": map_note,
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
    """The gate the flags describe, or None when no threshold and no mode was given.

    **Mates are pooled unless `--unpaired-gate` says otherwise**, because pooling is
    also correct for single-end rows (their partition is one row) while the per-row form
    over paired data judges a placement's mates independently and orphans one of them.
    Getting it wrong in the cheap direction is a silent correctness loss, so the default
    is the safe one — and `check_gate_diagnostics` refuses the unsafe combination
    anyway, naming the unpaired form as the way out of the case it cannot pool.

    `--circular-gate` selects the other axis, so it carries its own two thresholds and
    takes neither of the CIGAR ones; `AlignmentGate` refuses the combinations, and the
    checks here name the flags rather than the fields.
    """
    circular_thresholds = {
        "--circular-min-coverage": args.circular_min_coverage,
        "--circular-min-identity": args.circular_min_identity,
    }
    given = [flag for flag, value in circular_thresholds.items() if value is not _UNSET]
    if args.circular_gate:
        if args.min_identity is not None or args.min_query_coverage is not None:
            raise ValueError(
                "--circular-gate scores a read pooled over its records; --min-identity "
                "and --min-query-coverage score one record at a time, and a per-record "
                "coverage floor drops exactly the split records the pooling exists to "
                "rescue. Use --circular-min-identity / --circular-min-coverage."
            )
        if args.unpaired_gate:
            raise ValueError(
                "--unpaired-gate says how to pool a placement's mates, which "
                "--circular-gate does not do: it pools a read's records against one "
                "reference and keeps mates apart. Pass one or the other."
            )
        if args.denovo_alignment_idx is not None:
            raise ValueError(
                "--circular-gate re-judges every staged record against thresholds the "
                "producers never aligned at. For the de novo arm that is redundant — "
                "align_denovo already applied the circular gate before persisting a "
                "row — and for the reference arm it is wrong: align_sharded scored per "
                "record, not pooled, so this would newly admit or drop reference "
                "placements on a rule they were never judged by. Build the combined "
                "table without it."
            )
        thresholds = {
            name: default if value is _UNSET else value
            for name, value, default in (
                ("circular_min_coverage", args.circular_min_coverage, ft.CIRCULAR_MIN_COVERAGE),
                ("circular_min_identity", args.circular_min_identity, ft.CIRCULAR_MIN_IDENTITY),
            )
        }
        return ft.AlignmentGate(circular=True, **thresholds)
    if given:
        # Same shape as the --unpaired-gate refusal below: a flag that only says HOW to
        # judge does nothing on its own, and doing nothing quietly is the failure.
        raise ValueError(
            f"{' and '.join(given)} set the circular gate's thresholds, so without "
            f"--circular-gate they gate nothing at all."
        )
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


@dataclasses.dataclass(frozen=True)
class _Part:
    """One part of a build: an alignment that some of the cohort's prep_samples resolved
    to, its verified summary, and those prep_samples, ascending. A build over
    prep_samples that all resolve to one alignment has one part."""

    alignment_idx: int
    summary: dict[str, Any]
    samples: tuple[int, ...]


@dataclasses.dataclass(frozen=True)
class _DenovoPart(_Part):
    """A de novo arm's part: a de novo alignment, and the assembly run its params name
    (`denovo_alignment_processing_idx`), which every per-sample de novo read uses."""

    processing_idx: int


def _is_deprecated(summary: dict[str, Any]) -> bool:
    return summary.get("status") == AlignmentDefinitionStatus.DEPRECATED.value


def _reads_exactly(summary: dict[str, Any], *, flag: bool, role: str) -> bool:
    """Whether the build reads this named alignment's own results rather than its group:
    it is deprecated and --include-deprecated is set (the routes' `deprecated_read`
    rule). Refuses a deprecated alignment named without the flag, naming it, and prints
    the stderr line for one read with it."""
    if not _is_deprecated(summary):
        return False
    idx, reason = summary["alignment_idx"], summary.get("deprecation_reason")
    if not flag:
        raise ValueError(deprecated_read_refusal(role, idx, reason))
    _common.print_deprecated_read("alignment", idx, reason)
    return True


def _exact_resolution(
    summary: dict[str, Any], samples: list[int]
) -> DefinitionGroupResolveResponse:
    """The plan for an exact read of a deprecated alignment: every prep_sample on it
    alone. The cohort DoGet resolves the cohort exactly under `include_deprecated` and
    refuses a prep_sample that its own run does not cover."""
    idx = summary["alignment_idx"]
    mask_idx = summary.get("params", {}).get("mask_idx")
    return DefinitionGroupResolveResponse(
        definition_kind=DefinitionKind.ALIGNMENT,
        definition_idx=idx,
        group=[idx],
        samples=[
            SampleResolution(
                prep_sample_idx=sample,
                state=SampleResolutionState.PROCESSED,
                definition_kind=DefinitionKind.ALIGNMENT,
                definition_idx=idx,
                mask_idx=mask_idx if isinstance(mask_idx, int) else None,
            )
            for sample in sorted(set(samples))
        ],
    )


def _pools(args: argparse.Namespace) -> list[tuple[int, int]]:
    """The `(sequencing_run_idx, sequenced_pool_idx)` pairs the flags name, the Nth
    `--sequencing-run-idx` with the Nth `--sequenced-pool-idx`. Refuses unequal counts
    and a pair named twice."""
    runs, pools = list(args.sequencing_run_idx), list(args.sequenced_pool_idx)
    if len(runs) != len(pools):
        raise ValueError(
            f"{len(runs)} --sequencing-run-idx and {len(pools)} --sequenced-pool-idx were "
            f"given; each pool is named by one of each, paired in the order given."
        )
    pairs = list(zip(runs, pools, strict=True))
    repeated = sorted({pair for pair in pairs if pairs.count(pair) > 1})
    if repeated:
        raise ValueError(f"sequencing run / sequenced pool named more than once: {repeated}")
    return pairs


def _pool_listings(
    base_url: str, token: str, pools: list[tuple[int, int]]
) -> tuple[dict[str, Any], dict[tuple[int, int], set[int]]]:
    """The alignments over every named pool, merged into one listing shaped like a
    single pool's (each alignment once, ascending) for `_alignment_summary`, and the set
    of alignment_idxs listed per pool for the cohort reads."""
    merged: dict[int, dict[str, Any]] = {}
    per_pool: dict[tuple[int, int], set[int]] = {}
    for run_idx, pool_idx in pools:
        body = _fetch_pool_alignments(
            base_url, token, sequencing_run_idx=run_idx, sequenced_pool_idx=pool_idx
        )
        listed = body.get("alignments", [])
        per_pool[(run_idx, pool_idx)] = {summary["alignment_idx"] for summary in listed}
        for summary in listed:
            merged.setdefault(summary["alignment_idx"], summary)
    return {"alignments": [merged[idx] for idx in sorted(merged)]}, per_pool


def _member_summary(listing: dict[str, Any], alignment_idx: int, *, samples: int) -> dict:
    """`_alignment_summary` for an alignment that the resolver named, not the caller,
    refusing with the flags to add when it is over none of the named pools."""
    if all(summary["alignment_idx"] != alignment_idx for summary in listing["alignments"]):
        raise ValueError(
            f"{samples} prep_sample(s) of the cohort resolve to alignment {alignment_idx},"
            f" which is not over any pool named, so its params cannot be read and checked."
            f" Add the pool that those prep_samples are on as another --sequencing-run-idx /"
            f" --sequenced-pool-idx pair."
        )
    return _alignment_summary(listing, alignment_idx=alignment_idx)


@dataclasses.dataclass(frozen=True)
class _Cohort:
    """The prep_samples a build resolves, ascending. `listed_under` is None for a cohort
    named with `--prep-sample-idx`; for the default cohort it maps each prep_sample to
    the alignments of the group that the cohort route listed it completed under."""

    samples: list[int]
    listed_under: dict[int, set[int]] | None
    # The cohort route's `left_out` entries for the anchor (`_listed_left_out`); None
    # for a named cohort.
    listed_left_out: list[dict[str, Any]] | None = None


def _resolve_cohort(
    base_url: str,
    token: str,
    args: argparse.Namespace,
    *,
    per_pool: dict[tuple[int, int], set[int]],
    exact: bool = False,
) -> _Cohort:
    """The cohort to resolve: the prep_samples named on the command line, or else every
    prep_sample of the named pools that the caller may read and that is completed for an
    alignment in `--alignment-idx`'s group.

    The group comes from a resolve over the anchor's own completed prep_samples, so the
    anchor must have at least one in the named pools. An empty cohort is refused here:
    it is a legitimate answer from the cohort route, but it is not a table — both mints
    reject an empty cohort, so left alone this becomes a 422 naming a request body the
    user never wrote.

    `exact` (a deprecated `--alignment-idx` read under --include-deprecated): the
    default cohort is the prep_samples that the cohort route lists for the anchor read
    exactly, with no group. Under that flag or --include-deprecated-upstream, the
    prep_samples the cohort route leaves out for the anchor are kept for the manifest.
    """
    if args.prep_sample_idx is not None:
        return _Cohort(sorted(set(args.prep_sample_idx)), None)
    left_out: list[dict[str, Any]] = []

    def completed(pool: tuple[int, int], alignment_idx: int) -> list[int]:
        body = _fetch_alignment_cohort(
            base_url,
            token,
            sequencing_run_idx=pool[0],
            sequenced_pool_idx=pool[1],
            alignment_idx=alignment_idx,
            include_deprecated=args.include_deprecated,
            include_deprecated_upstream=args.include_deprecated_upstream,
            as_of_event=args.as_of_event,
        )
        left_out.extend(body.get("left_out") or [])
        return body["prep_sample_idx"]

    anchor = args.alignment_idx
    listed_under: dict[int, set[int]] = {}
    for pool, listed in per_pool.items():
        if anchor in listed:
            for sample in completed(pool, anchor):
                listed_under.setdefault(sample, set()).add(anchor)
    if not listed_under:
        raise ValueError(
            f"no prep_sample of the named sequenced_pool(s) {sorted(p for _, p in per_pool)}"
            f" is both readable by you and completed for alignment {anchor}, so there is no"
            f" cohort to build a table over. `qiita alignment list` shows each alignment's"
            f" completed count."
        )
    if exact:
        return _Cohort(
            sorted(listed_under),
            listed_under,
            _listed_left_out(left_out, f"alignment {anchor}", denovo=False),
        )
    group = set(
        _resolve_definition_group(
            base_url,
            token,
            kind=DefinitionKind.ALIGNMENT,
            definition_idx=anchor,
            prep_sample_idx=sorted(listed_under),
            as_of_event=args.as_of_event,
            include_deprecated_upstream=args.include_deprecated_upstream,
        ).group
    )
    for pool, listed in per_pool.items():
        for alignment_idx in sorted((listed & group) - {anchor}):
            for sample in completed(pool, alignment_idx):
                listed_under.setdefault(sample, set()).add(alignment_idx)
    # A prep_sample one member's listing left out but another listed is in the cohort,
    # and the group resolution decides it; one left out by every listing is recorded once.
    unlisted = {
        e["prep_sample_idx"]: e for e in left_out if e["prep_sample_idx"] not in listed_under
    }
    return _Cohort(
        sorted(listed_under),
        listed_under,
        _listed_left_out(list(unlisted.values()), f"alignment {anchor}'s group", denovo=False),
    )


# The de novo arm's left-out `state` for a run that failed or has not finished
# (`_listed_left_out`); the reference arm keeps the resolution state.
DENOVO_LEFT_OUT_FAILED = "failed"
DENOVO_LEFT_OUT_RUNNING = "running"
_DENOVO_LEFT_OUT_STATE = {
    SampleResolutionReason.FAILED.value: DENOVO_LEFT_OUT_FAILED,
    SampleResolutionReason.IN_PROGRESS.value: DENOVO_LEFT_OUT_RUNNING,
}


def _left_out_record(
    *,
    export_id: str | None,
    biosample_accession: str | None,
    biosample_export_entity_id: str | None,
    state: str,
    reason: str | None,
) -> dict[str, Any]:
    """One manifest `left_out` entry. It names its prep_sample by `export_id` when one
    was minted, else by its biosample's accession and `QB<n>` handle; a prep_sample with
    no completed run has no public handle of its own, so two prep_samples of one
    biosample can share an entry's handles. No `_idx`."""
    return {
        "export_id": export_id,
        "biosample_accession": biosample_accession,
        "biosample_export_entity_id": biosample_export_entity_id,
        "state": state,
        "reason": reason,
    }


def _left_out_sort_key(record: dict[str, Any]) -> tuple[str, str, str]:
    return (
        record["export_id"] or "",
        record["biosample_accession"] or "",
        record["biosample_export_entity_id"] or "",
    )


def _left_out_name(record: dict[str, Any]) -> str:
    return (
        record["export_id"]
        or record["biosample_accession"]
        or record["biosample_export_entity_id"]
        or "(no public handle)"
    )


def _listed_left_out(
    entries: list[dict[str, Any]], read: str, *, denovo: bool
) -> list[dict[str, Any]]:
    """The manifest's record of what a listing reading past deprecation left out, from
    the pool cohort route's `left_out` entries (the rules are stated at
    `definition_resolver.NO_RUN_TO_READ`), as `_left_out_record`s. `state` is the
    resolution state, or, on the de novo arm, `DENOVO_LEFT_OUT_FAILED` /
    `DENOVO_LEFT_OUT_RUNNING` for a run that failed or has not finished. Each is listed
    on stderr."""
    records = sorted(
        (
            _left_out_record(
                export_id=None,
                biosample_accession=e.get("biosample_accession"),
                biosample_export_entity_id=e.get("biosample_export_entity_id"),
                state=(
                    _DENOVO_LEFT_OUT_STATE.get(e["reason"], e["state"]) if denovo else e["state"]
                ),
                reason=e["reason"],
            )
            for e in entries
        ),
        key=_left_out_sort_key,
    )
    if records:
        print(
            f"note: {len(records)} prep_sample(s) with a run on {read} are left out of"
            f" this read of it; the manifest records them:",
            file=sys.stderr,
        )
        for r in records:
            print(f"  {_left_out_name(r)}: {r['state']} ({r['reason']})", file=sys.stderr)
    return records


def _skip_unprocessed(
    resolution: DefinitionGroupResolveResponse, cohort: _Cohort
) -> tuple[DefinitionGroupResolveResponse, list[SampleResolution]]:
    """For the default cohort, the resolution narrowed to its processed prep_samples, and
    the prep_samples left out, each listed on stderr with its state and reason. A named
    cohort is returned whole, for `_parts` to refuse. Refuses a default cohort with no
    processed prep_sample: there is no table to build."""
    if cohort.listed_under is None:
        return resolution, []
    processed = [e for e in resolution.samples if e.state is SampleResolutionState.PROCESSED]
    left_out = [e for e in resolution.samples if e.state is not SampleResolutionState.PROCESSED]
    if not processed:
        lines = "\n".join(f"  {entry.describe()}" for entry in left_out)
        raise ValueError(
            f"none of the {len(left_out)} prep_sample(s) of the cohort is processed in the"
            f" group of alignment {resolution.definition_idx}:\n{lines}\nThere is no table"
            f" to build."
        )
    if left_out:
        print(
            f"note: {len(left_out)} of {len(resolution.samples)} prep_sample(s) are left out"
            f" of the table, not processed in the group of alignment"
            f" {resolution.definition_idx}; the manifest records them under"
            f" table.left_out:",
            file=sys.stderr,
        )
        for entry in left_out:
            print(f"  {entry.describe()}", file=sys.stderr)
    return resolution.model_copy(update={"samples": processed}), left_out


def _left_out_identifiers(
    base_url: str,
    token: str,
    left_out: list[SampleResolution],
    cohort: _Cohort,
    *,
    include_deprecated: bool = False,
    include_deprecated_upstream: bool = False,
) -> list[dict[str, Any]]:
    """The manifest's record of the left-out prep_samples, as `_left_out_record`s named by
    `export_id`, sorted. The `export_id` is minted under the highest-idx
    alignment that the cohort route listed the prep_sample completed under, which the
    identifier mint admits."""
    if not left_out:
        return []
    if cohort.listed_under is None:
        raise RuntimeError(
            f"{len(left_out)} prep_sample(s) were left out of a cohort named with"
            " --prep-sample-idx, which _skip_unprocessed returns whole"
        )
    by_alignment: dict[int, list[SampleResolution]] = {}
    for entry in left_out:
        by_alignment.setdefault(max(cohort.listed_under[entry.prep_sample_idx]), []).append(entry)
    out = []
    for alignment_idx, entries in sorted(by_alignment.items()):
        minted_by_sample = {
            minted["prep_sample_idx"]: minted
            for minted in _mint_exported_identifiers(
                base_url,
                token,
                alignment_idx=alignment_idx,
                prep_sample_idx=[entry.prep_sample_idx for entry in entries],
                include_deprecated=include_deprecated,
                include_deprecated_upstream=include_deprecated_upstream,
            )
        }
        out.extend(
            _left_out_record(
                export_id=minted_by_sample[entry.prep_sample_idx]["export_id"],
                biosample_accession=minted_by_sample[entry.prep_sample_idx].get(
                    "biosample_accession"
                ),
                biosample_export_entity_id=None,
                state=entry.state.value,
                reason=entry.reason.value if entry.reason is not None else None,
            )
            for entry in entries
        )
    return sorted(out, key=_left_out_sort_key)


def _refuse_unprocessed(refused: list[SampleResolution], *, anchor: str) -> None:
    """Refuse a cohort with prep_samples that are not processed in `anchor`'s group, listing
    each with its state. A feature table is built over processed prep_samples only, and a
    cohort named with `--prep-sample-idx` is not narrowed: coverage is measured over the
    whole cohort, so a narrowed cohort is a different table from the one named. The
    default cohort is narrowed before this (`_skip_unprocessed`)."""
    if not refused:
        return
    lines = "\n".join(f"  {entry.describe()}" for entry in refused)
    raise ValueError(
        f"{len(refused)} prep_sample(s) of the cohort are not processed in the group of"
        f" {anchor}:\n{lines}\nA feature table is built only over processed prep_samples. Wait"
        f" for or re-run these, or leave them out of --prep-sample-idx."
    )


def _parts(
    resolution: DefinitionGroupResolveResponse, *, listing: dict[str, Any], reference_idx: int
) -> list[_Part]:
    """The cohort's prep_samples grouped into parts by the alignment each resolved to, in
    `alignment_idx` order. Refuses an incomplete or not-processed prep_sample
    (`_refuse_unprocessed`), a part whose alignment is over none of the named pools
    (`_member_summary`), and a part aligned against a different reference: the table is
    relabeled through one reference's genome map."""
    _refuse_unprocessed(
        [e for e in resolution.samples if e.state is not SampleResolutionState.PROCESSED],
        anchor=f"alignment {resolution.definition_idx}",
    )
    members: dict[int, list[int]] = {}
    for entry in resolution.samples:
        members.setdefault(entry.definition_idx, []).append(entry.prep_sample_idx)
    parts = []
    for alignment_idx, samples in sorted(members.items()):
        summary = _member_summary(listing, alignment_idx, samples=len(samples))
        part_reference = _alignment_reference_idx(summary)
        if part_reference != reference_idx:
            raise ValueError(
                f"{len(samples)} prep_sample(s) resolve to alignment {alignment_idx}, which"
                f" ran against reference {part_reference}; alignment"
                f" {resolution.definition_idx} ran against reference {reference_idx}. A"
                f" table is relabeled through one reference's genome map, so these cannot"
                f" be built into one table."
            )
        parts.append(_Part(alignment_idx, summary, tuple(sorted(samples))))
    return parts


# The de novo resolutions that leave a prep_sample reference-only (see `_plan_denovo`).
_NO_DENOVO_ARM = frozenset({SampleResolutionReason.NO_RUN, SampleResolutionReason.NO_DATA})


def _plan_denovo(
    base_url: str,
    token: str,
    *,
    args: argparse.Namespace,
    listing: dict[str, Any],
    anchor_summary: dict[str, Any],
    parts: list[_Part],
    cohort: list[int],
    per_pool: dict[tuple[int, int], set[int]],
    as_of_event: str | None = None,
) -> tuple[list[_DenovoPart], list[dict[str, Any]] | None]:
    """The de novo arm's parts: each cohort prep_sample's de novo alignment, resolved in
    `--denovo-alignment-idx`'s group, grouped by alignment in `alignment_idx` order; and,
    for an exact read, what it left out (`_listed_left_out`), else None.

    A prep_sample with no run in that group (`not_processed` / `no_run`), or whose assembly
    produced no contigs (`not_processed` / `no_data`), has no de novo arm and stays
    reference-only. Any other unprocessed state is refused (`_refuse_unprocessed`).

    `denovo_alignment_processing_idx` checks each de novo alignment against the reference
    part of every prep_sample that resolved to it, with the masks compared exactly: both
    resolutions put the prep_sample on the mask it resolves to, so within one snapshot the
    two arms' masks are equal, and two that differ were read across a change between the
    two requests. It checks the named de novo alignment against `--alignment-idx` too,
    there with the reference anchor's mask group (from a resolve of its mask) as the
    admissible masks, since a caller may name any member of either group. The assembly
    runs of the de novo alignments that some prep_sample resolved to are then checked for
    deprecation, before the refusal of unprocessed prep_samples, so a deprecated run is
    named by the rule that owns its wording.

    A deprecated `--denovo-alignment-idx` read under --include-deprecated is read alone:
    the cohort's prep_samples that the pool cohort route lists completed under it have
    its arm, every other one stays reference-only, and the cohort DoGet resolves the
    arm's prep_samples exactly. The cohort's prep_samples that the route leaves out of
    that cohort (its `left_out`, whose rule the route states) stay reference-only too,
    and are listed on stderr and returned for the manifest's `denovo.left_out`.

    The resolve of the de novo group carries both flags, so it reads past exactly the
    deprecated upstreams they admit (`admitted_deprecated_upstream_kinds`).
    """
    anchor = args.denovo_alignment_idx
    summaries = {anchor: _alignment_summary(listing, alignment_idx=anchor)}
    left_out: list[dict[str, Any]] | None = None
    if _reads_exactly(summaries[anchor], flag=args.include_deprecated, role="de novo alignment"):
        on_anchor: set[int] = set()
        listed_left_out: list[dict[str, Any]] = []
        for (run_idx, pool_idx), listed in per_pool.items():
            if anchor not in listed:
                continue
            body = _fetch_alignment_cohort(
                base_url,
                token,
                sequencing_run_idx=run_idx,
                sequenced_pool_idx=pool_idx,
                alignment_idx=anchor,
                include_deprecated=True,
                include_deprecated_upstream=args.include_deprecated_upstream,
            )
            on_anchor.update(body["prep_sample_idx"])
            listed_left_out.extend(body.get("left_out") or [])
        in_cohort = set(cohort)
        left_out = _listed_left_out(
            [e for e in listed_left_out if e["prep_sample_idx"] in in_cohort],
            f"de novo alignment {anchor}",
            denovo=True,
        )
        resolution = _exact_resolution(
            summaries[anchor], [sample for sample in cohort if sample in on_anchor]
        )
    else:
        resolution = _resolve_definition_group(
            base_url,
            token,
            kind=DefinitionKind.ALIGNMENT,
            definition_idx=anchor,
            prep_sample_idx=cohort,
            as_of_event=as_of_event,
            include_deprecated=args.include_deprecated,
            include_deprecated_upstream=args.include_deprecated_upstream,
        )
    resolved: dict[int, int] = {}
    refused: list[SampleResolution] = []
    for entry in resolution.samples:
        if entry.state is SampleResolutionState.PROCESSED:
            resolved[entry.prep_sample_idx] = entry.definition_idx
        elif entry.reason not in _NO_DENOVO_ARM:
            refused.append(entry)

    anchor_params = anchor_summary.get("params")
    anchor_mask = anchor_params.get("mask_idx") if isinstance(anchor_params, dict) else None
    denovo_alignment_processing_idx(
        denovo_alignment_idx=anchor,
        denovo_params=summaries[anchor].get("params"),
        reference_params=anchor_params,
        reference_mask_group=(
            frozenset(
                _resolve_definition_group(
                    base_url,
                    token,
                    kind=DefinitionKind.MASK,
                    definition_idx=anchor_mask,
                    prep_sample_idx=cohort,
                    as_of_event=as_of_event,
                ).group
            )
            if isinstance(anchor_mask, int)
            else None
        ),
    )

    part_of = {sample: part for part in parts for sample in part.samples}
    pairings = {
        (denovo_idx, part_of[sample].alignment_idx): part_of[sample].summary
        for sample, denovo_idx in resolved.items()
    }
    processing_of: dict[int, int] = {}
    for (denovo_idx, _), reference_summary in sorted(pairings.items()):
        if denovo_idx not in summaries:
            summaries[denovo_idx] = _member_summary(
                listing,
                denovo_idx,
                samples=sum(1 for d in resolved.values() if d == denovo_idx),
            )
        processing_of[denovo_idx] = denovo_alignment_processing_idx(
            denovo_alignment_idx=denovo_idx,
            denovo_params=summaries[denovo_idx].get("params"),
            reference_params=reference_summary.get("params"),
        )
    for processing_idx in sorted(set(processing_of.values())):
        _refuse_deprecated_denovo_run(
            base_url,
            token,
            processing_idx=processing_idx,
            admitted=DefinitionKind.PROCESSING
            in admitted_deprecated_upstream_kinds(
                include_deprecated=args.include_deprecated,
                include_deprecated_upstream=args.include_deprecated_upstream,
            ),
        )
    _refuse_unprocessed(refused, anchor=f"de novo alignment {anchor}")

    members: dict[int, list[int]] = {}
    for sample, denovo_idx in sorted(resolved.items()):
        members.setdefault(denovo_idx, []).append(sample)
    return [
        _DenovoPart(idx, summaries[idx], tuple(samples), processing_of[idx])
        for idx, samples in sorted(members.items())
    ], left_out


def _run_build(
    args: argparse.Namespace, token: str, con, *, gate: ft.AlignmentGate | None
) -> tuple[list[Path], int]:
    """The recipe, on an open connection: discover, resolve, stage, compute, relabel,
    write. Returns the files written and the published table's row count.

    **Parts.** The cohort may span pools, and each prep_sample is resolved
    (`/definition-group/resolve`) in `--alignment-idx`'s group; the prep_samples are read
    per resolved alignment, one part each, and the parts' streams are unioned before
    staging (`staged_streams`), so the analytic runs once over the whole cohort; the
    gate's diagnostics run per part (`_gate_clearance`).
    Identifiers and the manifest's processing handles are minted per part.

    Ordered so that everything cheap and refusable happens first: a wrong
    `--alignment-idx`, an empty or unprocessed cohort, or an over-cap genome map each
    stop the run before a byte of alignment data moves. The two refusals that need
    nothing but the flags — the gate's own coherence and an occupied output — are made by
    the handler, before this is called at all, so neither costs a connection.

    The Flight client lives only for the streams and the tickets that authorize them:
    each stream is drained by the one CREATE that materializes it, so nothing holds a gRPC
    connection open through the compute, the relabel, or the write. `FlightClient` is its
    own context manager, so that lifetime is the `with` block.
    """
    import pyarrow.flight as flight  # noqa: PLC0415

    pools = _pools(args)
    listing, per_pool = _pool_listings(args.base_url, token, pools)
    # One verified summary per alignment, read several times: the reference the table is
    # relabelled through, the digest the manifest cites, the aligner it names. Verifying
    # it once and reading it is what keeps those three from being three separate acts of
    # trust.
    anchor_summary = _alignment_summary(listing, alignment_idx=args.alignment_idx)
    reference_idx = _alignment_reference_idx(anchor_summary)
    # Read the reference BEFORE anything bulk, though only the manifest consumes it. It
    # is the cheapest thing here that can refuse — it 404s on a reference this alignment
    # names but the caller cannot read, which is exactly what every reference DoGet ticket
    # below would then fail on.
    reference = _fetch_reference(args.base_url, token, reference_idx=reference_idx)
    exact = _reads_exactly(anchor_summary, flag=args.include_deprecated, role="alignment")
    resolved_cohort = _resolve_cohort(args.base_url, token, args, per_pool=per_pool, exact=exact)
    resolution, left_out = _skip_unprocessed(
        _exact_resolution(anchor_summary, resolved_cohort.samples)
        if exact
        else _resolve_definition_group(
            args.base_url,
            token,
            kind=DefinitionKind.ALIGNMENT,
            definition_idx=args.alignment_idx,
            prep_sample_idx=resolved_cohort.samples,
            as_of_event=args.as_of_event,
            include_deprecated_upstream=args.include_deprecated_upstream,
        ),
        resolved_cohort,
    )
    # Every later resolve and DoGet of this build reads the anchor's decision set.
    pinned = _pinned(resolution, args.as_of_event)
    parts = _parts(resolution, listing=listing, reference_idx=reference_idx)
    cohort = [entry.prep_sample_idx for entry in resolution.samples]

    # The de novo arm, from the same merged listing that the reference arm came from,
    # checked with `denovo_alignment_processing_idx`, passing the reference arm's mask
    # group so that a de novo arm on any mask of that group is accepted.
    requested_denovo = args.denovo_alignment_idx is not None
    denovo_parts: list[_DenovoPart] = []
    denovo_left_out: list[dict[str, Any]] | None = None
    if requested_denovo:
        denovo_parts, denovo_left_out = _plan_denovo(
            args.base_url,
            token,
            args=args,
            listing=listing,
            anchor_summary=anchor_summary,
            parts=parts,
            cohort=cohort,
            per_pool=per_pool,
            as_of_event=pinned,
        )

    _stage_genome_map(con, _fetch_genome_map(args.base_url, token, reference_idx=reference_idx))
    if requested_denovo:
        # One map per prep_sample with a de novo alignment, folded into one relation with
        # the prep_sample key re-attached. A prep_sample that assembled nothing 404s, and
        # that is the design's graceful path rather than an error: it simply has no de
        # novo arm, so it is skipped here and stays reference-only downstream.
        _stage_denovo_genome_map(
            con,
            {
                prep_sample_idx: entries
                for part in denovo_parts
                for prep_sample_idx in part.samples
                if (
                    entries := _assembly_genome_map_or_none(
                        args.base_url,
                        token,
                        prep_sample_idx=prep_sample_idx,
                        processing_idx=part.processing_idx,
                    )
                )
                is not None
            },
        )
        # **The de novo arm's cohort is the prep_samples the MAP named, not the build's.**
        # Every de novo surface is asked for exactly these: the run-scoped lengths 404
        # for a prep_sample with no run, and the alignment mint refuses a cohort member with
        # no `alignment_sample` row — which is precisely a prep_sample that assembled
        # nothing, since `align-denovo` ends NO_DATA for it and never opens a gate row.
        staged = set(_staged_denovo_samples(con))
        denovo_parts = [
            narrowed
            for part in denovo_parts
            if (
                narrowed := dataclasses.replace(
                    part, samples=tuple(s for s in part.samples if s in staged)
                )
            ).samples
        ]
        if staged and len(staged) < len(cohort):
            print(
                f"note: {len(cohort) - len(staged)} of {len(cohort)} prep_sample(s) have no"
                f" de novo arm (no de novo alignment in the group of"
                f" {args.denovo_alignment_idx}, or no contigs) and are reference-only in"
                f" this table.",
                file=sys.stderr,
            )
        if not staged:
            # Nothing in the cohort has a second arm to reconcile against, so this is a
            # reference-only build rather than an empty one. Left as a combined build it
            # would sign an unscoped ticket, which the mint refuses, and bind against a
            # de novo slice nothing staged.
            print(
                f"note: no prep_sample in this cohort has a de novo arm, so"
                f" --denovo-alignment-idx {args.denovo_alignment_idx} contributed nothing"
                f" and this is a reference-only table. The manifest records that it was"
                f" requested.",
                file=sys.stderr,
            )
    left_out_records = _left_out_identifiers(
        args.base_url,
        token,
        left_out,
        resolved_cohort,
        include_deprecated=args.include_deprecated,
        include_deprecated_upstream=args.include_deprecated_upstream,
    )
    identifiers = sorted(
        (
            entry
            for part in parts
            for entry in _mint_exported_identifiers(
                args.base_url,
                token,
                alignment_idx=part.alignment_idx,
                prep_sample_idx=list(part.samples),
                include_deprecated=args.include_deprecated,
                include_deprecated_upstream=args.include_deprecated_upstream,
            )
        ),
        key=lambda entry: entry["prep_sample_idx"],
    )
    _stage_exported_identifiers(con, identifiers)

    # Each alignment's deprecated upstreams, as its cohort DoGet reported reading them.
    upstreams_read: dict[int, list[dict[str, Any]]] = {}
    with flight.FlightClient(args.data_plane_url) as flight_client:
        # Lengths first, though the alignment is the interesting stream: this one is a
        # whole-reference read that costs a ticket mint and a small aggregate, so a
        # failure in it should not come after a cohort's worth of alignment rows have
        # already crossed the wire. Same order as the server-side job.
        def _reference_stream(*, table: str, relation: str, table_sql) -> None:
            _stage_from_stream(
                con,
                flight_client,
                create_reference_doget_ticket(
                    args.base_url, token, reference_idx=reference_idx, table=table
                ),
                relation=relation,
                table_sql=table_sql,
            )

        # One stream, two possible shapes. A circular gate needs a length per FEATURE
        # (it asks how much of a read one contig explains), the coverage filter needs
        # one per genome — and the stream is one-shot, so when both are wanted the
        # per-feature table is what it is drained into and the roll-up reads that.
        circular_gate = gate is not None and gate.circular
        filtering = ft.coverage_filter_applies(args.coverage_threshold)
        if circular_gate or filtering:
            _reference_stream(
                table=_SEQUENCES_TABLE,
                relation=_LENGTHS_STREAM,
                table_sql=(
                    ft.feature_lengths_table_sql if circular_gate else ft.genome_lengths_table_sql
                ),
            )
            if circular_gate and filtering:
                con.execute(ft.genome_lengths_table_sql(ft.FEATURE_LENGTHS_TABLE))
        if args.taxonomy:
            _reference_stream(
                table=_TAXONOMY_TABLE,
                relation=_TAXONOMY_STREAM,
                table_sql=ft.taxonomy_table_sql,
            )
        if denovo_parts and filtering:
            # N run-scoped streams, appended into one relation before the roll-up
            # reads it — the assembly read-back has no whole-cohort form. Only the
            # prep_samples the map named: a prep_sample with no contigs has no run to read.
            run_of = {s: part.processing_idx for part in denovo_parts for s in part.samples}
            con.execute(ft.denovo_contig_lengths_table_sql())
            for prep_sample_idx in sorted(run_of):
                _stage_from_stream(
                    con,
                    flight_client,
                    _create_assembly_run_doget_ticket(
                        args.base_url,
                        token,
                        prep_sample_idx=prep_sample_idx,
                        processing_idx=run_of[prep_sample_idx],
                        # One row per contig — hash and length, no bytes. The
                        # chunks table beside it is what carries sequence, and
                        # this recipe never reads it.
                        table=ASSEMBLED_SEQUENCE_TABLE,
                    ),
                    relation=_DENOVO_LENGTHS_STREAM,
                    table_sql=ft.denovo_contig_lengths_insert_sql,
                )
            con.execute(ft.denovo_genome_lengths_insert_sql())
        columns = ft.gate_alignment_columns(gate)
        _stage_alignment(
            con,
            flight_client,
            parts,
            mint=lambda part: _create_alignment_doget_ticket(
                args.base_url,
                token,
                alignment_idx=part.alignment_idx,
                prep_sample_idx=list(part.samples),
                columns=columns,
                include_deprecated=args.include_deprecated,
                include_deprecated_upstream=args.include_deprecated_upstream,
                as_of_event=pinned,
                upstreams_read=upstreams_read,
                exact=args.include_deprecated and _is_deprecated(part.summary),
            ),
            gate=gate,
        )
        if denovo_parts:
            # The second arm and precedence over the first, in one sequence — see
            # `analytic.reconcile.denovo_alignment_statements`. The de novo slice takes
            # no gate: `--circular-gate` is refused alongside this flag, and the CIGAR
            # gate would judge it on a rule align_denovo already applied.
            denovo_mints = [
                functools.partial(
                    _create_alignment_doget_ticket,
                    args.base_url,
                    token,
                    alignment_idx=part.alignment_idx,
                    prep_sample_idx=list(part.samples),
                    columns=ft.ALIGNMENT_COLUMNS,
                    include_deprecated=args.include_deprecated,
                    include_deprecated_upstream=args.include_deprecated_upstream,
                    as_of_event=pinned,
                    upstreams_read=upstreams_read,
                    exact=args.include_deprecated and _is_deprecated(part.summary),
                )
                for part in denovo_parts
            ]
            with staged_streams(
                con, flight_client, denovo_mints, relation=_DENOVO_ALIGNMENT_STREAM
            ) as source:
                for sql in ft.denovo_alignment_statements(source):
                    con.execute(sql)
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
        combined=bool(denovo_parts),
    )
    # The row labels are minted here rather than beside the prep_sample labels above,
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

    # LAST of the round trips, after everything that could refuse, because they are
    # WRITES: minting a public handle for a build that then fails would leave a permanent
    # name for a bundle nobody has. One per part of each arm.
    def handles(arm: Sequence[_Part]) -> dict[int, str]:
        return {
            part.alignment_idx: _mint_exported_processing(
                args.base_url,
                token,
                alignment_idx=part.alignment_idx,
                prep_sample_idx=list(part.samples),
                include_deprecated=args.include_deprecated,
                include_deprecated_upstream=args.include_deprecated_upstream,
            )
            for part in arm
        }

    processing = _manifest_parts(parts, handles(parts), identifiers, upstreams_read)
    denovo_record = (
        _manifest_parts(denovo_parts, handles(denovo_parts), identifiers, upstreams_read)
        if denovo_parts
        else None
    )
    tools = _tool_versions(con)
    written = _write_bundle(
        con,
        table_path=args.output,
        fmt=args.format,
        identifiers=identifiers,
        manifest=lambda files: _manifest_payload(
            args=args,
            gate=gate,
            reference=reference,
            processing=processing,
            identifiers=identifiers,
            left_out=left_out_records + (resolved_cohort.listed_left_out or []),
            rows=clearance.rows,
            files=files,
            tools=tools,
            denovo=_manifest_denovo(
                requested=requested_denovo, record=denovo_record, left_out=denovo_left_out
            ),
            decisions=(
                None
                if resolution.decisions is None
                else resolution.decisions.model_dump(mode="json")
            ),
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
        _common.print_http_status_error(exc.response)
        return 1
    except _common.httpx.RemoteProtocolError as exc:
        # Ahead of RequestError, which it subclasses. httpx raises this both for a
        # body cut mid-transfer and for a peer that closed before any response, so
        # the message covers both rather than asserting which happened — the first
        # is a retry, the second is usually a wrong --base-url.
        print(
            f"error: the connection ended before a complete response arrived ({exc})."
            " Retry; a partial genome map is never used. If it persists, check"
            " --base-url / $QIITA_CONTROL_PLANE_URL.",
            file=sys.stderr,
        )
        return 1
    except _common.httpx.RequestError as exc:
        _common.print_request_error(exc)
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
