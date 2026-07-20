"""Runner read-ingest and staged-read bindings."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

import asyncpg
from qiita_common.api_paths import (
    compute_reads_staging_path,
)
from qiita_common.backend_failure import StepNoData
from qiita_common.parquet import validate_parquet_path

import qiita_control_plane.runner as _runner_pkg

from ..auth.tickets import sign_action, sign_ticket
from ..host_filter_resolver import is_control_sample
from ..miint import connect_with_miint
from ..repositories.block import fetch_mask_sample_state
from ..repositories.prep_sample import fetch_biosample_idx_for_prep_sample
from ._upload import _submission_bad_input, _submission_dp_fetch_failure

_log = logging.getLogger(__name__)

# =============================================================================
# Read ingest + staged-read bindings
# =============================================================================
#
# The bcl-convert workflow's `ingest_reads` step stores the pool's reads once;
# the repeatable read-mask workflow consumes them from a durable, prep_sample-
# addressable copy. Two runner-side bindings bridge the orchestrator's lack of
# DB access:
#   * `sample_map`  — the `{prep_sample_idx, pool_item_id}` roster the CP knows
#     and the ingest step needs, materialized to a Parquet (like the adapter
#     set) because `params:` only carry scalars.
#   * `reads`       — bound from `compute_reads_staging_path` for a mask
#     workflow, which has no step that produces reads.
# `reads_staging_root` hands the ingest step the scratch root it writes the
# durable copies under.

SAMPLE_MAP_BINDING = "sample_map"
STAGED_READS_BINDING = "reads"
# A workflow that consumes ready-for-consumption reads (assembly) declares
# `masked_reads_fastq`; the runner STREAMS the `read_masked` pass-set for a
# mask_idx from the data plane and writes it as gzip FASTQ (via miint's native
# COPY FORMAT FASTQ). A DISTINCT name from raw `reads` so the two never collide —
# read-mask workflows consume raw `reads` to CREATE a mask; assembly workflows
# consume `masked_reads_fastq`.
STAGED_MASKED_READS_BINDING = "masked_reads_fastq"
READS_STAGING_ROOT_BINDING = "reads_staging_root"


# Bindings a sharded build ticket's build steps consume: the per-shard feature
# roster Parquet (`shard_features`) and the shard ordinal (`shard_id`). The
# runner stages both BEFORE the step loop (see `_stage_shard_roster`), from the
# ticket's `shard_id` + `reference_membership.shard_id`; the shard build jobs
# (build_rype/minimap2/bowtie2_index) resolve them as their `Inputs`.
SHARD_FEATURES_BINDING = "shard_features"
SHARD_ID_BINDING = "shard_id"
_REFERENCE_SEQUENCES_TABLE = "reference_sequences"

# Binding the plan-shards arm sets to gate the whole-reference rype_router build
# entries (build_routing_index → register-index → finalize-shard) that follow it
# in the sharded reference-add flow. Present-and-True only when plan-shards fanned
# out (N > 0 shards); present-and-False otherwise so the `when: router_pending`
# gate skips those entries — an ABSENT gate key defaults ON, so the runner seeds
# this False before the step loop (below) to make the router build default-OFF
# even when plan-shards is skipped (shard_index explicitly false) or a no-op.
ROUTER_PENDING_BINDING = "router_pending"
# The runner-staged shard→bucket mapping Parquet `(feature_idx BIGINT,
# bucket_name VARCHAR = str(shard_id))` build_routing_index consumes. Staged by
# the plan-shards arm from qiita.reference_membership.shard_id right after the
# fan-out assigns it — shard_id is authoritative in Postgres (the DuckLake
# reference_membership has no shard_id column), so this is a direct PG read, not
# a Flight export.
SHARD_MAPPING_BINDING = "shard_mapping"


def _do_get_reference_sequences_roster(
    data_plane_url: str, ticket_bytes: bytes, out_path: Path
) -> int:
    """Synchronous Flight DoGet of a feature-scoped `reference_sequences` slice,
    written to a `(feature_idx BIGINT, sequence_length_bp BIGINT)` roster Parquet
    at `out_path`. Runs in a thread executor (pyarrow.flight is sync); isolated
    so `_stage_shard_roster`'s unit test stubs the whole seam. Returns the row
    count (the shard's feature count that has a sequence)."""
    import pyarrow.flight as flight  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415

    with flight.FlightClient(data_plane_url) as client:
        table = client.do_get(flight.Ticket(ticket_bytes)).read_all()
    # Project to exactly the roster columns the build jobs expect (drop
    # sequence_hash) — the shard build reads `feature_idx` to scope its own chunk
    # stream and `sequence_length_bp` for plan() sizing.
    roster = table.select(["feature_idx", "sequence_length_bp"])
    pq.write_table(roster, str(out_path), compression="snappy")
    return roster.num_rows


async def _stage_shard_roster(
    pool: asyncpg.Pool | asyncpg.Connection,
    reference_idx: int,
    shard_id: int,
    *,
    data_plane_url: str,
    signing_key: bytes,
    workspace: Path,
) -> dict[str, Any]:
    """Stage this shard's feature roster before the build step loop and bind it.

    The shard's features are the cover-map (`reference_membership.shard_id`);
    their `sequence_length_bp` lives in DuckLake `reference_sequences`, reachable
    only over Flight. So we read the shard's feature_idx set from Postgres, sign
    a `feature_idx`-scoped `reference_sequences` DoGet (the subset ticket — so
    each shard transfers only its own slice, not the whole reference N times),
    and write `<workspace>/shard_roster.parquet`. Binds `shard_features` (the
    roster path) and `shard_id` so the build steps' `Inputs` resolve.

    Like the other pre-loop resolvers, a Flight failure is wrapped as a
    SUBMISSION-attributed failure (via `_submission_dp_fetch_failure`: a DuckLake
    serialization conflict is retriable, everything else BAD_INPUT) so it lands in
    the outer FAILED handler instead of escaping as an untyped exception (which
    would violate the step-name CHECK). An empty membership shard is a
    misconfiguration — fail loud rather than build an empty index."""
    rows = await pool.fetch(
        "SELECT feature_idx FROM qiita.reference_membership"
        " WHERE reference_idx = $1 AND shard_id = $2",
        reference_idx,
        shard_id,
    )
    feature_idxs = [r["feature_idx"] for r in rows]
    if not feature_idxs:
        raise _submission_bad_input(
            f"shard {shard_id} of reference {reference_idx} has no member features "
            "(reference_membership.shard_id) — nothing to build"
        )
    ticket = sign_ticket(
        table=_REFERENCE_SEQUENCES_TABLE,
        filter={"reference_idx": [reference_idx], "feature_idx": feature_idxs},
        secret=signing_key,
    )
    workspace.mkdir(parents=True, exist_ok=True)
    roster_path = workspace / "shard_roster.parquet"
    try:
        await asyncio.get_event_loop().run_in_executor(
            None,
            _runner_pkg._do_get_reference_sequences_roster,
            data_plane_url,
            ticket,
            roster_path,
        )
    except Exception as exc:
        raise _submission_dp_fetch_failure(
            f"could not fetch reference_sequences for reference {reference_idx} "
            f"shard {shard_id} from the data plane: {type(exc).__name__}: {exc}",
            exc,
        ) from exc
    return {SHARD_FEATURES_BINDING: roster_path, SHARD_ID_BINDING: shard_id}


def _write_shard_mapping_parquet(rows: list[tuple[int, int]], out_path: Path) -> None:
    """Write `(feature_idx, shard_id)` rows to a
    `(feature_idx BIGINT, bucket_name VARCHAR)` Parquet — one row per sharded
    feature, `bucket_name = str(shard_id)`. This is exactly the shape
    `build_routing_index.Inputs.shard_mapping` expects (the router build's
    multi-bucket `rype_index_create` mapping table). pyarrow (already a Flight
    dependency) writes it directly, mirroring `_write_sample_map_parquet`."""
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415

    feature = [int(f) for f, _ in rows]
    bucket = [str(s) for _, s in rows]
    table = pa.table(
        {
            "feature_idx": pa.array(feature, type=pa.int64()),
            "bucket_name": pa.array(bucket, type=pa.string()),
        }
    )
    pq.write_table(table, str(out_path))


async def _stage_shard_mapping(
    pool: asyncpg.Pool | asyncpg.Connection,
    reference_idx: int,
    out_path: Path,
) -> Path:
    """Stage the whole-reference shard→bucket mapping the router build consumes.

    Exports `qiita.reference_membership` (the authoritative store for
    `shard_id` — the DuckLake mirror has no such column) to a
    `(feature_idx BIGINT, bucket_name VARCHAR)` Parquet at `out_path`, one row
    per feature assigned to a shard (`bucket_name = str(shard_id)`,
    `shard_id IS NOT NULL`). Called by the plan-shards arm right after
    `plan_and_submit_shards` has written the assignment (N > 0), so the rows are
    present; re-staged verbatim on resume from the durable assignment. A missing
    assignment where one is expected is a fail-loud bug (the caller only stages
    when the fan-out reported N > 0)."""
    rows = await pool.fetch(
        "SELECT feature_idx, shard_id FROM qiita.reference_membership"
        " WHERE reference_idx = $1 AND shard_id IS NOT NULL"
        " ORDER BY feature_idx",
        reference_idx,
    )
    if not rows:
        raise RuntimeError(
            f"reference {reference_idx}: no reference_membership.shard_id assignment "
            "to build a routing index from (plan-shards reported a fan-out but wrote "
            "no shard assignment)"
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    _write_shard_mapping_parquet([(r["feature_idx"], r["shard_id"]) for r in rows], out_path)
    return out_path


def _workflow_declares_input(steps: list[Any], name: str) -> bool:
    """True iff some entry declares `name` among its `inputs`/`optional_inputs`."""
    for entry in steps:
        names = list(getattr(entry, "inputs", []) or []) + list(
            getattr(entry, "optional_inputs", []) or []
        )
        if name in names:
            return True
    return False


def _workflow_needs_staged_reads(steps: list[Any]) -> bool:
    """True iff `reads` is consumed by some step but produced by none — so it must
    be bound externally from the prep_sample's stored reads (the read-mask
    workflow). The bcl-convert workflow produces reads internally (`ingest_reads`
    emits `read_staging_dir`, not `reads`), so it does not match."""
    if not _workflow_declares_input(steps, STAGED_READS_BINDING):
        return False
    for entry in steps:
        if STAGED_READS_BINDING in (getattr(entry, "outputs", []) or []):
            return False
    return True


def _workflow_needs_staged_masked_reads(steps: list[Any]) -> bool:
    """True iff `masked_reads_fastq` is consumed by some step but produced by
    none — so it must be bound externally from the sample's `read_masked` pass-set
    (the long-read-assembly workflow)."""
    if not _workflow_declares_input(steps, STAGED_MASKED_READS_BINDING):
        return False
    for entry in steps:
        if STAGED_MASKED_READS_BINDING in (getattr(entry, "outputs", []) or []):
            return False
    return True


def _write_sample_map_parquet(roster: list[dict[str, Any]], out_path: Path) -> None:
    """Write the `{prep_sample_idx, pool_item_id}` roster to a Parquet
    `(prep_sample_idx BIGINT, pool_item_id VARCHAR)` for the ingest step.
    pyarrow (already a Flight dependency) writes it directly — no DuckDB needed
    on the pre-loop path, mirroring `_write_adapter_parquet`."""
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415

    prep = [int(r["prep_sample_idx"]) for r in roster]
    items = [str(r["pool_item_id"]) for r in roster]
    table = pa.table(
        {
            "prep_sample_idx": pa.array(prep, type=pa.int64()),
            "pool_item_id": pa.array(items, type=pa.string()),
        }
    )
    pq.write_table(table, str(out_path))


async def _resolve_sample_map(action_context: dict[str, Any], workspace: Path) -> dict[str, Path]:
    """Materialize the bcl-convert pool roster from action_context into a local
    Parquet for the `ingest_reads` step. Same pre-loop, inside-try placement as
    the other resolvers so a failure lands in the outer FAILED handler. Raises a
    SUBMISSION-attributed BAD_INPUT on a missing/empty roster."""
    roster = action_context.get(SAMPLE_MAP_BINDING)
    if not roster:
        raise _submission_bad_input(
            "an ingest workflow requires a non-empty `sample_map` roster in "
            "action_context (the CP embeds it at submit-bcl-convert time)"
        )
    workspace.mkdir(parents=True, exist_ok=True)
    out = workspace / "sample_map.parquet"
    _write_sample_map_parquet(roster, out)
    return {SAMPLE_MAP_BINDING: out}


def _do_action_export(action_type: str, data_plane_url: str, token: bytes) -> dict[str, Any]:
    """Shared body for the read-export DoActions (`export_read`,
    `export_read_block`): run a synchronous Flight DoAction of `action_type` in a
    thread executor (pyarrow.flight is sync). The data plane writes the file; the
    bulk read bytes never transit the control plane. Returns `{"count": int,
    "dest": str}` with `count` already coerced to int, raising ValueError on a
    missing/garbled body or a non-integer `count` so the caller (inside its
    `except`) turns it into a clean SUBMISSION failure rather than a cryptic
    backtrace. Mirrors `actions.library._do_action`."""
    import pyarrow.flight as flight  # noqa: PLC0415

    with flight.FlightClient(data_plane_url) as client:
        results = list(client.do_action(flight.Action(action_type, token)))
    if not results:
        return {"count": 0, "dest": ""}
    body = results[0].body.to_pybytes()
    if not body:
        return {"count": 0, "dest": ""}
    # Parse + coerce here (inside the executor, so the caller's `except` wraps any
    # failure) — never hand the caller a `count` it must coerce outside its try.
    try:
        parsed = json.loads(body)
        count = int(parsed["count"])
    except (ValueError, TypeError, KeyError, AttributeError) as exc:
        raise ValueError(f"{action_type} returned an unparseable result body: {exc!r}") from exc
    return {"count": count, "dest": str(parsed.get("dest", ""))}


def _do_action_export_read(data_plane_url: str, token: bytes) -> dict[str, Any]:
    """`export_read` DoAction: the data plane re-materializes ONE prep_sample's
    reads from its DuckLake `read` table into a per-ticket Parquet on shared
    scratch. Isolated (thin wrapper over `_do_action_export`) so unit tests stub
    the real call by name."""
    return _do_action_export("export_read", data_plane_url, token)


def _do_action_export_read_block(data_plane_url: str, token: bytes) -> dict[str, Any]:
    """`export_read_block` DoAction: the data plane materializes the UNION of a
    block's `(prep_sample_idx, sequence_idx sub-range)` members from its DuckLake
    `read` table into one per-ticket Parquet. Isolated (thin wrapper over
    `_do_action_export`) so unit tests stub the real call by name."""
    return _do_action_export("export_read_block", data_plane_url, token)


def _do_action_export_read_masked_block(data_plane_url: str, token: bytes) -> dict[str, Any]:
    """`export_read_masked_block` DoAction: the data plane materializes the UNION
    of a block's members from its DuckLake `read_masked` VIEW (trimmed +
    host/QC-`pass`-filtered), scoped to the ticket's `mask_idx`, into one
    per-ticket Parquet — the MASKED-reads sibling of `export_read_block` (same
    output column shape). Isolated (thin wrapper over `_do_action_export`) so unit
    tests stub the real call by name."""
    return _do_action_export("export_read_masked_block", data_plane_url, token)


def _stream_masked_reads_to_fastq(data_plane_url: str, ticket_bytes: bytes, dest: Path) -> int:
    """Stream one prep_sample's `read_masked` pass-set from the data plane and write
    it as gzip FASTQ with miint's native `COPY … (FORMAT FASTQ)` — the EXACT
    capability the admin masked-read export uses. The data plane STREAMS the rows
    over a DoGet; it never writes a file, there is no intermediate Parquet, and no
    hand-rolled FASTQ writer. Returns the FASTQ record count (0 ⇒ the mask filtered
    everything out; the caller turns that into a terminal NO_DATA). Module-level so
    unit tests stub it by name.

    Single-pass over the stream: the DoGet reader is registered as `masked`, the
    COPY consumes it, and `COPY … TO` returns the rows it wrote. Realigning the
    incoming Flight buffers (DataTypeSpecific) mirrors the admin export — Acero
    warns per-batch on the zero-copy gRPC buffers otherwise (apache/arrow#37195)."""
    import pyarrow.flight as flight  # noqa: PLC0415
    import pyarrow.ipc as ipc  # noqa: PLC0415

    read_opts = flight.FlightCallOptions(
        read_options=ipc.IpcReadOptions(ensure_alignment=ipc.Alignment.DataTypeSpecific)
    )
    with flight.FlightClient(data_plane_url) as client, connect_with_miint() as con:
        reader = client.do_get(flight.Ticket(ticket_bytes), read_opts).to_reader()
        con.register("masked", reader)
        # Single-end long reads: sequence2/qual2 are NULL, so FORMAT FASTQ writes a
        # single-end fastq. Column order is the one the miint writer requires.
        (count,) = con.execute(
            "COPY (SELECT read_id, sequence1, qual1, sequence2, qual2 FROM masked) "
            f"TO '{validate_parquet_path(dest)}' (FORMAT FASTQ, COMPRESSION 'gzip')"
        ).fetchone()
    return int(count)


async def _prep_sample_is_expected_empty_control(pool: asyncpg.Pool, prep_sample_idx: int) -> bool:
    """True when this prep_sample's biosample is flagged an expected-empty control
    (host_taxon_id == "missing: control sample"). Resolves prep_sample → biosample,
    then defers the control classification to `is_control_sample` so the definition
    of "control" stays shared with the host-filter resolver. Fail-safe on a missing
    prep_sample (returns False → the zero-read ticket FAILs as a data well)."""
    biosample_idx = await fetch_biosample_idx_for_prep_sample(pool, prep_sample_idx)
    if biosample_idx is None:
        return False
    return await is_control_sample(pool, biosample_idx=biosample_idx)


async def _resolve_staged_reads(
    pool: asyncpg.Pool,
    scope_target: dict[str, Any],
    staging_root: Path,
    *,
    data_plane_url: str,
    signing_key: bytes,
    workspace: Path,
) -> dict[str, Path]:
    """Bind `reads` to the prep_sample's stored reads for a read-mask workflow.

    Fast path: the durable staging copy `ingest_reads` wrote
    (`compute_reads_staging_path`). That copy is ephemeral, so when it is gone
    (reprocessing a run stored earlier) fall back to the PERSISTENT store: ask the
    data plane to re-materialize the sample's reads from its DuckLake `read` table
    into a per-ticket `reads.parquet` via the `export_read` DoAction (the data
    plane writes the file; the bulk read bytes never transit the control plane).
    Either source binds the same `reads` path; they are byte-equivalent modulo row
    order, and qc / host_filter are order-independent.

    Zero stored reads splits on whether the well is an expected-empty control: a
    blank / no-template control legitimately produces no reads, so it ends the
    ticket at a benign terminal `no_data` (StepNoData); a data well with no reads
    is a genuine failure and stays SUBMISSION/BAD_INPUT (FAILED) — it must be
    ingested before a mask can be created over it. (An unreachable data plane is
    likewise SUBMISSION/BAD_INPUT.) Without this split every empty well — control
    or data — lands in the pool's `samples_failed`, burying real failures among
    blanks doing their job."""
    prep_sample_idx = scope_target["prep_sample_idx"]

    durable = compute_reads_staging_path(staging_root, prep_sample_idx)
    if durable.exists():
        return {STAGED_READS_BINDING: durable}

    # Ephemeral durable copy gone — source from the persistent DuckLake `read`
    # table. We name the per-ticket destination (under the shared scratch tree the
    # data plane validates); the data plane writes it.
    workspace.mkdir(parents=True, exist_ok=True)
    dest = workspace / "reads.parquet"
    token = sign_action(
        action="export_read",
        payload={"prep_sample_idx": prep_sample_idx, "dest": str(dest)},
        secret=signing_key,
    )
    # A Flight failure (data plane unreachable / errored) is NOT a BackendFailure;
    # wrap it as a SUBMISSION BAD_INPUT like the other pre-loop resolvers so the
    # outer handler FAILs the ticket cleanly (step_name=None) rather than letting
    # an untyped exception strand it in PROCESSING. (Not retried in place: the
    # operator resubmits if the data plane was down.)
    try:
        result = await asyncio.get_running_loop().run_in_executor(
            None, _runner_pkg._do_action_export_read, data_plane_url, token
        )
    except Exception as exc:
        raise _submission_dp_fetch_failure(
            f"could not materialize reads for prep_sample {prep_sample_idx} from "
            f"the data plane: {type(exc).__name__}: {exc}",
            exc,
        ) from exc

    # `count` is already an int (coerced in `_do_action_export_read`).
    if result.get("count", 0) == 0:
        # The persistent store has no reads for this sample either (the data plane
        # writes no file for an empty result). Split control from data: an
        # expected-empty control (blank / NTC) reading zero is the benign, correct
        # outcome → terminal no_data; a data well reading zero is a real failure →
        # BAD_INPUT (must be ingested before a mask can be created over it). The
        # control marker is the persisted biosample host_taxon_id == "missing:
        # control sample" (see `is_control_sample`).
        if await _prep_sample_is_expected_empty_control(pool, prep_sample_idx):
            raise StepNoData(
                reason=(
                    f"prep_sample {prep_sample_idx} is an expected-empty control "
                    "(blank / no-template control) with no stored reads; recording "
                    "no_data rather than a failure"
                )
            )
        raise _submission_bad_input(
            f"no stored reads for prep_sample {prep_sample_idx}; the sample must be "
            "ingested (submit-bcl-convert stores reads) before a read mask can be "
            "created over it"
        )
    if not dest.exists():
        # The data plane reported reads but no file landed at dest (a data-plane
        # bug, a full disk, or a mid-write failure). Fail at submission rather than
        # handing a downstream step a path that isn't there.
        raise _submission_bad_input(
            f"the data plane reported reads for prep_sample {prep_sample_idx} but "
            f"wrote no file at {dest}"
        )
    return {STAGED_READS_BINDING: dest}


async def _resolve_staged_masked_reads(
    pool: asyncpg.Pool,
    scope_target: dict[str, Any],
    mask_idx: int,
    *,
    data_plane_url: str,
    signing_key: bytes,
    workspace: Path,
) -> dict[str, Path]:
    """Bind `masked_reads_fastq` to a gzip FASTQ of a prep_sample's MASKED read
    pass-set for a mask_idx — the reads a downstream assembler consumes.

    Reuses the EXISTING streaming masked-read capability (the admin masked-read
    export): sign a `read_masked` DoGet ticket, STREAM the pass-set from the data
    plane, and write FASTQ with miint's native `COPY … (FORMAT FASTQ)`. No bespoke
    DoAction/payload, no intermediate Parquet, no hand-rolled FASTQ writer, and the
    data plane never writes a file — it streams (`_stream_masked_reads_to_fastq`).

    First enforces the `mask_sample` completion gate: a block-masked sample whose
    covering block is still in flight would expose a PARTIAL pass-set, so it is
    rejected (SUBMISSION/BAD_INPUT) — the same fail-closed gate the admin export
    enforces. A fully-masked-out sample (0 passing reads) is a COMMON, expected
    outcome, not an error: it is a terminal NO_DATA (logged; the outer handler
    transitions the ticket to NO_DATA). An unreachable data plane / no file written
    is SUBMISSION/BAD_INPUT."""
    prep_sample_idx = scope_target["prep_sample_idx"]

    # Completion gate: a non-completed `mask_sample` row means a covering block is
    # still masking this sample, so read_masked would expose only a partial
    # pass-set. Reject rather than assemble partial reads. No gate row (the
    # per-sample read-mask path) ⇒ allowed. Mirrors routes/admin masked-export.
    gate_state = await fetch_mask_sample_state(
        pool, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx
    )
    if gate_state is not None and gate_state != "completed":
        raise _submission_bad_input(
            f"mask_idx {mask_idx} is not completed for prep_sample {prep_sample_idx} "
            f"(mask_sample.state={gate_state!r}); a covering block is still masking. "
            "Resubmit once reconcile marks the mask completed."
        )

    # The SAME read_masked DoGet ticket the admin masked-read export mints — a
    # generic ticket scoped to exactly (prep_sample_idx, mask_idx), no bespoke
    # action or payload type.
    ticket = sign_ticket(
        table="read_masked",
        filter={"prep_sample_idx": [prep_sample_idx], "mask_idx": [mask_idx]},
        secret=signing_key,
    )
    workspace.mkdir(parents=True, exist_ok=True)
    dest = workspace / "masked_reads.fastq.gz"
    # Flight failure -> SUBMISSION BAD_INPUT like the other pre-loop resolvers
    # (step_name=None), so the outer handler FAILs the ticket cleanly rather than
    # stranding it in PROCESSING. The blocking stream+COPY runs off the event loop.
    try:
        count = await asyncio.get_running_loop().run_in_executor(
            None, _runner_pkg._stream_masked_reads_to_fastq, data_plane_url, ticket, dest
        )
    except Exception as exc:
        raise _submission_dp_fetch_failure(
            f"could not stream masked reads for prep_sample {prep_sample_idx} "
            f"(mask_idx {mask_idx}) from the data plane: {type(exc).__name__}: {exc}",
            exc,
        ) from exc

    if count == 0:
        # No reads pass this mask for this sample. COMMON — aggressive host/human
        # filtering can legitimately remove everything — so it is a terminal
        # NO_DATA, not a failure. Remove the empty fastq the COPY may have created;
        # log for operator visibility (the outer StepNoData handler moves the
        # ticket PROCESSING → NO_DATA).
        dest.unlink(missing_ok=True)
        _log.info(
            "assembly: no reads pass mask_idx %s for prep_sample %s — no data to assemble",
            mask_idx,
            prep_sample_idx,
        )
        raise StepNoData(
            reason=(
                f"no reads pass mask_idx {mask_idx} for prep_sample "
                f"{prep_sample_idx} — nothing to assemble"
            ),
        )
    if not dest.exists():
        raise _submission_bad_input(
            f"streamed {count} masked reads for prep_sample {prep_sample_idx} "
            f"(mask_idx {mask_idx}) but no fastq landed at {dest}"
        )
    return {STAGED_MASKED_READS_BINDING: dest}


async def _resolve_staged_reads_block(
    members: list[dict[str, int]],
    *,
    data_plane_url: str,
    signing_key: bytes,
    workspace: Path,
) -> dict[str, Path]:
    """Bind `reads` to a BLOCK's reads for a read-mask-block workflow — the
    multi-sample analog of `_resolve_staged_reads`.

    A block spans a set of `(prep_sample_idx, sequence_idx sub-range)` `members`
    that all resolve to one `mask_idx`. Because a block may hold only a sub-range
    of a large sample, the per-sample durable staging copy cannot serve it, so we
    always source from the PERSISTENT DuckLake `read` table: ask the data plane to
    materialize the union of the members' sub-ranges into a per-ticket
    `reads.parquet` via the `export_read_block` DoAction (the data plane writes
    the file; the bulk read bytes never transit the control plane). `qc` /
    `host_filter` read `prep_sample_idx` per-row, so a multi-sample file is fine.

    Fails SUBMISSION/BAD_INPUT (so the ticket FAILs cleanly, step_name=None) if:
    `members` is empty (a planning bug); the data plane is unreachable; the block
    selects zero reads (its members' ranges match nothing — a planning bug, since
    blocks are tiled from `sequence_range` bounds that must exist); or the data
    plane reported reads but no file landed."""
    if not members:
        raise _submission_bad_input("a read-mask block requires a non-empty members list")
    workspace.mkdir(parents=True, exist_ok=True)
    dest = workspace / "reads.parquet"
    # Coerce the member shape up front (a malformed member is a planner bug):
    # a missing key or non-int value must FAIL the ticket cleanly as BAD_INPUT,
    # not escape as an untyped KeyError/TypeError that strands it in PROCESSING.
    try:
        member_payload = [
            {
                "prep_sample_idx": int(m["prep_sample_idx"]),
                "sequence_idx_start": int(m["sequence_idx_start"]),
                "sequence_idx_stop": int(m["sequence_idx_stop"]),
            }
            for m in members
        ]
    except (KeyError, TypeError, ValueError) as exc:
        raise _submission_bad_input(
            f"malformed read-mask block member (a planning bug): {type(exc).__name__}: {exc}"
        ) from exc
    token = sign_action(
        action="export_read_block",
        payload={"dest": str(dest), "members": member_payload},
        secret=signing_key,
    )
    # A Flight failure (data plane unreachable / errored) is NOT a BackendFailure;
    # wrap it as a SUBMISSION BAD_INPUT like the per-sample resolver so the outer
    # handler FAILs the ticket cleanly rather than stranding it in PROCESSING.
    try:
        result = await asyncio.get_running_loop().run_in_executor(
            None, _runner_pkg._do_action_export_read_block, data_plane_url, token
        )
    except Exception as exc:
        raise _submission_dp_fetch_failure(
            f"could not materialize reads for the block from the data plane: "
            f"{type(exc).__name__}: {exc}",
            exc,
        ) from exc

    # `count` is already an int (coerced in `_do_action_export`).
    if result.get("count", 0) == 0:
        raise _submission_bad_input(
            "the block selected zero reads from the data plane; its members' "
            "sequence_idx ranges match no stored reads (a planning bug — blocks "
            "are tiled from qiita.sequence_range bounds that must exist)"
        )
    if not dest.exists():
        raise _submission_bad_input(
            f"the data plane reported reads for the block but wrote no file at {dest}"
        )
    return {STAGED_READS_BINDING: dest}


def _write_empty_reads_parquet(dest: Path) -> None:
    """Write a schema-correct 0-row `reads.parquet` at `dest`, in the exact
    `export_read_block` column shape (`prep_sample_idx, sequence_idx, read_id,
    sequence1, qual1, sequence2, qual2`) `align_sharded` binds. Used for the
    zero-masked-block no-op (see `_resolve_staged_masked_reads_block`): the data
    plane writes no file for an empty selection, so the align step still needs a
    valid empty input file to align over (emitting an empty alignment.parquet).
    pyarrow (already this module's Flight dependency) writes it directly."""
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415

    table = pa.table(
        {
            "prep_sample_idx": pa.array([], type=pa.int64()),
            "sequence_idx": pa.array([], type=pa.int64()),
            "read_id": pa.array([], type=pa.string()),
            "sequence1": pa.array([], type=pa.string()),
            "qual1": pa.array([], type=pa.string()),
            "sequence2": pa.array([], type=pa.string()),
            "qual2": pa.array([], type=pa.string()),
        }
    )
    pq.write_table(table, str(dest))


async def _resolve_staged_masked_reads_block(
    members: list[dict[str, int]],
    *,
    mask_idx: int,
    data_plane_url: str,
    signing_key: bytes,
    workspace: Path,
) -> dict[str, Path]:
    """Bind `reads` to a BLOCK's MASKED reads for an align workflow — the
    host-depleted, QC-passed twin of `_resolve_staged_reads_block`.

    Same block-member shape and per-ticket `reads.parquet` contract as the raw
    path, but the data plane sources the `read_masked` VIEW (trimmed +
    `pass`-filtered) scoped to `mask_idx` via the `export_read_masked_block`
    DoAction, so `align_sharded` aligns exactly the reads that survived the
    host-depletion mask. The output column shape is identical to
    `export_read_block`, so the `align_sharded.reads` contract is unchanged.

    `mask_idx` is the ticket's pre-resolved (plan-time) mask — the SAME
    completed mask the block's samples were masked under. Fails
    SUBMISSION/BAD_INPUT (so the ticket FAILs cleanly, step_name=None) if:
    `members` is empty (a planning bug); the data plane is unreachable; or the
    data plane reported reads but no file landed. **Unlike the raw path, zero
    selected reads is NOT a failure** — a completed mask can legitimately carry 0
    passing reads (a blank/control or fully host/QC-filtered sample the planner
    still tiles), so an empty selection binds an empty (schema-correct)
    reads.parquet and the block runs to a clean no-op completion (empty
    alignment.parquet → register 0 rows → gate flip)."""
    if not members:
        raise _submission_bad_input("an align block requires a non-empty members list")
    workspace.mkdir(parents=True, exist_ok=True)
    dest = workspace / "reads.parquet"
    try:
        member_payload = [
            {
                "prep_sample_idx": int(m["prep_sample_idx"]),
                "sequence_idx_start": int(m["sequence_idx_start"]),
                "sequence_idx_stop": int(m["sequence_idx_stop"]),
            }
            for m in members
        ]
    except (KeyError, TypeError, ValueError) as exc:
        raise _submission_bad_input(
            f"malformed align block member (a planning bug): {type(exc).__name__}: {exc}"
        ) from exc
    token = sign_action(
        action="export_read_masked_block",
        payload={"dest": str(dest), "mask_idx": int(mask_idx), "members": member_payload},
        secret=signing_key,
    )
    try:
        result = await asyncio.get_running_loop().run_in_executor(
            None, _runner_pkg._do_action_export_read_masked_block, data_plane_url, token
        )
    except Exception as exc:
        raise _submission_dp_fetch_failure(
            f"could not materialize masked reads for the block from the data plane: "
            f"{type(exc).__name__}: {exc}",
            exc,
        ) from exc

    if result.get("count", 0) == 0:
        # Zero passing masked reads is a LEGITIMATE no-op, NOT a failure — unlike
        # the raw `_resolve_staged_reads_block` path, where zero reads is a planning
        # bug. A block ticket is only fanned out over samples whose mask_sample gate
        # is `completed`, and a completed mask can carry 0 passing reads (a
        # blank/no-template control, or a fully host/QC-filtered sample); a block's
        # tail sub-range can also be entirely masked out. Failing here would
        # permanently wedge the ticket (the count is 0 on every redrive) and strand
        # the sample's alignment_sample gate at `pending` forever. Instead, bind an
        # empty (schema-correct) reads.parquet: `align_sharded` emits an empty
        # alignment.parquet (it guards the empty-read_to_shard case miint rejects),
        # register-files registers 0 rows, and reconcile flips the gate with no
        # rows (it has no count-assertion). The data plane writes NO file for an
        # empty selection, so materialize the empty file here.
        _write_empty_reads_parquet(dest)
        return {STAGED_READS_BINDING: dest}
    if not dest.exists():
        raise _submission_bad_input(
            f"the data plane reported masked reads for the block but wrote no file at {dest}"
        )
    return {STAGED_READS_BINDING: dest}
