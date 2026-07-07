"""Runner read-ingest and staged-read bindings."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from qiita_common.api_paths import (
    compute_reads_staging_path,
)

import qiita_control_plane.runner as _runner_pkg

from ..auth.tickets import sign_action
from ._upload import _submission_bad_input

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
# The MASKED sibling of `reads`: a workflow that consumes ready-for-consumption
# reads (pacbio assembly) declares `masked_reads`, which the runner materializes
# from the `read_masked` view's pass-set for a mask_idx. A DISTINCT name from
# `reads` (raw) so the two never collide — read-mask workflows consume raw `reads`
# to CREATE a mask; assembly workflows consume `masked_reads`.
STAGED_MASKED_READS_BINDING = "masked_reads"
READS_STAGING_ROOT_BINDING = "reads_staging_root"


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
    """True iff `masked_reads` is consumed by some step but produced by none — so
    it must be bound externally from the sample's `read_masked` pass-set (the
    pacbio-processing assembly workflow)."""
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


def _do_action_export_read_masked(data_plane_url: str, token: bytes) -> dict[str, Any]:
    """`export_read_masked` DoAction: the data plane materializes ONE prep_sample's
    `read_masked` pass-set (for a given mask_idx) into a per-ticket Parquet.
    Isolated (thin wrapper over `_do_action_export`) so unit tests stub the real
    call by name."""
    return _do_action_export("export_read_masked", data_plane_url, token)


async def _resolve_staged_reads(
    scope_target: dict[str, Any],
    staging_root: Path,
    *,
    data_plane_url: str,
    hmac_secret: bytes,
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

    Fails SUBMISSION/BAD_INPUT (so the ticket FAILs cleanly) if the sample has no
    stored reads in either place — it must be ingested before a mask can be
    created over it — or if the data plane is unreachable."""
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
        secret=hmac_secret,
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
        raise _submission_bad_input(
            f"could not materialize reads for prep_sample {prep_sample_idx} from "
            f"the data plane: {type(exc).__name__}: {exc}"
        ) from exc

    # `count` is already an int (coerced in `_do_action_export_read`).
    if result.get("count", 0) == 0:
        # The persistent store has no reads for this sample either (the data plane
        # writes no file for an empty result) — same "must be ingested" semantics.
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
    scope_target: dict[str, Any],
    mask_idx: int,
    *,
    data_plane_url: str,
    hmac_secret: bytes,
    workspace: Path,
) -> dict[str, Path]:
    """Bind `masked_reads` to a prep_sample's MASKED read pass-set for a mask_idx —
    the analog of `_resolve_staged_reads` for a workflow that consumes
    ready-for-consumption reads (pacbio assembly) rather than raw reads.

    Unlike `_resolve_staged_reads` there is NO durable fast-path copy: masking is
    downstream state layered over the raw `read` table, so we always ask the data
    plane to materialize the `read_masked` view's pass-set (host/human/QC-failing
    rows excluded, recorded trims applied) for (mask_idx, prep_sample_idx) into a
    per-ticket `masked_reads.parquet` via the `export_read_masked` DoAction (the
    bulk read bytes never transit the control plane).

    Fails SUBMISSION/BAD_INPUT (so the ticket FAILs cleanly) if no reads pass the
    mask — nothing to assemble — or the data plane is unreachable."""
    prep_sample_idx = scope_target["prep_sample_idx"]

    workspace.mkdir(parents=True, exist_ok=True)
    dest = workspace / "masked_reads.parquet"
    token = sign_action(
        action="export_read_masked",
        payload={
            "mask_idx": mask_idx,
            "prep_sample_idx": prep_sample_idx,
            "dest": str(dest),
        },
        secret=hmac_secret,
    )
    # Flight failure -> SUBMISSION BAD_INPUT like the other pre-loop resolvers
    # (step_name=None), so the outer handler FAILs the ticket cleanly rather than
    # stranding it in PROCESSING.
    try:
        result = await asyncio.get_running_loop().run_in_executor(
            None, _runner_pkg._do_action_export_read_masked, data_plane_url, token
        )
    except Exception as exc:
        raise _submission_bad_input(
            f"could not materialize masked reads for prep_sample {prep_sample_idx} "
            f"(mask_idx {mask_idx}) from the data plane: {type(exc).__name__}: {exc}"
        ) from exc

    if result.get("count", 0) == 0:
        # No reads pass this mask for this sample (data plane writes no file for an
        # empty result) — there is nothing to assemble.
        raise _submission_bad_input(
            f"no reads pass mask_idx {mask_idx} for prep_sample {prep_sample_idx}; "
            "there is nothing to assemble (is the sample masked under this mask?)"
        )
    if not dest.exists():
        raise _submission_bad_input(
            f"the data plane reported masked reads for prep_sample {prep_sample_idx} "
            f"(mask_idx {mask_idx}) but wrote no file at {dest}"
        )
    return {STAGED_MASKED_READS_BINDING: dest}


async def _resolve_staged_reads_block(
    members: list[dict[str, int]],
    *,
    data_plane_url: str,
    hmac_secret: bytes,
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
        secret=hmac_secret,
    )
    # A Flight failure (data plane unreachable / errored) is NOT a BackendFailure;
    # wrap it as a SUBMISSION BAD_INPUT like the per-sample resolver so the outer
    # handler FAILs the ticket cleanly rather than stranding it in PROCESSING.
    try:
        result = await asyncio.get_running_loop().run_in_executor(
            None, _runner_pkg._do_action_export_read_block, data_plane_url, token
        )
    except Exception as exc:
        raise _submission_bad_input(
            f"could not materialize reads for the block from the data plane: "
            f"{type(exc).__name__}: {exc}"
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
