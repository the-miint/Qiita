"""Runner restart-recovery output reconstruction and action-primitive dispatch."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import asyncpg
from qiita_common.actions import (
    WorkflowAction,
    WorkflowStep,
)
from qiita_common.api_paths import (
    LibraryPrimitive,
)
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.compute_backend_client import ComputeBackendClient
from qiita_common.models import (
    ComputeTarget,
    ScopeTargetKind,
    StepHandleWire,
    StepProgressState,
    StepStatus,
    StepStatusWire,
)

from .. import step_progress
from ..actions.library import LIBRARY, MINT_FEATURES_OUTPUT_BASENAME
from ._dispatch import _best_effort_record_failed, _result_with_infra_retry
from ._mask import MASK_IDX_BINDING
from ._processing import PROCESSING_IDX_BINDING

# =============================================================================
# Restart-recovery output reconstruction
# =============================================================================
#
# On resume, an entry already marked COMPLETED in a prior run must NOT be
# re-run (an in-process action: is not idempotent) — its outputs are rebuilt
# from the shared workspace instead, then bound forward exactly as a fresh run
# would. The per-attempt workspace layout (`<workspace>/<name>/attempt-<N>/`)
# is deterministic, so the producer's attempt number — read from the progress
# row — is enough to find every output on disk.


def _completed_progress_row(
    progress: list[step_progress.StepProgressRow], step_index: int
) -> step_progress.StepProgressRow | None:
    """The COMPLETED row for `step_index` across any attempt, or None. A step
    that failed attempt 0 but completed attempt 1 counts as completed."""
    for row in progress:
        if row.step_index == step_index and row.state is StepProgressState.COMPLETED:
            return row
    return None


def _attempt_is_unowned(
    prior_progress: list[step_progress.StepProgressRow], *, step_index: int, attempt: int
) -> bool:
    """Whether this entry's `(step_index, attempt)` is unowned by a start-of-run
    progress row — i.e. the caller may treat any attempt dir on disk as orphaned.

    Keyed on the START-OF-RUN progress (the snapshot loaded once before the
    loop). A pre-existing row for this exact `(step_index, attempt)` means a
    prior process owns the dir and we're resuming/adopting it — `_adopt_or_submit`
    re-attaches to that row's job and must reuse its workspace, so it is NOT
    unowned (return False; leave the dir alone). No such row means the attempt is
    unowned: either a first dispatch (dir absent — the caller just mkdirs it) or a
    re-run whose row was deliberately dropped (a `/run` redrive clearing failed
    rows, or `update-lane` invalidating a completed prep row). In the re-run case
    the prior attempt left stale, read-only (0o440) output + manifest on disk that
    must not be reused; the caller advances to a fresh attempt dir rather than
    deleting it (the output is owned by the SLURM job user — the control plane
    can't unlink or chmod it)."""
    return not any(
        row.step_index == step_index and row.attempt == attempt for row in prior_progress
    )


async def _reconstruct_completed_outputs(
    entry: WorkflowStep | WorkflowAction,
    completed: step_progress.StepProgressRow,
    workspace: Path,
    backend_client: ComputeBackendClient,
    *,
    pool: asyncpg.Pool,
    work_ticket_idx: int,
    poll_interval_seconds: float,
) -> dict[str, Any]:
    """Rebuild the bound outputs of an already-COMPLETED entry from disk,
    without re-running it.

    A `step:` entry re-reads its verified output manifest through `result_step`
    (reconstructing a handle from the progress row's job id + the deterministic
    per-attempt `output`/`logs` dirs). This doubles as the filesystem
    tiebreaker for a now-purged job: a valid manifest yields the outputs; a
    missing / broken one raises CONTRACT_VIOLATION → the resumed workflow
    fails, as it should when a completed step's output has vanished from
    scratch.

    An `action:` entry rebuilds its deterministic output paths in-process (see
    `_reconstruct_action_outputs`) — the in-process primitive must not re-run.

    A non-SLURM (local) completed step has no on-disk manifest to re-read;
    recovery is a SLURM-backend concern (local steps are synchronous and don't
    survive a restart mid-flight), so this returns its outputs empty — a
    downstream consumer that needs a missing binding fails loudly via KeyError."""
    attempt_workspace = workspace / entry.name / f"attempt-{completed.attempt}"
    if isinstance(entry, WorkflowAction):
        return _reconstruct_action_outputs(entry, attempt_workspace)
    if completed.compute_target is not ComputeTarget.SLURM:
        return {}
    handle = StepHandleWire(
        compute_target=ComputeTarget.SLURM,
        step_name=entry.name,
        slurm_job_id=completed.slurm_job_id,
        job_name=completed.job_name,
        output_path=str(attempt_workspace / "output"),
        logs_path=str(attempt_workspace / "logs"),
    )
    status = StepStatusWire(status=StepStatus.COMPLETED, raw_state="RECOVERED")
    raw_outputs = await _result_with_infra_retry(
        backend_client,
        handle,
        status,
        pool=pool,
        work_ticket_idx=work_ticket_idx,
        poll_interval_seconds=poll_interval_seconds,
    )
    return {name: Path(raw_outputs[name]) for name in entry.outputs}


def _reconstruct_action_outputs(entry: WorkflowAction, attempt_workspace: Path) -> dict[str, Any]:
    """Deterministic output paths an `action:` primitive wrote, for resume.
    Only `mint-features` contributes a binding (the feature-map Parquet it
    wrote into its workspace); the other primitives produce no bound output.
    The basename is single-sourced from `mint_features` itself
    (`MINT_FEATURES_OUTPUT_BASENAME`) so this resume path can't drift from where
    the primitive actually writes the file."""
    if entry.name == LibraryPrimitive.MINT_FEATURES:
        return {entry.outputs[0]: attempt_workspace / MINT_FEATURES_OUTPUT_BASENAME}
    return {}


async def _dispatch_action(
    pool: asyncpg.Pool,
    entry: WorkflowAction,
    bound: dict[str, Any],
    workspace: Path,
    scope_target: dict[str, Any],
    *,
    work_ticket_idx: int,
    step_index: int,
    attempt: int,
    hmac_secret: bytes,
    data_plane_url: str,
) -> dict[str, Any]:
    """Run one in-process `action:` entry and record its progress.

    Action entries run on the control plane (no backend hop, no SLURM job),
    so they are recorded with `compute_target='control_plane'`. They go in
    the progress table alongside compute `step:` entries because correct
    multi-step restart recovery needs to know which entries already completed
    — an `action:` that succeeded must be skipped (and its outputs rebound)
    on resume, not re-run.

    A primitive raising (plain Python or BackendFailure) marks this attempt's
    progress row failed before the exception propagates to the retry / outer
    handler — which owns the work_ticket-level FAILED transition. The
    exception is re-raised unchanged so the outer handler classifies it
    exactly as before."""
    await step_progress.record_submitting(
        pool,
        work_ticket_idx=work_ticket_idx,
        step_index=step_index,
        attempt=attempt,
        step_name=entry.name,
        compute_target=ComputeTarget.CONTROL_PLANE,
    )
    try:
        outputs = await _run_action_primitive(
            pool,
            entry,
            bound,
            workspace,
            scope_target,
            work_ticket_idx=work_ticket_idx,
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
        )
    except BackendFailure as exc:
        await _best_effort_record_failed(
            pool,
            work_ticket_idx=work_ticket_idx,
            step_index=step_index,
            attempt=attempt,
            failure_kind=exc.kind.value,
            failure_reason=exc.reason[:2000],
        )
        raise
    except Exception as exc:
        # Plain Python from a LIBRARY primitive (untyped failure / bug). The
        # outer run_workflow handler classifies it UNKNOWN_PERMANENT; record
        # the same on the progress row, then re-raise unchanged.
        await _best_effort_record_failed(
            pool,
            work_ticket_idx=work_ticket_idx,
            step_index=step_index,
            attempt=attempt,
            failure_kind=FailureKind.UNKNOWN_PERMANENT.value,
            failure_reason=f"{type(exc).__name__}: {exc!s}"[:2000],
        )
        raise
    await step_progress.record_completed(
        pool, work_ticket_idx=work_ticket_idx, step_index=step_index, attempt=attempt
    )
    return outputs


async def _run_action_primitive(
    pool: asyncpg.Pool,
    entry: WorkflowAction,
    bound: dict[str, Any],
    workspace: Path,
    scope_target: dict[str, Any],
    *,
    work_ticket_idx: int,
    hmac_secret: bytes,
    data_plane_url: str,
) -> dict[str, Any]:
    """Translate a workflow `action:` entry into the matching LIBRARY call.
    Per-primitive logic lives here because each primitive has its own
    input/output shape — a generic dispatcher would just push the same
    `if name == ...` ladder somewhere else."""
    if entry.name == LibraryPrimitive.MINT_FEATURES:
        manifest_path = Path(bound[entry.inputs[0]])
        # `genome_map_path` is a workflow-context optional, not an entry
        # input — the YAML's mint-features `inputs:` stays single-valued.
        # Pulled directly from `bound` so a ticket whose action_context
        # carries it picks up genome-association writes for free.
        genome_map = bound.get("genome_map_path")
        feature_map_path, _, _ = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
            pool,
            manifest_path,
            workspace,
            genome_map_path=Path(genome_map) if genome_map else None,
        )
        # YAML declares one output (typically "feature_map"); bind it.
        return {entry.outputs[0]: feature_map_path}

    if entry.name == LibraryPrimitive.WRITE_MEMBERSHIP:
        feature_map_path = Path(bound[entry.inputs[0]])
        await LIBRARY[LibraryPrimitive.WRITE_MEMBERSHIP](
            pool, scope_target["reference_idx"], feature_map_path
        )
        return {}

    if entry.name == LibraryPrimitive.WRITE_ASSEMBLY_MEMBERSHIP:
        # Assembly analogue of write-membership: link this prep_sample's
        # assembly-run contigs to qiita.assembly_membership, tagged by
        # (kind, bin_id). Inputs are resolved by their fixed binding names — not
        # positionally — so a YAML reorder can't silently swap them. bin_map +
        # manifest come from assembly_hash; feature_map from mint-features.
        # prep_sample_idx from the scope target; processing_idx from `bound` (the
        # runner minted it before the step loop because assembly_load threads it
        # via params — mirrors how the reference dispatch reads reference_idx).
        if set(entry.inputs) != {"bin_map", "manifest", "feature_map"}:
            raise RuntimeError(
                "write-assembly-membership expects inputs "
                f"[bin_map, manifest, feature_map]; got {entry.inputs!r}"
            )
        await LIBRARY[LibraryPrimitive.WRITE_ASSEMBLY_MEMBERSHIP](
            pool,
            scope_target["prep_sample_idx"],
            bound[PROCESSING_IDX_BINDING],
            Path(bound["bin_map"]),
            Path(bound["manifest"]),
            Path(bound["feature_map"]),
        )
        return {}

    if entry.name == LibraryPrimitive.REGISTER_FILES:
        staging_dir = Path(bound[entry.inputs[0]])
        # Filename → DuckLake table mapping derived from the staging dir.
        # Convention:
        #   - Top-level `<table>.parquet` files register as the table
        #     named after the file's stem (single-file table).
        #   - Top-level subdirs containing `*.parquet` files register
        #     each part as the table named after the directory
        #     (multi-file table). The filename in `files` carries the
        #     subdir prefix relative to staging_dir; the data plane
        #     normalises to basename when placing each part in the
        #     permanent per-table directory.
        # The multi-file form exists for `reference_sequence_chunks` —
        # at GG2 scale a single-file sort+write of ~30 GB of chunk_data
        # OOMs DuckDB; reference_load batches it into part files
        # instead (jobs/reference_load.py:_write_reference_sequence_chunks).
        files: dict[str, str] = {}
        for entry_path in sorted(staging_dir.iterdir()):
            if entry_path.is_file() and entry_path.suffix == ".parquet":
                files[entry_path.name] = entry_path.stem
            elif entry_path.is_dir():
                for part in sorted(entry_path.glob("*.parquet")):
                    rel = part.relative_to(staging_dir).as_posix()
                    files[rel] = entry_path.name
        if not files:
            raise RuntimeError(
                f"register-files: staging_dir {staging_dir} contains no Parquet files"
            )
        await LIBRARY[LibraryPrimitive.REGISTER_FILES](
            staging_dir=str(staging_dir),
            files=files,
            work_ticket_idx=work_ticket_idx,
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
        )
        return {}

    if entry.name == LibraryPrimitive.REGISTER_INDEX:
        # Native step outputs are paths (StepResultResponse.outputs is
        # dict[str, str]), so an index builder can't hand back the build params
        # as a dict binding — it writes a small meta JSON and exposes its path
        # (e.g. `rype_index_meta`, `minimap2_index_meta`). The binding name is
        # the step's single declared input, NOT hardcoded: a host reference runs
        # two register-index steps (rype + minimap2), each pointing at its own
        # meta. Read it for index_type / fs_path / params (index_type comes from
        # the builder, not hardcoded here).
        if len(entry.inputs) != 1:
            raise RuntimeError(
                f"register-index expects exactly one input (the index meta); got {entry.inputs!r}"
            )
        meta_path = Path(bound[entry.inputs[0]])
        meta = json.loads(meta_path.read_text())
        await LIBRARY[LibraryPrimitive.REGISTER_INDEX](
            pool,
            reference_idx=scope_target["reference_idx"],
            index_type=meta["index_type"],
            fs_path=meta["fs_path"],
            params=meta["params"],
        )
        return {}

    if entry.name == LibraryPrimitive.PERSIST_READ_METRICS:
        # Persist the three per-stage read counts onto this prep_sample's
        # 1:1 sequenced_sample, derived from the `read_mask` Parquet (one row per
        # read, carrying the per-read mask `reason`). The single declared input is
        # the read_mask path host_filter emitted; the primitive computes the
        # both-mates `_r1r2` totals from the mask (raw/biological/quality_filtered
        # by reason). Resolved by its fixed binding name, not positionally.
        if entry.inputs != ["read_mask"]:
            raise RuntimeError(
                f"persist-read-metrics expects inputs [read_mask]; got {entry.inputs!r}"
            )
        await LIBRARY[LibraryPrimitive.PERSIST_READ_METRICS](
            pool,
            scope_target["prep_sample_idx"],
            Path(bound["read_mask"]),
        )
        return {}

    if entry.name == LibraryPrimitive.PERSIST_QC_REPORT:
        # Persist the two fastqc-equivalent QC reports onto this prep_sample's
        # 1:1 sequenced_sample. Each declared input is a Path to a qc_report.json
        # sidecar (the qc_report_raw / qc_report_filtered step outputs); we read
        # each verbatim and hand the parsed dicts to the primitive. Inputs are
        # resolved by their fixed binding names — not positionally — so a YAML
        # reorder can't silently swap raw/filtered.
        if set(entry.inputs) != {"raw_qc_report", "filtered_qc_report"}:
            raise RuntimeError(
                "persist-qc-report expects inputs "
                "[raw_qc_report, filtered_qc_report]; "
                f"got {entry.inputs!r}"
            )

        def _report(name: str) -> dict[str, Any]:
            return json.loads(Path(bound[name]).read_text())

        await LIBRARY[LibraryPrimitive.PERSIST_QC_REPORT](
            pool,
            scope_target["prep_sample_idx"],
            _report("raw_qc_report"),
            _report("filtered_qc_report"),
        )
        return {}

    if entry.name == LibraryPrimitive.DELETE_READ_MASK_BLOCK:
        # Idempotent block replace: delete this block's exact read_mask footprint
        # BEFORE register-files re-writes it, so a re-run (retry, or a resubmitted
        # block covering the same footprint) never double-counts. Exact by
        # construction (per-member OR), so a split sample's sibling-block rows
        # survive. No file inputs: block_idx from the scope target, mask_idx from
        # the ticket (runner-bound above for the block branch).
        if scope_target["kind"] != ScopeTargetKind.BLOCK.value:
            raise RuntimeError(
                f"delete-block-mask requires a block-scoped ticket; got {scope_target['kind']!r}"
            )
        await LIBRARY[LibraryPrimitive.DELETE_READ_MASK_BLOCK](
            pool,
            block_idx=scope_target["block_idx"],
            mask_idx=bound[MASK_IDX_BINDING],
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
        )
        return {}

    if entry.name == LibraryPrimitive.RECONCILE_BLOCK:
        # Terminal step of the bulk-block read-mask workflow: mark this block
        # completed, then finalize each covered sample whose last covering block
        # just completed (per-sample rollup + mask_sample gate flip). Reads the
        # mask counts from DuckLake (across all the sample's blocks), so it runs
        # AFTER register-files. No file inputs: block_idx from the scope target,
        # mask_idx from the ticket (runner-bound above for the block branch).
        if scope_target["kind"] != ScopeTargetKind.BLOCK.value:
            raise RuntimeError(
                f"reconcile-block requires a block-scoped ticket; got {scope_target['kind']!r}"
            )
        await LIBRARY[LibraryPrimitive.RECONCILE_BLOCK](
            pool,
            block_idx=scope_target["block_idx"],
            mask_idx=bound[MASK_IDX_BINDING],
            hmac_secret=hmac_secret,
            data_plane_url=data_plane_url,
        )
        return {}

    raise RuntimeError(f"runner has no adapter for action {entry.name!r}")
