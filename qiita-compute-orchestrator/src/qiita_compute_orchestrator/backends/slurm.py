"""SLURM compute backend.

Submits each workflow step as a SLURM job via slurmrestd, reads its
status, runs the container-output verifier, and returns the parsed
`outputs` map. The four pure pieces in `qiita_compute_orchestrator.slurm`
(payload, verify, client, plus the ack and `parse_outputs_map` helper)
carry the implementation; this module is the wiring.

The work is split across the decoupled `submit_step` / `status_step` /
`result_step` so the control-plane runner can submit, poll, and finalize
without holding a connection open for the duration of the SLURM job. The
runner owns the poll loop; `status_step` is a single (non-looping) read.
`find_jobs_by_name` lets the runner adopt a job whose id it never
persisted (the write-ahead idempotency gap).

State => BackendFailure mapping lives here (rather than in the
slurmrestd client) because the workflow-level context — step name,
SUBMISSION vs STEP_RUN classification — is only meaningful here.
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path

from qiita_common.actions import BaselineResources
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData
from qiita_common.duckdb_miint import miint_job_env
from qiita_common.log_tail import contains_oom_signature, read_text_tail
from qiita_common.models import (
    StepBaselineResources,
    StepStatus,
    WorkTicketFailureStage,
)

from ..backend import (
    ComputeBackend,
    FoundJob,
    SlurmStepHandle,
    StepHandle,
    StepStatusInfo,
    assert_container_scope_supported,
)
from ..slurm import (
    SlurmJobInfo,
    SlurmrestdClient,
    SlurmrestdError,
    TerminalSlurmState,
    build_job_submit_payload,
    parse_launcher_failure,
    parse_launcher_no_data,
    parse_outputs_map,
    verify_container_output,
)
from ..slurm.contract import JOB_PARAMS_FILENAME, JobParams

_log = logging.getLogger(__name__)

# SLURM's job_state strings => FailureKind.
#
# SLURM has more terminal states than retry classes (e.g. BOOT_FAIL and
# NODE_FAIL both reduce to "infra failed, retry"). The mapping here is
# deliberately conservative: anything plausibly transient maps to a
# retriable kind; anything that indicates the workflow itself misbehaved
# (FAILED with non-zero exit, CANCELLED, DEADLINE) maps to permanent.
#
# TIMEOUT is treated as retriable: SLURM marks a job TIMEOUT when it
# exceeds walltime, which usually indicates a too-tight allocation —
# a retry on a less-loaded node may finish in time, and operators can
# raise the action's walltime ceiling if the pattern persists.
_STATE_TO_FAILURE_KIND: dict[TerminalSlurmState, FailureKind] = {
    TerminalSlurmState.FAILED: FailureKind.EXIT_NONZERO,
    TerminalSlurmState.CANCELLED: FailureKind.EXIT_NONZERO,
    TerminalSlurmState.DEADLINE: FailureKind.EXIT_NONZERO,
    TerminalSlurmState.SPECIAL_EXIT: FailureKind.EXIT_NONZERO,
    TerminalSlurmState.NODE_FAIL: FailureKind.NODE_FAIL,
    TerminalSlurmState.BOOT_FAIL: FailureKind.NODE_FAIL,
    TerminalSlurmState.OUT_OF_MEMORY: FailureKind.OOM_KILLED,
    TerminalSlurmState.PREEMPTED: FailureKind.PREEMPTED,
    TerminalSlurmState.TIMEOUT: FailureKind.TIMEOUT_BEFORE_START,
}

# Generic failure kinds an OOM stderr signature is allowed to upgrade to
# OOM_KILLED. A cgroup *step-level* oom_kill surfaces only as a coarse
# job-level FAILED/exit_code=1 (=> EXIT_NONZERO), so without this the OOM is
# invisible from `qiita ticket status`. We never reclassify a specific infra
# kind (NODE_FAIL / TIMEOUT / PREEMPTED) — those are already correct from the
# SLURM state and must not be downgraded.
_OOM_UPGRADABLE_KINDS: frozenset[FailureKind] = frozenset(
    {FailureKind.EXIT_NONZERO, FailureKind.UNKNOWN_PERMANENT}
)

# Bounds for the stderr tail folded into `failure_reason` on a state-based
# (no launcher-line) failure. Deliberately small — `failure_reason` is
# persisted on qiita.work_ticket_step / qiita.work_ticket and rendered by
# `qiita ticket status`; the fuller tail is available via `qiita ticket logs`.
_FAILURE_REASON_TAIL_LINES = 15
_FAILURE_REASON_TAIL_BYTES = 2048


class SlurmBackend(ComputeBackend):
    """Submits compute jobs to SLURM via slurmrestd. Each `submit_step`
    submits one SLURM job; map / reduce fan-out (one SLURM job per
    `prep_sample_idx`) is not supported yet — the backend handles a single
    SLURM job per step.

    See docs/architecture.md "Backend code-sharing" for the
    canonical-implementation contract: the SLURM container's entrypoint
    must execute the same DuckDB+miint logic that `LocalBackend`'s
    in-process helpers run, so dev / CI and production stay in sync.
    """

    def __init__(
        self,
        *,
        client: SlurmrestdClient,
        partition: str,
        account: str,
        native_python: str = "python",
        co_to_cp_token: str = "",
        cp_url: str = "",
        qos: str = "",
        path_derived_images: Path | None = None,
        path_scratch: str = "",
        path_derived: str = "",
        data_plane_url: str = "",
    ) -> None:
        self._client = client
        self._partition = partition
        self._account = account
        self._native_python = native_python
        # Shared-FS dir where built SIFs live (PATH_DERIVED/images). When
        # set, bare `container:` filenames in workflow YAML resolve as
        # `path_derived_images / filename` at submit time. When None,
        # container steps are rejected — see _resolve_container_image.
        # Production deploys with COMPUTE_BACKEND=slurm fail-fast at
        # Settings.from_env() if PATH_DERIVED is missing or invalid.
        self._path_derived_images = path_derived_images
        # CO→CP token + CP URL are propagated into the SLURM job env so
        # the native-step launcher running on a compute node can resolve
        # Settings.from_env() without reading /etc/qiita/*.token (which
        # is deploy-host-local). They're empty in unit tests that don't
        # care about the propagation; production wires real values in
        # main._build_backend(). The token lands in `scontrol show job`
        # — visible to cluster admins.
        #
        # The CP→CO *inbound* shared bearer is NOT propagated: the
        # launcher never serves the /step/* routes, so `get_settings()` on
        # the compute node falls back to Settings.from_env(
        # require_cp_to_co_token=False) and skips it. That keeps the
        # SLURM-env exposure surface to just the outbound PAT.
        self._co_to_cp_token = co_to_cp_token
        self._cp_url = cp_url
        # Shared scratch base (PATH_SCRATCH), propagated into the SLURM job
        # env for the same reason as cp_url: /etc/qiita is deploy-host-local,
        # invisible from compute nodes, so the native-step launcher's
        # `get_settings()` would otherwise fall back to the `$TMPDIR/qiita`
        # DEFAULT for `path_scratch`. PATH_SCRATCH is the per-ticket workspace
        # base; persistent index artifacts now derive from PATH_DERIVED instead
        # (see `_path_derived` below — `build_rype_index` writes the `.ryxdi`
        # under `{path_derived}/references/{idx}/rype/`). PATH_SCRATCH is still
        # propagated so any job resolving the scratch base sees the real shared
        # value, not node-local /tmp. Jobs that only write QIITA_OUTPUT_PATH
        # (the workspace) don't care.
        self._path_scratch = path_scratch
        # Derived-artifact root (PATH_DERIVED), propagated for the same reason
        # as PATH_SCRATCH: native index builders (build_rype_index,
        # build_minimap2_index) write `{path_derived}/references/{idx}/...`, so
        # the launcher's get_settings() on the compute node needs the real
        # value, not the $TMPDIR/qiita/derived dev fallback.
        self._path_derived = path_derived
        # Data-plane gRPC origin, propagated for the same reason as PATH_DERIVED:
        # a native job that streams reference chunks (Flight DoGet) resolves it
        # via the launcher's get_settings() on the compute node, so it needs the
        # real nginx-fronted origin, not the localhost dev fallback. Empty in unit
        # tests that don't exercise streaming; production wires it in
        # main._build_backend().
        self._data_plane_url = data_plane_url
        # Optional SLURM QOS to set on submit; empty string means "let
        # SLURM apply the submitting user's default QOS" (the orchestrator
        # doesn't override).
        self._qos = qos

    async def aclose(self) -> None:
        """Close the underlying httpx client so asyncio doesn't warn
        about an unclosed transport on shutdown."""
        await self._client.close()

    def _resolve_container_image(self, container: str, *, step_name: str) -> str:
        """Translate a YAML `container:` value into an apptainer-runnable
        argument.

        Three accepted shapes:

        * Registry URL (contains ``://``) — passed through verbatim
          (e.g. ``oras://`` / ``docker://``). Escape hatch for the rare
          case a workflow doesn't ship a bundled SIF.
        * Bare SIF filename (no path separators) — joined with
          ``self._path_derived_images`` to produce an absolute path.
        * Anything else — rejected as CONTRACT_VIOLATION. A path
          separator inside a non-URL value indicates the YAML author
          tried to override the deploy-time image tier, which we don't
          allow (production wants every SIF under the shared
          ``PATH_DERIVED/images``).

        The fail-fast pair: ``Settings.from_env()`` catches missing /
        invalid ``PATH_DERIVED`` at boot; this submit-time check
        catches malformed ``container:`` strings.
        """
        if "://" in container:
            return container
        if Path(container).is_absolute() or "/" in container:
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=step_name,
                reason=(
                    f"container value {container!r} must be either a bare SIF"
                    " filename (no path separators) or a registry URL containing"
                    " '://'; the deploy-time PATH_DERIVED/images supplies the path"
                ),
            )
        if self._path_derived_images is None:
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=step_name,
                reason=(
                    f"container step {step_name!r} requires PATH_DERIVED/images"
                    " to be configured on the orchestrator (bare SIF filenames"
                    " resolve against it); not set"
                ),
            )
        return str(self._path_derived_images / container)

    def _resolve_input_binds(self, inputs: dict[str, Path], *, step_name: str) -> list[Path]:
        """Compute the additional ``--bind`` directories a container step
        needs so apptainer can resolve each YAML-declared input path.

        For each input path:
          * If it's a directory, bind the directory itself.
          * If it's a file, bind its parent directory (apptainer's
            ``--bind`` is directory-granular).

        Paths are resolved to absolute form (``Path.resolve()``) and
        deduplicated, so two inputs that share a parent emit one bind.
        Relative paths are rejected at submit time as
        CONTRACT_VIOLATION — the runner has already resolved every input
        to an absolute host path via ``bound``, so a relative path here
        indicates a programming error upstream of submit.

        Path existence is NOT checked here: the orchestrator's filesystem
        view can legitimately differ from the compute nodes' (NFS that
        mounts only on compute, for example). A truly missing path will
        fail loudly inside apptainer when ``--bind`` evaluates against a
        non-existent host directory; the runner attributes that to
        STEP_RUN with the apptainer error in the SLURM stderr.
        """
        bind_dirs: set[Path] = set()
        for input_name, path in inputs.items():
            if not path.is_absolute():
                raise BackendFailure(
                    kind=FailureKind.CONTRACT_VIOLATION,
                    stage=WorkTicketFailureStage.STEP_RUN,
                    step_name=step_name,
                    reason=(
                        f"container input {input_name!r} must be an absolute"
                        f" host path; got {path!r}"
                    ),
                )
            # Don't resolve symlinks (Path.resolve() does) — we want to
            # bind the path as the workflow author wrote it. is_dir() is
            # safe on non-existent paths (returns False), which lets the
            # CONTRACT_VIOLATION check above stay as the only existence-
            # related guard.
            bind_dirs.add(path if path.is_dir() else path.parent)
        # Sorted output makes payload tests deterministic.
        return sorted(bind_dirs)

    async def submit_step(
        self,
        name: str,
        inputs: dict[str, Path],
        workspace: Path,
        *,
        scope_target: dict,
        work_ticket_idx: int,
        attempt: int = 0,
        container: str | None = None,
        module: str | None = None,
        entrypoint: str | None = None,
        baseline_resources: StepBaselineResources | None = None,
    ) -> StepHandle:
        """Lay out the workspace tree + params.json, submit the SLURM job,
        and return a StepHandle — without polling. Submission errors are
        classified into a retriable / permanent BackendFailure."""
        if (container is None) == (module is None):
            # Both None (neither runtime declared) and both set (ambiguous
            # runtime) are contract violations. The wire validator on
            # StepSubmitRequest catches this upstream; this guard protects
            # direct callers (tests, programmatic submission) and keeps
            # the failure shape identical for either flavor.
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=name,
                reason="SlurmBackend requires exactly one of `container` or `module` on the step",
            )
        if baseline_resources is None:
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=name,
                reason="SlurmBackend requires `baseline_resources` from the YAML step",
            )

        # Container-path scope-kind gate. Native steps
        # (`module is not None`) flow scope_target through
        # flatten_native_inputs and can target any scope kind, so they
        # bypass this gate; container steps today (hash, load) all
        # extract reference_idx from scope_target, so a non-reference
        # scope would either KeyError reading params.json inside the
        # container or silently produce garbage. Surface as
        # CONTRACT_VIOLATION at submit time instead. The shared helper
        # keeps SlurmBackend and LocalBackend in lockstep on both the
        # predicate and the error wording.
        if container is not None:
            assert_container_scope_supported(step_name=name, scope_target=scope_target)
            container = self._resolve_container_image(container, step_name=name)

        # Lay out the workspace tree the container reads/writes:
        #   <workspace>/input/   contains params.json (mounted as $QIITA_INPUT_PATH)
        #   <workspace>/output/  receives manifest.json + outputs (mounted as $QIITA_OUTPUT_PATH)
        #   <workspace>/logs/    SLURM stdout / stderr land here
        input_path = workspace / "input"
        output_path = workspace / "output"
        logs_path = workspace / "logs"
        for d in (input_path, output_path, logs_path):
            d.mkdir(parents=True, exist_ok=True)
        # params.json is the channel for workflow-specific data — never the
        # slurmrestd submit body, which is visible in `scontrol show job`
        # and SLURM accounting (no place for signed Flight tickets or
        # per-step parameters). The container reads it from
        # $QIITA_INPUT_PATH. `scope_target` is the work ticket's full
        # tagged-union scope (matches qiita_common.models.ScopeTarget);
        # the native-step launcher reads scope_target["kind"] to pick
        # the right idx scalars to merge into the job's Inputs model,
        # and container entrypoints inspect it for the same purpose.
        # The Pydantic shape lives in slurm/contract.py so the producer
        # here and the consumer (jobs/__main__.py) validate against the
        # same schema.
        params_path = input_path / JOB_PARAMS_FILENAME
        # Pretty-print so a human debugging a job's input dir can read it;
        # the consumer (jobs/__main__.py) parses with model_validate_json,
        # which is whitespace-insensitive.
        params_path.write_text(
            JobParams(
                step_name=name,
                scope_target=scope_target,
                work_ticket_idx=work_ticket_idx,
                inputs={k: str(v) for k, v in inputs.items()},
                output_path=str(output_path),
            ).model_dump_json(indent=2)
            + "\n"
        )

        baseline = BaselineResources(
            cpu=baseline_resources.cpu,
            mem_gb=baseline_resources.mem_gb,
            walltime=timedelta(seconds=baseline_resources.walltime_seconds),
            gpu=baseline_resources.gpu,
        )

        # Compose the SLURM job's extra env. The native-step launcher
        # on the compute node calls `get_settings()`, which falls back
        # to `Settings.from_env(require_cp_to_co_token=False)` — so it
        # needs only the *outbound* CO→CP token + QIITA_CP_URL, never
        # the inbound CP→CO shared bearer. Files under /etc/qiita/ are
        # deploy-host-local and not visible from compute nodes; we
        # propagate the resolved value via env and flip
        # QIITA_ALLOW_TOKEN_ENV so the launcher accepts it. Empty values
        # mean "don't propagate" (unit tests that don't exercise the
        # launcher path leave them empty); production wires real values
        # in main._build_backend().
        extra_env: dict[str, str] = {}
        if self._co_to_cp_token:
            extra_env["CO_TO_CP_TOKEN"] = self._co_to_cp_token
            extra_env["QIITA_ALLOW_TOKEN_ENV"] = "true"
        if self._cp_url:
            extra_env["QIITA_CP_URL"] = self._cp_url
        if self._path_scratch:
            extra_env["PATH_SCRATCH"] = self._path_scratch
        if self._path_derived:
            extra_env["PATH_DERIVED"] = self._path_derived
        if self._data_plane_url:
            extra_env["DATA_PLANE_URL"] = self._data_plane_url
        # Native jobs LOAD miint from the deploy-staged MIINT_EXTENSION_DIRECTORY
        # (open_miint_conn); the compute node sees it only if we propagate it.
        # Single-sourced with the compute-readiness probe via miint_job_env() so
        # the diagnostic and the real jobs can't drift.
        extra_env.update(miint_job_env())

        # For container steps, expose the parent directory of every
        # YAML-declared input path so the entrypoint can read it via
        # apptainer's host-mounted view. Native steps don't need extra
        # binds — the launcher runs outside any container.
        extra_bind_dirs: list[Path] | None = None
        if container is not None:
            extra_bind_dirs = self._resolve_input_binds(inputs, step_name=name)

        payload = build_job_submit_payload(
            step_name=name,
            work_ticket_idx=work_ticket_idx,
            container=container,
            module=module,
            entrypoint=entrypoint,
            baseline_resources=baseline,
            input_path=input_path,
            output_path=output_path,
            workspace=workspace,
            log_stdout=logs_path / "stdout",
            log_stderr=logs_path / "stderr",
            partition=self._partition,
            account=self._account,
            native_python=self._native_python,
            attempt=attempt,
            extra_env=extra_env or None,
            extra_bind_dirs=extra_bind_dirs,
            qos=self._qos,
        )

        try:
            job_id = await self._client.submit_job(payload)
        except SlurmrestdError as exc:
            raise self._classify_submit_error(exc, name) from exc

        return SlurmStepHandle(
            step_name=name,
            slurm_job_id=job_id,
            job_name=payload["job"]["name"],
            output_path=output_path,
            logs_path=logs_path,
        )

    async def status_step(self, handle: StepHandle) -> StepStatusInfo:
        """Single slurmrestd read => coarse StepStatus. The control-plane
        runner owns the poll loop and the timeout; this never blocks.

        slurmrestd errors are classified into a typed BackendFailure so the
        caller sees the same surface as submit/result: transport / 5xx / 401
        => retriable SLURMRESTD_UNREACHABLE (the runner keeps polling); other
        4xx (e.g. 404 purged) => UNKNOWN_PERMANENT (status unknowable; Phase 5
        recovery uses the filesystem tiebreaker)."""
        self._require_slurm_handle(handle)
        try:
            info = await self._client.get_job(handle.slurm_job_id)
        except SlurmrestdError as exc:
            raise self._classify_status_error(exc, handle.step_name) from exc
        return self._status_info_from_job(info)

    async def result_step(self, handle: StepHandle, status: StepStatusInfo) -> dict[str, Path]:
        """Finalize a terminal SLURM step: verify + parse outputs on
        success, or raise the classified BackendFailure on failure. The
        single home for the post-poll terminal logic, called by the runner
        once `status_step` reports the job terminal."""
        self._require_slurm_handle(handle)
        if status.status == StepStatus.COMPLETED:
            failures = verify_container_output(handle.output_path)
            if failures:
                # Container exited 0 but didn't honor the contract — gate
                # 2/3/4 violation. Permanent: same container against same
                # output dir will produce the same broken manifest.
                detail = "; ".join(
                    f"{f.reason}" + (f" ({f.detail})" if f.detail else "") for f in failures[:5]
                )
                raise BackendFailure(
                    kind=FailureKind.CONTRACT_VIOLATION,
                    stage=WorkTicketFailureStage.STEP_RUN,
                    step_name=handle.step_name,
                    reason=f"container output failed verification: {detail}",
                )
            try:
                return parse_outputs_map(handle.output_path)
            except (OSError, json.JSONDecodeError, KeyError) as exc:
                # Verifier already validated the manifest, so we don't
                # expect to land here in practice — but if we do, treat
                # it as a contract violation (the verifier's contract
                # drifted from parse_outputs_map's expectations).
                raise BackendFailure(
                    kind=FailureKind.CONTRACT_VIOLATION,
                    stage=WorkTicketFailureStage.STEP_RUN,
                    step_name=handle.step_name,
                    reason=f"could not parse manifest.json outputs: {exc!s}",
                ) from exc

        if status.status != StepStatus.FAILED:
            # A non-terminal status reached result_step — a caller bug:
            # the runner must poll until terminal before finalizing.
            raise BackendFailure(
                kind=FailureKind.UNKNOWN_PERMANENT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=handle.step_name,
                reason=f"result_step called on non-terminal status {status.status.value!r}",
            )

        # Terminal-but-not-success: map state => FailureKind.
        kind = _STATE_TO_FAILURE_KIND.get(status.raw_state, FailureKind.UNKNOWN_PERMANENT)
        reason_parts = [f"SLURM job {handle.slurm_job_id} ended in state {status.raw_state!r}"]
        if status.exit_code is not None:
            reason_parts.append(f"exit_code={status.exit_code}")
        if status.reason:
            reason_parts.append(f"slurm_reason={status.reason}")
        state_reason = ", ".join(reason_parts)

        # A native-step job that hit a terminal no-data outcome (an empty FASTQ
        # well) writes a structured no-data line to stderr and exits non-zero
        # (so SLURM marks it FAILED), then exits without a manifest. Parse that
        # line FIRST: a no-data outcome is NOT a failure, so raise StepNoData —
        # the step route serializes it with the no-data header and the runner
        # transitions the ticket to NO_DATA, never FAILED. Only if no no-data
        # line is present do we fall through to failure classification.
        no_data = parse_launcher_no_data(handle.logs_path / "stderr")
        if no_data is not None:
            raise StepNoData(step_name=no_data.step_name, reason=no_data.reason)

        # Native-step jobs write a structured failure line to stderr
        # before exit (jobs/__main__.py). If we find one, prefer the
        # launcher's classification + message — it carries the actual
        # FailureKind / reason from the Python side, which is strictly
        # more useful than the slurmrestd-state inference. Container
        # steps and infra-killed jobs (NODE_FAIL, OOM, ...) won't have
        # the line; in those cases parse_launcher_failure returns None
        # and the state-based classification stands.
        launcher_failure = parse_launcher_failure(handle.logs_path / "stderr")
        if launcher_failure is not None:
            raise BackendFailure(
                kind=launcher_failure.kind,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=launcher_failure.step_name,
                # Combine the launcher's reason with the SLURM-side
                # detail: the operator gets the application-level
                # message AND the SLURM job context (job id, exit code).
                reason=f"{launcher_failure.reason} [{state_reason}]",
            )

        # No structured launcher line (a container step, or the process was
        # infra-killed — including by OOM — before it could write one). Read
        # the stderr tail: a step-level cgroup oom_kill surfaces only as the
        # coarse FAILED/exit_code=1 above, so the stderr text is the one
        # in-band signal that an otherwise-opaque failure was a memory kill.
        # Fold the tail into failure_reason either way so `qiita ticket status`
        # carries the real error without a host shell.
        stderr_tail, _ = read_text_tail(
            handle.logs_path / "stderr",
            max_lines=_FAILURE_REASON_TAIL_LINES,
            max_bytes=_FAILURE_REASON_TAIL_BYTES,
        )
        if kind in _OOM_UPGRADABLE_KINDS and contains_oom_signature(stderr_tail):
            kind = FailureKind.OOM_KILLED
        reason = f"{state_reason}; stderr tail: {stderr_tail}" if stderr_tail else state_reason
        raise BackendFailure(
            kind=kind,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=handle.step_name,
            reason=reason,
        )

    async def find_jobs_by_name(self, job_name: str) -> list[FoundJob]:
        """Look up live SLURM jobs by their deterministic name (the CP
        idempotency / recovery path). slurmrestd errors classify exactly like
        `status_step` — transport / 5xx / 401 => retriable
        SLURMRESTD_UNREACHABLE (recovery retries); other 4xx =>
        UNKNOWN_PERMANENT (job list unreadable). A matched job carrying no id
        is skipped — it can't be adopted by id."""
        try:
            infos = await self._client.find_jobs_by_name(job_name)
        except SlurmrestdError as exc:
            raise self._classify_status_error(exc, job_name) from exc
        return [
            FoundJob(
                slurm_job_id=info.job_id,
                job_name=info.name or job_name,
                status=self._status_info_from_job(info),
            )
            for info in infos
            if info.job_id is not None
        ]

    @staticmethod
    def _require_slurm_handle(handle: StepHandle) -> None:
        """Guard: status_step / result_step operate only on a SLURM handle
        (one carrying a job id and the workspace paths). A handle from a
        different backend reaching here is a caller bug — fail loudly with
        a typed BackendFailure rather than dereferencing a missing field into
        an opaque AttributeError. `SlurmStepHandle`'s required job-id / paths
        make the fields non-None once this passes, so the callers below can
        dereference them directly."""
        if not isinstance(handle, SlurmStepHandle):
            raise BackendFailure(
                kind=FailureKind.UNKNOWN_PERMANENT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=handle.step_name,
                reason=(
                    "SlurmBackend status_step/result_step require a SLURM handle"
                    f" (job id + workspace paths); got {handle!r}"
                ),
            )

    @staticmethod
    def _status_info_from_job(info: SlurmJobInfo) -> StepStatusInfo:
        """Classify a slurmrestd snapshot into the coarse StepStatus.
        COMPLETED requires a clean (zero / unset) exit code; everything
        else terminal is FAILED. PENDING stays PENDING; any other
        non-terminal state (CONFIGURING, COMPLETING, SUSPENDED, ...) reads
        as RUNNING for the summary's purposes."""
        if info.is_terminal:
            if info.state == TerminalSlurmState.COMPLETED and (
                info.exit_code is None or info.exit_code == 0
            ):
                status = StepStatus.COMPLETED
            else:
                status = StepStatus.FAILED
        elif info.state == "PENDING":
            status = StepStatus.PENDING
        else:
            status = StepStatus.RUNNING
        return StepStatusInfo(
            status=status,
            raw_state=info.state,
            exit_code=info.exit_code,
            reason=info.reason,
        )

    def _classify_submit_error(self, exc: SlurmrestdError, step_name: str) -> BackendFailure:
        """Map a slurmrestd error from job/submit to a BackendFailure.

        - 5xx / transport (status_code is None) == SLURMRESTD_UNREACHABLE
          (retriable — slurmctld restart, transient network).
        - 401 == SLURMRESTD_UNREACHABLE (retriable — operator-fixable).
          The slurmrestd client already retried after refreshing the JWT
          before raising, so a 401 here means the rotation pipeline is
          broken (token unreadable, wrong principal, expired and the
          rotation script hasn't run). That's an ops issue, not a
          workflow contract violation; classify retriable so the runner
          gives ops a window to fix the token before the ticket fails.
        - Other 4xx == CONTRACT_VIOLATION (permanent — bad payload won't
          be fixed by retry).
        """
        if exc.status_code is None or exc.status_code >= 500 or exc.status_code == 401:
            kind = FailureKind.SLURMRESTD_UNREACHABLE
        else:
            kind = FailureKind.CONTRACT_VIOLATION
        # Log the EXACT status + body before they're flattened into the
        # BackendFailure reason. A recurring "unreachable" submit failure
        # was ambiguous in the logs (was it a 401, a 5xx, a transport drop?);
        # this records which, so the next stuck-on-submit incident is
        # diagnosable without a repro.
        _log.warning(
            "slurmrestd submit failed for step %r → %s (status_code=%s, url=%s): %s",
            step_name,
            kind.value,
            exc.status_code,
            exc.url,
            (exc.body or str(exc))[:500],
        )
        return BackendFailure(
            kind=kind,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=step_name,
            reason=f"slurmrestd submit failed: {exc!s}",
        )

    def _classify_status_error(self, exc: SlurmrestdError, step_name: str) -> BackendFailure:
        """Map a slurmrestd get_job / job-list error to a BackendFailure. A
        4xx other than 401 means the job state is unknowable (permanent —
        purged job; the runner's filesystem tiebreaker then decides from the
        on-disk manifest); 5xx / transport / 401 are transiently unreachable
        (the runner retries). 401 is retriable, consistent with
        `_classify_submit_error`: a 401 surviving the client's JWT-refresh
        retry is a broken rotation pipeline (operator-fixable), not a terminal
        step outcome.

        Note for the runner: a BackendFailure out of `status_step` (or
        `find_jobs_by_name`) always means "could not read status" (transport /
        infra), never "the step failed" — retry on a transient kind rather
        than recording a terminal step failure."""
        if exc.status_code is not None and 400 <= exc.status_code < 500 and exc.status_code != 401:
            kind = FailureKind.UNKNOWN_PERMANENT
        else:
            kind = FailureKind.SLURMRESTD_UNREACHABLE
        return BackendFailure(
            kind=kind,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=step_name,
            reason=f"slurmrestd get_job failed: {exc!s}",
        )
