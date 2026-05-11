"""SLURM compute backend.

Submits each workflow step as a SLURM job via slurmrestd, polls until
the job reaches a terminal state, runs the container-output verifier,
and returns the parsed `outputs` map. The four pure pieces in
`qiita_compute_orchestrator.slurm` (payload, verify, client, plus the
ack and `parse_outputs_map` helper) carry the implementation; this
module is the wiring.

State => BackendFailure mapping lives here (rather than in the
slurmrestd client) because the workflow-level context — step name,
SUBMISSION vs STEP_RUN classification — is only meaningful here.
"""

from __future__ import annotations

import asyncio
import json
import random
import time
from datetime import timedelta
from pathlib import Path

from qiita_common.actions import BaselineResources
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import StepBaselineResources, WorkTicketFailureStage

from ..backend import ComputeBackend
from ..slurm import (
    SlurmJobInfo,
    SlurmrestdClient,
    SlurmrestdError,
    TerminalSlurmState,
    build_job_submit_payload,
    parse_outputs_map,
    verify_container_output,
)

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


class SlurmBackend(ComputeBackend):
    """Submits compute jobs to SLURM via slurmrestd. Each call to
    `run_step` submits one SLURM job and waits for it to terminate;
    map / reduce fan-out (one SLURM job per `prep_sample_idx`) is not
    supported yet — the backend handles a single SLURM job per step.

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
        poll_interval_seconds: int,
        job_timeout_seconds: int,
    ) -> None:
        self._client = client
        self._partition = partition
        self._account = account
        self._poll_interval = poll_interval_seconds
        self._job_timeout = job_timeout_seconds

    async def run_step(
        self,
        name: str,
        inputs: dict[str, Path],
        workspace: Path,
        *,
        reference_idx: int,
        work_ticket_idx: int,
        container: str | None = None,
        entrypoint: str | None = None,
        baseline_resources: StepBaselineResources | None = None,
    ) -> dict[str, Path]:
        if container is None:
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=name,
                reason="SlurmBackend requires `container` from the YAML step",
            )
        if baseline_resources is None:
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=name,
                reason="SlurmBackend requires `baseline_resources` from the YAML step",
            )

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
        # $QIITA_INPUT_PATH. Extend this dict when new step types land:
        # per-sample steps add a prep_sample_idx list; tunable steps add
        # an action-parameters block; data-plane-reading steps add a
        # control-plane-minted Flight ticket.
        params_path = input_path / "params.json"
        params_path.write_text(
            json.dumps(
                {
                    "step_name": name,
                    "reference_idx": reference_idx,
                    "work_ticket_idx": work_ticket_idx,
                    "inputs": {k: str(v) for k, v in inputs.items()},
                    "output_path": str(output_path),
                }
            )
        )

        baseline = BaselineResources(
            cpu=baseline_resources.cpu,
            mem_gb=baseline_resources.mem_gb,
            walltime=timedelta(seconds=baseline_resources.walltime_seconds),
            gpu=baseline_resources.gpu,
        )

        payload = build_job_submit_payload(
            step_name=name,
            work_ticket_idx=work_ticket_idx,
            container=container,
            entrypoint=entrypoint,
            baseline_resources=baseline,
            input_path=input_path,
            output_path=output_path,
            workspace=workspace,
            log_stdout=logs_path / "stdout",
            log_stderr=logs_path / "stderr",
            partition=self._partition,
            account=self._account,
        )

        try:
            job_id = await self._client.submit_job(payload)
        except SlurmrestdError as exc:
            raise self._classify_submit_error(exc, name) from exc

        info = await self._poll_until_terminal(job_id, name)

        if info.state == TerminalSlurmState.COMPLETED and (
            info.exit_code is None or info.exit_code == 0
        ):
            failures = verify_container_output(output_path)
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
                    step_name=name,
                    reason=f"container output failed verification: {detail}",
                )
            try:
                return parse_outputs_map(output_path)
            except (OSError, json.JSONDecodeError, KeyError) as exc:
                # Verifier already validated the manifest, so we don't
                # expect to land here in practice — but if we do, treat
                # it as a contract violation (the verifier's contract
                # drifted from parse_outputs_map's expectations).
                raise BackendFailure(
                    kind=FailureKind.CONTRACT_VIOLATION,
                    stage=WorkTicketFailureStage.STEP_RUN,
                    step_name=name,
                    reason=f"could not parse manifest.json outputs: {exc!s}",
                ) from exc

        # Terminal-but-not-success: map state => FailureKind.
        kind = _STATE_TO_FAILURE_KIND.get(info.state, FailureKind.UNKNOWN_PERMANENT)
        reason_parts = [f"SLURM job {job_id} ended in state {info.state!r}"]
        if info.exit_code is not None:
            reason_parts.append(f"exit_code={info.exit_code}")
        if info.reason:
            reason_parts.append(f"slurm_reason={info.reason}")
        raise BackendFailure(
            kind=kind,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=name,
            reason=", ".join(reason_parts),
        )

    async def _poll_until_terminal(self, job_id: int, step_name: str) -> SlurmJobInfo:
        """Poll slurmrestd at `poll_interval_seconds` until the job
        reaches a terminal state or `job_timeout_seconds` elapses.

        On total-timeout: raise a retriable PROCESS_RESTARTED — a job
        that doesn't terminate within 24h either has a misconfigured
        walltime (will hit TIMEOUT eventually) or is genuinely stuck
        (operator-fixable). PROCESS_RESTARTED rather than a stuck-job
        kind because there's no kind for "we gave up watching"; the
        orchestrator may have been restarted and this job is dangling.

        slurmrestd 5xx / transport errors during polling don't bail the
        whole step — they retry on the next interval. A 4xx (e.g. 404
        because the job was purged) does bail.

        Each tick's sleep is jittered by ±50% so N orchestrator
        instances don't all hit slurmrestd in lockstep."""
        deadline = time.monotonic() + self._job_timeout
        while True:
            try:
                info = await self._client.get_job(job_id)
            except SlurmrestdError as exc:
                # 4xx errors mean the job state is unknowable — bail.
                # 5xx and transport errors are transient: keep polling.
                if exc.status_code is not None and 400 <= exc.status_code < 500:
                    raise BackendFailure(
                        kind=FailureKind.UNKNOWN_PERMANENT,
                        stage=WorkTicketFailureStage.STEP_RUN,
                        step_name=step_name,
                        reason=(
                            f"slurmrestd get_job({job_id}) returned {exc.status_code}: {exc!s}"
                        ),
                    ) from exc
                # Transient: log via the polling loop and retry.
                info = None  # type: ignore[assignment]
            if info is not None and info.is_terminal:
                return info
            if time.monotonic() >= deadline:
                raise BackendFailure(
                    kind=FailureKind.PROCESS_RESTARTED,
                    stage=WorkTicketFailureStage.STEP_RUN,
                    step_name=step_name,
                    reason=(
                        f"SLURM job {job_id} did not reach terminal state "
                        f"within {self._job_timeout}s; orchestrator gave up watching"
                    ),
                )
            await asyncio.sleep(self._poll_interval * random.uniform(0.5, 1.5))

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
        return BackendFailure(
            kind=kind,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=step_name,
            reason=f"slurmrestd submit failed: {exc!s}",
        )
