"""slurmrestd job-submit payload builder.

Pure function: takes a workflow step's metadata and the deploy's SLURM
configuration, returns the JSON dict ready for POST
`/slurm/{api_version}/job/submit`. No I/O — the caller owns writing
`params.json`, executing the HTTP request, etc.

The JSON shape targets slurmrestd API v0.0.40 (the LTS at time of
writing). Newer versions add fields but the subset used here is stable;
if a deploy needs an older or newer version, only the
`number/set/infinite` envelope and the field names need adjusting.

The builder supports two runtimes, selected by which of `container` or
`module` is set:

- Container form (apptainer):
      #!/bin/bash
      set -euo pipefail
      apptainer exec [args] <container_image> [<entrypoint>]
  Apptainer is the SLURM-cluster convention (Linux Foundation's
  continuation of Singularity); the cluster has it installed.

- Native form (`python -m`):
      #!/bin/bash
      set -euo pipefail
      srun python -m qiita_compute_orchestrator.jobs --job <short_name>
  `<short_name>` is `module` with `NATIVE_MODULE_PREFIX` stripped.
  The shared launcher (`jobs/__main__.py`) reads
  `$QIITA_INPUT_PATH/params.json` and routes through `run_native_job`.

Either way, the producer is responsible for the qiita output contract —
reading `$QIITA_INPUT_PATH/params.json`, writing outputs and
`manifest.json` to `$QIITA_OUTPUT_PATH`, chmod 440 on every file (see
`slurm/contract.py` for the load-bearing constants and
`slurm/verify.py` for the checker).
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

from qiita_common.actions import NATIVE_MODULE_PREFIX, BaselineResources


def number_envelope(value: int) -> dict[str, Any]:
    """slurmrestd's typed-numeric envelope. Used for memory / cpus /
    time fields so missing values can be expressed as set=False rather
    than a sentinel like 0 (which would mean "use default partition
    limit," not "unset")."""
    return {"number": value, "set": True, "infinite": False}


def _walltime_minutes(walltime: timedelta) -> int:
    """SLURM time_limit is minutes (integer). Round up so a job declared
    as "PT90S" gets 2 minutes rather than 1 (which would risk a 30s
    overshoot triggering TIMEOUT). Floor of 1 minute — slurmrestd
    rejects a 0-minute time_limit."""
    seconds = walltime.total_seconds()
    if seconds <= 0:
        raise ValueError(f"walltime must be positive, got {walltime!r}")
    minutes = int(-(-seconds // 60))  # ceil-div
    return max(1, minutes)


def _build_script(
    *,
    container: str,
    entrypoint: str | None,
    apptainer_extra_args: list[str] | None = None,
) -> str:
    """Shell launcher the SLURM job runs. apptainer exec is unprivileged;
    --containall isolates filesystem so the workflow can't poke at
    /opt/qiita or arbitrary host paths beyond what the bind mounts
    expose. Bind mounts for QIITA_INPUT_PATH and QIITA_OUTPUT_PATH are
    set up by the caller via apptainer_extra_args; this function just
    formats the script."""
    extra = " ".join(apptainer_extra_args or [])
    if extra:
        extra = f" {extra}"
    cmd = f"apptainer exec --containall{extra} {container}"
    if entrypoint:
        cmd = f"{cmd} {entrypoint}"
    return f"#!/bin/bash\nset -euo pipefail\n{cmd}\n"


def _build_native_script(*, module: str, python: str) -> str:
    """Shell launcher for a native-step job. Invokes the shared
    Python launcher (`jobs/__main__.py`) with the short job name,
    which reads `$QIITA_INPUT_PATH/params.json` and routes through
    `run_native_job` exactly as `LocalBackend` does in-process —
    same dispatcher, same error classification, same manifest
    contract.

    `python` is the Python interpreter the SBATCH script invokes. The
    caller (build_job_submit_payload) threads it from
    Settings.slurm.native_python, which the orchestrator resolves from
    SLURM_NATIVE_PYTHON (default "python"). Sites whose compute nodes
    don't have a Python with qiita_compute_orchestrator on PATH point
    this at an absolute interpreter path under a shared-filesystem venv.
    """
    if not python:
        raise ValueError("python must be a non-empty string")
    short = module.removeprefix(NATIVE_MODULE_PREFIX)
    cmd = f"srun {python} -m qiita_compute_orchestrator.jobs --job {short}"
    return f"#!/bin/bash\nset -euo pipefail\n{cmd}\n"


def build_job_submit_payload(
    *,
    step_name: str,
    work_ticket_idx: int,
    container: str | None,
    module: str | None = None,
    entrypoint: str | None,
    baseline_resources: BaselineResources,
    input_path: Path,
    output_path: Path,
    workspace: Path,
    log_stdout: Path,
    log_stderr: Path,
    partition: str,
    account: str,
    attempt: int = 0,
    extra_env: dict[str, str] | None = None,
    extra_bind_dirs: list[Path] | None = None,
    native_python: str = "python",
    qos: str = "",
) -> dict[str, Any]:
    """Build the slurmrestd `POST /slurm/{version}/job/submit` JSON body.

    Exactly one of `container` or `module` must be set — the wire
    validator on StepSubmitRequest enforces this upstream; this builder
    re-checks defensively because direct callers (tests) skip the wire.

    Args:
        step_name: YAML step name; used in the SLURM job name for
            ops visibility. Not the FailureKind step_name (which is
            on BackendFailure construction).
        work_ticket_idx: control-plane work_ticket id; in the job name
            so a SLURM scheduler dump can be cross-referenced back to
            the originating ticket.
        attempt: retry attempt number, encoded into the deterministic
            job name so a retry doesn't collide with the previous
            attempt's (terminal) job and so the recovery path can find
            the current attempt's job by name.
        container: apptainer-runnable image (e.g.
            `/opt/qiita/containers/reference-hash:1.0.0.sif`,
            `qiita/reference-hash:1.0.0` for community-extension hosts).
            Mutually exclusive with `module`.
        module: native-job module path under `NATIVE_MODULE_PREFIX`
            (e.g. `qiita_compute_orchestrator.jobs.fastq_to_parquet`).
            Mutually exclusive with `container`. When set, the SBATCH
            script invokes the shared `python -m` launcher instead of
            `apptainer exec`; bind mounts are not emitted because
            there's no container to bind into.
        entrypoint: optional binary inside the container. None means
            the container's own ENTRYPOINT runs. Meaningful only when
            `container` is set.
        baseline_resources: CPU / memory / walltime from the YAML step.
            Used as-is — there is no originator-profile multiplier
            applied here. Caller is responsible for clamping against
            the action's ceiling before passing in.
        input_path: `$QIITA_INPUT_PATH`. For container steps this is
            bind-mounted; for native steps the launcher reads from it
            directly. Caller writes `params.json` here before submit.
        output_path: `$QIITA_OUTPUT_PATH`. Same dual role; the
            producer (container or launcher) writes outputs +
            `manifest.json` here.
        workspace: SLURM job's `current_working_directory`. Distinct
            from input/output (which are container-internal paths).
        log_stdout, log_stderr: Where slurmd writes job logs. Caller
            provides absolute paths under the orchestrator's log root.
        partition: SLURM partition (e.g. "qiita" or "compute"). Set by
            deploy config; this builder doesn't synthesize a default.
        account: SLURM account for usage reporting. Same source as
            partition.
        extra_env: optional extra env vars to inject. Used by tests and
            future per-step overrides; kept as a flat name => value map
            (slurmrestd's `environment` field is a list of "KEY=VAL"
            strings, which we serialize from this dict).

    Returns:
        A dict ready to pass directly to slurmrestd's job/submit
        endpoint. The caller is responsible for adding the SLURM JWT
        header and POSTing.
    """
    if (container is None) == (module is None):
        raise ValueError("build_job_submit_payload requires exactly one of `container` or `module`")
    if container is not None and not container:
        raise ValueError("container must be a non-empty string")
    if module is not None and not module:
        raise ValueError("module must be a non-empty string")
    if not partition:
        raise ValueError("partition must be set on the orchestrator config")
    if not account:
        raise ValueError("account must be set on the orchestrator config")

    # QIITA_WORK_TICKET_IDX is mirrored from params.json so producers
    # that want to stamp output filenames or logs with the originating
    # ticket don't have to JSON-parse params just to read one scalar.
    # params.json (typed as JobParams in slurm/contract.py) remains the
    # contract source of truth for everything else — step_name,
    # scope_target, inputs, output_path.
    #
    # HOME=<workspace> is set because the native-step jobs run DuckDB
    # with the miint extension, which caches the extension shared
    # library under $HOME/.duckdb/extensions/. SLURM jobs on this
    # cluster don't get a useful HOME by default; pointing at the
    # per-ticket workspace gives each job a writable scratch HOME
    # that's cleaned up with the workspace.
    env: dict[str, str] = {
        "QIITA_INPUT_PATH": str(input_path),
        "QIITA_OUTPUT_PATH": str(output_path),
        "QIITA_WORK_TICKET_IDX": str(work_ticket_idx),
        "HOME": str(workspace),
    }
    if extra_env:
        env.update(extra_env)

    if module is not None:
        # Native: no bind mounts — the launcher runs in the
        # orchestrator's installed Python env on the compute node,
        # reading/writing host paths directly via QIITA_*_PATH.
        script = _build_native_script(module=module, python=native_python)
    else:
        # Container: bind mounts let apptainer see input_path /
        # output_path under the same names from inside the container.
        # `extra_bind_dirs` exposes additional host directories the step
        # needs to read — typically the parent dirs of YAML-declared
        # `inputs:` paths so the entrypoint can `jq` a host path out of
        # params.json and apptainer can resolve it.
        #
        # `--home <workspace>` pins the container's home mount to the
        # per-ticket workspace. `--containall` takes the home mount target
        # from the job user's passwd entry, not $HOME — and qiita-job is a
        # service account whose passwd home is `/dev/null`, which collides
        # with the device of the same name in the container layout. The
        # src:dest form keeps the in-container home equal to the HOME env
        # set below.
        #
        # `--env` re-injects the container contract vars. `--containall`
        # contains the *environment* too, so the QIITA_* vars set in the
        # SLURM job env below do not cross into the container — the
        # entrypoint reads `$QIITA_INPUT_PATH/params.json` and writes to
        # `$QIITA_OUTPUT_PATH`, so without this it exits 64 ("QIITA_INPUT_PATH
        # not set"). Only the contract vars are forwarded; native-only env
        # (CO→CP token, miint dirs) is deliberately not exposed to containers.
        apptainer_args = [
            "--home",
            f"{workspace}:{workspace}",
            "--env",
            f"QIITA_INPUT_PATH={input_path}",
            "--env",
            f"QIITA_OUTPUT_PATH={output_path}",
            "--env",
            f"QIITA_WORK_TICKET_IDX={work_ticket_idx}",
            "--bind",
            f"{input_path}:{input_path}",
            "--bind",
            f"{output_path}:{output_path}",
        ]
        for bind_dir in extra_bind_dirs or ():
            apptainer_args.extend(("--bind", f"{bind_dir}:{bind_dir}"))
        script = _build_script(
            container=container,
            entrypoint=entrypoint,
            apptainer_extra_args=apptainer_args,
        )

    job: dict[str, Any] = {
        # Deterministic, recovery-findable name: ticket, then step, then
        # attempt. SlurmrestdClient.find_jobs_by_name keys off this exact
        # shape to re-adopt a job the control plane submitted but may not
        # have persisted the id for. The attempt suffix keeps a retry's
        # job from colliding with the terminal previous attempt's job.
        "name": f"qiita-wt{work_ticket_idx}-{step_name}-a{attempt}",
        "account": account,
        "partition": partition,
        "current_working_directory": str(workspace),
        # slurmrestd takes environment as a list of "KEY=VAL" strings.
        # Sorted for determinism so payload tests are stable.
        "environment": [f"{k}={v}" for k, v in sorted(env.items())],
        "memory_per_node": number_envelope(baseline_resources.mem_gb * 1024),
        "tasks": 1,
        "cpus_per_task": baseline_resources.cpu,
        "time_limit": number_envelope(_walltime_minutes(baseline_resources.walltime)),
        "standard_output": str(log_stdout),
        "standard_error": str(log_stderr),
    }
    # Only emit qos when the operator set one — omitting the field lets
    # slurmrestd apply the submitting user's default QOS, which is the
    # right behavior for sites that don't run a multi-QOS cluster.
    if qos:
        job["qos"] = qos
    return {
        "script": script,
        "job": job,
    }
