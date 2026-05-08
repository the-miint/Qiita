"""slurmrestd job-submit payload builder.

Pure function: takes a workflow step's metadata and the deploy's SLURM
configuration, returns the JSON dict ready for POST
`/slurm/{api_version}/job/submit`. No I/O — the caller owns writing
`params.json`, executing the HTTP request, etc.

The JSON shape targets slurmrestd API v0.0.40 (the LTS at time of
writing). Newer versions add fields but the subset used here is stable;
if a deploy needs an older or newer version, only the
`number/set/infinite` envelope and the field names need adjusting.

The script body invokes the workflow container via apptainer:

    #!/bin/bash
    set -euo pipefail
    apptainer exec [args] <container_image> [<entrypoint>]

Apptainer is the SLURM-cluster convention (Linux Foundation's
continuation of Singularity); the cluster has it installed. The
container is responsible for the qiita container contract — reading
`$QIITA_INPUT_PATH/params.json`, writing outputs to
`$QIITA_OUTPUT_PATH`, producing `manifest.json` with size_bytes per
file, chmod 440 on outputs.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any

from qiita_common.actions import BaselineResources

# v1 ships with a single hardcoded SLURM API version so payload shape
# tests are deterministic. Operators override via env at the
# SlurmrestdClient layer; the shape below stays valid across v0.0.39 →
# v0.0.41 by holding to the lowest-common-denominator schema. If a
# breaking schema bump lands in slurmrestd, branch on api_version here.
DEFAULT_SLURM_API_VERSION = "v0.0.40"


def _number_envelope(value: int) -> dict[str, Any]:
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


def build_job_submit_payload(
    *,
    step_name: str,
    work_ticket_idx: int,
    container: str,
    entrypoint: str | None,
    baseline_resources: BaselineResources,
    input_path: Path,
    output_path: Path,
    workspace: Path,
    log_stdout: Path,
    log_stderr: Path,
    partition: str,
    account: str,
    extra_env: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build the slurmrestd `POST /slurm/{version}/job/submit` JSON body.

    Args:
        step_name: YAML step name; used in the SLURM job name for
            ops visibility. Not the FailureKind step_name (which is
            on BackendFailure construction).
        work_ticket_idx: control-plane work_ticket id; in the job name
            so a SLURM scheduler dump can be cross-referenced back to
            the originating ticket.
        container: apptainer-runnable image (e.g.
            `/opt/qiita/containers/reference-hash:1.0.0.sif`,
            `qiita/reference-hash:1.0.0` for community-extension hosts).
        entrypoint: optional binary inside the container. None means the
            container's own ENTRYPOINT runs.
        baseline_resources: CPU / memory / walltime from the YAML step.
            v1 uses these values directly; profile multiplication and
            ceiling clamping are deferred (no originator profile
            concept exists yet).
        input_path: Bind-mounted as `$QIITA_INPUT_PATH` inside the
            container. Caller writes `params.json` here before submit.
        output_path: Bind-mounted as `$QIITA_OUTPUT_PATH`. Container
            writes outputs + `manifest.json` here.
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
    if not container:
        raise ValueError("container must be a non-empty string")
    if not partition:
        raise ValueError("partition must be set on the orchestrator config")
    if not account:
        raise ValueError("account must be set on the orchestrator config")

    env: dict[str, str] = {
        "QIITA_INPUT_PATH": str(input_path),
        "QIITA_OUTPUT_PATH": str(output_path),
    }
    if extra_env:
        env.update(extra_env)

    # Bind mounts let the container see the host paths under their own
    # names. Without --bind, the container can't access input_path /
    # output_path on the shared filesystem.
    apptainer_args = [
        "--bind",
        f"{input_path}:{input_path}",
        "--bind",
        f"{output_path}:{output_path}",
    ]

    return {
        "script": _build_script(
            container=container,
            entrypoint=entrypoint,
            apptainer_extra_args=apptainer_args,
        ),
        "job": {
            "name": f"qiita-{step_name}-wt{work_ticket_idx}",
            "account": account,
            "partition": partition,
            "current_working_directory": str(workspace),
            # slurmrestd takes environment as a list of "KEY=VAL" strings.
            # Sorted for determinism so payload tests are stable.
            "environment": [f"{k}={v}" for k, v in sorted(env.items())],
            "memory_per_node": _number_envelope(baseline_resources.mem_gb * 1024),
            "tasks": 1,
            "cpus_per_task": baseline_resources.cpu,
            "time_limit": _number_envelope(_walltime_minutes(baseline_resources.walltime)),
            "standard_output": str(log_stdout),
            "standard_error": str(log_stderr),
        },
    }
