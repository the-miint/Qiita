"""qiita-admin CLI — compute-readiness subcommand.

Split out of the former single-file ``cli.admin`` module; behavior unchanged.
"""

import argparse
import subprocess
import sys
from pathlib import Path

# Production install location for the orchestrator's venv. Same path
# the deploy script writes to and the systemd unit launches from —
# this constant is a default for the operator-side wrapper, not the
# source of truth; --orchestrator-venv overrides for dev hosts or
# unusual layouts. The wrapper subprocess-execs `<venv>/bin/python -m
# qiita_compute_orchestrator.cli.compute_readiness`.
_DEFAULT_ORCHESTRATOR_VENV = Path("/opt/qiita/compute-orchestrator/.venv")


def _handle_compute_readiness(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Subprocess into the orchestrator's venv to run the compute-readiness
    diagnostic. The orchestrator owns the actual checks (it has the
    Settings.from_env() + SlurmrestdClient surface); this wrapper is a
    thin pass-through so operators have a single `qiita-admin` UX
    surface for cluster-side problems too.

    Returns the subprocess's exit code verbatim so non-zero from any
    check failure propagates up through `qiita-admin` cleanly.
    """
    venv: Path = args.orchestrator_venv
    python = venv / "bin" / "python"
    if not python.exists():
        print(
            f"error: orchestrator python not found at {python}."
            " Pass --orchestrator-venv if the venv is installed elsewhere.",
            file=sys.stderr,
        )
        return 2
    cmd = [str(python), "-m", "qiita_compute_orchestrator.cli.compute_readiness"]
    if args.no_slurm_probe:
        cmd.append("--no-slurm-probe")
    if args.emit_json:
        cmd.append("--json")
    if args.probe_timeout_seconds is not None:
        cmd += ["--probe-timeout-seconds", str(args.probe_timeout_seconds)]
    return subprocess.call(cmd)
