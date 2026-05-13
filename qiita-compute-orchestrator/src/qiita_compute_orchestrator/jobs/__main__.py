"""Shared SLURM launcher for native jobs.

Invoked from the SBATCH script as

    python -m qiita_compute_orchestrator.jobs --job <short_name>

where `<short_name>` is the module path with `NATIVE_MODULE_PREFIX`
stripped (e.g. `fastq_to_parquet`). The launcher:

1. Parses `--job`.
2. Reads `params.json` from `$QIITA_INPUT_PATH`.
3. Flattens the work-ticket scalars (`reference_idx`, `work_ticket_idx`)
   and the per-step `inputs` map into a single raw-inputs dict and
   calls `run_native_job(...)`.
4. On success, walks the output map, chmods every file to 0o440,
   writes `manifest.json` to `$QIITA_OUTPUT_PATH` matching the
   verifier's contract (see `slurm/verify.py`), and exits 0.
5. On `BackendFailure` (raised by `run_native_job`), prints a
   structured error line to stderr and exits 1 — `slurmrestd` reports
   the non-zero exit and the orchestrator-side polling classifies it.

The manifest format mirrors what container steps produce so the
verifier walks both uniformly:
    {
      "files":   [{"path": "...", "size_bytes": N}, ...],
      "outputs": {"<step output name>": "<relative path>", ...}
    }
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from qiita_common.actions import NATIVE_MODULE_PREFIX
from qiita_common.backend_failure import BackendFailure

from . import run_native_job

# Mode bit set on every file the launcher writes — matches what the
# data plane and orchestrator's verifier require (qiita-data-plane
# refuses to register Parquet that isn't 0o440).
_OUTPUT_FILE_MODE = 0o440


def _flatten_params(params: dict) -> dict:
    """Combine the work-ticket scalars and the step's inputs map into
    a single dict ready for `Inputs.model_validate`.

    `params.json` is written by SlurmBackend with the same shape today;
    keep this helper consistent with it."""
    return {
        "reference_idx": params["reference_idx"],
        "work_ticket_idx": params["work_ticket_idx"],
        **params.get("inputs", {}),
    }


def _collect_files(output_path: Path) -> list[Path]:
    """Every file under output_path that's NOT manifest.json. Used to
    build the manifest's `files` array — a job may emit directory
    outputs (e.g. `staging_dir`) and the verifier wants each constituent
    file listed."""
    return [
        p
        for p in output_path.rglob("*")
        if p.is_file() and p.resolve() != (output_path / "manifest.json").resolve()
    ]


def _chmod_440(paths) -> None:
    for p in paths:
        p.chmod(_OUTPUT_FILE_MODE)


def _write_manifest(output_path: Path, outputs: dict[str, Path]) -> None:
    """Produce manifest.json. Outputs map values are stored as paths
    relative to `output_path` so the verifier can rebuild them with
    `output_path / value`."""
    output_path_resolved = output_path.resolve()
    files_listing = []
    for p in _collect_files(output_path):
        files_listing.append(
            {
                "path": str(p.resolve().relative_to(output_path_resolved)),
                "size_bytes": p.stat().st_size,
            }
        )
    outputs_map = {}
    for name, p in outputs.items():
        resolved = p.resolve()
        if resolved == output_path_resolved:
            # The output IS the directory itself (e.g. `staging_dir`).
            outputs_map[name] = "."
        else:
            outputs_map[name] = str(resolved.relative_to(output_path_resolved))
    manifest = {"files": files_listing, "outputs": outputs_map}
    manifest_path = output_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    manifest_path.chmod(_OUTPUT_FILE_MODE)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="qiita-job-launcher")
    parser.add_argument(
        "--job",
        required=True,
        help="short job name (the module path with NATIVE_MODULE_PREFIX stripped)",
    )
    args = parser.parse_args(argv)

    input_path = Path(os.environ["QIITA_INPUT_PATH"])
    output_path = Path(os.environ["QIITA_OUTPUT_PATH"])

    params = json.loads((input_path / "params.json").read_text())
    raw_inputs = _flatten_params(params)
    module_name = f"{NATIVE_MODULE_PREFIX}{args.job}"

    try:
        outputs = asyncio.run(run_native_job(module_name, raw_inputs, output_path))
    except BackendFailure as exc:
        # Structured error line on stderr — the orchestrator-side
        # slurmrestd polling will see exit=1 and classify based on
        # SLURM state. The reason text helps a human looking at the
        # job log understand what happened.
        print(
            json.dumps({"kind": exc.kind.value, "step_name": exc.step_name, "reason": exc.reason}),
            file=sys.stderr,
        )
        return 1

    _chmod_440(_collect_files(output_path))
    _write_manifest(output_path, outputs)
    return 0


if __name__ == "__main__":
    sys.exit(main())
