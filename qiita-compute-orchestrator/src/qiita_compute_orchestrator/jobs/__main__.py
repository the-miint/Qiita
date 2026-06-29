"""Shared SLURM launcher for native jobs.

Invoked from the SBATCH script as

    python -m qiita_compute_orchestrator.jobs --job <short_name>

where `<short_name>` is the module path with `NATIVE_MODULE_PREFIX`
stripped (e.g. `fastq_to_parquet`). The launcher:

1. Parses `--job`.
2. Reads `params.json` from `$QIITA_INPUT_PATH` and validates it
   against the `JobParams` Pydantic model in `slurm/contract.py`.
3. Flattens the work-ticket scalars (the scope_target's kind-specific
   idx fields plus `work_ticket_idx`) and the per-step `inputs` map
   into a single raw-inputs dict — see `jobs/__init__.py`'s
   `SCOPE_SCALARS_BY_KIND` for the per-scope rules — and calls
   `run_native_job(...)`.
4. On success, walks the output map, chmods every file to 0o440,
   writes `manifest.json` to `$QIITA_OUTPUT_PATH` matching the
   verifier's contract (see `slurm/verify.py`), and exits 0.
5. On `BackendFailure` (raised by `run_native_job`), prints a
   structured error line to stderr and exits 1. `SlurmBackend` reads
   that line via `slurm/launcher_failure.py:parse_launcher_failure`
   after a non-zero SLURM exit and uses it to enrich the
   `BackendFailure` it propagates to the runner — so the launcher's
   real `kind` / `step_name` / `reason` end up on the work_ticket
   row's `failure_*` columns instead of a generic state-based
   classification ("job FAILED with exit_code=1").

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
from qiita_common.backend_failure import BackendFailure, StepNoData

from ..slurm.contract import (
    EXPECTED_FILE_MODE,
    JOB_PARAMS_FILENAME,
    MANIFEST_FILENAME,
    JobParams,
)
from ..slurm.launcher_failure import NO_DATA_MARKER_KIND
from . import flatten_native_inputs, run_native_job


def _flatten_params(params: JobParams) -> dict:
    """Combine the work-ticket scalars and the step's inputs map into a
    single dict ready for `Inputs.model_validate`.

    Producer side of the contract is `SlurmBackend.submit_step`, which
    constructs a `JobParams` (slurm/contract.py) and writes its
    `model_dump_json()` to `params.json`. `output_path` rides along on
    the model but is ignored here — the env var ($QIITA_OUTPUT_PATH)
    wins.

    Delegates the merge + reserved-key check to `flatten_native_inputs`
    so the launcher and LocalBackend share one code path. A collision
    or unknown scope kind surfaces as BackendFailure(CONTRACT_VIOLATION)
    — carrying the YAML step name from params — which `main()` catches
    and renders to structured stderr.
    """
    return flatten_native_inputs(
        params.inputs,
        step_name=params.step_name,
        scope_target=params.scope_target,
        work_ticket_idx=params.work_ticket_idx,
    )


def _collect_files(output_path: Path) -> list[Path]:
    """Every file under output_path that's NOT the manifest. Used to
    build the manifest's `files` array — a job may emit directory
    outputs (e.g. `staging_dir`) and the verifier wants each constituent
    file listed."""
    manifest_resolved = (output_path / MANIFEST_FILENAME).resolve()
    return [p for p in output_path.rglob("*") if p.is_file() and p.resolve() != manifest_resolved]


def _chmod(paths) -> None:
    for p in paths:
        p.chmod(EXPECTED_FILE_MODE)


def _write_manifest(
    output_path: Path,
    outputs: dict[str, Path],
    files: list[Path],
) -> None:
    """Produce the manifest. Outputs map values are stored as paths
    relative to `output_path` so the verifier can rebuild them with
    `output_path / value`.

    `files` is the pre-computed output-file walk; the caller already
    needs it for `_chmod`, so passing it in avoids a second `rglob`."""
    output_path_resolved = output_path.resolve()
    files_listing = []
    for p in files:
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
            continue
        try:
            rel = resolved.relative_to(output_path_resolved)
        except ValueError:
            # A native job returned an output living OUTSIDE
            # $QIITA_OUTPUT_PATH. The manifest + verifier contract requires
            # every declared output to resolve under the output dir, so a
            # persistent artifact written elsewhere (e.g. a derived-tier index
            # under PATH_DERIVED) must NOT be a step output — its location
            # belongs in an in-tree meta JSON the consuming action reads. Fail
            # with the rule rather than letting relative_to's opaque "is not in
            # the subpath of" message leak as the failure reason.
            raise ValueError(
                f"output {name!r} resolves to {resolved}, which is outside "
                f"$QIITA_OUTPUT_PATH ({output_path_resolved}); a step output must "
                "live under the output dir. A persistent/derived artifact is not a "
                "step output — record its path in an in-tree meta file (see "
                "build_rype_index/build_minimap2_index)."
            ) from None
        outputs_map[name] = str(rel)
    manifest = {"files": files_listing, "outputs": outputs_map}
    manifest_path = output_path / MANIFEST_FILENAME
    # Pretty-print + sort keys so a human reading a job's output dir can
    # diff/scan it; mirrors workflows/_shared/manifest_writer.py (the
    # container-side twin). The verifier parses with json.loads, which
    # ignores whitespace.
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    manifest_path.chmod(EXPECTED_FILE_MODE)


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
    module_name = f"{NATIVE_MODULE_PREFIX}{args.job}"

    params = JobParams.model_validate_json((input_path / JOB_PARAMS_FILENAME).read_text())
    step_name = params.step_name
    try:
        raw_inputs = _flatten_params(params)
        outputs = asyncio.run(
            run_native_job(module_name, raw_inputs, output_path, step_name=step_name)
        )
    except StepNoData as exc:
        # Terminal no-data outcome (an empty FASTQ well) — NOT a failure. Write a
        # structured no-data line to stderr (a sibling of the failure line below,
        # tagged with NO_DATA_MARKER_KIND in the `kind` slot) and exit non-zero.
        # SlurmBackend.result_step parses this BEFORE the failure parse and
        # reconstructs a StepNoData, which the runner turns into a NO_DATA ticket
        # rather than a FAILED one. The job wrote no manifest and no output.
        print(
            json.dumps(
                {"kind": NO_DATA_MARKER_KIND, "step_name": exc.step_name, "reason": exc.reason}
            ),
            file=sys.stderr,
        )
        return 1
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

    try:
        files = _collect_files(output_path)
        _chmod(files)
        _write_manifest(output_path, outputs, files)
    except Exception as exc:
        # execute() succeeded but the launcher couldn't honor the
        # output contract (chmod failed, manifest write failed, ...).
        # Same shape the verifier uses for container-side manifest
        # failures — typed permanent failure, not a raw traceback.
        print(
            json.dumps(
                {
                    "kind": "contract_violation",
                    "step_name": step_name,
                    "reason": (f"post-success manifest write failed: {type(exc).__name__}: {exc}"),
                }
            ),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
