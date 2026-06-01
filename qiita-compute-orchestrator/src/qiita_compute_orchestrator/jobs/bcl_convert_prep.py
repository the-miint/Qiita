"""bcl-convert prep step.

Step 1 of the bcl-convert workflow: fetch the pool's run_preflight_blob
from the CP, rehydrate it to a sample-sheet CSV in the workspace, parse
the BCL run-folder name to derive the Illumina instrument model, and
write the model to a sidecar file for the runner's A4 resource lookup.

The downstream bcl_convert step (container:) consumes:
  - samplesheet: the CSV at workspace/samplesheet.csv
  - bcl_input_dir: the host path to the BCL run folder (threaded through
    from action_context — not produced by this step)

The instrument_model file is consumed by the RUNNER (not the container)
to resolve baseline_resources.profiles at dispatch time. Its contents
must exactly match a key in the workflow YAML's profiles dict.

Folder-name parsing lives in qiita_common.illumina so the user CLI
(qiita submit-bcl-convert) and the launcher-side prep step share one
implementation against one vendored prefix table.

Why this step is native (``module:``) and not a container, despite
CLAUDE.md's "bioinformatics deps belong in a container" rule: the work
is a CP fetch + a SQLite→CSV rehydrate + a folder-name string parse — no
heavy bioinformatics binaries and no system packages. The one external
dep (``run_preflight``) is a pure-Python, git-pinned library light enough
to ship in the orchestrator's ``pyproject.toml``; the actual heavy lifting
(``bcl-convert`` itself) is the downstream ``container:`` step. Keeping the
prep native avoids a second SIF for what is otherwise a few hundred lines
of glue.

External dependency: ``run_preflight.legacy.api.save_legacy_csv``. The
import is deferred to ``execute()`` so module collection (pytest) and the
launcher's import-on-dispatch never fail just because the dep is not
installed in a non-bcl-convert environment.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from pydantic import BaseModel
from qiita_common.illumina import instrument_model_from_run_folder

from ..cp_client import make_cp_client
from ..sequencing_run import fetch_sequenced_pool_preflight


class Inputs(BaseModel):
    """Typed input contract for bcl_convert_prep.

    ``bcl_input_dir`` is the workflow's action_context-supplied absolute
    path to the BCL run folder. ``sequenced_pool_idx`` and
    ``sequencing_run_idx`` are framework-injected by
    ``flatten_native_inputs`` (per ``SCOPE_SCALARS_BY_KIND[SEQUENCED_POOL]``).
    ``work_ticket_idx`` is always available."""

    bcl_input_dir: Path
    sequenced_pool_idx: int
    sequencing_run_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Run the bcl-convert prep step.

    Returns the outputs map the launcher exposes as the YAML step's
    ``outputs:`` list:
      * ``samplesheet`` — the CSV at ``workspace/samplesheet.csv``.
      * ``instrument_model`` — the model-name string at
        ``workspace/instrument_model.txt``. The runner reads this at
        dispatch of the bcl_convert step to look up
        ``baseline_resources.profiles[<model_name>]``.

    Side-effect file: ``workspace/preflight.db`` is the raw blob written
    for ``save_legacy_csv``. The launcher walks the entire output tree
    and includes every file in the manifest (chmodded 0o440), so this
    intermediate is tracked even though it isn't a named output. The
    verifier accepts that.
    """
    # `run_preflight` is the git-pinned `kl-run-preflight` dependency
    # (see qiita-compute-orchestrator/pyproject.toml); `save_legacy_csv` is
    # called below to rehydrate the preflight blob to a sample-sheet CSV.
    # "legacy" is upstream's own module name for the legacy Qiita
    # sample-sheet format — it is not dead/legacy code on our side.
    # Imported here (not at module top) so the orchestrator's pytest
    # collection / launcher boot doesn't blow up in environments that don't
    # ship the dep. The dep ships in this component's pyproject.toml.
    from run_preflight.legacy.api import save_legacy_csv  # noqa: PLC0415

    if not inputs.bcl_input_dir.is_absolute():
        raise ValueError(f"bcl_input_dir must be absolute, got {inputs.bcl_input_dir!r}")
    if not inputs.bcl_input_dir.exists() or not inputs.bcl_input_dir.is_dir():
        raise ValueError(
            f"BCL input directory not found or not a directory: {inputs.bcl_input_dir}"
        )

    # Parse instrument model up-front so an unparseable folder name fails
    # before any CP round-trip. The runner's A4 resolution at dispatch
    # of the bcl_convert step reads instrument_model.txt; if we couldn't
    # write a valid value, fail here with the precise reason.
    instrument_model = instrument_model_from_run_folder(inputs.bcl_input_dir.name)

    workspace.mkdir(parents=True, exist_ok=True)

    async with make_cp_client() as http:
        preflight = await fetch_sequenced_pool_preflight(
            http=http,
            sequencing_run_idx=inputs.sequencing_run_idx,
            sequenced_pool_idx=inputs.sequenced_pool_idx,
        )

    preflight_db = workspace / "preflight.db"
    preflight_db.write_bytes(preflight.run_preflight_blob)

    samplesheet = workspace / "samplesheet.csv"
    with sqlite3.connect(str(preflight_db)) as conn:
        save_legacy_csv(conn, str(samplesheet))

    instrument_model_file = workspace / "instrument_model.txt"
    instrument_model_file.write_text(instrument_model, encoding="utf-8")

    return {
        "samplesheet": samplesheet,
        "instrument_model": instrument_model_file,
    }
