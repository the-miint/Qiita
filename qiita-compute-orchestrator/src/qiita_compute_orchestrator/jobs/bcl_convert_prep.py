"""bcl-convert prep step.

Step 1 of the bcl-convert workflow: produce the sample sheet and read the BCL
run folder's RunInfo.xml to derive the Illumina instrument model, writing the
model to a sidecar file for the bcl_convert step's lookup baseline_resources
population. Metagenomic runs fetch the pool's run_preflight_blob from the CP and
rehydrate it to a per-sample CSV; amplicon runs build a no-index dummy sheet from
RunInfo.xml (``amplicon`` input), sending every read to Undetermined so the Golay
barcode is emitted for golay_demux.

The downstream bcl_convert step (container:) consumes:
  - samplesheet: the CSV at workspace/samplesheet.csv
  - bcl_input_dir: the host path to the BCL run folder (threaded through
    from action_context — not produced by this step)

The instrument_model file is consumed by the RUNNER (not the container)
to resolve baseline_resources.profiles at dispatch time. Its contents
must exactly match a key in the workflow YAML's profiles dict.

RunInfo.xml parsing lives in qiita_common.illumina so the user CLI
(qiita submit-bcl-convert) and the launcher-side prep step share one
implementation against one vendored prefix table.

Why this step is native (``module:``) and not a container, despite
CLAUDE.md's "bioinformatics deps belong in a container" rule: the work
is a CP fetch + a SQLite→CSV rehydrate + a RunInfo.xml read — no heavy
bioinformatics binaries and no system packages. The one external
dep (``run_preflight``) is a pure-Python, git-pinned library light enough
to ship in the orchestrator's ``pyproject.toml``; the actual heavy lifting
(``bcl-convert`` itself) is the downstream ``container:`` step. Keeping the
prep native avoids a second SIF for what is otherwise a few hundred lines
of glue.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from pydantic import BaseModel
from qiita_common.illumina import (
    build_amplicon_dummy_sample_sheet,
    read_instrument_run_info,
    read_run_reads,
)

from ..cp_client import make_cp_client
from ..sequencing_run import fetch_sequenced_pool_preflight


class Inputs(BaseModel):
    """Typed input contract for bcl_convert_prep.

    ``bcl_input_dir`` is the workflow's action_context-supplied absolute
    path to the BCL run folder. ``sequenced_pool_idx`` and
    ``sequencing_run_idx`` are framework-injected by
    ``flatten_native_inputs`` (per ``SCOPE_SCALARS_BY_KIND[SEQUENCED_POOL]``).
    ``work_ticket_idx`` is always available.

    ``amplicon`` selects the sample sheet: a per-sample sheet rehydrated from the
    pool's preflight (the default, metagenomic), or a no-index dummy sheet built
    from RunInfo.xml so every read lands in Undetermined and the in-index Golay
    barcode is emitted for golay_demux to demultiplex on."""

    bcl_input_dir: Path
    sequenced_pool_idx: int
    sequencing_run_idx: int
    work_ticket_idx: int
    amplicon: bool = False


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Run the bcl-convert prep step.

    Returns the outputs map the launcher exposes as the YAML step's
    ``outputs:`` list:
      * ``samplesheet`` — the CSV at ``workspace/samplesheet.csv``.
      * ``instrument_model`` — the model-name string at
        ``workspace/instrument_model.txt``. The runner reads this at
        dispatch of the bcl_convert step to look up
        ``baseline_resources.profiles[<model_name>]``.

    Side-effect file (metagenomic only): ``workspace/preflight.db`` is the raw
    blob written for ``save_bclconvert_v1_csv``. The launcher walks the entire
    output tree and includes every file in the manifest (chmodded 0o440), so this
    intermediate is tracked even though it isn't a named output. The verifier
    accepts that. The amplicon path builds the dummy sheet from RunInfo.xml and
    fetches no preflight, so it writes no ``preflight.db``.
    """
    if not inputs.bcl_input_dir.is_absolute():
        raise ValueError(f"bcl_input_dir must be absolute, got {inputs.bcl_input_dir!r}")
    if not inputs.bcl_input_dir.exists() or not inputs.bcl_input_dir.is_dir():
        raise ValueError(
            f"BCL input directory not found or not a directory: {inputs.bcl_input_dir}"
        )

    # Read the instrument model from RunInfo.xml up-front so an
    # absent/malformed file fails before any CP round-trip. The runner's
    # lookup-population resolution at dispatch of the bcl_convert step reads
    # instrument_model.txt; if we couldn't write a valid value, fail here
    # with the precise reason.
    instrument_model = read_instrument_run_info(inputs.bcl_input_dir).instrument_model

    workspace.mkdir(parents=True, exist_ok=True)
    samplesheet = workspace / "samplesheet.csv"

    if inputs.amplicon:
        # No per-sample indices for EMP 16S: a dummy sheet from RunInfo.xml sends
        # every read to Undetermined and emits the Golay index as a FASTQ.
        reads = read_run_reads(inputs.bcl_input_dir)
        sample_id = f"{inputs.bcl_input_dir.name}_SMPL1"
        samplesheet.write_text(
            build_amplicon_dummy_sample_sheet(reads, sample_id), encoding="utf-8"
        )
    else:
        from run_preflight import save_bclconvert_v1_csv  # noqa: PLC0415

        async with make_cp_client() as http:
            preflight = await fetch_sequenced_pool_preflight(
                http=http,
                sequencing_run_idx=inputs.sequencing_run_idx,
                sequenced_pool_idx=inputs.sequenced_pool_idx,
            )
        preflight_db = workspace / "preflight.db"
        preflight_db.write_bytes(preflight.run_preflight_blob)
        with sqlite3.connect(str(preflight_db)) as conn:
            save_bclconvert_v1_csv(conn, str(samplesheet))

    instrument_model_file = workspace / "instrument_model.txt"
    instrument_model_file.write_text(instrument_model, encoding="utf-8")

    return {
        "samplesheet": samplesheet,
        "instrument_model": instrument_model_file,
    }
