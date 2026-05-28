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

Vendored prefix table: ``sequencer_types.yml`` is a one-time snapshot of
biocore/kl-metapool/metapool/config/sequencer_types.yml. See its header
comment for the re-vendor protocol.

External dependency: ``run_preflight.legacy.api.save_legacy_csv``. The
import is deferred to ``execute()`` so module collection (pytest) and the
launcher's import-on-dispatch never fail just because the dep isn't
installed in a non-bcl-convert environment.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import yaml
from pydantic import BaseModel

from ..sequence_range import fetch_sequenced_pool_preflight, make_cp_client

_SEQUENCER_TYPES_YAML = Path(__file__).parent / "sequencer_types.yml"

# Illumina-only filter. The vendored YAML preserves PacBio Revio
# verbatim (see header comment); the prep step ignores it because
# bcl-convert is Illumina-only and a PacBio prefix accidentally
# resolving against the workflow would surface as an unknown profile
# key downstream rather than at parse time.
_ILLUMINA_MODEL_PREFIX = "Illumina "


def _load_instrument_prefix_table() -> dict[str, str]:
    """Build ``{machine_prefix: model_name}`` from the vendored snapshot.

    Filters:
      * Skips entries without a ``machine_prefix`` (the 4 Illumina
        families plan-D's out-of-scope #6 calls out as currently
        unparseable).
      * Skips entries whose ``model_name`` doesn't start with
        ``"Illumina "`` (excludes PacBio Revio's ``r`` prefix from the
        parser's reach — bcl-convert is Illumina-only).
    """
    with _SEQUENCER_TYPES_YAML.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    table: dict[str, str] = {}
    for entry in raw.values():
        prefix = entry.get("machine_prefix")
        model_name = entry.get("model_name")
        if not prefix or not model_name:
            continue
        if not model_name.startswith(_ILLUMINA_MODEL_PREFIX):
            continue
        table[prefix] = model_name
    return table


# Module-level constant: the prefix-table file is read once at import.
# Re-vendoring requires a process restart, which matches deploy lifecycle.
_INSTRUMENT_PREFIXES = _load_instrument_prefix_table()


def _instrument_model_from_run_folder(folder_name: str) -> str:
    """Parse an Illumina BCL run-folder name into the corresponding
    ``model_name`` string.

    Convention: ``<YYMMDD>_<InstrumentSerial>_<RunNum>_<FlowcellID>``.
    The instrument-serial's leading characters identify the family.

    Match policy: longest prefix wins. The vendored prefix table contains
    overlapping entries (``LH`` vs ``L``, ``MN`` vs ``M``, ``SL`` vs
    ``S``, ``SH`` vs ``S``); without longest-match semantics, a NovaSeq X
    serial ``LH00345`` would resolve to whatever 1-char prefix is checked
    first. Sorting prefixes by length descending makes the match
    deterministic.

    Raises ``ValueError`` on malformed folder names and unrecognized
    prefixes; the launcher framework maps ValueError to
    BackendFailure(BAD_INPUT) so the work_ticket fails fast with a
    structured reason.
    """
    parts = folder_name.split("_")
    if len(parts) < 4:
        raise ValueError(
            f"BCL run folder name does not match Illumina convention "
            f"<YYMMDD>_<InstrumentSerial>_<RunNum>_<FlowcellID>: {folder_name!r}"
        )
    serial = parts[1]
    for prefix in sorted(_INSTRUMENT_PREFIXES, key=len, reverse=True):
        if serial.startswith(prefix):
            return _INSTRUMENT_PREFIXES[prefix]
    raise ValueError(
        f"unknown instrument serial prefix in {folder_name!r}; "
        f"add a machine_prefix entry to kl-metapool's sequencer_types.yml "
        f"and re-vendor, or rename the folder to use a recognized prefix"
    )


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
    # Deferred import so the orchestrator's pytest collection / launcher
    # boot doesn't blow up in environments that don't ship the
    # run-preflight dep. The dep is added to pyproject.toml in the same
    # PR as this module (see CHANGELOG).
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
    instrument_model = _instrument_model_from_run_folder(inputs.bcl_input_dir.name)

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
