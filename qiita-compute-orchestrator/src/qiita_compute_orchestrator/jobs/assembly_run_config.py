"""Native job: emit the assembly run-config (the chosen assembler).

The `assembler` choice is written twice, for two readers that cannot share a file.
`run_config.json` is the `assemble` container's copy, read with `jq`.
`assembler.txt` is the runner's: it reads that file's stripped contents at dispatch
to key `assemble`'s `profiles:` lookup, so the key cannot be a field inside the
JSON. Same shape as bcl-convert's `instrument_model.txt`.

The assembler rides through a native step at all because a scalar can't ride a
container step's inputs — the runner treats a container input as a bind-mount path.

This step does not touch read data: the masked reads are streamed to FASTQ
separately by the control-plane runner (the `read_masked` DoGet + miint's native
`COPY … FORMAT FASTQ`; see the runner's `_resolve_staged_masked_reads`), with no
intermediate Parquet and no hand-rolled FASTQ.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

# YAML step name this module implements.
YAML_STEP_NAME = "assembly_run_config"

# Output basename the `assemble` container reads via params.json `.inputs.run_config`.
_RUN_CONFIG_NAME = "run_config.json"

# Output basename the RUNNER reads to key `assemble`'s `profiles:` lookup. The
# lookup strips the contents, and the YAML's profile keys are the bare assembler
# names, so this file holds exactly the name and nothing else.
_ASSEMBLER_NAME = "assembler.txt"


class Inputs(BaseModel):
    """Typed input contract. `assembler` selects the step-1 tool and is stamped
    into run_config.json. `prep_sample_idx` / `work_ticket_idx` are
    framework-injected scope scalars (part of the native contract)."""

    assembler: Literal["hifiasm_meta", "myloasm"] = "hifiasm_meta"
    prep_sample_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    workspace.mkdir(parents=True, exist_ok=True)
    run_config_out = workspace / _RUN_CONFIG_NAME
    run_config_out.write_text(json.dumps({"assembler": inputs.assembler}) + "\n")
    assembler_out = workspace / _ASSEMBLER_NAME
    assembler_out.write_text(inputs.assembler, encoding="utf-8")
    return {"run_config": run_config_out, "assembler": assembler_out}
