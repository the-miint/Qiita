"""Native job: emit the assembly run-config (the chosen assembler).

The masked reads are now STREAMED to FASTQ by the control-plane runner (the
`read_masked` DoGet + miint's native `COPY … FORMAT FASTQ`; see the runner's
`_resolve_staged_masked_reads`), so this step no longer touches read data — there
is no intermediate Parquet and no hand-rolled FASTQ. Its only job is to write
`run_config.json` carrying the `assembler` choice: a scalar can't ride a container
step's inputs (the runner treats a container input as a bind-mount path), so it
flows through this native step into a small file the `assemble` container reads
with `jq`.
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
    return {"run_config": run_config_out}
