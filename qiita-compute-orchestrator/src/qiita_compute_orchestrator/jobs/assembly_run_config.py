"""Native job: emit the assembly run-config (the chosen assembler).

Writes `run_config.json` carrying the `assembler` choice, for two readers. The
`assemble` container reads the `.assembler` field with `jq`. The runner reads the
WHOLE FILE, stripped, as the key of `assemble`'s `profiles:` resource lookup — so
this file's exact serialized bytes are a contract, not an implementation detail,
and `test_run_config_bytes_are_the_resource_profile_keys` pins them against the
profile keys in the workflow YAML. Keep `json.dumps` and its default separators;
adding a field, reordering, or changing spacing changes the key and fails
`assemble` at dispatch.

The lookup needs a declared output of an upstream step, and this file is one that
already exists. Adding a dedicated one instead would read better and would break
tickets in flight: a completed step's bindings are rebuilt on resume as
`{name: manifest[name] for name in entry.outputs}` against the spec in force at
resume time, so a name the old manifest lacks raises, for any ticket that had
completed this step however far past it.

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
