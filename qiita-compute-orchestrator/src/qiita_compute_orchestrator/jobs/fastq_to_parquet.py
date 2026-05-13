"""Native job: fastq → parquet conversion. Skeleton.

`execute` raises NotImplementedError today; the real conversion lands
in a follow-up PR. The skeleton's role is to exercise the native-step
dispatch path in tests (and to validate the job-module convention:
`Inputs` Pydantic model + async `execute`).
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel


class Inputs(BaseModel):
    """Typed input contract for fastq_to_parquet.

    Path fields get coerced from `str` (the wire ships paths as strings);
    integer scalars come straight from the work-ticket scope. Validation
    errors surface as `BackendFailure(BAD_INPUT)` via `run_native_job`
    before `execute` is called.
    """

    fastq_path: Path
    reference_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Skeleton: raise NotImplementedError. `run_native_job` translates
    this into `BackendFailure(UNKNOWN_PERMANENT)` for the runner."""
    raise NotImplementedError(
        f"fastq_to_parquet not yet implemented (fastq_path={inputs.fastq_path},"
        f" workspace={workspace})"
    )
