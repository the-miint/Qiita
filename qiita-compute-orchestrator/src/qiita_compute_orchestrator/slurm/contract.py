"""Shared container/native-output contract.

These constants and types are the surface where the producer
(`SlurmBackend.submit_step` for params.json, the container's own
entrypoint or `jobs/__main__.py` for manifest.json) and the consumer
(the launcher in `jobs/__main__.py` for params.json, `slurm/verify.py`
for manifest.json) meet. Keeping them in one module means a change
here forces both sides to update together.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

# Mode the data plane requires before it'll register a Parquet file.
# Owner-and-group read, no write, no other. Both verifier and launcher
# read this value rather than re-typing 0o440 — drift between them
# would make some valid outputs look like contract violations.
EXPECTED_FILE_MODE: int = 0o440

# Filename the producer writes inside $QIITA_OUTPUT_PATH (final act
# before chmod; its presence is the completion marker). The verifier
# reads it; the launcher writes it. Constant here so a rename touches
# both sites at once.
MANIFEST_FILENAME: str = "manifest.json"

# Filename SlurmBackend writes inside $QIITA_INPUT_PATH and the launcher
# (jobs/__main__.py) reads. Same drift-prevention rationale as
# MANIFEST_FILENAME.
JOB_PARAMS_FILENAME: str = "params.json"


class JobParams(BaseModel):
    """Typed shape of params.json — the channel for workflow-specific
    data the SLURM job needs at execution time.

    Producer: `SlurmBackend.submit_step` constructs one and writes its
    `model_dump_json()` to `<QIITA_INPUT_PATH>/<JOB_PARAMS_FILENAME>`.
    Consumer: the launcher in `jobs/__main__.py` reads the file and
    `model_validate_json`s it. Both sides type-check against this
    class, so a field rename or type drift fails at the producer's
    construction step (Pydantic) and at the consumer's parse step
    (Pydantic), not silently downstream.

    `scope_target` is typed as `dict[str, Any]` rather than
    `qiita_common.models.ScopeTarget` so this module can stay
    dependency-light; the discriminated-union validation runs inside
    `flatten_native_inputs` via `SCOPE_SCALARS_BY_KIND`.

    Extend by adding a field here — both sides pick up the new shape
    immediately, and the test that round-trips params.json on the
    producer side catches any mismatch.
    """

    step_name: str
    scope_target: dict[str, Any]
    work_ticket_idx: int
    inputs: dict[str, str]
    output_path: str
