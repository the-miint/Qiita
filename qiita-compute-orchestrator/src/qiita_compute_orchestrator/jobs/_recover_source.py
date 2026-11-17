"""Find a completed long-read-assembly ticket's step and action outputs on scratch, for
the one-off `assembly-gene-call-recover` jobs (`gene_call_recover`,
`assembly_load_recover`). The workflow's header says when they are removed.

A source is named by `(source_work_ticket_idx, entry name, attempt)`, which
`qiita_common.workspace` turns into a path under `PATH_SCRATCH`, never by a path a
submitter could point elsewhere. A step's outputs are read from the `outputs` map of
the `manifest.json` it wrote when it finished; an action writes no manifest, so its
output is found by the basename its primitive writes under.
"""

from __future__ import annotations

import json
from pathlib import Path

from qiita_common.actions import STEP_MANIFEST_FILENAME
from qiita_common.workspace import (
    WORK_TICKET_SUBDIR,
    step_attempt_dir,
    step_output_dir,
    ticket_workspace,
)

from ..config import get_settings

# The long-read-assembly entries whose outputs the recovery reads, as every version
# names them. `assembly_hash` and `mint-features` are named by their own constants
# (`assembly_hash.YAML_STEP_NAME`, `LibraryPrimitive.MINT_FEATURES`).
ASSEMBLE_STEP = "assemble"
BIN_REFINE_STEP = "bin_refine"
CHECKM_STEP = "checkm"


def _exists(path: Path) -> bool:
    """Whether `path` exists. Only absence reads as False: a path that cannot be
    looked at (no permission to traverse its parent) raises, where `Path.exists()`
    would report it missing."""
    try:
        path.stat()
    except FileNotFoundError, NotADirectoryError:
        return False
    return True


def source_ticket_dir(source_work_ticket_idx: int) -> Path:
    """The source ticket's workspace under `PATH_SCRATCH`."""
    return ticket_workspace(
        Path(get_settings().path_scratch) / WORK_TICKET_SUBDIR, source_work_ticket_idx
    )


def step_outputs(
    ticket_dir: Path, step: str, attempt: int, names: tuple[str, ...]
) -> dict[str, Path]:
    """`names` from the outputs map of `step`'s `attempt` under `ticket_dir`, each as
    an absolute path. Raises FileNotFoundError, naming the path, when the manifest or
    an output is missing, and ValueError when the manifest does not name an output or
    names one outside the attempt's output directory."""
    output = step_output_dir(step_attempt_dir(ticket_dir, step, attempt))
    manifest = output / STEP_MANIFEST_FILENAME
    if not _exists(manifest):
        raise FileNotFoundError(
            f"no {STEP_MANIFEST_FILENAME} at {manifest}: no finished {step!r} attempt there"
        )
    listed = json.loads(manifest.read_text())["outputs"]
    found: dict[str, Path] = {}
    for name in names:
        if name not in listed:
            raise ValueError(f"{manifest} names no output {name!r}; it names {sorted(listed)}")
        path = (output / listed[name]).resolve()
        if not path.is_relative_to(output.resolve()):
            raise ValueError(f"{manifest} output {name!r} resolves outside {output}: {path}")
        if not _exists(path):
            raise FileNotFoundError(
                f"{step!r} output {name!r} is listed in {manifest} but missing: {path}"
            )
        found[name] = path
    return found


def action_output(ticket_dir: Path, action: str, attempt: int, basename: str) -> Path:
    """The file `action`'s `attempt` wrote under `basename` in `ticket_dir`. Raises
    FileNotFoundError, naming the path, when it is missing."""
    path = step_attempt_dir(ticket_dir, action, attempt) / basename
    if not _exists(path):
        raise FileNotFoundError(f"no {action!r} output at {path}")
    return path
