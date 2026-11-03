"""Where a work ticket's steps write on the shared scratch filesystem.

`PATH_SCRATCH/ticket/<work_ticket_idx>/<step>/attempt-<n>/`, holding the step's
`output/` (with its manifest) and `logs/`. Anything that finds a step's files
derives the path here.
"""

from pathlib import Path

from qiita_common.actions import STEP_OUTPUT_SUBDIR

# The per-ticket workspace root's name under PATH_SCRATCH.
WORK_TICKET_SUBDIR = "ticket"

__all__ = [
    "WORK_TICKET_SUBDIR",
    "step_attempt_dir",
    "step_output_dir",
    "ticket_workspace",
]


def ticket_workspace(workspace_root: Path, work_ticket_idx: int) -> Path:
    """The directory holding every step of one ticket."""
    return workspace_root / str(work_ticket_idx)


def step_attempt_dir(ticket_dir: Path, step_name: str, attempt: int) -> Path:
    """One attempt of one step, under `ticket_workspace(...)`."""
    return ticket_dir / step_name / f"attempt-{attempt}"


def step_output_dir(attempt_dir: Path) -> Path:
    """Where the attempt writes its outputs and `STEP_MANIFEST_FILENAME`."""
    return attempt_dir / STEP_OUTPUT_SUBDIR
