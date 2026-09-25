"""Where a work ticket's steps write on the shared scratch filesystem.

`PATH_SCRATCH/ticket/<work_ticket_idx>/<step>/attempt-<n>/`, holding the step's
`output/` (with its manifest) and `logs/`. The runner creates each attempt directory
and hands it to the compute backend; everything that later finds a step's files — the
runner on resume, the step-logs route, the admin backfills — derives the path here.
"""

from pathlib import Path

from qiita_common.actions import STEP_MANIFEST_FILENAME

# The per-ticket workspace root's name under PATH_SCRATCH.
WORK_TICKET_SUBDIR = "ticket"

__all__ = [
    "STEP_MANIFEST_FILENAME",
    "WORK_TICKET_SUBDIR",
    "step_attempt_dir",
    "step_logs_dir",
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
    return attempt_dir / "output"


def step_logs_dir(attempt_dir: Path) -> Path:
    """Where the attempt's stdout and stderr land."""
    return attempt_dir / "logs"
