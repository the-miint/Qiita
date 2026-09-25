"""Where a work ticket's steps write on the shared scratch filesystem.

`PATH_SCRATCH/ticket/<work_ticket_idx>/<step>/attempt-<n>/`, holding the step's
`output/` (with its manifest) and `logs/`. The runner creates each attempt directory
and hands it to the compute backend; everything that later finds a step's files — the
runner on resume, the step-logs route, the admin backfills — derives the path here.
"""

from pathlib import Path

# The per-ticket workspace root's name under PATH_SCRATCH.
WORK_TICKET_SUBDIR = "ticket"

# The manifest a step writes into its `output/` directory, naming each declared
# output. Defined by the orchestrator (`qiita_compute_orchestrator.slurm.contract.
# MANIFEST_FILENAME`), which the control plane does not import; this is the control
# plane's one copy.
STEP_MANIFEST_FILENAME = "manifest.json"


def ticket_workspace(workspace_root: Path, work_ticket_idx: int) -> Path:
    """The directory holding every step of one ticket."""
    return workspace_root / str(work_ticket_idx)


def step_attempt_dir(ticket_dir: Path, step_name: str, attempt: int) -> Path:
    """One attempt of one step, under `ticket_workspace(...)`."""
    return ticket_dir / step_name / f"attempt-{attempt}"
