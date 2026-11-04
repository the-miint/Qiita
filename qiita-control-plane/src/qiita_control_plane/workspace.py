"""Where a work ticket's steps write on the shared scratch filesystem.

`PATH_SCRATCH/ticket/<work_ticket_idx>/<step>/attempt-<n>/`, holding the step's
`output/` (with its manifest) and `logs/`, plus, for an `action:` entry that records
what it sent, `payload-<digest>.json` beside the attempts. The runner creates each
attempt directory and hands it to the compute backend; everything that later finds a
step's files — the runner on resume, the step-logs route, the admin backfills — derives
the path here.
"""

import hashlib
from pathlib import Path

from qiita_common.actions import STEP_LOGS_SUBDIR, STEP_MANIFEST_FILENAME, STEP_OUTPUT_SUBDIR

# The per-ticket workspace root's name under PATH_SCRATCH.
WORK_TICKET_SUBDIR = "ticket"
ATTEMPT_DIR_PREFIX = "attempt-"

__all__ = [
    "ATTEMPT_DIR_PREFIX",
    "STEP_MANIFEST_FILENAME",
    "WORK_TICKET_SUBDIR",
    "action_payload_record",
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
    return ticket_dir / step_name / f"{ATTEMPT_DIR_PREFIX}{attempt}"


def step_output_dir(attempt_dir: Path) -> Path:
    """Where the attempt writes its outputs and `STEP_MANIFEST_FILENAME`."""
    return attempt_dir / STEP_OUTPUT_SUBDIR


def step_logs_dir(attempt_dir: Path) -> Path:
    """Where the attempt's stdout and stderr land."""
    return attempt_dir / STEP_LOGS_SUBDIR


def action_payload_record(attempt_dir: Path, key: str) -> Path:
    """Where an `action:` entry records the payload it sent for `key`, beside its
    attempt directories, so every attempt of the entry finds the same record.

    A redrive needs this record, as it needs the completed steps' outputs, so a
    ticket's workspace may be reclaimed only once no `/run` can redrive it
    again: completed, `no_data`, or superseded by a newer download. A ticket
    redriven without it resends what is left in staging, not what the first
    call sent."""
    digest = hashlib.sha256(key.encode()).hexdigest()
    return attempt_dir.parent / f"payload-{digest}.json"
