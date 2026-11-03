"""The control plane's view of a ticket's workspace on the shared scratch filesystem.

The layout itself — `PATH_SCRATCH/ticket/<work_ticket_idx>/<step>/attempt-<n>/` and
the helpers that build it — is shared with the compute side and lives in
`qiita_common.workspace`; it is re-exported here so the control plane's callers
import one module. What only the control plane needs is defined below: the logs
directory that it reads, and the payload record that an `action:` entry keeps
beside its attempts.
"""

import hashlib
from pathlib import Path

from qiita_common.actions import STEP_LOGS_SUBDIR, STEP_MANIFEST_FILENAME
from qiita_common.workspace import (
    WORK_TICKET_SUBDIR,
    step_attempt_dir,
    step_output_dir,
    ticket_workspace,
)

__all__ = [
    "STEP_MANIFEST_FILENAME",
    "WORK_TICKET_SUBDIR",
    "action_payload_record",
    "step_attempt_dir",
    "step_logs_dir",
    "step_output_dir",
    "ticket_workspace",
]


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
