"""The ticket workspace layout is shared: the control plane creates and resumes
attempts in it, and a native job that reads another ticket's finished step finds
it the same way. Pin the shape, so a change to it is a change both sides see."""

from pathlib import Path

from qiita_common.actions import STEP_MANIFEST_FILENAME
from qiita_common.workspace import (
    WORK_TICKET_SUBDIR,
    step_attempt_dir,
    step_output_dir,
    ticket_workspace,
)


def test_a_step_attempts_output_sits_at_the_documented_path():
    scratch = Path("/scratch")
    out = step_output_dir(
        step_attempt_dir(ticket_workspace(scratch / WORK_TICKET_SUBDIR, 4834), "assemble", 2)
    )
    assert out == Path("/scratch/ticket/4834/assemble/attempt-2/output")
    assert out / STEP_MANIFEST_FILENAME == Path(
        "/scratch/ticket/4834/assemble/attempt-2/output/manifest.json"
    )
