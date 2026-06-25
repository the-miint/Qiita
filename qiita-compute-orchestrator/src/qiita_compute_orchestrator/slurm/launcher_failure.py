"""Parser for the structured failure line the native-job launcher
(`jobs/__main__.py`) writes to stderr on a non-zero exit.

The launcher prints a single JSON object to stderr just before
returning 1:

    {"kind": "<FailureKind value>", "step_name": "<YAML step name>",
     "reason": "<human-readable detail>"}

`SlurmBackend.result_step` reads the SLURM job's stderr file after a
terminal-but-not-success state and uses this parser to enrich the
`BackendFailure` it raises — without it, the failure that surfaces
on `qiita.work_ticket.failure_reason` would only carry the
slurmrestd-state-based classification ("job FAILED with exit_code=1"),
which is strictly less useful than the launcher's own message.

Container steps don't write this line, so the parser returns None
and SlurmBackend falls back to the state-based classification.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from qiita_common.backend_failure import FailureKind

# Sentinel `kind` value the launcher writes for a terminal no-data outcome (an
# empty FASTQ well). It is deliberately NOT a FailureKind — no_data is a
# terminal-success-ish outcome, not a failure — so `parse_launcher_failure`
# (which only accepts FailureKind values) ignores the line, and the dedicated
# `parse_launcher_no_data` recognizes it instead. Producer side is
# jobs/__main__.py's StepNoData arm.
NO_DATA_MARKER_KIND = "no_data"


@dataclass(frozen=True)
class LauncherFailure:
    """Structured failure detail recovered from the launcher's stderr.
    Field set matches what `jobs/__main__.py` writes."""

    kind: FailureKind
    step_name: str
    reason: str


@dataclass(frozen=True)
class LauncherNoData:
    """Structured no-data detail recovered from the launcher's stderr — the
    terminal no-data outcome (an empty FASTQ well), distinct from a failure."""

    step_name: str
    reason: str


def parse_launcher_failure(stderr_path: Path) -> LauncherFailure | None:
    """Walk `stderr_path` from the end and return the last valid
    structured failure line. None if no such line is present (the
    file is missing/empty, the launcher died before writing, or the
    job was a container step).

    Robust to:
    - the file not existing (job killed before stderr was created)
    - other content surrounding the structured line (asyncio shutdown
      warnings, container stderr, etc.) — the parser walks line-by-line
      and skips anything that isn't a valid JSON object with the
      required fields
    - unknown `kind` values (returns None rather than raise) — keeps
      the parser tolerant of forward-compatible launcher changes
    """
    if not stderr_path.is_file():
        return None
    try:
        text = stderr_path.read_text()
    except OSError:
        return None
    for line in reversed(text.splitlines()):
        line = line.strip()
        if not line or not line.startswith("{"):
            # Cheap pre-filter: the JSON object always starts with `{`.
            # Skips uvicorn-style log prefixes, tracebacks, etc. before
            # paying the json.loads cost.
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        kind_str = obj.get("kind")
        step_name = obj.get("step_name")
        reason = obj.get("reason")
        if not (
            isinstance(kind_str, str) and isinstance(step_name, str) and isinstance(reason, str)
        ):
            continue
        try:
            kind = FailureKind(kind_str)
        except ValueError:
            continue
        return LauncherFailure(kind=kind, step_name=step_name, reason=reason)
    return None


def parse_launcher_no_data(stderr_path: Path) -> LauncherNoData | None:
    """Walk `stderr_path` from the end and return the last structured no-data
    line (the line whose `kind` is `NO_DATA_MARKER_KIND`). None if no such line
    is present. Mirrors `parse_launcher_failure`'s tolerance — missing file,
    surrounding noise, malformed lines are all skipped.

    `SlurmBackend.result_step` calls this BEFORE `parse_launcher_failure` on a
    terminal-but-not-success job so an empty-well no-data exit is reconstructed
    as a StepNoData (→ NO_DATA ticket) rather than a state-based failure."""
    if not stderr_path.is_file():
        return None
    try:
        text = stderr_path.read_text()
    except OSError:
        return None
    for line in reversed(text.splitlines()):
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        if obj.get("kind") != NO_DATA_MARKER_KIND:
            continue
        step_name = obj.get("step_name")
        reason = obj.get("reason")
        if not (isinstance(step_name, str) and isinstance(reason, str)):
            continue
        return LauncherNoData(step_name=step_name, reason=reason)
    return None
