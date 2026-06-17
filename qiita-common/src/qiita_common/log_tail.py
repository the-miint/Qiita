"""Bounded log-tail reading and OOM-signature detection.

Shared by the compute orchestrator (which enriches a step's
``failure_reason`` with the tail of its SLURM stderr and reclassifies
an otherwise-opaque ``FAILED`` as ``OOM_KILLED``) and the control plane
(which serves a step's stdout/stderr tail over the work-ticket logs
endpoint). One home so the byte/line bounding and the OOM patterns can't
drift between the two sides.
"""

from __future__ import annotations

from pathlib import Path

# Substrings (matched case-insensitively) that a cgroup / kernel OOM kill
# leaves in a step's stderr. A step-level `oom_kill` surfaces to slurmrestd
# only as a job-level FAILED/exit_code=1, so the stderr text is the only
# in-band signal that the kill was a memory kill.
#
# `Killed` is broad — any SIGKILL prints it, not just OOM — and matching it
# upgrades the failure to OOM_KILLED, which is *retriable* (transient=True).
# So a non-memory SIGKILL (e.g. an external `kill` that lands as FAILED) would
# be auto-retried as if more memory could help. That trade is deliberate: for a
# memory-tight bioinformatics step a bare `Killed` almost always *is* an OOM,
# and the match is consulted only on the already-unclassified failure path
# (`_OOM_UPGRADABLE_KINDS` in SlurmBackend.result_step) — never downgrading a
# specific infra kind (NODE_FAIL / TIMEOUT / PREEMPTED).
_OOM_SIGNATURES: tuple[str, ...] = ("oom_kill", "out of memory", "killed")


def read_text_tail(path: Path, *, max_lines: int, max_bytes: int) -> tuple[str, bool]:
    """Return ``(text, truncated)`` for the tail of ``path``, bounded to at
    most ``max_lines`` lines and ``max_bytes`` bytes (whichever bites first).

    ``truncated`` is True when content was dropped from the front. A missing
    or unreadable file yields ``("", False)`` rather than raising — callers
    treat an absent log as "the job never wrote one", not an error. Decoded
    as UTF-8 with ``errors="replace"`` so binary noise in a log can't crash
    the reader.
    """
    try:
        size = path.stat().st_size
    except OSError:
        return "", False
    truncated = False
    try:
        with path.open("rb") as fh:
            if size > max_bytes:
                fh.seek(size - max_bytes)
                truncated = True
            data = fh.read()
    except OSError:
        return "", False
    text = data.decode("utf-8", errors="replace")
    if truncated:
        # We may have seeked into the middle of a line; drop the partial
        # leading fragment so the tail starts on a clean line boundary.
        newline = text.find("\n")
        text = text[newline + 1 :] if newline != -1 else text
    lines = text.splitlines()
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
        truncated = True
    return "\n".join(lines), truncated


def contains_oom_signature(text: str) -> bool:
    """True if ``text`` carries any known OOM-kill signature."""
    lowered = text.lower()
    return any(sig in lowered for sig in _OOM_SIGNATURES)
