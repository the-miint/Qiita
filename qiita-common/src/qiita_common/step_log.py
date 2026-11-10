"""One shape for a native job's two log streams.

**stderr is the step's narrative**: a handful of `<time> <LEVEL> <step>: <message>`
lines — what came in, what each stage kept or dropped, what went out, and how long
it took. Counts and durations, never one line per read or row; WARNING only for
something a person should look at (samples that dropped out, say).

**stdout is the step's receipt**: exactly one JSON line when the step ends,

    {"kind": "completed", "step_name": "denoise", "summary": {"samples": 173, ...}}

or, for an outcome other than success, the same object with `kind` set to
`no_data` or the failure kind and a `reason` in place of `summary`. The summary
holds the counts a job reported with `summarize()`.

A native job gets both from its launcher; a control-plane `action:` gets both from
`capture_to()`, which the runner wraps around the primitive.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# The `kind` of a receipt for a step that succeeded. A step that did not succeed
# carries `no_data` or its failure kind instead.
COMPLETED_KIND = "completed"

# How many members a log line names before it says how many more there are.
_LISTED = 20

STEP_LOG_FORMAT = "%(asctime)s %(levelname)s %(step)s: %(message)s"
STEP_LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"


@dataclass(eq=False)
class StepScope:
    """The step a block of code is running for, and the counts it has reported."""

    step_name: str
    summary: dict[str, Any] = field(default_factory=dict)


_SCOPE: ContextVar[StepScope | None] = ContextVar("qiita_step_scope", default=None)


class _StepFilter(logging.Filter):
    """Stamp `record.step` with the running step's name (the logger name outside
    one), and with `only` set, drop records logged outside that step: on the
    control plane, concurrent tickets share the root logger."""

    def __init__(self, only: StepScope | None = None) -> None:
        super().__init__()
        self._only = only

    def filter(self, record: logging.LogRecord) -> bool:
        scope = _SCOPE.get()
        if self._only is not None and scope is not self._only:
            return False
        record.step = scope.step_name if scope is not None else record.name
        return True


def step_formatter() -> logging.Formatter:
    return logging.Formatter(STEP_LOG_FORMAT, datefmt=STEP_LOG_DATEFMT)


def step_handler(handler: logging.Handler, only: StepScope | None = None) -> logging.Handler:
    """Give `handler` the step-log format and step filter."""
    handler.setFormatter(step_formatter())
    handler.addFilter(_StepFilter(only))
    return handler


@contextmanager
def step_scope(step_name: str) -> Iterator[StepScope]:
    """Run the block as `step_name`: its log lines carry the name and its
    `summarize()` calls land on the yielded scope."""
    scope = StepScope(step_name)
    token = _SCOPE.set(scope)
    try:
        yield scope
    finally:
        _SCOPE.reset(token)


def summarize(**counts: Any) -> None:
    """Add counts to the running step's receipt. Outside a step this does nothing,
    so a helper shared with non-workflow callers may call it freely."""
    scope = _SCOPE.get()
    if scope is not None:
        scope.summary.update(counts)


def listed(items: list[Any]) -> str:
    """`items` for a log line: the first few, then how many more."""
    shown = ", ".join(map(str, items[:_LISTED]))
    rest = len(items) - _LISTED
    return shown + (f", … ({rest} more)" if rest > 0 else "")


def receipt(
    kind: str,
    step_name: str | None,
    *,
    summary: dict[str, Any] | None = None,
    reason: str | None = None,
) -> str:
    """The one JSON line a step writes to stdout when it ends."""
    body: dict[str, Any] = {"kind": kind, "step_name": step_name}
    if summary is not None:
        body["summary"] = summary
    if reason is not None:
        body["reason"] = reason
    return json.dumps(body, default=str)


@contextmanager
def capture_to(logs_dir: Path, step_name: str) -> Iterator[StepScope]:
    """Run the block as `step_name`, appending its log lines to `logs_dir/stderr`.

    For a step that runs inside a long-lived service, where no job scheduler
    captures its output. Records reach the file only from code running in this
    block's context — tasks it starts inherit it, other tickets' do not — and
    only at the root logger's level or above. The caller writes the receipt to
    `logs_dir/stdout` with `write_receipt` once it knows the outcome."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    with step_scope(step_name) as scope:
        handler = step_handler(logging.FileHandler(logs_dir / "stderr", encoding="utf-8"), scope)
        root = logging.getLogger()
        root.addHandler(handler)
        try:
            yield scope
        finally:
            root.removeHandler(handler)
            handler.close()


def write_receipt(logs_dir: Path, line: str) -> None:
    with (logs_dir / "stdout").open("a", encoding="utf-8") as out:
        out.write(line + "\n")
