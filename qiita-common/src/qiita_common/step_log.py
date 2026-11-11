"""One shape for a step's two log streams — a native job's, and a control-plane
`action:`'s.

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

from qiita_common.log import AuthorizationScrubFilter

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
    block's context — tasks it starts inherit it, other tickets' do not — and only
    at the emitting logger's effective level or above. The file is scrubbed of
    `Authorization` values, because it is served to the ticket originator (the
    boot-time scrubber covers only the handlers present at boot, not this one). The
    caller writes the receipt to `logs_dir/stdout` with `write_receipt` once it knows
    the outcome.

    The log file is best-effort: if it cannot be opened the block still runs and its
    outcome is still recorded — a step is not failed because its log could not be
    written."""
    with step_scope(step_name) as scope:
        handler: logging.Handler | None = None
        try:
            logs_dir.mkdir(parents=True, exist_ok=True)
            handler = step_handler(
                logging.FileHandler(logs_dir / "stderr", encoding="utf-8"), scope
            )
            handler.addFilter(AuthorizationScrubFilter())
            logging.getLogger().addHandler(handler)
        except OSError:
            logging.getLogger(__name__).warning(
                "step log capture unavailable for %s", step_name, exc_info=True
            )
            handler = None
        try:
            yield scope
        finally:
            if handler is not None:
                logging.getLogger().removeHandler(handler)
                handler.close()


@contextmanager
def no_step_scope() -> Iterator[None]:
    """Run the block outside any step scope. For a task dispatched from within a
    step's scope that is its own unit of work — a fan-out child ticket — so its log
    lines and `summarize()` calls are not attributed to the dispatching step:
    `asyncio.create_task` copies the scheduling context, step scope and all."""
    token = _SCOPE.set(None)
    try:
        yield
    finally:
        _SCOPE.reset(token)


def write_receipt(logs_dir: Path, line: str) -> None:
    """Append the step's receipt line to `logs_dir/stdout`. Best-effort: a failure to
    write the receipt file must not mask the step's real outcome, which the caller
    records separately (the progress row)."""
    try:
        with (logs_dir / "stdout").open("a", encoding="utf-8") as out:
            out.write(line + "\n")
    except OSError:
        logging.getLogger(__name__).warning(
            "could not write step receipt to %s", logs_dir, exc_info=True
        )
