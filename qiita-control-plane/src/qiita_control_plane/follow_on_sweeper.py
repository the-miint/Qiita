"""In-process sweeper that retries un-submitted follow-ons on an interval.

A follow-on whose submission failed with something other than a definitive
refusal — a 5xx, a database error — records no outcome on its parent and is left
for `reconcile_follow_ons` to resubmit. The startup hook runs that reconcile once;
without this sweeper the only *other* retry is the next control-plane start, so a
transient failure could leave a completed ticket's follow-on un-submitted for an
arbitrarily long window. This sweeper runs the same reconcile once per
`FOLLOW_ON_SWEEP_INTERVAL_SECONDS`, bounding that window to roughly one interval.

Retries are uncapped by design: a non-refusal failure is treated as transient and
retried every interval. A follow-on that keeps failing (a persistent 5xx or
database error) is therefore retried indefinitely, logging each pass — a stuck
follow-on is a real fault worth surfacing, not one to silently cap. A refusal (a
4xx) is recorded and never retried, so it does not accumulate here.

No advisory lock, unlike the notify and cli_login_code sweepers: those guard a
scan/DELETE that a second runner would merely duplicate wastefully, whereas a
follow-on submission is create-at-most-once (the parent's outcome is written in
the new ticket's INSERT transaction, and a racing submit raises
`FollowOnAlreadyRecordedError`). An overlapping reconcile — a sweep pass meeting
the startup reconcile, or a stray second process — therefore finds nothing to do
or loses the race safely, so the pass needs no lock and holds no connection
across its submits.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from .follow_on import reconcile_follow_ons

if TYPE_CHECKING:
    from fastapi import FastAPI

    from .config import Settings

_log = logging.getLogger(__name__)


async def run_follow_on_sweeper(app: FastAPI, settings: Settings) -> None:
    """Long-lived loop: one `reconcile_follow_ons` per
    `FOLLOW_ON_SWEEP_INTERVAL_SECONDS`, so a transiently-failed follow-on is
    retried within an interval instead of waiting for the next CP start.

    Sleeps before the first pass: the lifespan already runs one reconcile at
    startup, so an immediate pass here would only repeat it. A bad pass logs and
    continues — the loop must outlive any single failure. Cancelled at shutdown
    (the CancelledError propagates out to end the task).
    """
    _log.info(
        "follow-on sweeper started (interval=%ds)",
        settings.follow_on_sweep_interval_seconds,
    )
    while True:
        await asyncio.sleep(settings.follow_on_sweep_interval_seconds)
        try:
            await reconcile_follow_ons(app)
        except asyncio.CancelledError:
            raise
        except Exception:
            _log.exception("follow-on sweep pass failed; continuing")
