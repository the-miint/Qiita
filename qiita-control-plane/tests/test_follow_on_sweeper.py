"""Unit tests for the follow-on retry sweeper loop.

The pass itself (`reconcile_follow_ons`) is exercised against a real database in
tests/routes/test_work_ticket.py; here we only pin the loop's contract: it runs
the reconcile once per configured interval, survives a failing pass, and ends
cleanly on cancellation.
"""

import asyncio
from types import SimpleNamespace

import pytest

import qiita_control_plane.follow_on_sweeper as sweeper_mod


async def test_follow_on_sweeper_loops_survives_a_bad_pass_and_cancels():
    """A failing reconcile pass logs and the loop continues to the next; each
    pass sleeps the configured interval; a CancelledError ends the task."""
    reconcile_calls = []

    async def _fake_reconcile(app):
        reconcile_calls.append(app)
        if len(reconcile_calls) == 1:
            raise RuntimeError("transient reconcile failure")  # the loop must survive this

    sleeps = []

    async def _fake_sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) >= 3:
            raise asyncio.CancelledError  # end the loop after two full passes

    app = object()
    settings = SimpleNamespace(follow_on_sweep_interval_seconds=42)

    with pytest.MonkeyPatch.context() as m:
        m.setattr(sweeper_mod, "reconcile_follow_ons", _fake_reconcile)
        m.setattr(sweeper_mod.asyncio, "sleep", _fake_sleep)
        with pytest.raises(asyncio.CancelledError):
            await sweeper_mod.run_follow_on_sweeper(app, settings)

    assert reconcile_calls == [app, app]  # ran a second pass after the first raised
    assert sleeps == [42, 42, 42]  # sleeps before each pass; the third ends the loop
