"""Unit tests for `qiita-admin alignment rerun-blocks`: one batch, never a second POST,
an ambiguous reply stops the batch and is reported unknown, and the wait goes on
through warnings and through GET errors until every ticket is terminal. The control
plane is a fake behind `_common.call`."""

import argparse
import itertools

import httpx
import pytest

from qiita_control_plane.actions.align_rerun import NOT_COMMITTED_REASONS
from qiita_control_plane.cli import _common
from qiita_control_plane.cli.admin import alignment_rerun as rr

_ALN = 3
_BASE_TS = "2026-11-03 19:47:26.465866-08"
_SAME_TS = "2026-11-04T03:47:26.465866+00:00"  # the same instant, as the API writes it
_LATER_TS = "2026-11-10T21:00:00+00:00"


class _FakeCP:
    """Tickets by idx; POST outcomes by idx; each GET after a ticket's pre-check
    walks `states`, after first raising `get_errors` connection errors."""

    def __init__(self, tickets, post=None, states=None, get_errors=0):
        self.tickets = tickets
        self.post = post or {}
        self.states = {k: iter(v) for k, v in (states or {}).items()}
        self.get_errors = get_errors
        self.posts: list[int] = []
        self.timeouts: list[float] = []
        self.prechecked: set[int] = set()

    def __call__(self, method, base_url, token, path, *, json=None, params=None, timeout=10):
        if method == "POST":
            idx = json["work_ticket_idx"]
            self.posts.append(idx)
            self.timeouts.append(timeout)
            outcome = self.post.get(idx, "ok")
            if outcome == "ok":
                return {"work_ticket_idx": idx}
            if outcome == "timeout":
                raise httpx.ReadTimeout("slow")
            status, reason = outcome if isinstance(outcome, tuple) else (outcome, "x")
            request = httpx.Request("POST", "http://cp")
            response = httpx.Response(status, json={"detail": {"reason": reason}}, request=request)
            raise httpx.HTTPStatusError("refused", request=request, response=response)
        idx = int(path.rsplit("/", 1)[1])
        t = dict(self.tickets[idx])
        # The first GET of a ticket is the pre-check; the rest are the wait.
        if idx not in self.prechecked:
            self.prechecked.add(idx)
            return t
        if self.get_errors:
            self.get_errors -= 1
            raise httpx.ConnectError("control plane restarting")
        it = self.states.get(idx)
        if it is not None:
            t["state"] = next(it, t["state"])
        return t


def _ticket(idx, *, state="completed", updated_at=_SAME_TS, alignment=_ALN, action="align"):
    return {
        "work_ticket_idx": idx,
        "action_id": action,
        "state": state,
        "updated_at": updated_at,
        "action_context": {"alignment_idx": alignment},
    }


def _args(tmp_path, idxs, **kw):
    baseline = tmp_path / "baseline.csv"
    baseline.write_text(
        "work_ticket_idx,state,updated_at\n" + "".join(f"{i},completed,{_BASE_TS}\n" for i in idxs)
    )
    defaults = dict(
        base_url="http://cp",
        alignment_idx=_ALN,
        work_ticket_idx=idxs,
        reason="r",
        baseline_file=baseline,
        allow_deprecated_mask=False,
        force_rearm=False,
        wait_only=False,
        execute=True,
        pending_warn_min=30.0,
        poll_seconds=60.0,
        wait_deadline_h=48.0,
    )
    defaults.update(kw)
    return argparse.Namespace(**defaults)


def _run(monkeypatch, cp, args, clock_step=60.0, sleeps=None):
    monkeypatch.setattr(_common, "call", cp)
    ticks = itertools.count(0.0, clock_step)
    lines: list[str] = []
    sleeps = [] if sleeps is None else sleeps
    report, ok = rr.run_batch(
        args, "tok", sleep=sleeps.append, clock=lambda: next(ticks), emit=lines.append
    )
    return report, ok, lines


def test_a_batch_posts_each_ticket_once_and_waits_until_terminal(monkeypatch, tmp_path):
    cp = _FakeCP(
        {1: _ticket(1), 2: _ticket(2)},
        states={1: ["pending", "processing", "completed"], 2: ["processing", "completed"]},
    )
    report, ok, lines = _run(monkeypatch, cp, _args(tmp_path, [1, 2]))
    assert ok
    assert cp.posts == [1, 2]
    assert all(t > _common.CLI_HTTP_TIMEOUT_SECONDS for t in cp.timeouts)
    assert {i: t["final"]["state"] for i, t in report["tickets"].items()} == {
        1: "completed",
        2: "completed",
    }
    # Each response is printed as it comes back.
    assert len([line for line in lines if '"status": 200' in line]) == 2


def test_dry_run_posts_nothing_and_flags_mismatches(monkeypatch, tmp_path):
    cp = _FakeCP({1: _ticket(1), 2: _ticket(2, alignment=2), 3: _ticket(3, updated_at=_LATER_TS)})
    report, ok, _ = _run(monkeypatch, cp, _args(tmp_path, [1, 2, 3], execute=False))
    assert not ok
    assert cp.posts == []
    pre = {i: t["pre_check"] for i, t in report["tickets"].items()}
    assert pre[1]["matches"] and not pre[1]["changed_since_baseline"]
    assert not pre[2]["matches"]
    assert pre[3]["changed_since_baseline"]


def test_a_ticket_changed_since_the_baseline_is_not_posted_without_force(monkeypatch, tmp_path):
    cp = _FakeCP({1: _ticket(1, updated_at=_LATER_TS)}, states={1: ["completed"]})
    report, ok, _ = _run(monkeypatch, cp, _args(tmp_path, [1]))
    assert not ok
    assert cp.posts == []
    assert report["tickets"][1]["post"] == {"refused_locally": "changed_since_baseline"}

    cp = _FakeCP({1: _ticket(1), 2: _ticket(2)}, states={1: ["completed"]})
    args = _args(tmp_path, [1, 2])
    args.baseline_file.write_text(f"work_ticket_idx,state,updated_at\n1,completed,{_BASE_TS}\n")
    report, ok, _ = _run(monkeypatch, cp, args)
    assert not ok
    assert cp.posts == [1]
    assert report["tickets"][2]["post"] == {"refused_locally": "not_in_baseline"}

    cp = _FakeCP({1: _ticket(1, updated_at=_LATER_TS)}, states={1: ["completed"]})
    _report, ok, _ = _run(monkeypatch, cp, _args(tmp_path, [1], force_rearm=True))
    assert ok
    assert cp.posts == [1]


def test_a_refusal_is_reported_and_the_batch_goes_on_without_a_second_post(monkeypatch, tmp_path):
    cp = _FakeCP({1: _ticket(1), 2: _ticket(2)}, post={1: 409}, states={2: ["completed"]})
    report, ok, _ = _run(monkeypatch, cp, _args(tmp_path, [1, 2]))
    assert not ok
    assert cp.posts == [1, 2]
    assert report["tickets"][1]["post"]["status"] == 409
    assert report["tickets"][2]["final"]["state"] == "completed"
    assert report["unknown"] == []


@pytest.mark.parametrize("outcome", [502, "timeout"])
def test_an_ambiguous_reply_stops_the_batch_and_is_reported_unknown(monkeypatch, tmp_path, outcome):
    cp = _FakeCP(
        {1: _ticket(1), 2: _ticket(2), 3: _ticket(3)},
        post={2: outcome},
        states={1: ["completed"]},
    )
    report, ok, _ = _run(monkeypatch, cp, _args(tmp_path, [1, 2, 3]))
    assert not ok
    assert cp.posts == [1, 2]
    assert report["unknown"] == [2]
    assert report["unknown_decide_after_s"] > 0
    assert report["not_attempted"] == [3]
    # The ticket that did get through is still waited on; the unknown one is not judged.
    assert report["tickets"][1]["final"]["state"] == "completed"
    assert "final" not in report["tickets"][2]


@pytest.mark.parametrize("reason", sorted(NOT_COMMITTED_REASONS))
def test_a_503_that_did_not_commit_is_a_refusal_and_the_batch_goes_on(
    monkeypatch, tmp_path, reason
):
    cp = _FakeCP({1: _ticket(1), 2: _ticket(2)}, post={1: (503, reason)}, states={2: ["completed"]})
    report, ok, _ = _run(monkeypatch, cp, _args(tmp_path, [1, 2]))
    assert not ok
    assert cp.posts == [1, 2]
    assert report["unknown"] == []
    assert report["tickets"][1]["post"]["status"] == 503
    assert report["tickets"][2]["final"]["state"] == "completed"


def test_a_long_pending_ticket_is_warned_about_repeatedly_and_still_waited_on(
    monkeypatch, tmp_path
):
    """The clock moves 60 s per reading, one reading per poll, and the warning interval
    is 30 min: 100 pending polls warn at minutes 31, 62 and 93."""
    cp = _FakeCP({1: _ticket(1)}, states={1: ["pending"] * 100 + ["completed"]})
    report, ok, lines = _run(monkeypatch, cp, _args(tmp_path, [1]))
    assert ok
    warnings = [line for line in lines if "still pending" in line]
    assert len(warnings) == 3
    assert "queued behind the dispatch cap" in warnings[0]
    assert report["tickets"][1]["final"]["state"] == "completed"


def test_get_errors_while_waiting_are_retried_with_backoff(monkeypatch, tmp_path):
    cp = _FakeCP({1: _ticket(1)}, states={1: ["completed"]}, get_errors=12)
    sleeps: list[float] = []
    report, ok, lines = _run(monkeypatch, cp, _args(tmp_path, [1]), sleeps=sleeps)
    assert ok
    assert report["tickets"][1]["final"]["state"] == "completed"
    assert len([line for line in lines if "failed" in line]) == 12
    assert sleeps[:4] == [120.0, 240.0, 480.0, rr._MAX_BACKOFF_S]
    assert max(sleeps) == rr._MAX_BACKOFF_S


def test_only_unbroken_get_failures_reach_the_wait_deadline(monkeypatch, tmp_path):
    """A ticket running past the deadline is still waited on; GETs failing for longer
    than it raise with a resume hint. The clock moves 10 min per reading."""
    cp = _FakeCP({1: _ticket(1)}, states={1: ["processing"] * 50 + ["completed"]})
    _report, ok, _ = _run(monkeypatch, cp, _args(tmp_path, [1], wait_deadline_h=1.0), 600.0)
    assert ok

    cp = _FakeCP({1: _ticket(1)}, get_errors=10_000)
    report, ok, _ = _run(monkeypatch, cp, _args(tmp_path, [1], wait_deadline_h=1.0), 600.0)
    assert not ok
    assert "--wait-only" in report["wait_stopped"]
    assert report["tickets"][1]["post"]["status"] == 200


class _FlakyGets(_FakeCP):
    """After the pre-checks, `fail(idx, n)` says whether the n-th wait GET of idx fails."""

    def __init__(self, tickets, fail, **kw):
        super().__init__(tickets, **kw)
        self.fail = fail
        self.n: dict[int, int] = {}

    def __call__(self, method, base_url, token, path, **kw):
        idx = int(path.rsplit("/", 1)[1]) if method == "GET" else None
        if idx in self.prechecked:
            self.n[idx] = self.n.get(idx, 0) + 1
            if self.fail(idx, self.n[idx]):
                raise httpx.ConnectError("flaky")
        return super().__call__(method, base_url, token, path, **kw)


def test_a_clean_round_restarts_the_wait_deadline(monkeypatch, tmp_path):
    """Every other round fails, for 50 rounds at 10 min per clock reading: far past a
    1 h deadline in total, never 1 h without a clean round."""
    cp = _FlakyGets(
        {1: _ticket(1)}, lambda _i, n: n % 2 == 1, states={1: ["processing"] * 25 + ["completed"]}
    )
    _report, ok, _ = _run(monkeypatch, cp, _args(tmp_path, [1], wait_deadline_h=1.0), 600.0)
    assert ok


def test_one_tickets_failing_gets_reach_the_deadline_while_another_answers(monkeypatch, tmp_path):
    cp = _FlakyGets(
        {1: _ticket(1), 2: _ticket(2)},
        lambda i, _n: i == 2,
        states={1: ["processing"] * 10_000},
    )
    report, ok, _ = _run(monkeypatch, cp, _args(tmp_path, [1, 2], wait_deadline_h=1.0), 600.0)
    assert not ok
    assert "--wait-only" in report["wait_stopped"]


def test_wait_only_does_not_count_a_ticket_that_was_never_rearmed(monkeypatch, tmp_path):
    """A batch stopped before its POSTs: the unposted ticket is still the original
    run's `completed`, unchanged since the baseline."""
    cp = _FakeCP({1: _ticket(1), 2: _ticket(2, updated_at=_LATER_TS)}, states={2: ["completed"]})
    report, ok, _ = _run(monkeypatch, cp, _args(tmp_path, [1, 2], wait_only=True, execute=False))
    assert not ok
    assert report["not_rearmed"] == [1]


def test_a_4xx_while_waiting_stops_at_once_and_keeps_the_report(monkeypatch, tmp_path):
    cp = _FakeCP({1: _ticket(1), 2: _ticket(2)}, post={2: 409}, states={1: ["processing"]})
    calls = {"n": 0}

    def get_then_401(method, base_url, token, path, **kw):
        if method == "GET" and cp.prechecked >= {1, 2}:
            calls["n"] += 1
            request = httpx.Request("GET", "http://cp")
            raise httpx.HTTPStatusError(
                "expired", request=request, response=httpx.Response(401, request=request)
            )
        return cp(method, base_url, token, path, **kw)

    monkeypatch.setattr(_common, "call", get_then_401)
    report, ok = rr.run_batch(
        _args(tmp_path, [1, 2]), "tok", sleep=lambda _s: None, clock=lambda: 0.0, emit=print
    )
    assert not ok
    assert calls["n"] == 1
    assert "401" in report["wait_stopped"]
    assert report["tickets"][2]["post"]["status"] == 409


def test_wait_only_posts_nothing_and_reports(monkeypatch, tmp_path):
    cp = _FakeCP({1: _ticket(1, updated_at=_LATER_TS)}, states={1: ["failed"]})
    report, ok, _ = _run(monkeypatch, cp, _args(tmp_path, [1], wait_only=True, execute=False))
    assert cp.posts == []
    assert report["mode"] == "wait-only"
    assert report["tickets"][1]["final"]["state"] == "failed"
    assert not ok


def test_more_than_six_tickets_or_repeats_are_refused(capsys):
    parser = argparse.ArgumentParser()
    ns = argparse.Namespace(work_ticket_idx=list(range(1, 8)), execute=True, wait_only=False)
    with pytest.raises(SystemExit):
        rr._handle_rerun_blocks(ns, parser)
    ns = argparse.Namespace(work_ticket_idx=[1, 1], execute=True, wait_only=False)
    with pytest.raises(SystemExit):
        rr._handle_rerun_blocks(ns, parser)
