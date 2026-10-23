"""`qiita ticket redrive` — select tickets with the `ticket list` filters and
redrive each through POST /work-ticket/{idx}/run, one at a time."""

import json

import httpx
import pytest
from qiita_common.api_paths import (
    URL_WORK_TICKET_BY_IDX,
    URL_WORK_TICKET_LIST,
    URL_WORK_TICKET_RUN,
)

from qiita_control_plane.cli import _common
from qiita_control_plane.cli.user import main

_BASE = "https://q.example.test"


def _ticket(idx: int, *, failure_type: str = "permanent") -> dict:
    return {
        "work_ticket_idx": idx,
        "action_id": "read-mask",
        "action_version": "1.0.0",
        "originator_principal_idx": 7,
        "scope_target": {"kind": "prep_sample", "prep_sample_idx": 55},
        "action_context": {},
        "state": "failed",
        "retry_count": 0,
        "max_retries": 3,
        "failure_type": failure_type,
        "failure_stage": "step_run",
        "failure_step_name": "qc",
        "failure_reason": "boom",
        "created_at": "2026-05-20T00:00:00+00:00",
        "updated_at": "2026-05-20T00:00:01+00:00",
    }


def _stub(monkeypatch, *, tickets, run_status=None, truncated=False, states=None):
    """Answer the list GET with `tickets`, each /run POST with the status
    `run_status` maps its idx to (default 200), and each single-ticket GET with
    the next of the states `states` lists for its idx. Returns the request log."""
    run_status = run_status or {}
    states = states or {}
    log: list[tuple[str, str, dict | None]] = []

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        log.append((method, url, params))
        request = httpx.Request(method, url)
        if method == "GET" and url == f"{_BASE}{URL_WORK_TICKET_LIST}":
            body = {"tickets": tickets, "count": len(tickets), "truncated": truncated}
            return httpx.Response(200, json=body, request=request)
        for ticket in tickets:
            idx = ticket["work_ticket_idx"]
            if (
                method == "GET"
                and url == f"{_BASE}{URL_WORK_TICKET_BY_IDX.format(work_ticket_idx=idx)}"
            ):
                body = {**ticket, "state": states[idx].pop(0)}
                return httpx.Response(200, json=body, request=request)
            if (
                method == "POST"
                and url == f"{_BASE}{URL_WORK_TICKET_RUN.format(work_ticket_idx=idx)}"
            ):
                status = run_status.get(idx, 200)
                body = {"idx": idx, "state": "pending"} if status == 200 else {"detail": "no"}
                return httpx.Response(status, json=body, request=request)
        raise AssertionError(f"unexpected request {method} {url}")

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test_user")
    return log


def _posts(log) -> list[str]:
    return [url for method, url, _ in log if method == "POST"]


def test_redrive_is_a_dry_run_by_default(monkeypatch, capsys):
    log = _stub(monkeypatch, tickets=[_ticket(11), _ticket(12)])

    rc = main(["--base-url", _BASE, "ticket", "redrive", "--action-id", "read-mask"])

    assert rc == 0
    assert _posts(log) == []
    assert log[0][2] == {"state": "failed", "action_id": "read-mask"}
    report = json.loads(capsys.readouterr().out)
    assert report["mode"] == "dry-run"
    assert [t["work_ticket_idx"] for t in report["selected"]] == [11, 12]


def test_redrive_execute_runs_each_selected_ticket_in_turn(monkeypatch, capsys):
    log = _stub(
        monkeypatch,
        tickets=[_ticket(11), _ticket(12, failure_type="retriable"), _ticket(13)],
    )

    rc = main(
        ["--base-url", _BASE, "ticket", "redrive", "--failure-type", "permanent", "--execute"]
    )

    assert rc == 0
    assert _posts(log) == [
        f"{_BASE}{URL_WORK_TICKET_RUN.format(work_ticket_idx=11)}",
        f"{_BASE}{URL_WORK_TICKET_RUN.format(work_ticket_idx=13)}",
    ]
    report = json.loads(capsys.readouterr().out)
    assert [r["idx"] for r in report["redriven"]] == [11, 13]
    assert report["refused"] == []


def test_redrive_records_a_refused_ticket_and_goes_on(monkeypatch, capsys):
    log = _stub(monkeypatch, tickets=[_ticket(11), _ticket(12)], run_status={11: 409})

    rc = main(["--base-url", _BASE, "ticket", "redrive", "--execute"])

    assert rc == 1
    assert len(_posts(log)) == 2
    out, err = capsys.readouterr()
    assert out == ""
    report = json.loads(err)
    assert [r["work_ticket_idx"] for r in report["refused"]] == [11]
    assert [r["idx"] for r in report["redriven"]] == [12]


def test_redrive_stops_on_an_answer_that_is_not_about_the_ticket(monkeypatch, capsys):
    log = _stub(monkeypatch, tickets=[_ticket(11), _ticket(12), _ticket(13)], run_status={12: 503})

    rc = main(["--base-url", _BASE, "ticket", "redrive", "--execute"])

    assert rc == 1
    assert len(_posts(log)) == 2
    out, err = capsys.readouterr()
    assert out == ""
    report = json.loads(err)
    assert report["stopped"]["work_ticket_idx"] == 12
    assert report["stopped"]["status"] == 503
    assert report["not_attempted"] == [13]


def test_redrive_refuses_a_truncated_selection(monkeypatch, capsys):
    log = _stub(monkeypatch, tickets=[_ticket(11)], truncated=True)

    rc = main(["--base-url", _BASE, "ticket", "redrive", "--limit", "1", "--execute"])

    assert rc == 1
    assert _posts(log) == []
    assert "raise --limit" in capsys.readouterr().err


def test_redrive_sleeps_between_redrives(monkeypatch):
    _stub(monkeypatch, tickets=[_ticket(11), _ticket(12), _ticket(13)])
    sleeps: list[float] = []
    monkeypatch.setattr("qiita_control_plane.cli.user.ticket.time.sleep", sleeps.append)

    rc = main(["--base-url", _BASE, "ticket", "redrive", "--execute", "--rate", "2.5"])

    assert rc == 0
    assert sleeps == [2.5, 2.5]


def test_redrive_rejects_a_negative_rate(monkeypatch):
    _stub(monkeypatch, tickets=[])
    with pytest.raises(SystemExit):
        main(["--base-url", _BASE, "ticket", "redrive", "--rate", "-1"])


def test_redrive_sends_the_failure_type_to_the_server(monkeypatch, capsys):
    log = _stub(monkeypatch, tickets=[_ticket(11)])

    main(["--base-url", _BASE, "ticket", "redrive", "--failure-type", "retriable"])

    assert log[0][2]["failure_type"] == "retriable"


def test_redrive_waits_while_max_inflight_tickets_are_unfinished(monkeypatch, capsys):
    """With --max-inflight 1, the second redrive waits until the first ticket is
    no longer pending, queued or processing."""
    log = _stub(
        monkeypatch,
        tickets=[_ticket(11), _ticket(12)],
        states={11: ["processing", "completed"]},
    )
    sleeps: list[float] = []
    monkeypatch.setattr("qiita_control_plane.cli.user.ticket.time.sleep", sleeps.append)

    rc = main(
        [
            "--base-url",
            _BASE,
            "ticket",
            "redrive",
            "--execute",
            "--max-inflight",
            "1",
            "--poll-seconds",
            "3",
        ]
    )

    assert rc == 0
    calls = [(method, url) for method, url, _ in log]
    run_12 = calls.index(("POST", f"{_BASE}{URL_WORK_TICKET_RUN.format(work_ticket_idx=12)}"))
    reads_11 = [
        i
        for i, call in enumerate(calls)
        if call == ("GET", f"{_BASE}{URL_WORK_TICKET_BY_IDX.format(work_ticket_idx=11)}")
    ]
    assert len(reads_11) == 2 and max(reads_11) < run_12
    assert sleeps == [3.0, 3.0]


def test_redrive_rejects_a_max_inflight_below_one(monkeypatch):
    _stub(monkeypatch, tickets=[])
    with pytest.raises(SystemExit):
        main(["--base-url", _BASE, "ticket", "redrive", "--max-inflight", "0"])
