"""CLI-side tests for `qiita submit-ena-import` / `qiita ena-import-status`.

Local validation (source refusal, accession shape) is exercised through the
argparse shim directly; wire behavior (POST body, poll progression, exit
codes) patches `httpx.AsyncClient` to use a `httpx.MockTransport`, mirroring
`test_submit_reads.py`. `ena-import-status` is a plain GET, tested the way
`test_user_cli.py::test_ticket_status_*` tests `ticket status`.
"""

from __future__ import annotations

import json

import httpx
import pytest
from qiita_common.api_paths import URL_ENA_IMPORT_BATCH_BY_IDX


def _item(accession: str, state: str, **extra) -> dict:
    base = {
        "ena_study_accession": accession,
        "state": state,
        "study_idx": None,
        "failure_reason": None,
        "download_work_ticket_idxs": [],
        "ena_runs": [],
    }
    base.update(extra)
    return base


def _make_handler(*, post_status=202, post_body=None, get_bodies=None):
    """Scripts the ena-import-batch POST and successive GET polls. `calls`
    records every request as (method, path, json_body) in order."""
    calls: list[tuple[str, str, dict | None]] = []
    remaining_gets = list(get_bodies or [])

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else None
        calls.append((request.method, request.url.path, body))
        if request.method == "POST":
            return httpx.Response(post_status, json=post_body)
        return httpx.Response(200, json=remaining_gets.pop(0))

    return handler, calls


def _forbidden_handler(request: httpx.Request) -> httpx.Response:
    raise AssertionError(f"unexpected HTTP call to {request.url}")


@pytest.fixture
def patch_async_client(monkeypatch):
    """Redirects every `httpx.AsyncClient(...)` construction inside the CLI
    to a `MockTransport`, so `_run_submit_ena_import`'s real client
    construction is exercised without a live control plane."""
    real_async_client = httpx.AsyncClient

    def _patch(transport):
        def _fake(*args, **kwargs):
            kwargs["transport"] = transport
            return real_async_client(*args, **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", _fake)

    return _patch


# ---------------------------------------------------------------------------
# Argparse shim: defaults, flags, source refusals
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("argv_extra", "expected"),
    [
        (
            ["PRJEB11419"],
            {"no_watch": False, "poll_interval_seconds": 2.0, "timeout_seconds": 24 * 3600},
        ),
        (["PRJEB11419", "--no-watch"], {"no_watch": True}),
        (["PRJEB11419", "--poll-interval-seconds", "0.5"], {"poll_interval_seconds": 0.5}),
        (["PRJEB11419", "--timeout-seconds", "10"], {"timeout_seconds": 10.0}),
    ],
)
def test_parser_defaults_and_flags(argv_extra, expected):
    from qiita_control_plane.cli.user._parser import _build_parser

    ns = _build_parser().parse_args(["submit-ena-import", *argv_extra])
    for key, value in expected.items():
        assert getattr(ns, key) == value


@pytest.mark.parametrize(
    ("flag", "raw"),
    [
        ("--poll-interval-seconds", "0"),
        ("--poll-interval-seconds", "-5"),
        ("--poll-interval-seconds", "nan"),
        ("--poll-interval-seconds", "inf"),
        ("--timeout-seconds", "-1"),
        ("--timeout-seconds", "nan"),
        ("--timeout-seconds", "inf"),
    ],
)
def test_parser_rejects_out_of_range_watch_flags(flag, raw, capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    with pytest.raises(SystemExit) as exc_info:
        _build_parser().parse_args(["submit-ena-import", "PRJEB11419", flag, raw])
    assert exc_info.value.code == 2
    assert flag in capsys.readouterr().err


@pytest.mark.parametrize(
    ("flag", "raw", "expected"),
    [
        ("--timeout-seconds", "0", 0.0),
        ("--poll-interval-seconds", "0.5", 0.5),
    ],
)
def test_parser_accepts_boundary_watch_flags(flag, raw, expected):
    from qiita_control_plane.cli.user._parser import _build_parser

    ns = _build_parser().parse_args(["submit-ena-import", "PRJEB11419", flag, raw])
    dest = flag.lstrip("-").replace("-", "_")
    assert getattr(ns, dest) == expected


def test_handler_refuses_no_source(capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    parser = _build_parser()
    ns = parser.parse_args(["submit-ena-import"])
    with pytest.raises(SystemExit) as exc_info:
        ns.handler(ns, parser)
    assert exc_info.value.code == 2
    assert "accession" in capsys.readouterr().err.lower()


def test_handler_refuses_positionals_and_from_file_together(tmp_path, capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    accession_file = tmp_path / "accessions.txt"
    accession_file.write_text("PRJEB11419\n")
    parser = _build_parser()
    ns = parser.parse_args(["submit-ena-import", "PRJEB11419", "--from-file", str(accession_file)])
    with pytest.raises(SystemExit) as exc_info:
        ns.handler(ns, parser)
    assert exc_info.value.code == 2
    assert "mutually exclusive" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# --from-file: comment/blank skipping, exact POST order
# ---------------------------------------------------------------------------


def test_from_file_skips_blank_and_comment_lines_and_posts_in_order(
    tmp_path, monkeypatch, patch_async_client
):
    from qiita_control_plane.cli.user._parser import _build_parser

    accession_file = tmp_path / "accessions.txt"
    accession_file.write_text("PRJEB11419\n\n  \n# a comment\nPRJNA555783\n   PRJDB4321   \n")
    post_body = {
        "ena_import_batch_idx": 7,
        "items": [
            _item("PRJEB11419", "pending"),
            _item("PRJNA555783", "pending"),
            _item("PRJDB4321", "pending"),
        ],
    }
    handler, calls = _make_handler(post_body=post_body)
    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        [
            "--base-url",
            "https://q.example.test",
            "submit-ena-import",
            "--from-file",
            str(accession_file),
            "--no-watch",
        ]
    )
    rc = ns.handler(ns, parser)

    assert rc == 0
    posts = [b for m, _p, b in calls if m == "POST"]
    assert posts == [{"accessions": ["PRJEB11419", "PRJNA555783", "PRJDB4321"]}]


def test_handler_refuses_comments_only_file(tmp_path, capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    accession_file = tmp_path / "accessions.txt"
    accession_file.write_text("# just a comment\n\n   \n")
    parser = _build_parser()
    ns = parser.parse_args(["submit-ena-import", "--from-file", str(accession_file)])
    with pytest.raises(SystemExit) as exc_info:
        ns.handler(ns, parser)
    assert exc_info.value.code == 2
    assert "no accessions" in capsys.readouterr().err


def test_handler_refuses_a_missing_file(tmp_path, capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    missing = tmp_path / "nope.txt"
    parser = _build_parser()
    ns = parser.parse_args(["submit-ena-import", "--from-file", str(missing)])
    with pytest.raises(SystemExit) as exc_info:
        ns.handler(ns, parser)
    assert exc_info.value.code == 2
    assert str(missing) in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Accession validation, before any HTTP call
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("accession", "expected_message"),
    [
        ("NOTANACCESSION", "does not match a known"),
        ("SRR1234567", "not a study accession"),
    ],
)
def test_handler_refuses_invalid_accession_before_any_http_call(
    accession, expected_message, monkeypatch, patch_async_client, capsys
):
    from qiita_control_plane.cli.user._parser import _build_parser

    patch_async_client(httpx.MockTransport(_forbidden_handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(["submit-ena-import", "PRJEB11419", accession])
    with pytest.raises(SystemExit) as exc_info:
        ns.handler(ns, parser)
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert accession in err
    assert expected_message in err


# ---------------------------------------------------------------------------
# Wire behavior: submit, watch, exit codes
# ---------------------------------------------------------------------------


def test_no_watch_posts_once_and_does_not_poll(monkeypatch, patch_async_client, capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    post_body = {"ena_import_batch_idx": 42, "items": [_item("PRJEB11419", "pending")]}
    handler, calls = _make_handler(post_body=post_body)
    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        ["--base-url", "https://q.example.test", "submit-ena-import", "PRJEB11419", "--no-watch"]
    )
    rc = ns.handler(ns, parser)

    assert rc == 0
    assert [m for m, _p, _b in calls] == ["POST"]
    assert calls[0][2] == {"accessions": ["PRJEB11419"]}
    out, err = capsys.readouterr()
    assert "42" in out
    assert "42" in err


def test_watch_polls_until_all_items_terminal(monkeypatch, patch_async_client, capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    post_body = {"ena_import_batch_idx": 5, "items": [_item("PRJEB11419", "pending")]}
    get_bodies = [
        {
            "ena_import_batch_idx": 5,
            "items": [_item("PRJEB11419", "downloading", download_work_ticket_idxs=[99])],
        },
        {
            "ena_import_batch_idx": 5,
            "items": [_item("PRJEB11419", "done", download_work_ticket_idxs=[99])],
        },
    ]
    handler, calls = _make_handler(post_body=post_body, get_bodies=get_bodies)
    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        [
            "--base-url",
            "https://q.example.test",
            "submit-ena-import",
            "PRJEB11419",
            "--poll-interval-seconds",
            "0.001",
        ]
    )
    rc = ns.handler(ns, parser)

    assert rc == 0
    # Stops as soon as the item is all-terminal — exactly the two polls
    # scripted above, never a third.
    assert [m for m, _p, _b in calls] == ["POST", "GET", "GET"]
    err = capsys.readouterr().err
    assert "downloading" in err
    assert "done" in err
    assert "99" in err


def test_watch_exits_1_when_any_item_failed(monkeypatch, patch_async_client, capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    post_body = {
        "ena_import_batch_idx": 5,
        "items": [_item("PRJEB11419", "pending"), _item("PRJNA555783", "pending")],
    }
    get_bodies = [
        {
            "ena_import_batch_idx": 5,
            "items": [
                _item("PRJEB11419", "done"),
                _item("PRJNA555783", "failed", failure_reason="ENA returned no runs"),
            ],
        },
    ]
    handler, calls = _make_handler(post_body=post_body, get_bodies=get_bodies)
    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        [
            "--base-url",
            "https://q.example.test",
            "submit-ena-import",
            "PRJEB11419",
            "PRJNA555783",
            "--poll-interval-seconds",
            "0.001",
        ]
    )
    rc = ns.handler(ns, parser)

    assert rc == 1
    out, err = capsys.readouterr()
    assert "ENA returned no runs" in err
    assert "ENA returned no runs" in out


def test_watch_exits_0_when_a_done_item_has_failed_ena_runs(
    monkeypatch, patch_async_client, capsys
):
    """A `done` item's rolled-up state wins even if some of its per-run
    outcomes failed — item-level `failed` is what drives the exit code."""
    from qiita_control_plane.cli.user._parser import _build_parser

    post_body = {"ena_import_batch_idx": 5, "items": [_item("PRJEB11419", "pending")]}
    get_bodies = [
        {
            "ena_import_batch_idx": 5,
            "items": [
                _item(
                    "PRJEB11419",
                    "done",
                    ena_runs=[
                        {
                            "run_accession": "ERR1",
                            "status": "failed",
                            "failure_reason": "md5 mismatch",
                        }
                    ],
                )
            ],
        },
    ]
    handler, calls = _make_handler(post_body=post_body, get_bodies=get_bodies)
    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        [
            "--base-url",
            "https://q.example.test",
            "submit-ena-import",
            "PRJEB11419",
            "--poll-interval-seconds",
            "0.001",
        ]
    )
    rc = ns.handler(ns, parser)

    assert rc == 0


@pytest.mark.parametrize("status_code", [403, 422])
def test_non_202_submit_response_surfaces_status_and_body(
    status_code, monkeypatch, patch_async_client, capsys
):
    from qiita_control_plane.cli.user._parser import _build_parser

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json={"detail": "nope"})

    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        ["--base-url", "https://q.example.test", "submit-ena-import", "PRJEB11419"]
    )
    rc = ns.handler(ns, parser)

    assert rc == 1
    err = capsys.readouterr().err
    assert str(status_code) in err
    assert "nope" in err


def test_non_202_submit_response_shows_stale_scope_prompt_not_expected_202(
    monkeypatch, patch_async_client, capsys
):
    from qiita_common.auth_constants import STALE_TOKEN_SCOPE_HEADER

    from qiita_control_plane.cli.user._parser import _build_parser

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403, headers={STALE_TOKEN_SCOPE_HEADER: "1"}, json={"detail": "forbidden"}
        )

    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        ["--base-url", "https://q.example.test", "submit-ena-import", "PRJEB11419"]
    )
    rc = ns.handler(ns, parser)

    assert rc == 1
    err = capsys.readouterr().err
    assert "qiita login" in err
    assert "expected 202" not in err


def test_non_202_submit_response_200_surfaces_status(monkeypatch, patch_async_client, capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"detail": "not the expected 202"})

    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        ["--base-url", "https://q.example.test", "submit-ena-import", "PRJEB11419"]
    )
    rc = ns.handler(ns, parser)

    assert rc == 1
    assert "200" in capsys.readouterr().err


def test_post_connect_error_names_url_and_base_url_flag(monkeypatch, patch_async_client, capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("reset", request=request)

    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        ["--base-url", "https://q.example.test", "submit-ena-import", "PRJEB11419"]
    )
    rc = ns.handler(ns, parser)

    assert rc == 1
    err = capsys.readouterr().err
    assert "https://q.example.test/api/v1/ena-import-batch" in err
    assert "--base-url" in err


def test_first_watch_get_stale_scope_403_shows_relogin_prompt(
    monkeypatch, patch_async_client, capsys
):
    from qiita_common.auth_constants import STALE_TOKEN_SCOPE_HEADER

    from qiita_control_plane.cli.user._parser import _build_parser

    post_body = {"ena_import_batch_idx": 5, "items": [_item("PRJEB11419", "pending")]}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json=post_body)
        return httpx.Response(
            403, headers={STALE_TOKEN_SCOPE_HEADER: "1"}, json={"detail": "forbidden"}
        )

    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        [
            "--base-url",
            "https://q.example.test",
            "submit-ena-import",
            "PRJEB11419",
            "--poll-interval-seconds",
            "0.001",
        ]
    )
    rc = ns.handler(ns, parser)

    assert rc == 1
    assert "qiita login" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Watch loop: transient-error retry, fatal errors, and the timeout message
# ---------------------------------------------------------------------------


def test_watch_retries_transient_503_then_succeeds(monkeypatch, patch_async_client, capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    post_body = {"ena_import_batch_idx": 9, "items": [_item("PRJEB11419", "pending")]}
    get_responses = [
        httpx.Response(503, text="upstream restarting"),
        httpx.Response(
            200, json={"ena_import_batch_idx": 9, "items": [_item("PRJEB11419", "done")]}
        ),
    ]
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "POST":
            return httpx.Response(202, json=post_body)
        return get_responses.pop(0)

    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        [
            "--base-url",
            "https://q.example.test",
            "submit-ena-import",
            "PRJEB11419",
            "--poll-interval-seconds",
            "0.001",
        ]
    )
    rc = ns.handler(ns, parser)

    assert rc == 0
    assert calls == ["POST", "GET", "GET"]
    assert "503" in capsys.readouterr().err


def test_watch_retries_connect_error_then_succeeds(monkeypatch, patch_async_client, capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    post_body = {"ena_import_batch_idx": 9, "items": [_item("PRJEB11419", "pending")]}
    get_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json=post_body)
        get_calls["n"] += 1
        if get_calls["n"] == 1:
            raise httpx.ConnectError("reset", request=request)
        return httpx.Response(
            200, json={"ena_import_batch_idx": 9, "items": [_item("PRJEB11419", "done")]}
        )

    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        [
            "--base-url",
            "https://q.example.test",
            "submit-ena-import",
            "PRJEB11419",
            "--poll-interval-seconds",
            "0.001",
        ]
    )
    rc = ns.handler(ns, parser)

    assert rc == 0
    assert get_calls["n"] == 2


def test_watch_fatal_404_stops_after_one_poll(monkeypatch, patch_async_client, capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    post_body = {"ena_import_batch_idx": 9, "items": [_item("PRJEB11419", "pending")]}
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "POST":
            return httpx.Response(202, json=post_body)
        return httpx.Response(404, json={"detail": "batch not found"})

    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        [
            "--base-url",
            "https://q.example.test",
            "submit-ena-import",
            "PRJEB11419",
            "--poll-interval-seconds",
            "0.001",
        ]
    )
    rc = ns.handler(ns, parser)

    assert rc == 1
    assert calls == ["POST", "GET"]
    assert "http error 404" in capsys.readouterr().err


def test_watch_retries_503_forever_until_zero_timeout_fires(
    monkeypatch, patch_async_client, capsys
):
    from qiita_control_plane.cli.user._parser import _build_parser

    post_body = {"ena_import_batch_idx": 9, "items": [_item("PRJEB11419", "pending")]}
    get_calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(202, json=post_body)
        get_calls["n"] += 1
        assert get_calls["n"] <= 5, "watch loop kept retrying past its deadline"
        return httpx.Response(503, text="still restarting")

    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        [
            "--base-url",
            "https://q.example.test",
            "submit-ena-import",
            "PRJEB11419",
            "--poll-interval-seconds",
            "0.001",
            "--timeout-seconds",
            "0",
        ]
    )
    rc = ns.handler(ns, parser)

    assert rc == 1
    err = capsys.readouterr().err
    assert "PRJEB11419" in err
    assert "did not reach a terminal state" in err


def test_watch_times_out_naming_the_batch(monkeypatch, patch_async_client, capsys):
    from qiita_control_plane.cli.user._parser import _build_parser

    post_body = {
        "ena_import_batch_idx": 9,
        "items": [_item("PRJEB11419", "pending"), _item("PRJNA555783", "pending")],
    }
    get_bodies = [
        {
            "ena_import_batch_idx": 9,
            "items": [
                _item("PRJEB11419", "done"),
                _item("PRJNA555783", "resolving"),
            ],
        }
    ]
    handler, calls = _make_handler(post_body=post_body, get_bodies=get_bodies)
    patch_async_client(httpx.MockTransport(handler))
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    parser = _build_parser()
    ns = parser.parse_args(
        [
            "--base-url",
            "https://q.example.test",
            "submit-ena-import",
            "PRJEB11419",
            "PRJNA555783",
            "--poll-interval-seconds",
            "0.001",
            "--timeout-seconds",
            "0",
        ]
    )
    rc = ns.handler(ns, parser)

    assert rc == 1
    err = capsys.readouterr().err
    # The submit announcement — naming the batch idx — is already on stderr
    # by the time the timeout fires.
    assert "ena_import_batch 9" in err
    timeout_line = next(
        line for line in err.splitlines() if "did not reach a terminal state" in line
    )
    assert "PRJNA555783" in timeout_line
    assert "resolving" in timeout_line
    assert "PRJEB11419" not in timeout_line


# ---------------------------------------------------------------------------
# ena-import-status
# ---------------------------------------------------------------------------


def test_ena_import_status_issues_get_against_the_idx(monkeypatch):
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    response_json = {
        "ena_import_batch_idx": 5,
        "items": [_item("PRJEB11419", "failed", failure_reason="boom")],
    }

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["json"] = json
        return httpx.Response(200, json=response_json, request=httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    rc = main(["--base-url", "https://q.example.test", "ena-import-status", "5"])

    assert rc == 0
    assert captured["method"] == "GET"
    assert captured["url"] == (
        f"https://q.example.test{URL_ENA_IMPORT_BATCH_BY_IDX.format(ena_import_batch_idx=5)}"
    )
    assert captured["json"] is None


def test_ena_import_status_non_200_surfaces_status_and_body(monkeypatch, capsys):
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        request = httpx.Request(method, url)
        return httpx.Response(500, json={"detail": "boom"}, request=request)

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    rc = main(["--base-url", "https://q.example.test", "ena-import-status", "5"])

    assert rc == 1
    err = capsys.readouterr().err
    assert "500" in err
    assert "boom" in err


def test_ena_import_status_stale_scope_403_shows_relogin_prompt(monkeypatch, capsys):
    from qiita_common.auth_constants import STALE_TOKEN_SCOPE_HEADER

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        request = httpx.Request(method, url)
        return httpx.Response(
            403,
            headers={STALE_TOKEN_SCOPE_HEADER: "1"},
            json={"detail": "forbidden"},
            request=request,
        )

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    rc = main(["--base-url", "https://q.example.test", "ena-import-status", "5"])

    assert rc == 1
    assert "qiita login" in capsys.readouterr().err


def test_ena_import_status_requires_idx(capsys):
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["ena-import-status"])
    assert exc_info.value.code == 2
    assert "ena_import_batch_idx" in capsys.readouterr().err
