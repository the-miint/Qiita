"""Unit tests for qiita-admin CLI helpers (no DB, no live HTTP).

set-system-role tests use a real DB and live in tests/integration/test_admin_cli.py
(deferred — for now, set-system-role is exercised manually during the
first-deploy flow). The HTTP subcommand helpers are tested here against
a stubbed httpx response.

Shared helpers (token I/O, loopback login, whoami) live in
qiita_control_plane.cli._common and are tested directly there or via
test_cli_login.py. This file covers admin-specific surface plus the
argparse wiring on the admin entry point.
"""

import httpx
import pytest
from qiita_common.auth_constants import BEARER_PREFIX


def test_read_token_from_env(monkeypatch):
    from qiita_control_plane.cli._common import read_token

    monkeypatch.setenv("QIITA_TOKEN", "qk_test_token")
    assert read_token() == "qk_test_token"


def test_read_token_from_file(monkeypatch, tmp_path):
    from qiita_control_plane.cli._common import read_token

    monkeypatch.delenv("QIITA_TOKEN", raising=False)
    f = tmp_path / "token"
    f.write_text("qk_from_file\n")
    assert read_token(token_file=f) == "qk_from_file"


def test_read_token_raises_with_actionable_message(monkeypatch, tmp_path):
    from qiita_control_plane.cli._common import read_token

    monkeypatch.delenv("QIITA_TOKEN", raising=False)
    nonexistent = tmp_path / "nope"
    with pytest.raises(RuntimeError, match="QIITA_TOKEN"):
        read_token(token_file=nonexistent)


def test_whoami_calls_correct_url(monkeypatch):
    """`_common.whoami` round-trips through the unified `call` helper:
    base-url trimmed, API_PREFIX prepended, BEARER_PREFIX in the auth
    header. Patches httpx.request (the verb-agnostic entry point `call`
    delegates to) so a single test exercises the shared shape."""
    from qiita_control_plane.cli import _common

    captured = {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["auth"] = headers["Authorization"]
        return httpx.Response(
            200,
            json={"kind": "human", "principal_idx": 7},
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    body = _common.whoami("https://api.example.com/", "qk_X")
    assert captured["method"] == "GET"
    assert captured["url"] == "https://api.example.com/api/v1/auth/whoami"
    assert captured["auth"] == f"{BEARER_PREFIX}qk_X"
    assert body == {"kind": "human", "principal_idx": 7}


def test_token_revoke_all_calls_correct_url(monkeypatch):
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    captured = {}

    def fake_request(method, url, headers=None, json=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["auth"] = headers["Authorization"]
        return httpx.Response(
            200,
            json={"revoked_token_idxs": [1, 2], "already_revoked_count": 0},
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    body = cli._token_revoke_all("http://localhost:8080", "qk_admin", 42)
    assert captured["method"] == "POST"
    assert captured["url"] == "http://localhost:8080/api/v1/admin/principal/42/revoke-all-tokens"
    assert captured["auth"] == f"{BEARER_PREFIX}qk_admin"
    assert body["revoked_token_idxs"] == [1, 2]


def test_main_login_dispatches_to_do_login(monkeypatch):
    """Wiring test: `qiita-admin login` calls `_common.do_login` with the parsed
    --base-url and --token-file, plus cli_command="qiita-admin login" so
    error messages tell the user to re-run the admin binary. The actual
    flow logic is exercised by test_cli_login.py (helpers) and
    tests/integration (end-to-end)."""
    from pathlib import Path

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    captured: dict = {}

    def fake_do_login(*, base_url: str, token_file: Path, cli_command: str) -> int:
        captured["base_url"] = base_url
        captured["token_file"] = token_file
        captured["cli_command"] = cli_command
        return 0

    monkeypatch.setattr(_common, "do_login", fake_do_login)

    rc = cli.main(
        ["--base-url", "https://qiita.example.test", "login", "--token-file", "/tmp/qiita-cli-test"]
    )
    assert rc == 0
    assert captured["base_url"] == "https://qiita.example.test"
    assert captured["token_file"] == Path("/tmp/qiita-cli-test")
    assert captured["cli_command"] == "qiita-admin login"


def test_main_set_system_role_validates_role():
    import asyncio

    from qiita_control_plane.cli.admin import _set_system_role

    with pytest.raises(ValueError, match="role must be one of"):
        asyncio.run(_set_system_role("postgres://x", "x@x.com", "super_admin"))


def test_main_whoami_without_token(monkeypatch, tmp_path, capsys):
    """If QIITA_TOKEN is unset and no token file exists, whoami exits 1."""
    from qiita_control_plane.cli.admin import main

    monkeypatch.delenv("QIITA_TOKEN", raising=False)
    monkeypatch.setattr(
        "qiita_control_plane.cli._common.TOKEN_FILE_DEFAULT",
        tmp_path / "absent",
    )
    rc = main(["whoami"])
    assert rc == 1
    assert "QIITA_TOKEN" in capsys.readouterr().err


# ----------------------------------------------------------------------------
# ticket force-fail — client-side CHECK-constraint mirror
# ----------------------------------------------------------------------------


def test_force_fail_requires_step_name_when_stage_is_step_run():
    """Mirrors work_ticket_failure_step_name_consistent: stage=step_run
    must be paired with a non-empty failure_step_name. Surface the
    constraint client-side so the operator gets a direct error message
    instead of an asyncpg CheckViolationError."""
    from qiita_control_plane.cli.admin import _validate_force_fail_args

    with pytest.raises(ValueError, match="--step-name is required when --stage=step_run"):
        _validate_force_fail_args("step_run", None)


def test_force_fail_rejects_step_name_when_stage_is_submission():
    """Mirrors the other half of the CHECK constraint: stages other
    than step_run must NOT carry a failure_step_name."""
    from qiita_control_plane.cli.admin import _validate_force_fail_args

    with pytest.raises(ValueError, match="--step-name must not be set when --stage=submission"):
        _validate_force_fail_args("submission", "fastq")


def test_force_fail_rejects_step_name_when_stage_is_finalize():
    from qiita_control_plane.cli.admin import _validate_force_fail_args

    with pytest.raises(ValueError, match="--step-name must not be set when --stage=finalize"):
        _validate_force_fail_args("finalize", "register")


def test_force_fail_happy_path_validation():
    """Allowed combinations don't raise."""
    from qiita_control_plane.cli.admin import _validate_force_fail_args

    _validate_force_fail_args("step_run", "fastq")
    _validate_force_fail_args("submission", None)
    _validate_force_fail_args("finalize", None)


def test_force_fail_refuses_terminal_ticket(monkeypatch):
    """Even with valid stage/step-name, the DB-facing function refuses
    to overwrite a ticket that's already in a terminal state. This
    test stubs asyncpg so we exercise the eligibility check without
    needing a live DB."""
    import asyncio

    from qiita_control_plane.cli import admin as cli

    class _FakeTx:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return None

    class _FakeConn:
        def __init__(self):
            self.executed: list = []

        def transaction(self):
            return _FakeTx()

        async def fetchval(self, query, *args):
            assert "FOR UPDATE" in query
            return "completed"  # terminal

        async def execute(self, query, *args):
            self.executed.append((query, args))

        async def close(self):
            pass

    fake = _FakeConn()

    async def fake_connect(database_url, **kwargs):
        return fake

    monkeypatch.setattr(cli.asyncpg, "connect", fake_connect)
    with pytest.raises(RuntimeError, match="terminal state 'completed'"):
        asyncio.run(
            cli._force_fail_ticket(
                "postgres://x",
                work_ticket_idx=42,
                stage="step_run",
                step_name="fastq",
                reason="stuck",
            )
        )
    # No UPDATE issued because the eligibility check failed first.
    assert fake.executed == []


def test_force_fail_happy_path_runs_update(monkeypatch):
    """A processing ticket transitions cleanly: eligibility check
    passes, UPDATE is issued with the expected column values, and the
    handler returns a dict carrying the previous state for the operator."""
    import asyncio

    from qiita_control_plane.cli import admin as cli

    class _FakeTx:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return None

    class _FakeConn:
        def __init__(self):
            self.executed: list = []

        def transaction(self):
            return _FakeTx()

        async def fetchval(self, query, *args):
            return "processing"

        async def execute(self, query, *args):
            self.executed.append((query, args))

        async def close(self):
            pass

    fake = _FakeConn()

    async def fake_connect(database_url, **kwargs):
        return fake

    monkeypatch.setattr(cli.asyncpg, "connect", fake_connect)
    result = asyncio.run(
        cli._force_fail_ticket(
            "postgres://x",
            work_ticket_idx=7,
            stage="step_run",
            step_name="fastq",
            reason="hand-failed by operator",
        )
    )
    assert result["work_ticket_idx"] == 7
    assert result["previous_state"] == "processing"
    assert result["state"] == "failed"
    assert result["failure_stage"] == "step_run"
    assert result["failure_step_name"] == "fastq"
    assert result["failure_type"] == "permanent"
    assert result["failure_reason"] == "hand-failed by operator"
    # The UPDATE was issued with the expected scalars.
    assert len(fake.executed) == 1
    _query, params = fake.executed[0]
    assert params == (7, "step_run", "fastq", "hand-failed by operator")


# ---------------------------------------------------------------------------
# compute-readiness wrapper
# ---------------------------------------------------------------------------


def test_compute_readiness_missing_venv_returns_2(monkeypatch, tmp_path, capsys):
    """When the orchestrator venv doesn't exist, the wrapper must
    short-circuit before invoking subprocess.call — and the error
    message must name the path it looked for so the operator can fix
    the typo or pass --orchestrator-venv."""
    from qiita_control_plane.cli import admin as cli

    bogus = tmp_path / "no-such-venv"
    # subprocess.call should NOT be reached on this path.
    monkeypatch.setattr(
        cli.subprocess,
        "call",
        lambda *a, **k: pytest.fail("subprocess.call should not run when python is missing"),
    )
    rc = cli.main(["compute-readiness", "--orchestrator-venv", str(bogus)])
    assert rc == 2
    err = capsys.readouterr().err
    assert str(bogus / "bin" / "python") in err
    assert "--orchestrator-venv" in err


def test_compute_readiness_invokes_orchestrator_module(monkeypatch, tmp_path):
    """Happy path: when the venv's python exists, the wrapper exec's
    `<python> -m qiita_compute_orchestrator.cli.compute_readiness`
    and returns whatever the subprocess returned. No translation."""
    from qiita_control_plane.cli import admin as cli

    venv = tmp_path / "venv"
    python = venv / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text("#!/bin/sh\nexit 0\n")
    python.chmod(0o755)

    captured: dict[str, list] = {}

    def fake_call(cmd, *a, **k):
        captured["cmd"] = list(cmd)
        return 0

    monkeypatch.setattr(cli.subprocess, "call", fake_call)
    rc = cli.main(["compute-readiness", "--orchestrator-venv", str(venv)])
    assert rc == 0
    assert captured["cmd"][0] == str(python)
    assert captured["cmd"][1:4] == [
        "-m",
        "qiita_compute_orchestrator.cli.compute_readiness",
    ]
    # No optional flags were passed → none appear in the command.
    assert "--no-slurm-probe" not in captured["cmd"]
    assert "--json" not in captured["cmd"]
    assert "--probe-timeout-seconds" not in captured["cmd"]


def test_compute_readiness_propagates_flags(monkeypatch, tmp_path):
    """Optional flags forward to the orchestrator-side CLI exactly. A
    propagation bug here would silently mask the operator's intent
    (e.g., a `--no-slurm-probe` request running the SLURM probe
    anyway), so each flag is asserted independently."""
    from qiita_control_plane.cli import admin as cli

    venv = tmp_path / "venv"
    python = venv / "bin" / "python"
    python.parent.mkdir(parents=True)
    python.write_text("#!/bin/sh\nexit 0\n")
    python.chmod(0o755)

    captured: dict[str, list] = {}

    def fake_call(cmd, *a, **k):
        captured["cmd"] = list(cmd)
        return 1

    monkeypatch.setattr(cli.subprocess, "call", fake_call)
    rc = cli.main(
        [
            "compute-readiness",
            "--orchestrator-venv",
            str(venv),
            "--no-slurm-probe",
            "--json",
            "--probe-timeout-seconds",
            "120",
        ]
    )
    assert rc == 1  # forwarded from subprocess
    assert "--no-slurm-probe" in captured["cmd"]
    assert "--json" in captured["cmd"]
    # --probe-timeout-seconds is passed with its value as the next arg.
    idx = captured["cmd"].index("--probe-timeout-seconds")
    assert captured["cmd"][idx + 1] == "120.0"
