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
from qiita_common.api_paths import (
    URL_ADMIN_PRINCIPAL_REVOKE_ALL_TOKENS,
    URL_ADMIN_STUDY_OWNER_BIOSAMPLE_ID,
    URL_AUTH_WHOAMI,
)
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

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
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
    assert captured["url"] == f"https://api.example.com{URL_AUTH_WHOAMI}"
    assert captured["auth"] == f"{BEARER_PREFIX}qk_X"
    assert body == {"kind": "human", "principal_idx": 7}


def test_token_revoke_all_calls_correct_url(monkeypatch):
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    captured = {}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
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
    assert captured["url"] == (
        f"http://localhost:8080{URL_ADMIN_PRINCIPAL_REVOKE_ALL_TOKENS.format(principal_idx=42)}"
    )
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
        cli.compute_readiness.subprocess,
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

    monkeypatch.setattr(cli.compute_readiness.subprocess, "call", fake_call)
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

    monkeypatch.setattr(cli.compute_readiness.subprocess, "call", fake_call)
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


# ---------------------------------------------------------------------------
# owner-biosample-id export
# ---------------------------------------------------------------------------


def _fake_export_request(captured, body):
    """Build a fake httpx.request that records the call and returns `body`."""

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["params"] = params
        return httpx.Response(200, json=body, request=httpx.Request(method, url))

    return fake_request


def test_owner_biosample_id_study_export_writes_tsv(monkeypatch, tmp_path, capsys):
    """Study-wide export writes the base columns; NULL accession becomes an
    empty cell, the file is 0600, and owner names never reach stdout."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    captured: dict = {}
    body = {
        "study_idx": 7,
        "sequenced_pool_idx": None,
        "row_count": 2,
        "rows": [
            {
                "biosample_idx": 100,
                "biosample_accession": "SAMN1",
                "owner_biosample_id": "OWNER-A",
                "prep_sample_idx": None,
                "ena_experiment_accession": None,
                "ena_run_accession": None,
            },
            {
                "biosample_idx": 101,
                "biosample_accession": None,
                "owner_biosample_id": "OWNER-B",
                "prep_sample_idx": None,
                "ena_experiment_accession": None,
                "ena_run_accession": None,
            },
        ],
    }
    monkeypatch.setattr(_common.httpx, "request", _fake_export_request(captured, body))

    out = tmp_path / "owner.tsv"
    rc = cli.main(["owner-biosample-id", "--study-idx", "7", "--output", str(out)])
    assert rc == 0
    assert captured["method"] == "GET"
    assert captured["url"].endswith(URL_ADMIN_STUDY_OWNER_BIOSAMPLE_ID.format(study_idx=7))
    assert captured["params"] is None  # no pool filter

    lines = out.read_text().splitlines()
    assert lines[0] == "biosample_idx\tbiosample_accession\towner_biosample_id"
    assert lines[1] == "100\tSAMN1\tOWNER-A"
    assert lines[2] == "101\t\tOWNER-B"  # NULL accession -> empty cell
    assert (out.stat().st_mode & 0o777) == 0o600

    captured_io = capsys.readouterr()
    assert "wrote 2 rows" in captured_io.out
    # The PII owner names go to the file only, never to stdout.
    assert "OWNER-A" not in captured_io.out


def test_owner_biosample_id_pool_filter_writes_pathway_columns(monkeypatch, tmp_path):
    """With --sequenced-pool-idx the pool filter is sent as a query param and
    the TSV carries the prep_sample_idx + ENA accession columns."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    captured: dict = {}
    body = {
        "study_idx": 7,
        "sequenced_pool_idx": 5,
        "row_count": 1,
        "rows": [
            {
                "biosample_idx": 100,
                "biosample_accession": "SAMN1",
                "owner_biosample_id": "OWNER-A",
                "prep_sample_idx": 200,
                "ena_experiment_accession": "ERX1",
                "ena_run_accession": "ERR1",
            }
        ],
    }
    monkeypatch.setattr(_common.httpx, "request", _fake_export_request(captured, body))

    out = tmp_path / "owner-pool.tsv"
    rc = cli.main(
        [
            "owner-biosample-id",
            "--study-idx",
            "7",
            "--sequenced-pool-idx",
            "5",
            "--output",
            str(out),
        ]
    )
    assert rc == 0
    assert captured["params"] == {"sequenced_pool_idx": 5}

    lines = out.read_text().splitlines()
    assert lines[0] == (
        "biosample_idx\tbiosample_accession\tprep_sample_idx"
        "\tena_experiment_accession\tena_run_accession\towner_biosample_id"
    )
    assert lines[1] == "100\tSAMN1\t200\tERX1\tERR1\tOWNER-A"
    assert (out.stat().st_mode & 0o777) == 0o600


def test_owner_biosample_id_missing_output_dir_fails_clean(monkeypatch, tmp_path, capsys):
    """A nonexistent --output directory is rejected up front (exit 2, clean
    message) BEFORE any HTTP call, so the PII export is never even fetched."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    called = {"http": False}

    def fake_request(*a, **k):
        called["http"] = True
        raise AssertionError("HTTP must not be called when the --output dir is missing")

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    missing = tmp_path / "nope" / "out.tsv"
    rc = cli.main(["owner-biosample-id", "--study-idx", "7", "--output", str(missing)])
    assert rc == 2
    assert "output directory does not exist" in capsys.readouterr().err
    assert not called["http"]
    assert not missing.exists()


def test_owner_biosample_id_write_failure_preserves_existing_file(monkeypatch, tmp_path, capsys):
    """If the atomic rename fails mid-write, the pre-existing export is left
    intact (no truncation), no stray temp file remains, and the command exits 1
    with a clean message rather than a traceback."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    captured: dict = {}
    body = {
        "study_idx": 7,
        "sequenced_pool_idx": None,
        "row_count": 1,
        "rows": [
            {
                "biosample_idx": 1,
                "biosample_accession": "S",
                "owner_biosample_id": "O",
                "prep_sample_idx": None,
                "ena_experiment_accession": None,
                "ena_run_accession": None,
            }
        ],
    }
    monkeypatch.setattr(_common.httpx, "request", _fake_export_request(captured, body))

    out = tmp_path / "owner.tsv"
    out.write_text("OLD CONTENT\n")

    def boom(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(cli.os, "replace", boom)
    rc = cli.main(["owner-biosample-id", "--study-idx", "7", "--output", str(out)])
    assert rc == 1
    assert "could not write" in capsys.readouterr().err
    assert out.read_text() == "OLD CONTENT\n"  # untouched
    leftovers = sorted(p.name for p in tmp_path.iterdir() if p.name != "owner.tsv")
    assert leftovers == [], f"stray temp files left behind: {leftovers}"


# ---------------------------------------------------------------------------
# masked-read-export (parquet path)
# ---------------------------------------------------------------------------


class _FakeFlightStream:
    """Stands in for a pyarrow FlightStreamReader: .to_reader() yields a
    streaming RecordBatchReader, exactly what the CLI feeds into DuckDB."""

    def __init__(self, table):
        self._table = table

    def to_reader(self):
        return self._table.to_reader()


class _FakeFlightResult:
    """Stands in for a pyarrow Flight DoAction result: `.body.to_pybytes()`
    yields the JSON bytes the CLI parses (here, `{"count": N}`)."""

    class _Buf:
        def __init__(self, payload: bytes):
            self._payload = payload

        def to_pybytes(self) -> bytes:
            return self._payload

    def __init__(self, payload: bytes):
        self.body = self._Buf(payload)


def _fake_flight_client_class(tables_by_prep, counts_by_prep=None):
    """Build a fake pyarrow.flight.FlightClient class whose do_get returns the
    queued table for the prep_sample_idx encoded in the (fake) ticket. The
    monkeypatched ticket endpoint encodes {"prep_sample_idx": N} as the ticket
    bytes, so the fake maps a DoGet back to its sample without real signing.

    do_action serves the `count_masked` probe: it returns `{"count": N}` for the
    ticket's prep, taking N from `counts_by_prep` when given (to simulate a
    data-plane count that differs from what's on disk) else the queued table's
    row count (the consistent "nothing changed" case)."""
    import json as _json

    class _FakeFlightClient:
        # Every constructed instance is recorded so a test can read back the
        # FlightCallOptions the CLI passed to do_get (buffer-alignment fix) and
        # which preps were streamed vs. only counted.
        instances: list = []

        def __init__(self, url):
            self.url = url
            self.do_get_calls = []
            self.do_get_options = []
            self.do_action_calls = []
            _FakeFlightClient.instances.append(self)

        def do_get(self, ticket, options=None):
            prep = _json.loads(bytes(ticket.ticket))["prep_sample_idx"]
            self.do_get_calls.append(prep)
            self.do_get_options.append(options)
            return _FakeFlightStream(tables_by_prep[prep])

        def do_action(self, action, options=None):
            assert action.type == "count_masked"
            prep = _json.loads(action.body.to_pybytes())["prep_sample_idx"]
            self.do_action_calls.append(prep)
            if counts_by_prep is not None and prep in counts_by_prep:
                count = counts_by_prep[prep]
            else:
                count = tables_by_prep[prep].num_rows
            return [_FakeFlightResult(_json.dumps({"count": count}).encode())]

        def close(self):
            pass

    _FakeFlightClient.instances = []
    return _FakeFlightClient


def _fake_masked_export_http(manifest):
    """A fake httpx.request that serves the manifest GET and the ticket POST
    (encoding prep_sample_idx into the returned ticket bytes)."""
    import base64 as _b64
    import json as _json

    from qiita_common.api_paths import (
        PATH_ADMIN_MASKED_READ_EXPORT_TICKET,
        PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT,
    )

    pool_idx = manifest["sequenced_pool_idx"]
    manifest_suffix = PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT.format(
        sequenced_pool_idx=pool_idx
    )

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        if method == "GET" and url.endswith(manifest_suffix):
            return httpx.Response(200, json=manifest, request=httpx.Request(method, url))
        if method == "POST" and url.endswith(PATH_ADMIN_MASKED_READ_EXPORT_TICKET):
            tok = _b64.b64encode(
                _json.dumps({"prep_sample_idx": json["prep_sample_idx"]}).encode()
            ).decode()
            return httpx.Response(201, json={"ticket": tok}, request=httpx.Request(method, url))
        return httpx.Response(404, request=httpx.Request(method, url))

    return fake_request


def test_masked_read_export_writes_parquet_per_sample(monkeypatch, tmp_path):
    """Happy path: one <accession>.<run>.<pool>.<prep>.parquet per sample, with
    the streamed rows, written 0600."""
    import duckdb
    import pyarrow as pa

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    manifest = {
        "sequenced_pool_idx": 7,
        "sequencing_run_idx": 5,
        "mask_idx": 3,
        "samples": [
            {"prep_sample_idx": 42, "biosample_accession": "SAMN_A"},
            {"prep_sample_idx": 43, "biosample_accession": "SAMN_B"},
        ],
    }
    monkeypatch.setattr(_common.httpx, "request", _fake_masked_export_http(manifest))
    tables = {
        42: pa.table({"read_id": ["rA0", "rA1"], "sequence1": ["ACGT", "TTGG"]}),
        43: pa.table({"read_id": ["rB0"], "sequence1": ["CCAA"]}),
    }
    fake_cls = _fake_flight_client_class(tables)
    monkeypatch.setattr("pyarrow.flight.FlightClient", fake_cls)

    out_dir = tmp_path / "exp"
    out_dir.mkdir()
    rc = cli.main(
        [
            "masked-read-export",
            "--sequenced-pool-idx",
            "7",
            "--mask-idx",
            "3",
            "--format",
            "parquet",
            "--output-dir",
            str(out_dir),
            "--data-plane-url",
            "grpc://dp:50051",
        ]
    )
    assert rc == 0

    # Parquet streams straight to a ParquetWriter (no DuckDB/Acero), so it passes
    # no buffer-realign option — only the fastq path needs one.
    assert fake_cls.instances[0].do_get_options == [None, None]

    f_a = out_dir / "SAMN_A.5.7.42.parquet"
    f_b = out_dir / "SAMN_B.5.7.43.parquet"
    assert f_a.is_file() and f_b.is_file()
    assert (f_a.stat().st_mode & 0o777) == 0o600
    rows_a = (
        duckdb.connect(":memory:")
        .execute(f"SELECT read_id FROM read_parquet('{f_a}') ORDER BY read_id")
        .fetchall()
    )
    assert [r[0] for r in rows_a] == ["rA0", "rA1"]
    n_b = (
        duckdb.connect(":memory:")
        .execute(f"SELECT count(*) FROM read_parquet('{f_b}')")
        .fetchone()[0]
    )
    assert n_b == 1
    # No .partial temp files left behind.
    assert sorted(p.suffix for p in out_dir.iterdir()) == [".parquet", ".parquet"]


def test_masked_read_export_empty_sample_parquet(monkeypatch, tmp_path):
    """A sample with zero masked reads → a valid empty `<stem>.parquet`. The
    ParquetWriter is opened from `reader.schema`, so even a zero-batch stream
    produces a schema-carrying file (no crash, no missing output)."""
    import duckdb
    import pyarrow as pa

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    manifest = {
        "sequenced_pool_idx": 7,
        "sequencing_run_idx": 5,
        "mask_idx": 3,
        "samples": [{"prep_sample_idx": 42, "biosample_accession": "SAMN_A"}],
    }
    monkeypatch.setattr(_common.httpx, "request", _fake_masked_export_http(manifest))
    empty = pa.table(
        {"read_id": pa.array([], type=pa.string()), "sequence1": pa.array([], type=pa.string())}
    )
    monkeypatch.setattr("pyarrow.flight.FlightClient", _fake_flight_client_class({42: empty}))

    out_dir = tmp_path / "exp"
    out_dir.mkdir()
    rc = cli.main(
        [
            "masked-read-export",
            "--sequenced-pool-idx",
            "7",
            "--mask-idx",
            "3",
            "--format",
            "parquet",
            "--output-dir",
            str(out_dir),
            "--data-plane-url",
            "grpc://dp:50051",
        ]
    )
    assert rc == 0
    f = out_dir / "SAMN_A.5.7.42.parquet"
    assert f.is_file()
    assert (f.stat().st_mode & 0o777) == 0o600
    assert duckdb.connect(":memory:").execute(f"SELECT count(*) FROM '{f}'").fetchone()[0] == 0
    assert sorted(p.name for p in out_dir.iterdir()) == ["SAMN_A.5.7.42.parquet"]


def test_masked_read_export_parquet_coalesces_row_groups(monkeypatch, tmp_path):
    """A multi-batch stream (the data plane sends ~2048-row DataChunks) must be
    coalesced into row groups sized by ROW_GROUP_SIZE_BYTES, not written one tiny
    row group per batch. Here the whole sample is well under the byte cap, so the
    three input batches land in a single row group (guards the fragmentation
    regression a naive write_batch-per-batch would cause)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    manifest = {
        "sequenced_pool_idx": 7,
        "sequencing_run_idx": 5,
        "mask_idx": 3,
        "samples": [{"prep_sample_idx": 42, "biosample_accession": "SAMN_A"}],
    }
    monkeypatch.setattr(_common.httpx, "request", _fake_masked_export_http(manifest))
    batches = [
        pa.record_batch({"read_id": [f"r{b}{i}" for i in range(4)], "sequence1": ["ACGT"] * 4})
        for b in range(3)
    ]
    table = pa.Table.from_batches(batches)
    assert table.to_reader().read_all().num_rows == 12  # sanity: 3 batches kept
    monkeypatch.setattr("pyarrow.flight.FlightClient", _fake_flight_client_class({42: table}))

    out_dir = tmp_path / "exp"
    out_dir.mkdir()
    rc = cli.main(
        [
            "masked-read-export",
            "--sequenced-pool-idx",
            "7",
            "--mask-idx",
            "3",
            "--format",
            "parquet",
            "--output-dir",
            str(out_dir),
            "--data-plane-url",
            "grpc://dp:50051",
        ]
    )
    assert rc == 0
    f = out_dir / "SAMN_A.5.7.42.parquet"
    md = pq.ParquetFile(f).metadata
    assert md.num_rows == 12
    assert md.num_row_groups == 1  # coalesced, not one row group per input batch


def test_masked_read_export_fastq_realigns_flight_buffers(monkeypatch, tmp_path):
    """The fastq path feeds Flight batches into DuckDB (the miint FORMAT FASTQ
    writer) → pyarrow.dataset → Acero. Flight zero-copies the gRPC body at an
    arbitrary base, so without realignment Acero logs a "poorly aligned input
    buffer" warning per column per batch (apache/arrow#37195). Regression guard:
    every fastq DoGet carries read_options with ensure_alignment=DataTypeSpecific."""
    import pyarrow as pa
    import pyarrow.ipc as ipc

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    manifest = {
        "sequenced_pool_idx": 7,
        "sequencing_run_idx": 5,
        "mask_idx": 3,
        "samples": [
            {"prep_sample_idx": 42, "biosample_accession": "SAMN_A"},
            {"prep_sample_idx": 43, "biosample_accession": "SAMN_B"},
        ],
    }
    monkeypatch.setattr(_common.httpx, "request", _fake_masked_export_http(manifest))
    tables = {
        42: pa.table(
            {
                "read_id": ["rA0"],
                "sequence1": ["ACGT"],
                "qual1": _qual([[40, 40, 40, 40]]),
                "sequence2": pa.array([None], type=pa.string()),
                "qual2": _qual([None]),
            }
        ),
        43: pa.table(
            {
                "read_id": ["rB0"],
                "sequence1": ["CCAA"],
                "qual1": _qual([[40, 40, 40, 40]]),
                "sequence2": pa.array([None], type=pa.string()),
                "qual2": _qual([None]),
            }
        ),
    }
    fake_cls = _fake_flight_client_class(tables)
    monkeypatch.setattr("pyarrow.flight.FlightClient", fake_cls)

    out_dir = tmp_path / "exp"
    out_dir.mkdir()
    rc = cli.main(
        [
            "masked-read-export",
            "--sequenced-pool-idx",
            "7",
            "--mask-idx",
            "3",
            "--format",
            "fastq",
            "--output-dir",
            str(out_dir),
            "--data-plane-url",
            "grpc://dp:50051",
        ]
    )
    assert rc == 0

    # One client, two DoGets (one per sample), each carrying the realign option.
    assert len(fake_cls.instances) == 1
    options = fake_cls.instances[0].do_get_options
    assert len(options) == 2
    for opt in options:
        assert opt is not None, "fastq do_get called without FlightCallOptions"
        assert opt.read_options.ensure_alignment == ipc.Alignment.DataTypeSpecific


def test_masked_read_export_aborts_on_null_accession(monkeypatch, tmp_path, capsys):
    """A sample with a null biosample_accession (unsubmitted) fails the whole
    export loudly before any download — no partial output."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    manifest = {
        "sequenced_pool_idx": 7,
        "sequencing_run_idx": 5,
        "mask_idx": 3,
        "samples": [
            {"prep_sample_idx": 42, "biosample_accession": "SAMN_A"},
            {"prep_sample_idx": 43, "biosample_accession": None},
        ],
    }
    monkeypatch.setattr(_common.httpx, "request", _fake_masked_export_http(manifest))

    class _BoomFlightClient:
        def __init__(self, url):
            pass

        def do_get(self, ticket):
            raise AssertionError("must not DoGet when a sample is missing its accession")

        def close(self):
            pass

    monkeypatch.setattr("pyarrow.flight.FlightClient", _BoomFlightClient)

    out_dir = tmp_path / "exp"
    out_dir.mkdir()
    rc = cli.main(
        [
            "masked-read-export",
            "--sequenced-pool-idx",
            "7",
            "--mask-idx",
            "3",
            "--format",
            "parquet",
            "--output-dir",
            str(out_dir),
            "--data-plane-url",
            "grpc://dp:50051",
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "biosample_accession" in err
    assert "43" in err  # names the offending prep_sample_idx
    assert list(out_dir.iterdir()) == []  # nothing written


def test_masked_read_export_aborts_on_unsafe_accession(monkeypatch, tmp_path, capsys):
    """An accession outside [A-Za-z0-9._-] (here a path separator) fails the
    export up front — guarding the filename path against traversal / SQL-string
    injection. Exit 1, no download, nothing written."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    manifest = {
        "sequenced_pool_idx": 7,
        "sequencing_run_idx": 5,
        "mask_idx": 3,
        "samples": [
            {"prep_sample_idx": 42, "biosample_accession": "SAMN_A"},
            {"prep_sample_idx": 99, "biosample_accession": "../evil'"},
        ],
    }
    monkeypatch.setattr(_common.httpx, "request", _fake_masked_export_http(manifest))

    class _BoomFlightClient:
        def __init__(self, url):
            pass

        def do_get(self, ticket):
            raise AssertionError("must not DoGet when a sample's accession is unsafe")

        def close(self):
            pass

    monkeypatch.setattr("pyarrow.flight.FlightClient", _BoomFlightClient)

    out_dir = tmp_path / "exp"
    out_dir.mkdir()
    rc = cli.main(
        [
            "masked-read-export",
            "--sequenced-pool-idx",
            "7",
            "--mask-idx",
            "3",
            "--format",
            "parquet",
            "--output-dir",
            str(out_dir),
            "--data-plane-url",
            "grpc://dp:50051",
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "99" in err  # names the offending prep_sample_idx
    assert list(out_dir.iterdir()) == []


def test_masked_read_export_creates_missing_output_dir(monkeypatch, tmp_path):
    """The output directory (and any missing parents) is created on demand — it
    need not pre-exist."""
    import pyarrow as pa

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    manifest = {
        "sequenced_pool_idx": 7,
        "sequencing_run_idx": 5,
        "mask_idx": 3,
        "samples": [{"prep_sample_idx": 42, "biosample_accession": "SAMN_A"}],
    }
    monkeypatch.setattr(_common.httpx, "request", _fake_masked_export_http(manifest))
    monkeypatch.setattr(
        "pyarrow.flight.FlightClient",
        _fake_flight_client_class({42: pa.table({"read_id": ["rA0"], "sequence1": ["ACGT"]})}),
    )

    out_dir = tmp_path / "new" / "nested" / "exp"  # none of these exist yet
    assert not out_dir.exists()
    rc = cli.main(
        [
            "masked-read-export",
            "--sequenced-pool-idx",
            "7",
            "--mask-idx",
            "3",
            "--format",
            "parquet",
            "--output-dir",
            str(out_dir),
            "--data-plane-url",
            "grpc://dp:50051",
        ]
    )
    assert rc == 0
    assert (out_dir / "SAMN_A.5.7.42.parquet").is_file()


def test_masked_read_export_skips_unchanged_parquet(monkeypatch, tmp_path, capsys):
    """Re-exporting when nothing changed skips every sample: the second run probes
    the data plane's count (count_masked), finds it equals the on-disk record
    count, and neither re-streams (no DoGet) nor rewrites the file."""
    import pyarrow as pa

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    manifest = {
        "sequenced_pool_idx": 7,
        "sequencing_run_idx": 5,
        "mask_idx": 3,
        "samples": [
            {"prep_sample_idx": 42, "biosample_accession": "SAMN_A"},
            {"prep_sample_idx": 43, "biosample_accession": "SAMN_B"},
        ],
    }
    monkeypatch.setattr(_common.httpx, "request", _fake_masked_export_http(manifest))
    tables = {
        42: pa.table({"read_id": ["rA0", "rA1"], "sequence1": ["ACGT", "TTGG"]}),
        43: pa.table({"read_id": ["rB0"], "sequence1": ["CCAA"]}),
    }
    fake_cls = _fake_flight_client_class(tables)
    monkeypatch.setattr("pyarrow.flight.FlightClient", fake_cls)

    out_dir = tmp_path / "exp"
    argv = [
        "masked-read-export",
        "--sequenced-pool-idx",
        "7",
        "--mask-idx",
        "3",
        "--format",
        "parquet",
        "--output-dir",
        str(out_dir),
        "--data-plane-url",
        "grpc://dp:50051",
    ]
    assert cli.main(argv) == 0  # first export writes both files
    assert fake_cls.instances[0].do_get_calls == [42, 43]
    capsys.readouterr()  # discard first-run output

    assert cli.main(argv) == 0  # second export: nothing changed
    assert "exported 0 sample(s) (skipped 2 already up to date)" in capsys.readouterr().out
    # The re-run client streamed nothing (every sample skipped) but counted both.
    rerun = fake_cls.instances[-1]
    assert rerun.do_get_calls == []
    assert sorted(rerun.do_action_calls) == [42, 43]


def test_masked_read_export_overwrites_changed_parquet(monkeypatch, tmp_path):
    """When the data plane's count differs from the on-disk file (reads added or
    removed since the last export), the sample is re-streamed and overwritten."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    manifest = {
        "sequenced_pool_idx": 7,
        "sequencing_run_idx": 5,
        "mask_idx": 3,
        "samples": [{"prep_sample_idx": 42, "biosample_accession": "SAMN_A"}],
    }
    monkeypatch.setattr(_common.httpx, "request", _fake_masked_export_http(manifest))

    out_dir = tmp_path / "exp"
    out_dir.mkdir()
    # A stale on-disk export with 1 row; the data plane now reports 2.
    pq.write_table(
        pa.table({"read_id": ["old"], "sequence1": ["AAAA"]}),
        out_dir / "SAMN_A.5.7.42.parquet",
    )
    fake_cls = _fake_flight_client_class(  # count_masked defaults to the table's 2 rows
        {42: pa.table({"read_id": ["rA0", "rA1"], "sequence1": ["ACGT", "TTGG"]})}
    )
    monkeypatch.setattr("pyarrow.flight.FlightClient", fake_cls)

    rc = cli.main(
        [
            "masked-read-export",
            "--sequenced-pool-idx",
            "7",
            "--mask-idx",
            "3",
            "--format",
            "parquet",
            "--output-dir",
            str(out_dir),
            "--data-plane-url",
            "grpc://dp:50051",
        ]
    )
    assert rc == 0
    # On-disk (1) != remote (2) → counted, then re-streamed and overwritten.
    assert fake_cls.instances[-1].do_action_calls == [42]
    assert fake_cls.instances[-1].do_get_calls == [42]
    assert pq.ParquetFile(out_dir / "SAMN_A.5.7.42.parquet").metadata.num_rows == 2


# ---------------------------------------------------------------------------
# masked-read-export (fastq path — R1/R2)
#
# These run the REAL miint FORMAT FASTQ writer (miint is a core dependency,
# installed/loaded by setup_miint_test_env in tests/conftest.py). The fake
# Flight stream hands DuckDB a table carrying the read_masked view's columns
# (read_id, sequence1, qual1, sequence2, qual2; qual* are UTINYINT[]); pairing
# is read from sequence2 null-ness, since the manifest carries no paired flag.
# ---------------------------------------------------------------------------


def _qual(rows):
    """A UTINYINT[]-typed Arrow column (list<uint8>) — what read_fastx emits and
    the FASTQ writer encodes back as ASCII phred+33."""
    import pyarrow as pa

    return pa.array(rows, type=pa.list_(pa.uint8()))


def _read_gz_text(path):
    """Decompress a gzip-compressed fastq output to text (fastq is written
    `<stem>.fastq.gz` via the FORMAT FASTQ writer's COMPRESSION 'gzip')."""
    import gzip

    with gzip.open(path, "rt") as fh:
        return fh.read()


def _run_fastq_export(monkeypatch, tmp_path, table):
    """Drive a single-sample fastq export against a fake Flight stream serving
    `table`, returning (rc, out_dir)."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    manifest = {
        "sequenced_pool_idx": 7,
        "sequencing_run_idx": 5,
        "mask_idx": 3,
        "samples": [{"prep_sample_idx": 42, "biosample_accession": "SAMN_A"}],
    }
    monkeypatch.setattr(_common.httpx, "request", _fake_masked_export_http(manifest))
    monkeypatch.setattr("pyarrow.flight.FlightClient", _fake_flight_client_class({42: table}))

    out_dir = tmp_path / "exp"
    out_dir.mkdir()
    rc = cli.main(
        [
            "masked-read-export",
            "--sequenced-pool-idx",
            "7",
            "--mask-idx",
            "3",
            "--format",
            "fastq",
            "--output-dir",
            str(out_dir),
            "--data-plane-url",
            "grpc://dp:50051",
        ]
    )
    return rc, out_dir


def test_masked_read_export_single_end_fastq(monkeypatch, tmp_path):
    """A single-end sample (sequence2 NULL throughout) → one gzip <stem>.fastq.gz,
    0600, no R1/R2 split. UTINYINT[] qual written ASCII phred+33 (Q40 → 'I')."""
    import pyarrow as pa

    table = pa.table(
        {
            "read_id": pa.array(["rS0", "rS1"]),
            "sequence1": pa.array(["GGGGCCCC", "AAAACCCC"]),
            "qual1": _qual([[40] * 8, [40] * 8]),
            "sequence2": pa.array([None, None], type=pa.string()),
            "qual2": _qual([None, None]),
        }
    )
    rc, out_dir = _run_fastq_export(monkeypatch, tmp_path, table)
    assert rc == 0

    f = out_dir / "SAMN_A.5.7.42.fastq.gz"
    assert f.is_file()
    assert (f.stat().st_mode & 0o777) == 0o600
    lines = _read_gz_text(f).splitlines()
    assert lines[:4] == ["@rS0", "GGGGCCCC", "+", "IIIIIIII"]
    assert len(lines) == 8  # two single-end records, 4 lines each
    # No paired split, no leftover .partial.
    assert sorted(p.name for p in out_dir.iterdir()) == ["SAMN_A.5.7.42.fastq.gz"]


def test_masked_read_export_paired_fastq_r1_r2(monkeypatch, tmp_path):
    """A paired sample (sequence2 set) → <stem>.R1.fastq.gz + <stem>.R2.fastq.gz via
    the {ORIENTATION} placeholder, each 0600, no single-file <stem>.fastq.gz. Mates
    split sequence1→R1 / sequence2→R2; qual phred+33 (Q30..37 → '?@ABCDEF')."""
    import pyarrow as pa

    table = pa.table(
        {
            "read_id": pa.array(["readP1", "readP2"]),
            "sequence1": pa.array(["ACGTACGT", "AAAACCCC"]),
            "qual1": _qual([[30, 31, 32, 33, 34, 35, 36, 37], [38] * 8]),
            "sequence2": pa.array(["TTGGCCAA", "GGGGTTTT"]),
            "qual2": _qual([[20, 21, 22, 23, 24, 25, 26, 27], [28] * 8]),
        }
    )
    rc, out_dir = _run_fastq_export(monkeypatch, tmp_path, table)
    assert rc == 0

    r1 = out_dir / "SAMN_A.5.7.42.R1.fastq.gz"
    r2 = out_dir / "SAMN_A.5.7.42.R2.fastq.gz"
    assert r1.is_file() and r2.is_file()
    assert (r1.stat().st_mode & 0o777) == 0o600
    assert (r2.stat().st_mode & 0o777) == 0o600
    assert _read_gz_text(r1).splitlines()[:4] == ["@readP1", "ACGTACGT", "+", "?@ABCDEF"]
    assert _read_gz_text(r2).splitlines()[:4] == ["@readP1", "TTGGCCAA", "+", "56789:;<"]
    assert len(_read_gz_text(r1).splitlines()) == 8  # two records per mate
    assert len(_read_gz_text(r2).splitlines()) == 8
    # Split output only — no single-file form, no leftover .partial.
    assert sorted(p.name for p in out_dir.iterdir()) == [
        "SAMN_A.5.7.42.R1.fastq.gz",
        "SAMN_A.5.7.42.R2.fastq.gz",
    ]


def test_masked_read_export_paired_fastq_streams_all_batches(monkeypatch, tmp_path):
    """Pairing is decided by peeking the FIRST batch; the rest of the stream must
    still reach the COPY. A 2-batch paired stream → both reads land in R1/R2,
    guarding against the peeked prefix being dropped (not chained back)."""
    import pyarrow as pa

    b1 = pa.record_batch(
        {
            "read_id": pa.array(["p1"]),
            "sequence1": pa.array(["ACGTACGT"]),
            "qual1": _qual([[40] * 8]),
            "sequence2": pa.array(["TTGGCCAA"]),
            "qual2": _qual([[40] * 8]),
        }
    )
    b2 = pa.record_batch(
        {
            "read_id": pa.array(["p2"]),
            "sequence1": pa.array(["AAAACCCC"]),
            "qual1": _qual([[40] * 8]),
            "sequence2": pa.array(["GGGGTTTT"]),
            "qual2": _qual([[40] * 8]),
        }
    )
    rc, out_dir = _run_fastq_export(monkeypatch, tmp_path, pa.Table.from_batches([b1, b2]))
    assert rc == 0

    r1 = _read_gz_text(out_dir / "SAMN_A.5.7.42.R1.fastq.gz").splitlines()
    r2 = _read_gz_text(out_dir / "SAMN_A.5.7.42.R2.fastq.gz").splitlines()
    assert "@p1" in r1 and "@p2" in r1  # both reads, not only the peeked first batch
    assert "@p1" in r2 and "@p2" in r2


def test_masked_read_export_empty_sample_fastq(monkeypatch, tmp_path):
    """A sample with zero masked reads → one empty <stem>.fastq.gz, no R1/R2 split.
    `bool_or(...)` over zero rows is SQL NULL, so pairing detection must treat an
    empty sample as single-end (not crash, not split)."""
    import pyarrow as pa

    table = pa.table(
        {
            "read_id": pa.array([], type=pa.string()),
            "sequence1": pa.array([], type=pa.string()),
            "qual1": _qual([]),
            "sequence2": pa.array([], type=pa.string()),
            "qual2": _qual([]),
        }
    )
    rc, out_dir = _run_fastq_export(monkeypatch, tmp_path, table)
    assert rc == 0

    f = out_dir / "SAMN_A.5.7.42.fastq.gz"
    assert f.is_file()
    assert (f.stat().st_mode & 0o777) == 0o600
    assert _read_gz_text(f) == ""  # zero records
    assert sorted(p.name for p in out_dir.iterdir()) == ["SAMN_A.5.7.42.fastq.gz"]


def test_masked_read_export_fastq_refuses_existing_file(monkeypatch, tmp_path, capsys):
    """fastq export never overwrites: if any target filename already exists, the
    command fails up front (exit 1) before constructing a Flight client or
    streaming — no count probe, no DoGet, and the existing file is left intact.
    A lone R1 (from a prior paired export) is enough to block the whole run."""
    import pyarrow as pa

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli import admin as cli

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    manifest = {
        "sequenced_pool_idx": 7,
        "sequencing_run_idx": 5,
        "mask_idx": 3,
        "samples": [
            {"prep_sample_idx": 42, "biosample_accession": "SAMN_A"},
            {"prep_sample_idx": 43, "biosample_accession": "SAMN_B"},
        ],
    }
    monkeypatch.setattr(_common.httpx, "request", _fake_masked_export_http(manifest))
    se = {
        "qual1": _qual([[40] * 4]),
        "sequence2": pa.array([None], type=pa.string()),
        "qual2": _qual([None]),
    }
    fake_cls = _fake_flight_client_class(
        {
            42: pa.table({"read_id": ["rA0"], "sequence1": ["ACGT"], **se}),
            43: pa.table({"read_id": ["rB0"], "sequence1": ["CCAA"], **se}),
        }
    )
    monkeypatch.setattr("pyarrow.flight.FlightClient", fake_cls)

    out_dir = tmp_path / "exp"
    out_dir.mkdir()
    preexisting = out_dir / "SAMN_B.5.7.43.R1.fastq.gz"
    preexisting.write_bytes(b"stale")

    rc = cli.main(
        [
            "masked-read-export",
            "--sequenced-pool-idx",
            "7",
            "--mask-idx",
            "3",
            "--format",
            "fastq",
            "--output-dir",
            str(out_dir),
            "--data-plane-url",
            "grpc://dp:50051",
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "refusing to overwrite" in err
    assert "SAMN_B.5.7.43.R1.fastq.gz" in err
    # Failed before any Flight work; the stale file is the only thing on disk.
    assert fake_cls.instances == []
    assert preexisting.read_bytes() == b"stale"
    assert sorted(p.name for p in out_dir.iterdir()) == ["SAMN_B.5.7.43.R1.fastq.gz"]
