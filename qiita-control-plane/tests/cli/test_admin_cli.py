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


def _fake_flight_client_class(tables_by_prep):
    """Build a fake pyarrow.flight.FlightClient class whose do_get returns the
    queued table for the prep_sample_idx encoded in the (fake) ticket. The
    monkeypatched ticket endpoint encodes {"prep_sample_idx": N} as the ticket
    bytes, so the fake maps a DoGet back to its sample without real signing."""
    import json as _json

    class _FakeFlightClient:
        def __init__(self, url):
            self.url = url
            self.do_get_calls = []

        def do_get(self, ticket):
            prep = _json.loads(bytes(ticket.ticket))["prep_sample_idx"]
            self.do_get_calls.append(prep)
            return _FakeFlightStream(tables_by_prep[prep])

        def close(self):
            pass

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
    monkeypatch.setattr("pyarrow.flight.FlightClient", _fake_flight_client_class(tables))

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
