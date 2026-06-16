"""Tests for qiita_compute_orchestrator.cli.compute_readiness.

The bash probe script itself is intentionally not covered here — it
runs on a SLURM compute node, not in our test env. Instead we cover:

- The Python-side checks (JWT decode, CP /healthz) that run on the
  orchestrator host. Tests use monkeypatch for env, MockTransport for
  the CP.
- The probe orchestration flow: submit → poll-running → poll-terminal
  → parse log. Uses MockTransport against the slurmrestd routes plus
  a pre-staged log file (the probe never actually runs).
- The log-line parser and `key=value` → CheckResult classifier.
"""

from __future__ import annotations

import base64
import hmac
import json
import time
from pathlib import Path

import httpx
import pytest

from qiita_compute_orchestrator.cli import compute_readiness as cr
from qiita_compute_orchestrator.config import Settings, SlurmSettings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_jwt(sun: str, *, exp: int | None = None) -> str:
    """Build a fake JWT with the given `sun` and `exp`. Signed with a
    static secret — only payload claims matter for these tests; the
    SlurmrestdClient never verifies the signature (slurmrestd does)."""
    if exp is None:
        exp = int(time.time()) + 3600
    header = base64.urlsafe_b64encode(b'{"alg":"HS256","typ":"JWT"}').rstrip(b"=").decode()
    payload_obj = {"sun": sun, "exp": exp}
    payload = base64.urlsafe_b64encode(json.dumps(payload_obj).encode()).rstrip(b"=").decode()
    sig = hmac.new(b"test", f"{header}.{payload}".encode(), "sha256").hexdigest()
    return f"{header}.{payload}.{sig}"


def _make_slurm_settings(jwt_path: Path) -> SlurmSettings:
    return SlurmSettings(
        base_url="http://slurm-test:6820",
        jwt_path=jwt_path,
        user_name="qiita-orch",
        partition="qiita",
        account="qiita-prod",
        api_version="v0.0.40",
        poll_interval_seconds=0,
        job_timeout_seconds=60,
        native_python="/opt/qiita/compute-orchestrator/.venv/bin/python",
        qos="",
    )


def _make_settings(jwt_path: Path) -> Settings:
    return Settings(
        backend_type="slurm",
        path_scratch="/scratch/qiita",
        path_derived="/scratch/persistent",
        cp_to_co_token="",
        cp_url="https://qiita.example.org",
        co_to_cp_token="probe-co-to-cp-token",
        slurm=_make_slurm_settings(jwt_path),
    )


# ---------------------------------------------------------------------------
# Local checks
# ---------------------------------------------------------------------------


def test_check_jwt_happy_path(tmp_path):
    jwt = tmp_path / "jwt"
    jwt.write_text(_make_jwt("qiita-orch"))
    results = cr.check_jwt(jwt, "qiita-orch")
    statuses = {r.name: r.status for r in results}
    assert statuses == {
        "jwt-readable": "pass",
        "jwt-shape": "pass",
        "jwt-sun-match": "pass",
        "jwt-exp": "pass",
    }


def test_check_jwt_sun_mismatch(tmp_path):
    jwt = tmp_path / "jwt"
    jwt.write_text(_make_jwt("someone-else"))
    results = cr.check_jwt(jwt, "qiita-orch")
    sun_check = next(r for r in results if r.name == "jwt-sun-match")
    assert sun_check.status == "fail"
    assert "someone-else" in sun_check.detail
    assert "qiita-orch" in sun_check.detail


def test_check_jwt_expired(tmp_path):
    jwt = tmp_path / "jwt"
    jwt.write_text(_make_jwt("qiita-orch", exp=int(time.time()) - 120))
    results = cr.check_jwt(jwt, "qiita-orch")
    exp_check = next(r for r in results if r.name == "jwt-exp")
    assert exp_check.status == "fail"
    assert "expired" in exp_check.detail


def test_check_jwt_unreadable(tmp_path):
    """A path that doesn't exist surfaces as a single fail row — the
    subsequent decode checks short-circuit so the report names the
    actual problem."""
    results = cr.check_jwt(tmp_path / "missing", "qiita-orch")
    assert len(results) == 1
    assert results[0].name == "jwt-readable"
    assert results[0].status == "fail"


def test_check_jwt_malformed_shape(tmp_path):
    jwt = tmp_path / "jwt"
    jwt.write_text("not-a-jwt-at-all")
    results = cr.check_jwt(jwt, "qiita-orch")
    # First check (readable) passes, second (shape) fails, no further.
    statuses = [(r.name, r.status) for r in results]
    assert statuses == [("jwt-readable", "pass"), ("jwt-shape", "fail")]


def test_check_native_python_on_host_missing(tmp_path):
    r = cr.check_native_python_on_host(str(tmp_path / "no-such-python"))
    assert r.status == "fail"
    assert "does not exist" in r.detail


def test_check_native_python_on_host_skip_for_bare_python():
    r = cr.check_native_python_on_host("python")
    assert r.status == "skip"


def test_check_native_python_on_host_pass(tmp_path):
    fake_python = tmp_path / "python"
    fake_python.write_text("#!/bin/sh\nexit 0\n")
    fake_python.chmod(0o755)
    r = cr.check_native_python_on_host(str(fake_python))
    assert r.status == "pass"


@pytest.mark.asyncio
async def test_check_cp_healthz_pass(monkeypatch):
    """Replace httpx.AsyncClient with a mock that returns 200 on the
    /healthz call, asserts the Bearer header is set correctly."""
    seen_headers: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen_headers.update(request.headers)
        assert request.url.path.endswith("/healthz")
        return httpx.Response(200, text="ok")

    transport = httpx.MockTransport(handler)
    real_client = httpx.AsyncClient

    def fake_client(*args, **kwargs):  # noqa: ANN001
        kwargs.setdefault("transport", transport)
        return real_client(*args, **kwargs)

    monkeypatch.setattr(cr.httpx, "AsyncClient", fake_client)
    r = await cr.check_cp_healthz("https://qiita.example.org", "tok-abc")
    assert r.status == "pass"
    assert seen_headers["authorization"] == "Bearer tok-abc"


@pytest.mark.asyncio
async def test_check_cp_healthz_skip_when_unconfigured():
    r1 = await cr.check_cp_healthz("", "tok")
    r2 = await cr.check_cp_healthz("https://x", "")
    assert r1.status == "skip"
    assert r2.status == "skip"


# ---------------------------------------------------------------------------
# Probe-log parser
# ---------------------------------------------------------------------------


def test_parse_probe_log_classifies_each_line():
    log = """\
random preamble line that should be ignored
compute-readiness: hostname=node42
compute-readiness: uid=1234
compute-readiness: user=qiita-job
compute-readiness: native-python-on-compute=ok path=/opt/qiita/co/.venv/bin/python
compute-readiness: native-import=ok
compute-readiness: miint-read-fastx=ok
compute-readiness: shared-fs-visible=ok path=/scratch/qiita
compute-readiness: shared-fs-writable=ok
compute-readiness: cp-from-compute=ok cp_url=https://qiita.example.org
trailing line outside the prefix
"""
    results = cr._parse_probe_log(log)
    names_statuses = [(r.name, r.status) for r in results]
    assert names_statuses == [
        ("probe/hostname", "pass"),
        ("probe/uid", "pass"),
        ("probe/user", "pass"),
        ("probe/native-python-on-compute", "pass"),
        ("probe/native-import", "pass"),
        ("probe/miint-read-fastx", "pass"),
        ("probe/shared-fs-visible", "pass"),
        ("probe/shared-fs-writable", "pass"),
        ("probe/cp-from-compute", "pass"),
    ]


def test_parse_probe_log_failures_propagate():
    log = """\
compute-readiness: native-python-on-compute=fail path=/opt/qiita/co/.venv/bin/python
compute-readiness: native-import=fail
compute-readiness: miint-read-fastx=fail
compute-readiness: shared-fs-visible=fail path=/scratch/qiita
compute-readiness: cp-from-compute=skip cp_url_set=fail token_set=fail
"""
    statuses = {r.name: r.status for r in cr._parse_probe_log(log)}
    assert statuses == {
        "probe/native-python-on-compute": "fail",
        "probe/native-import": "fail",
        "probe/miint-read-fastx": "fail",
        "probe/shared-fs-visible": "fail",
        "probe/cp-from-compute": "skip",
    }


def test_probe_script_emits_only_known_values():
    """Script-vs-parser contract test. The bash probe script's emitted
    `<key>=<value>` literals must all be drawn from the parser's known
    alphabet (pass / fail / skip), otherwise the parser would reject
    them as contract drift and the report would silently show every
    drifted line as `fail`.

    Pulls every `compute-readiness: <key>=<value>` token out of the
    script string and asserts each `<value>` is recognized. Excludes
    the informational keys (hostname/uid/user) whose values are
    runtime-substituted shell expansions, not literal status strings.
    """
    import re

    script = cr.build_probe_script(path_scratch="/scratch/qiita")
    known = cr._PROBE_PASS_VALUES | cr._PROBE_FAIL_VALUES | cr._PROBE_SKIP_VALUES
    # Match `<prefix> <key>=<value>` where value is a bare word (no
    # `$(...)`, no quotes, no shell expansion). Informational lines
    # like `hostname=$(hostname)` are deliberately excluded by the
    # `[a-z]+` value class.
    pattern = re.compile(rf"{re.escape(cr._PROBE_LINE_PREFIX)} ([a-z][a-z0-9-]*)=([a-z]+)\b")
    found = pattern.findall(script)
    assert found, "script-vs-parser parity check found no testable lines"
    for key, value in found:
        if key in cr._INFORMATIONAL_KEYS:
            continue
        assert value in known, (
            f"script emits {key}={value!r} but parser's alphabet is {sorted(known)};"
            " update _PROBE_*_VALUES or the script to keep them in sync."
        )


def test_probe_script_checks_miint_read_fastx():
    """The probe LOADs the deploy-staged miint and runs the jobs' read_fastx call
    on the compute node — a missing/wrong staged build should fail at deploy, not
    at the first reference-load job."""
    script = cr.build_probe_script(path_scratch="/scratch/qiita")
    assert "miint-read-fastx=ok" in script
    assert "miint-read-fastx=fail" in script
    assert "max_batch_bytes" in script


def test_probe_script_is_valid_bash():
    """The generated probe script must parse as valid bash. Regression for an
    f-string newline-escape written inside a comment that expanded to a real
    newline, splitting the comment and leaving an unmatched backtick — so bash
    aborted at parse time (exit 2, ~1s on the cluster) before any check ran. The
    Python-side substring assertions above never caught it; only parsing the
    whole script does. `bash -n` reads the script on stdin and checks syntax
    without executing it."""
    import shutil
    import subprocess

    bash = shutil.which("bash")
    if bash is None:  # pragma: no cover - bash is present on CI (ubuntu + macos)
        pytest.skip("bash not available")
    script = cr.build_probe_script(path_scratch="/scratch/qiita")
    proc = subprocess.run([bash, "-n"], input=script, capture_output=True, text=True)
    assert proc.returncode == 0, f"probe script is not valid bash:\n{proc.stderr}"


def test_probe_script_checks_miint_sequence_split():
    """The probe also exercises miint's native `sequence_split` chunker, which is
    newer than read_fastx — a staged build that predates it passes the read_fastx
    probe but fails here, catching the stale-stage case at deploy rather than at
    the first reference-load job."""
    script = cr.build_probe_script(path_scratch="/scratch/qiita")
    assert "sequence_split(" in script
    assert "miint-sequence-split=ok" in script
    assert "miint-sequence-split=fail" in script


def test_parse_probe_log_unknown_value_defaults_to_fail():
    """If the probe emits a value the parser doesn't recognize, it
    should be reported as a failure rather than silently passed —
    contract drift between the script and parser is itself a bug
    the operator should see."""
    log = "compute-readiness: native-import=weird-new-state\n"
    results = cr._parse_probe_log(log)
    assert len(results) == 1
    assert results[0].name == "probe/native-import"
    assert results[0].status == "fail"


# ---------------------------------------------------------------------------
# Probe submission orchestration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_submit_probe_and_collect_happy_path(monkeypatch, tmp_path):
    """End-to-end orchestration: submit, poll once running, poll
    again completed, read pre-staged log, parse. The slurmrestd
    surface is mocked via httpx.MockTransport; the log is written by
    the test so submit_probe_and_collect's read succeeds."""
    jwt_path = tmp_path / "jwt"
    jwt_path.write_text(_make_jwt("qiita-orch"))
    settings = _make_settings(jwt_path)

    # submit_probe_and_collect accepts log_path explicitly for tests
    # so we can pre-stage a deterministic file. Production omits it
    # and the path is computed from pid + random suffix.
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    log_path = log_dir / "probe.log"
    log_path.write_text(
        "compute-readiness: hostname=node42\n"
        "compute-readiness: native-python-on-compute=ok path=/x/python\n"
        "compute-readiness: native-import=ok\n"
        "compute-readiness: shared-fs-visible=ok path=/scratch/qiita\n"
        "compute-readiness: shared-fs-writable=ok\n"
        "compute-readiness: cp-from-compute=ok cp_url=https://qiita.example.org\n"
    )

    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(f"{request.method} {request.url.path}")
        if request.method == "POST" and request.url.path.endswith("/job/submit"):
            return httpx.Response(200, json={"job_id": 7})
        if request.method == "GET" and "/job/7" in request.url.path:
            n_get = sum(1 for c in call_log if c.startswith("GET"))
            state = "RUNNING" if n_get == 1 else "COMPLETED"
            return httpx.Response(
                200, json={"jobs": [{"job_id": 7, "job_state": [state], "exit_code": {}}]}
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    # Patch SlurmrestdClient as imported into the `cr` module so the
    # MockTransport wires into `submit_probe_and_collect`'s `async with`.
    real_cls = cr.SlurmrestdClient

    class _PatchedClient(real_cls):
        def __init__(self, *args, **kwargs):
            kwargs["http_client"] = httpx.AsyncClient(
                base_url=kwargs.get("base_url", "http://slurm-test:6820"),
                transport=httpx.MockTransport(handler),
                timeout=5,
            )
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(cr, "SlurmrestdClient", _PatchedClient)

    results = await cr.submit_probe_and_collect(
        settings,
        poll_interval_seconds=0,
        probe_timeout_seconds=30,
        log_path=log_path,
    )
    by_name = {r.name: r for r in results}
    assert by_name["slurm-submit"].status == "pass"
    assert by_name["slurm-probe-completed"].status == "pass"
    assert by_name["probe/native-python-on-compute"].status == "pass"
    assert by_name["probe/cp-from-compute"].status == "pass"


def test_default_probe_log_dir_is_on_shared_fs(tmp_path):
    """The probe log must default onto the shared filesystem (PATH_SCRATCH/ticket),
    NOT node-local /tmp: SLURM writes the probe's stdout on the compute node and
    the head node reads it back to surface native-import / miint-read-fastx. A
    node-local /tmp path is written on the compute node and unreadable across that
    boundary, so those results never surface."""
    jwt = tmp_path / "jwt"
    jwt.write_text(_make_jwt("qiita-orch"))
    settings = _make_settings(jwt)  # path_scratch="/scratch/qiita"
    assert cr._default_probe_log_dir(settings) == Path("/scratch/qiita/ticket")


def test_build_probe_submit_payload_includes_required_fields(tmp_path):
    jwt = tmp_path / "jwt"
    jwt.write_text(_make_jwt("qiita-orch"))
    settings = _make_settings(jwt)
    payload = cr.build_probe_submit_payload(
        script="#!/bin/bash\nexit 0\n",
        settings=settings,
        log_path=Path("/tmp/probe.log"),
    )
    assert payload["script"].startswith("#!/bin/bash")
    job = payload["job"]
    assert job["name"] == cr._PROBE_JOB_NAME
    assert job["account"] == "qiita-prod"
    assert job["partition"] == "qiita"
    assert job["cpus_per_task"] == cr._PROBE_CPU
    # env list must include the cp_url and co_to_cp_token the probe
    # script reads — these are the probe-from-compute knobs.
    env = dict(item.split("=", 1) for item in job["environment"])
    assert env["QIITA_CP_URL"] == "https://qiita.example.org"
    assert env["CO_TO_CP_TOKEN"] == "probe-co-to-cp-token"
    # PATH_SCRATCH is passed through so the probe can check
    # PATH_SCRATCH/ticket (the per-ticket workspace) on the compute node.
    assert env["PATH_SCRATCH"] == "/scratch/qiita"
    # `QIITA_NATIVE_PYTHON`, not `SLURM_NATIVE_PYTHON`: slurmd handles
    # its own `SLURM_*` namespace on the compute node and may reset
    # user-set vars in it. Renaming sidesteps the collision.
    assert env["QIITA_NATIVE_PYTHON"] == settings.slurm.native_python
    assert "SLURM_NATIVE_PYTHON" not in env
    # qos is omitted when settings.slurm.qos is empty.
    assert "qos" not in job


def test_build_probe_submit_payload_emits_qos_when_set(tmp_path):
    jwt = tmp_path / "jwt"
    jwt.write_text(_make_jwt("qiita-orch"))
    settings = _make_settings(jwt)
    # Replace the slurm settings with one that has a qos. Using
    # dataclasses.replace because SlurmSettings is frozen.
    import dataclasses as _dc

    settings = _dc.replace(settings, slurm=_dc.replace(settings.slurm, qos="qiita_norm"))
    payload = cr.build_probe_submit_payload(
        script="#!/bin/bash\nexit 0\n",
        settings=settings,
        log_path=Path("/tmp/probe.log"),
    )
    assert payload["job"]["qos"] == "qiita_norm"


# ---------------------------------------------------------------------------
# Top-level renderer
# ---------------------------------------------------------------------------


def test_render_human_summary():
    results = [
        cr.CheckResult("a", "pass", "ok"),
        cr.CheckResult("b", "fail", "bad"),
        cr.CheckResult("c", "skip", "n/a"),
    ]
    text = cr._render_human(results)
    assert "✓ a: ok" in text
    assert "✗ b: bad" in text
    assert "· c: n/a" in text
    assert "1 pass, 1 fail, 1 skip" in text


def test_render_json_shape():
    results = [cr.CheckResult("a", "pass", "ok"), cr.CheckResult("b", "fail", "bad")]
    parsed = json.loads(cr._render_json(results))
    assert parsed["summary"] == {"pass": 1, "fail": 1, "skip": 0}
    assert parsed["results"][0] == {"name": "a", "status": "pass", "detail": "ok"}


# ---------------------------------------------------------------------------
# Misinvocation guard (issue #72): a non-slurm backend on a real orchestrator
# host means the command was run as the wrong user / with the wrong env sourced,
# so COMPUTE_BACKEND defaulted to "local". That used to be a benign skip + exit
# 0 (reads as a pass). The "real host" signal is the orchestrator env file
# existing — file-existence ONLY, so a dev box / CI runner still skips.
# ---------------------------------------------------------------------------


def _local_settings() -> Settings:
    return Settings(
        backend_type="local",
        path_scratch="/scratch/qiita",
        path_derived="/scratch/persistent",
        cp_to_co_token="",
        cp_url="",
        co_to_cp_token="",
        slurm=None,
    )


def test_main_misinvoked_local_on_orchestrator_host_fails(monkeypatch, tmp_path, capsys):
    """COMPUTE_BACKEND defaulted to local but the orchestrator env file exists →
    loud fail + exit 1 naming the correct `sudo -u qiita-orch` invocation."""
    monkeypatch.setattr(cr.Settings, "from_env", classmethod(lambda cls, **kw: _local_settings()))
    env_file = tmp_path / "compute-orchestrator.env"
    env_file.write_text("COMPUTE_BACKEND=local\n")
    monkeypatch.setattr(cr, "ORCHESTRATOR_ENV_PATH", str(env_file))

    rc = cr.main(["--no-slurm-probe"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "backend-is-slurm" in out
    assert "sudo -u qiita-orch" in out


def test_main_local_on_dev_box_still_skips(monkeypatch, tmp_path, capsys):
    """Genuine dev-box local backend (no orchestrator env file) stays a skip /
    exit 0 — the heuristic is file-existence only, never env-var presence."""
    monkeypatch.setattr(cr.Settings, "from_env", classmethod(lambda cls, **kw: _local_settings()))
    monkeypatch.setattr(cr, "ORCHESTRATOR_ENV_PATH", str(tmp_path / "absent.env"))

    rc = cr.main(["--no-slurm-probe"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "0 fail" in out
