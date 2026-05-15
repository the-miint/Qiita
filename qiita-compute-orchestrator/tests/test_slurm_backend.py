"""Functional tests for SlurmBackend.run_step.

Wires payload + client + verify together. Driven by httpx.MockTransport
so no live SLURM controller is needed; the test handler shapes
slurmrestd responses to drive each branch of the SLURM-state =>
FailureKind mapping.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import StepBaselineResources, WorkTicketFailureStage
from qiita_common.testing.native_steps import FASTQ_TO_PARQUET_MODULE

from qiita_compute_orchestrator.backends.slurm import SlurmBackend
from qiita_compute_orchestrator.slurm import SlurmrestdClient


@pytest.fixture
def jwt_path(tmp_path):
    p = tmp_path / "jwt"
    p.write_text("test-jwt")
    return p


@pytest.fixture
def baseline():
    return StepBaselineResources(cpu=1, mem_gb=1, walltime_seconds=60)


def _make_backend(handler, jwt_path) -> SlurmBackend:
    client = SlurmrestdClient(
        base_url="http://slurm-test:6820",
        jwt_path=jwt_path,
        user_name="qiita-orch",
        http_client=httpx.AsyncClient(
            base_url="http://slurm-test:6820",
            transport=handler,
            timeout=5,
        ),
    )
    return SlurmBackend(
        client=client,
        partition="qiita",
        account="qiita-prod",
        poll_interval_seconds=0,  # 0 so tests don't sleep
        job_timeout_seconds=60,
    )


def _job_running_then(state: str, *, exit_code: int | None = None, reason: str | None = None):
    """Return a request handler that:
    1st request:  POST /job/submit => returns job_id 1
    2nd request:  GET  /job/1 => RUNNING (drives one polling loop)
    3rd+ request: GET  /job/1 => `state` with optional exit_code / reason
    """
    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(f"{request.method} {request.url.path}")
        if request.method == "POST" and request.url.path.endswith("/job/submit"):
            return httpx.Response(200, json={"job_id": 1})
        if request.method == "GET" and "/job/1" in request.url.path:
            n_get = sum(1 for c in call_log if c.startswith("GET"))
            if n_get == 1:
                return httpx.Response(
                    200,
                    json={"jobs": [{"job_id": 1, "job_state": ["RUNNING"], "exit_code": {}}]},
                )
            payload = {"job_state": [state], "exit_code": {}}
            if exit_code is not None:
                payload["exit_code"] = {
                    "return_code": {"number": exit_code, "set": True, "infinite": False}
                }
            if reason is not None:
                payload["state_reason"] = reason
            return httpx.Response(200, json={"jobs": [{"job_id": 1, **payload}]})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    return httpx.MockTransport(handler), call_log


def _write_completed_output(workspace: Path, *, manifest_extra: dict | None = None) -> None:
    """Pre-create the workspace tree the backend would otherwise build,
    plus a successful manifest.json + the file it declares. Tests that
    drive a COMPLETED state need this so the post-poll verifier finds
    a valid output dir."""
    out = workspace / "output"
    out.mkdir(parents=True, exist_ok=True)
    parquet = out / "result.parquet"
    parquet.write_bytes(b"abc")
    parquet.chmod(0o440)
    manifest = {
        "files": [{"path": "result.parquet", "size_bytes": 3}],
        "outputs": {"manifest": "result.parquet"},
    }
    if manifest_extra:
        manifest.update(manifest_extra)
    (out / "manifest.json").write_text(json.dumps(manifest))
    (out / "manifest.json").chmod(0o440)


# ============================================================================
# Pre-flight validation
# ============================================================================


@pytest.mark.asyncio
async def test_run_step_requires_container_or_module(jwt_path, baseline, tmp_path):
    """Neither runtime field set → CONTRACT_VIOLATION. The wire validator
    on StepRunRequest catches this upstream, but direct callers (and
    this test) bypass the wire, so SlurmBackend re-checks."""
    handler = httpx.MockTransport(lambda req: httpx.Response(500))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container=None,
            module=None,
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert "exactly one" in ei.value.reason


@pytest.mark.asyncio
async def test_run_step_rejects_both_container_and_module(jwt_path, baseline, tmp_path):
    """Both set is a contract violation too — must surface as a typed
    BackendFailure, not as a raw ValueError leaking out of the payload
    builder. Both this case and the "neither set" case above ride the
    same `exactly-one` guard."""
    handler = httpx.MockTransport(lambda req: httpx.Response(500))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="qiita/hash:1.0.0",
            module=FASTQ_TO_PARQUET_MODULE,
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert "exactly one" in ei.value.reason


@pytest.mark.asyncio
async def test_run_step_requires_baseline_resources(jwt_path, tmp_path):
    handler = httpx.MockTransport(lambda req: httpx.Response(500))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="qiita/hash:1.0.0",
            entrypoint=None,
            baseline_resources=None,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert "baseline_resources" in ei.value.reason


@pytest.mark.asyncio
async def test_run_step_container_rejects_non_reference_scope(jwt_path, baseline, tmp_path):
    """Mirror of LocalBackend's container-path scope gate (S4):
    SlurmBackend container steps assume reference_idx is on the
    scope_target. A prep_sample-scoped or study_prep-scoped ticket
    dispatched to a container step would silently produce wrong
    params.json; the gate 422s at submit time instead."""
    handler = httpx.MockTransport(lambda req: httpx.Response(500))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
            work_ticket_idx=99,
            container="qiita/hash:1.0.0",
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert "requires a reference-scoped ticket" in ei.value.reason
    assert "prep_sample" in ei.value.reason


@pytest.mark.asyncio
async def test_run_step_native_accepts_non_reference_scope(jwt_path, baseline, tmp_path):
    """The S4 container-path gate must NOT fire for native steps —
    they thread scope_target through flatten_native_inputs and can
    target any scope kind. Verify by submitting a module step with a
    prep_sample scope; the run gets past the gate and proceeds to the
    slurmrestd submission (which we let fail downstream — we just
    need to confirm the gate didn't catch it)."""
    transport = httpx.MockTransport(
        lambda req: httpx.Response(500, content=b"slurmrestd unreachable")
    )
    backend = _make_backend(transport, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "fastq",
            {},
            tmp_path,
            scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
            work_ticket_idx=99,
            module="qiita_compute_orchestrator.jobs.fastq_to_parquet",
            entrypoint=None,
            baseline_resources=baseline,
        )
    # Past the container-path gate; failure here is from slurmrestd
    # mock returning 500, NOT from a scope-kind contract violation.
    # The kind would be a SLURMRESTD_UNREACHABLE-style failure, not
    # CONTRACT_VIOLATION with a "reference-scoped" reason.
    assert "requires a reference-scoped ticket" not in (ei.value.reason or "")


# ============================================================================
# Happy path
# ============================================================================


@pytest.mark.asyncio
async def test_run_step_completed_returns_outputs(jwt_path, baseline, tmp_path):
    transport, _ = _job_running_then("COMPLETED", exit_code=0)
    backend = _make_backend(transport, jwt_path)

    # Pre-write the output dir's manifest so the verifier passes.
    _write_completed_output(tmp_path)

    outputs = await backend.run_step(
        "hash",
        {"fasta_path": tmp_path / "x.fa"},
        tmp_path,
        scope_target={"kind": "reference", "reference_idx": 42},
        work_ticket_idx=99,
        container="qiita/hash:1.0.0",
        entrypoint="/usr/local/bin/hash",
        baseline_resources=baseline,
    )
    # The manifest declares outputs={"manifest": "result.parquet"}.
    out_dir = tmp_path / "output"
    assert outputs == {"manifest": (out_dir / "result.parquet").resolve()}


@pytest.mark.asyncio
async def test_run_step_writes_params_json(jwt_path, baseline, tmp_path):
    """params.json must end up at <workspace>/input/params.json so the
    container can read it via $QIITA_INPUT_PATH."""
    transport, _ = _job_running_then("COMPLETED", exit_code=0)
    backend = _make_backend(transport, jwt_path)
    _write_completed_output(tmp_path)

    await backend.run_step(
        "hash",
        {"fasta_path": tmp_path / "input.fa"},
        tmp_path,
        scope_target={"kind": "reference", "reference_idx": 42},
        work_ticket_idx=99,
        container="qiita/hash:1.0.0",
        entrypoint=None,
        baseline_resources=baseline,
    )
    params = json.loads((tmp_path / "input" / "params.json").read_text())
    assert params["step_name"] == "hash"
    assert params["scope_target"] == {"kind": "reference", "reference_idx": 42}
    assert params["work_ticket_idx"] == 99
    assert params["inputs"]["fasta_path"].endswith("input.fa")


# ============================================================================
# State => FailureKind mapping
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("slurm_state", "expected_kind", "expected_transient"),
    [
        ("FAILED", FailureKind.EXIT_NONZERO, False),
        ("CANCELLED", FailureKind.EXIT_NONZERO, False),
        ("DEADLINE", FailureKind.EXIT_NONZERO, False),
        ("NODE_FAIL", FailureKind.NODE_FAIL, True),
        ("BOOT_FAIL", FailureKind.NODE_FAIL, True),
        ("OUT_OF_MEMORY", FailureKind.OOM_KILLED, True),
        ("PREEMPTED", FailureKind.PREEMPTED, True),
        ("TIMEOUT", FailureKind.TIMEOUT_BEFORE_START, True),
    ],
)
async def test_run_step_terminal_states_map_to_kinds(
    jwt_path, baseline, tmp_path, slurm_state, expected_kind, expected_transient
):
    """Each SLURM terminal failure state maps to the documented
    FailureKind, and the resulting BackendFailure has the right
    transient-classification."""
    transport, _ = _job_running_then(slurm_state, exit_code=1, reason="something")
    backend = _make_backend(transport, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="qiita/hash:1.0.0",
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == expected_kind
    assert ei.value.transient is expected_transient
    assert ei.value.stage == WorkTicketFailureStage.STEP_RUN
    assert ei.value.step_name == "hash"
    # State name surfaces in reason for ops triage.
    assert slurm_state in ei.value.reason


# ============================================================================
# Launcher-stderr enrichment for native steps
# ============================================================================


@pytest.mark.asyncio
async def test_run_step_native_failure_enriched_from_launcher_stderr(jwt_path, baseline, tmp_path):
    """A native step that fails inside its execute() writes a structured
    JSON line to stderr; SlurmBackend reads that line and uses it to
    populate the BackendFailure so the work_ticket's failure_reason
    carries the application-level message (not just "exit_code=1").

    Sets up SLURM to report FAILED, drops a real stderr file under
    `<workspace>/logs/stderr` matching what jobs/__main__.py would
    emit on a NotImplementedError, and asserts the BackendFailure
    surface."""
    import json

    transport, _ = _job_running_then("FAILED", exit_code=1)
    backend = _make_backend(transport, jwt_path)

    # SlurmBackend creates <workspace>/logs/ itself, but we pre-create
    # the stderr file before run_step so the launcher's would-be output
    # is in place when SlurmBackend looks for it post-poll.
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    launcher_reason = f"native job {FASTQ_TO_PARQUET_MODULE!r} not implemented: skeleton"
    (logs_dir / "stderr").write_text(
        json.dumps(
            {
                "kind": "unknown_permanent",
                "step_name": "fastq",
                "reason": launcher_reason,
            }
        )
        + "\n"
    )

    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "fastq",  # YAML step name passed in by the runner
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            module=FASTQ_TO_PARQUET_MODULE,
            baseline_resources=baseline,
        )

    # Launcher's kind/step_name/reason override the state-based defaults.
    assert ei.value.kind is FailureKind.UNKNOWN_PERMANENT  # not EXIT_NONZERO
    assert ei.value.step_name == "fastq"  # YAML name, not the job name passed at `name=`
    assert "fastq_to_parquet" in ei.value.reason
    assert "not implemented" in ei.value.reason
    # SLURM-side context is preserved in the reason (bracketed suffix)
    # so operators still see the job_id / state / exit_code.
    assert "FAILED" in ei.value.reason
    assert "exit_code=1" in ei.value.reason


@pytest.mark.asyncio
async def test_run_step_falls_back_to_state_based_kind_when_no_launcher_line(
    jwt_path, baseline, tmp_path
):
    """When stderr has no structured line (container step, or an
    infra-killed native job that died before flushing), SlurmBackend
    falls back to the state-based classification — preserves the
    current behavior unchanged."""
    transport, _ = _job_running_then("FAILED", exit_code=1, reason="container exited nonzero")
    backend = _make_backend(transport, jwt_path)

    # Pre-create logs/ with a stderr file that has only container-style
    # text (no JSON line). This is what the container case looks like.
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    (logs_dir / "stderr").write_text("ERROR: container exited with code 1\n")

    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="qiita/hash:1.0.0",
            entrypoint=None,
            baseline_resources=baseline,
        )
    # State-based classification stands.
    assert ei.value.kind is FailureKind.EXIT_NONZERO
    assert ei.value.step_name == "hash"  # the `name` arg, not a launcher-supplied value
    assert "FAILED" in ei.value.reason
    assert "exit_code=1" in ei.value.reason


# ============================================================================
# Verifier integration
# ============================================================================


@pytest.mark.asyncio
async def test_run_step_completed_but_missing_manifest_is_contract_violation(
    jwt_path, baseline, tmp_path
):
    """SLURM job exits 0 but the container didn't write manifest.json.
    Verifier flags it; backend surfaces as CONTRACT_VIOLATION
    (permanent) — retry won't fix a workflow that can't honor the
    contract."""
    transport, _ = _job_running_then("COMPLETED", exit_code=0)
    backend = _make_backend(transport, jwt_path)
    # Don't pre-create the output dir / manifest. The backend creates
    # the directory itself, but the container would have populated it
    # with a manifest — we leave it empty.

    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="qiita/hash:1.0.0",
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert not ei.value.transient
    assert "manifest" in ei.value.reason.lower()


# ============================================================================
# slurmrestd error classification
# ============================================================================


@pytest.mark.asyncio
async def test_run_step_submit_5xx_is_unreachable(jwt_path, baseline, tmp_path):
    """slurmrestd 5xx on submit => SLURMRESTD_UNREACHABLE (retriable)."""
    handler = httpx.MockTransport(lambda req: httpx.Response(503, text="slurmctld down"))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="qiita/hash:1.0.0",
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.SLURMRESTD_UNREACHABLE
    assert ei.value.transient is True


@pytest.mark.asyncio
async def test_run_step_submit_4xx_is_contract_violation(jwt_path, baseline, tmp_path):
    """slurmrestd 4xx on submit => CONTRACT_VIOLATION (permanent) —
    bad payload won't fix itself on retry."""
    handler = httpx.MockTransport(lambda req: httpx.Response(400, json={"errors": ["malformed"]}))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="qiita/hash:1.0.0",
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert ei.value.transient is False


@pytest.mark.asyncio
async def test_run_step_submit_persistent_401_is_unreachable(jwt_path, baseline, tmp_path):
    """slurmrestd 401 that survives the client's JWT-refresh retry must
    classify as SLURMRESTD_UNREACHABLE (retriable). The user's rotation
    pipeline is broken (token unreadable / wrong principal / not
    refreshed) — operator-fixable, not a workflow contract violation."""
    handler = httpx.MockTransport(lambda req: httpx.Response(401, text="bad token"))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="qiita/hash:1.0.0",
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.SLURMRESTD_UNREACHABLE
    assert ei.value.transient is True


@pytest.mark.asyncio
async def test_run_step_submit_transport_error_is_unreachable(jwt_path, baseline, tmp_path):
    """A connection error on submit => SLURMRESTD_UNREACHABLE."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    backend = _make_backend(httpx.MockTransport(handler), jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="qiita/hash:1.0.0",
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.SLURMRESTD_UNREACHABLE


@pytest.mark.asyncio
async def test_run_step_polling_4xx_bails_permanent(jwt_path, baseline, tmp_path):
    """If the job is purged (404) during polling, the backend can't
    know whether it succeeded — bail with UNKNOWN_PERMANENT (the
    failure isn't classifiable)."""
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(f"{request.method} {request.url.path}")
        if request.method == "POST":
            return httpx.Response(200, json={"job_id": 1})
        return httpx.Response(404, json={"errors": ["unknown job"]})

    backend = _make_backend(httpx.MockTransport(handler), jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="qiita/hash:1.0.0",
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.UNKNOWN_PERMANENT
    assert "404" in ei.value.reason
