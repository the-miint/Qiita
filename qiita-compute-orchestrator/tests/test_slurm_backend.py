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
async def test_run_step_requires_container(jwt_path, baseline, tmp_path):
    handler = httpx.MockTransport(lambda req: httpx.Response(500))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            reference_idx=1,
            container=None,
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert "container" in ei.value.reason


@pytest.mark.asyncio
async def test_run_step_requires_baseline_resources(jwt_path, tmp_path):
    handler = httpx.MockTransport(lambda req: httpx.Response(500))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "hash",
            {},
            tmp_path,
            reference_idx=1,
            container="qiita/hash:1.0.0",
            entrypoint=None,
            baseline_resources=None,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert "baseline_resources" in ei.value.reason


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
        reference_idx=42,
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
        reference_idx=42,
        container="qiita/hash:1.0.0",
        entrypoint=None,
        baseline_resources=baseline,
    )
    params = json.loads((tmp_path / "input" / "params.json").read_text())
    assert params["step_name"] == "hash"
    assert params["reference_idx"] == 42
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
            reference_idx=1,
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
            reference_idx=1,
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
            reference_idx=1,
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
            reference_idx=1,
            container="qiita/hash:1.0.0",
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert ei.value.transient is False


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
            reference_idx=1,
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
            reference_idx=1,
            container="qiita/hash:1.0.0",
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.UNKNOWN_PERMANENT
    assert "404" in ei.value.reason
