"""Tests for ComputeBackendClient (auth params + submit / status / result /
find-by-name wire shape)."""

import json
from pathlib import Path

import httpx
import pytest

from qiita_common.testing.containers import REFERENCE_HASH_CONTAINER


def test_client_importable():
    from qiita_common.compute_backend_client import ComputeBackendClient

    assert ComputeBackendClient is not None


def test_client_raises_when_both_token_and_token_path_set(tmp_path):
    from qiita_common.compute_backend_client import ComputeBackendClient

    p = tmp_path / "tok"
    p.write_text("cp-to-co-token")
    with pytest.raises(ValueError, match="mutually exclusive"):
        ComputeBackendClient(
            "http://localhost:8081",
            api_token="inline",
            api_token_path=p,
        )


def test_client_raises_when_neither_token_nor_token_path_set():
    from qiita_common.compute_backend_client import ComputeBackendClient

    with pytest.raises(ValueError, match="exactly one"):
        ComputeBackendClient("http://localhost:8081")


def test_client_reads_token_from_path(tmp_path):
    from qiita_common.compute_backend_client import ComputeBackendClient

    p = tmp_path / "tok"
    p.write_text("from-file\n")  # trailing newline must be stripped
    client = ComputeBackendClient("http://localhost:8081", api_token_path=p)
    assert client._token == "from-file"


def test_client_attaches_authorization_header(tmp_path):
    from qiita_common.compute_backend_client import ComputeBackendClient

    p = tmp_path / "tok"
    p.write_text("AAAA")
    client = ComputeBackendClient("http://localhost:8081", api_token_path=p)
    assert client._http.headers["Authorization"] == "Bearer AAAA"


def test_client_repr_redacts_token(tmp_path):
    from qiita_common.compute_backend_client import ComputeBackendClient

    p = tmp_path / "tok"
    p.write_text("BBBB")
    client = ComputeBackendClient("http://localhost:8081", api_token_path=p)
    s = repr(client)
    assert "BBBB" not in s
    assert "<redacted>" in s


def _capture_transport(captured: list, response_body: dict):
    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200,
            content=json.dumps(response_body).encode(),
            headers={"content-type": "application/json"},
        )

    return httpx.MockTransport(handler)


# ============================================================================
# Decoupled client methods: submit_step / status_step / result_step
# ============================================================================


def _client_with(transport: httpx.MockTransport):
    from qiita_common.compute_backend_client import ComputeBackendClient

    custom = httpx.AsyncClient(
        base_url="http://localhost:8081",
        transport=transport,
        headers={"Authorization": "Bearer xx"},
    )
    return ComputeBackendClient("http://localhost:8081", api_token="unused", http_client=custom)


def _failure_transport(kind: str):
    from qiita_common.backend_failure import BACKEND_FAILURE_HEADER, BACKEND_FAILURE_HTTP_STATUS

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            BACKEND_FAILURE_HTTP_STATUS,
            content=json.dumps(
                {"kind": kind, "stage": "step_run", "step_name": "hash", "reason": "boom"}
            ).encode(),
            headers={"content-type": "application/json", BACKEND_FAILURE_HEADER: "1"},
        )

    return httpx.MockTransport(handler)


async def test_submit_step_posts_to_submit_endpoint_and_returns_handle():
    from qiita_common.api_paths import URL_STEP_SUBMIT
    from qiita_common.models import ComputeTarget, StepHandleWire

    captured: list[httpx.Request] = []
    transport = _capture_transport(
        captured,
        StepHandleWire(
            compute_target=ComputeTarget.SLURM,
            step_name="hash",
            slurm_job_id=4242,
            job_name="qiita-wt99-hash-a2",
            output_path="/ws/output",
            logs_path="/ws/logs",
        ).model_dump(),
    )
    handle = await _client_with(transport).submit_step(
        step_name="hash",
        inputs={"fasta_path": Path("/data/in.fa")},
        workspace=Path("/ws"),
        scope_target={"kind": "reference", "reference_idx": 42},
        work_ticket_idx=99,
        attempt=2,
        container=REFERENCE_HASH_CONTAINER,
    )
    assert len(captured) == 1
    assert captured[0].url.path == URL_STEP_SUBMIT
    body = json.loads(captured[0].content)
    assert body["attempt"] == 2
    assert body["work_ticket_idx"] == 99
    assert handle.compute_target == ComputeTarget.SLURM
    assert handle.slurm_job_id == 4242


async def test_status_step_posts_handle_and_returns_status():
    from qiita_common.api_paths import URL_STEP_STATUS
    from qiita_common.models import ComputeTarget, StepHandleWire, StepStatus, StepStatusWire

    captured: list[httpx.Request] = []
    transport = _capture_transport(
        captured,
        StepStatusWire(status=StepStatus.RUNNING, raw_state="RUNNING").model_dump(),
    )
    handle = StepHandleWire(compute_target=ComputeTarget.SLURM, step_name="hash", slurm_job_id=4242)
    status = await _client_with(transport).status_step(handle)
    assert captured[0].url.path == URL_STEP_STATUS
    assert json.loads(captured[0].content)["handle"]["slurm_job_id"] == 4242
    assert status.status == StepStatus.RUNNING


async def test_result_step_posts_and_returns_output_paths():
    from qiita_common.api_paths import URL_STEP_RESULT
    from qiita_common.models import (
        ComputeTarget,
        StepHandleWire,
        StepResultResponse,
        StepStatus,
        StepStatusWire,
    )

    captured: list[httpx.Request] = []
    transport = _capture_transport(
        captured, StepResultResponse(outputs={"result": "/ws/result.parquet"}).model_dump()
    )
    outputs = await _client_with(transport).result_step(
        StepHandleWire(compute_target=ComputeTarget.SLURM, step_name="hash", slurm_job_id=1),
        StepStatusWire(status=StepStatus.COMPLETED, raw_state="COMPLETED", exit_code=0),
    )
    assert captured[0].url.path == URL_STEP_RESULT
    assert outputs == {"result": Path("/ws/result.parquet")}


async def test_find_jobs_by_name_posts_and_returns_jobs():
    from qiita_common.api_paths import URL_STEP_FIND_BY_NAME
    from qiita_common.models import (
        FoundJobWire,
        StepFindByNameResponse,
        StepStatus,
        StepStatusWire,
    )

    captured: list[httpx.Request] = []
    transport = _capture_transport(
        captured,
        StepFindByNameResponse(
            jobs=[
                FoundJobWire(
                    slurm_job_id=4242,
                    job_name="qiita-wt99-hash-a0",
                    status=StepStatusWire(status=StepStatus.RUNNING, raw_state="RUNNING"),
                )
            ]
        ).model_dump(),
    )
    jobs = await _client_with(transport).find_jobs_by_name("qiita-wt99-hash-a0")
    assert len(captured) == 1
    assert captured[0].url.path == URL_STEP_FIND_BY_NAME
    assert json.loads(captured[0].content)["job_name"] == "qiita-wt99-hash-a0"
    assert len(jobs) == 1
    assert jobs[0].slurm_job_id == 4242
    assert jobs[0].status.status == StepStatus.RUNNING


async def test_find_jobs_by_name_empty_when_no_match():
    from qiita_common.models import StepFindByNameResponse

    transport = _capture_transport([], StepFindByNameResponse(jobs=[]).model_dump())
    jobs = await _client_with(transport).find_jobs_by_name("qiita-wt1-x-a0")
    assert jobs == []


async def test_find_jobs_by_name_reconstructs_transient_backend_failure():
    """An unreachable slurmrestd surfaces as a transient BackendFailure so the
    runner's recovery retries the lookup rather than failing the ticket."""
    from qiita_common.backend_failure import BackendFailure, FailureKind

    with pytest.raises(BackendFailure) as ei:
        await _client_with(_failure_transport("slurmrestd_unreachable")).find_jobs_by_name(
            "qiita-wt1-x-a0"
        )
    assert ei.value.kind is FailureKind.SLURMRESTD_UNREACHABLE
    assert ei.value.transient is True


async def test_find_jobs_by_name_transport_error_becomes_orchestrator_unreachable():
    from qiita_common.backend_failure import BackendFailure, FailureKind

    with pytest.raises(BackendFailure) as ei:
        await _client_with(
            _transport_error_transport(httpx.ConnectError("refused"))
        ).find_jobs_by_name("qiita-wt1-x-a0")
    assert ei.value.kind is FailureKind.ORCHESTRATOR_UNREACHABLE
    assert ei.value.transient is True


async def test_submit_step_reconstructs_backend_failure():
    from qiita_common.backend_failure import BackendFailure, FailureKind
    from qiita_common.models import ComputeTarget

    with pytest.raises(BackendFailure) as ei:
        await _client_with(_failure_transport("contract_violation")).submit_step(
            step_name="hash",
            inputs={},
            workspace=Path("/ws"),
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=1,
            container=REFERENCE_HASH_CONTAINER,
        )
    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    assert ComputeTarget  # imported to keep parity with the handle-shaped tests


async def test_status_step_reconstructs_transient_backend_failure():
    """A SLURMRESTD_UNREACHABLE from status_step reconstructs as a transient
    BackendFailure so the runner keeps polling rather than failing."""
    from qiita_common.backend_failure import BackendFailure, FailureKind
    from qiita_common.models import ComputeTarget, StepHandleWire

    with pytest.raises(BackendFailure) as ei:
        await _client_with(_failure_transport("slurmrestd_unreachable")).status_step(
            StepHandleWire(compute_target=ComputeTarget.SLURM, step_name="hash", slurm_job_id=1)
        )
    assert ei.value.kind is FailureKind.SLURMRESTD_UNREACHABLE
    assert ei.value.transient is True


def _transport_error_transport(exc: Exception):
    """A transport that raises `exc` for every request — simulates the CP
    failing to reach the orchestrator (connection refused, timeout, ...)."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise exc

    return httpx.MockTransport(handler)


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ConnectError("connection refused"),
        httpx.ReadTimeout("timed out"),
        httpx.ConnectTimeout("connect timed out"),
    ],
)
async def test_status_step_transport_error_becomes_orchestrator_unreachable(exc):
    """A raw httpx transport/timeout error on the CP→CO hop must surface as a
    transient BackendFailure(ORCHESTRATOR_UNREACHABLE) — not leak as an httpx
    error that the runner's outer handler would mark FAILED (the 600s bug)."""
    from qiita_common.backend_failure import BackendFailure, FailureKind
    from qiita_common.models import ComputeTarget, StepHandleWire

    with pytest.raises(BackendFailure) as ei:
        await _client_with(_transport_error_transport(exc)).status_step(
            StepHandleWire(compute_target=ComputeTarget.SLURM, step_name="hash", slurm_job_id=1)
        )
    assert ei.value.kind is FailureKind.ORCHESTRATOR_UNREACHABLE
    assert ei.value.transient is True
    assert ei.value.step_name == "hash"


async def test_submit_step_transport_error_becomes_orchestrator_unreachable():
    from qiita_common.backend_failure import BackendFailure, FailureKind

    with pytest.raises(BackendFailure) as ei:
        await _client_with(_transport_error_transport(httpx.ConnectError("refused"))).submit_step(
            step_name="hash",
            inputs={},
            workspace=Path("/ws"),
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=1,
            container=REFERENCE_HASH_CONTAINER,
        )
    assert ei.value.kind is FailureKind.ORCHESTRATOR_UNREACHABLE
    assert ei.value.transient is True


async def test_result_step_transport_error_becomes_orchestrator_unreachable():
    from qiita_common.backend_failure import BackendFailure, FailureKind
    from qiita_common.models import ComputeTarget, StepHandleWire, StepStatus, StepStatusWire

    with pytest.raises(BackendFailure) as ei:
        await _client_with(_transport_error_transport(httpx.ReadTimeout("slow"))).result_step(
            StepHandleWire(compute_target=ComputeTarget.SLURM, step_name="hash", slurm_job_id=1),
            StepStatusWire(status=StepStatus.COMPLETED, raw_state="COMPLETED", exit_code=0),
        )
    assert ei.value.kind is FailureKind.ORCHESTRATOR_UNREACHABLE
    assert ei.value.transient is True


def _status_transport(status_code: int):
    """A transport that returns the given HTTP status with no body and no
    BackendFailure discriminator header."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, content=b"")

    return httpx.MockTransport(handler)


@pytest.mark.parametrize("status_code", [500, 502, 503, 504])
async def test_status_step_5xx_becomes_orchestrator_unreachable(status_code):
    """A 5xx from the CO (or the nginx in front of it) — up but borked,
    e.g. restarting during a deploy — is transient: the runner keeps polling
    rather than failing a still-running ticket."""
    from qiita_common.backend_failure import BackendFailure, FailureKind
    from qiita_common.models import ComputeTarget, StepHandleWire

    with pytest.raises(BackendFailure) as ei:
        await _client_with(_status_transport(status_code)).status_step(
            StepHandleWire(compute_target=ComputeTarget.SLURM, step_name="hash", slurm_job_id=1)
        )
    assert ei.value.kind is FailureKind.ORCHESTRATOR_UNREACHABLE
    assert ei.value.transient is True


@pytest.mark.parametrize("status_code", [401, 403, 404])
async def test_status_step_4xx_without_header_stays_http_error(status_code):
    """A 4xx without the discriminator header is a permanent contract / auth
    problem — it must surface as HTTPStatusError (→ a loud ticket failure),
    NOT get swallowed into an infinite retry like a 5xx outage."""
    from qiita_common.models import ComputeTarget, StepHandleWire

    with pytest.raises(httpx.HTTPStatusError):
        await _client_with(_status_transport(status_code)).status_step(
            StepHandleWire(compute_target=ComputeTarget.SLURM, step_name="hash", slurm_job_id=1)
        )
