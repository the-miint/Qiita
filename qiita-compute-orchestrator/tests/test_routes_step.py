"""Tests for the POST /api/v1/step/* routes (submit / status / result /
find-by-name).

Bearer-token enforcement, request-shape validation, and dispatch wiring
are exercised; the backend itself is stubbed so we don't need DuckDB or
miint to test the route surface.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from qiita_common.models import ComputeTarget, StepStatus
from qiita_common.testing.containers import REFERENCE_HASH_CONTAINER

from qiita_compute_orchestrator.backend import (
    ComputeBackend,
    FoundJob,
    StepHandle,
    StepStatusInfo,
)
from qiita_compute_orchestrator.main import app


@dataclass(frozen=True)
class _RecordedCall:
    """One recorded call into `_RecordingBackend.submit_step`. Per-attribute
    access keeps test assertions readable; adding a new protocol kwarg
    means adding one field here rather than re-counting tuple slots."""

    name: str
    inputs: dict[str, Path]
    workspace: Path
    scope_target: dict[str, Any]
    work_ticket_idx: int
    container: str | None
    module: str | None
    entrypoint: str | None
    baseline_resources: Any


class _RecordingBackend(ComputeBackend):
    def __init__(self) -> None:
        self.calls: list[_RecordedCall] = []
        # find-by-name: tests set `found_jobs` to script the route's response
        # and read `find_by_name_calls` to assert what was looked up.
        self.found_jobs: list[FoundJob] = []
        self.find_by_name_calls: list[str] = []

    # Stubbed as a synchronous backend: submit_step records the forwarded
    # call and returns a terminal handle (no SLURM hop), so the route tests
    # can assert dispatch wiring without DuckDB / miint.
    async def submit_step(
        self,
        name: str,
        inputs: dict[str, Path],
        workspace: Path,
        *,
        scope_target: dict[str, Any],
        work_ticket_idx: int,
        attempt: int = 0,
        container: str | None = None,
        module: str | None = None,
        entrypoint: str | None = None,
        baseline_resources=None,
    ) -> StepHandle:
        self.calls.append(
            _RecordedCall(
                name=name,
                inputs=dict(inputs),
                workspace=workspace,
                scope_target=scope_target,
                work_ticket_idx=work_ticket_idx,
                container=container,
                module=module,
                entrypoint=entrypoint,
                baseline_resources=baseline_resources,
            )
        )
        return StepHandle(
            compute_target=ComputeTarget.LOCAL,
            step_name=name,
            terminal_outputs={"manifest": workspace / "manifest.parquet"},
        )

    async def status_step(self, handle: StepHandle) -> StepStatusInfo:
        return StepStatusInfo(status=StepStatus.COMPLETED)

    async def result_step(self, handle: StepHandle, status: StepStatusInfo) -> dict[str, Path]:
        return handle.terminal_outputs or {}

    async def find_jobs_by_name(self, job_name: str) -> list[FoundJob]:
        self.find_by_name_calls.append(job_name)
        return list(self.found_jobs)


@pytest.fixture
def http_client():
    """A TestClient with a recording backend swapped in. The stock
    LocalBackend would try to install miint on every test."""
    with TestClient(app) as client:
        backend = _RecordingBackend()
        app.state.backend = backend
        yield client, backend


def test_settings_resolves_token_from_env():
    """Sanity-check Settings.from_env reads the dev-mode env override."""
    from qiita_compute_orchestrator.config import Settings

    assert os.environ.get("QIITA_ALLOW_TOKEN_ENV") == "true"
    s = Settings.from_env()
    assert s.cp_to_co_token == os.environ["CP_TO_CO_TOKEN"]


# ============================================================================
# Decoupled routes: /step/submit, /step/status, /step/result
# ============================================================================


def test_step_submit_requires_bearer_token(http_client):
    from qiita_common.api_paths import URL_STEP_SUBMIT

    client, _ = http_client
    resp = client.post(
        URL_STEP_SUBMIT,
        json={
            "step_name": "hash",
            "inputs": {},
            "workspace": "/tmp/ws",
            "scope_target": {"kind": "reference", "reference_idx": 1},
            "work_ticket_idx": 1,
            "container": REFERENCE_HASH_CONTAINER,
        },
    )
    assert resp.status_code == 401


def test_step_submit_dispatches_and_returns_handle(http_client, cp_to_co_token, tmp_path):
    """POST /step/submit forwards to backend.submit_step and serializes the
    returned StepHandle to the wire shape; `attempt` rides through."""
    from qiita_common.api_paths import URL_STEP_SUBMIT

    client, backend = http_client
    resp = client.post(
        URL_STEP_SUBMIT,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "hash",
            "inputs": {"fasta_path": "/scratch/x.fa"},
            "workspace": str(tmp_path),
            "scope_target": {"kind": "reference", "reference_idx": 7},
            "work_ticket_idx": 99,
            "attempt": 2,
            "container": REFERENCE_HASH_CONTAINER,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["compute_target"] == "local"  # _RecordingBackend is synchronous
    assert body["step_name"] == "hash"
    assert body["terminal_outputs"]["manifest"].endswith("manifest.parquet")
    # The recording backend's submit_step records the forwarded call.
    assert len(backend.calls) == 1
    assert backend.calls[0].work_ticket_idx == 99


def test_step_status_returns_status(http_client, cp_to_co_token):
    from qiita_common.api_paths import URL_STEP_STATUS

    client, _ = http_client
    resp = client.post(
        URL_STEP_STATUS,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "handle": {
                "compute_target": "slurm",
                "step_name": "hash",
                "slurm_job_id": 4242,
                "output_path": "/scratch/ws/output",
                "logs_path": "/scratch/ws/logs",
            }
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "completed"  # _RecordingBackend.status_step


def test_step_result_returns_outputs(http_client, cp_to_co_token):
    from qiita_common.api_paths import URL_STEP_RESULT

    client, _ = http_client
    resp = client.post(
        URL_STEP_RESULT,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "handle": {
                "compute_target": "local",
                "step_name": "fastq",
                "terminal_outputs": {"result": "/scratch/ws/result.parquet"},
            },
            "status": {"status": "completed"},
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["outputs"] == {"result": "/scratch/ws/result.parquet"}


def test_step_submit_serializes_backend_failure(http_client, cp_to_co_token, tmp_path):
    """A BackendFailure from submit_step serializes through the route into
    the same structured shape the runner reconstructs for retry."""
    from qiita_common.api_paths import URL_STEP_SUBMIT
    from qiita_common.backend_failure import (
        BACKEND_FAILURE_HEADER,
        BACKEND_FAILURE_HTTP_STATUS,
        BackendFailure,
        FailureKind,
    )
    from qiita_common.models import WorkTicketFailureStage

    client, backend = http_client

    async def boom(*args, **kwargs):
        raise BackendFailure(
            kind=FailureKind.SLURMRESTD_UNREACHABLE,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name="hash",
            reason="slurmrestd 503 on submit",
        )

    backend.submit_step = boom  # type: ignore[method-assign]
    resp = client.post(
        URL_STEP_SUBMIT,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "hash",
            "inputs": {},
            "workspace": str(tmp_path),
            "scope_target": {"kind": "reference", "reference_idx": 1},
            "work_ticket_idx": 1,
            "container": REFERENCE_HASH_CONTAINER,
        },
    )
    assert resp.status_code == BACKEND_FAILURE_HTTP_STATUS
    assert resp.headers[BACKEND_FAILURE_HEADER] == "1"
    assert resp.json()["kind"] == "slurmrestd_unreachable"


def test_step_status_and_result_require_bearer_token(http_client):
    """Both new endpoints are gated by the CP↔CO token, same as submit."""
    from qiita_common.api_paths import URL_STEP_RESULT, URL_STEP_STATUS

    client, _ = http_client
    handle = {"compute_target": "slurm", "step_name": "hash", "slurm_job_id": 1}
    assert client.post(URL_STEP_STATUS, json={"handle": handle}).status_code == 401
    assert (
        client.post(
            URL_STEP_RESULT, json={"handle": handle, "status": {"status": "completed"}}
        ).status_code
        == 401
    )


def test_step_result_serializes_backend_failure(http_client, cp_to_co_token):
    """A BackendFailure from result_step (e.g. a contract violation on a
    terminal-but-broken output) serializes through the route."""
    from qiita_common.api_paths import URL_STEP_RESULT
    from qiita_common.backend_failure import (
        BACKEND_FAILURE_HEADER,
        BACKEND_FAILURE_HTTP_STATUS,
        BackendFailure,
        FailureKind,
    )
    from qiita_common.models import WorkTicketFailureStage

    client, backend = http_client

    async def boom(*args, **kwargs):
        raise BackendFailure(
            kind=FailureKind.CONTRACT_VIOLATION,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name="hash",
            reason="manifest missing",
        )

    backend.result_step = boom  # type: ignore[method-assign]
    resp = client.post(
        URL_STEP_RESULT,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "handle": {"compute_target": "slurm", "step_name": "hash", "slurm_job_id": 1},
            "status": {"status": "completed", "raw_state": "COMPLETED", "exit_code": 0},
        },
    )
    assert resp.status_code == BACKEND_FAILURE_HTTP_STATUS
    assert resp.headers[BACKEND_FAILURE_HEADER] == "1"
    assert resp.json()["kind"] == "contract_violation"


def test_step_submit_rejects_wrong_prefix_module(http_client, cp_to_co_token, tmp_path):
    """The module-prefix defense applies to /step/submit too."""
    from qiita_common.api_paths import URL_STEP_SUBMIT

    client, backend = http_client
    resp = client.post(
        URL_STEP_SUBMIT,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "x",
            "inputs": {},
            "workspace": str(tmp_path),
            "scope_target": {"kind": "reference", "reference_idx": 1},
            "work_ticket_idx": 1,
            "module": "os.system",
        },
    )
    assert resp.status_code == 422
    assert "qiita_compute_orchestrator.jobs." in resp.text
    assert backend.calls == []


# ============================================================================
# Decoupled route: /step/find-by-name (idempotency / recovery)
# ============================================================================


def test_step_find_by_name_requires_bearer_token(http_client):
    from qiita_common.api_paths import URL_STEP_FIND_BY_NAME

    client, _ = http_client
    resp = client.post(URL_STEP_FIND_BY_NAME, json={"job_name": "qiita-wt1-hash-a0"})
    assert resp.status_code == 401


def test_step_find_by_name_returns_matching_jobs(http_client, cp_to_co_token):
    """POST /step/find-by-name forwards to backend.find_jobs_by_name and
    serializes the matches (id + status snapshot)."""
    from qiita_common.api_paths import URL_STEP_FIND_BY_NAME

    client, backend = http_client
    backend.found_jobs = [
        FoundJob(
            slurm_job_id=4242,
            job_name="qiita-wt99-hash-a0",
            status=StepStatusInfo(status=StepStatus.RUNNING, raw_state="RUNNING"),
        )
    ]
    resp = client.post(
        URL_STEP_FIND_BY_NAME,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={"job_name": "qiita-wt99-hash-a0"},
    )
    assert resp.status_code == 200, resp.text
    assert backend.find_by_name_calls == ["qiita-wt99-hash-a0"]
    jobs = resp.json()["jobs"]
    assert len(jobs) == 1
    assert jobs[0]["slurm_job_id"] == 4242
    assert jobs[0]["job_name"] == "qiita-wt99-hash-a0"
    assert jobs[0]["status"]["status"] == "running"


def test_step_find_by_name_empty_when_no_match(http_client, cp_to_co_token):
    from qiita_common.api_paths import URL_STEP_FIND_BY_NAME

    client, backend = http_client
    backend.found_jobs = []
    resp = client.post(
        URL_STEP_FIND_BY_NAME,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={"job_name": "qiita-wt1-hash-a0"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["jobs"] == []


def test_step_find_by_name_serializes_backend_failure(http_client, cp_to_co_token):
    """An unreachable slurmrestd serializes the typed BackendFailure so the
    runner's recovery treats it as transient and retries the lookup."""
    from qiita_common.api_paths import URL_STEP_FIND_BY_NAME
    from qiita_common.backend_failure import (
        BACKEND_FAILURE_HEADER,
        BACKEND_FAILURE_HTTP_STATUS,
        BackendFailure,
        FailureKind,
    )
    from qiita_common.models import WorkTicketFailureStage

    client, backend = http_client

    async def boom(*args, **kwargs):
        raise BackendFailure(
            kind=FailureKind.SLURMRESTD_UNREACHABLE,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name="qiita-wt1-hash-a0",
            reason="slurmrestd 503 on job list",
        )

    backend.find_jobs_by_name = boom  # type: ignore[method-assign]
    resp = client.post(
        URL_STEP_FIND_BY_NAME,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={"job_name": "qiita-wt1-hash-a0"},
    )
    assert resp.status_code == BACKEND_FAILURE_HTTP_STATUS
    assert resp.headers[BACKEND_FAILURE_HEADER] == "1"
    assert resp.json()["kind"] == "slurmrestd_unreachable"
