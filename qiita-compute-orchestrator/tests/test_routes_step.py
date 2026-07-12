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
from qiita_common.models import StepStatus
from qiita_common.testing.containers import REFERENCE_HASH_CONTAINER

from qiita_compute_orchestrator.backend import (
    ComputeBackend,
    FoundJob,
    LocalStepHandle,
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
    derived_inputs: dict[str, str]


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
        derived_inputs: dict[str, str] | None = None,
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
                derived_inputs=dict(derived_inputs or {}),
            )
        )
        return LocalStepHandle(
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
            "handle": {
                "compute_target": "slurm",
                "step_name": "hash",
                "slurm_job_id": 1,
                "output_path": "/scratch/ws/output",
                "logs_path": "/scratch/ws/logs",
            },
            "status": {"status": "completed", "raw_state": "COMPLETED", "exit_code": 0},
        },
    )
    assert resp.status_code == BACKEND_FAILURE_HTTP_STATUS
    assert resp.headers[BACKEND_FAILURE_HEADER] == "1"
    assert resp.json()["kind"] == "contract_violation"


def test_step_submit_serializes_step_no_data(http_client, cp_to_co_token, tmp_path):
    """A StepNoData from submit_step (LocalBackend's empty-well path) serializes
    through the route with the no-data header — distinct from the failure header
    — so the runner reconstructs the terminal no-data signal, not a failure."""
    from qiita_common.api_paths import URL_STEP_SUBMIT
    from qiita_common.backend_failure import (
        BACKEND_FAILURE_HEADER,
        STEP_NO_DATA_HEADER,
        STEP_NO_DATA_HTTP_STATUS,
        StepNoData,
    )

    client, backend = http_client

    async def empty(*args, **kwargs):
        raise StepNoData(step_name="fastq", reason="FASTQ file contains no records: x.fastq")

    backend.submit_step = empty  # type: ignore[method-assign]
    resp = client.post(
        URL_STEP_SUBMIT,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "fastq",
            "inputs": {},
            "workspace": str(tmp_path),
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": 1},
            "work_ticket_idx": 1,
            "module": "qiita_compute_orchestrator.jobs.fastq_to_parquet",
        },
    )
    assert resp.status_code == STEP_NO_DATA_HTTP_STATUS
    assert resp.headers[STEP_NO_DATA_HEADER] == "1"
    # The failure header is NOT set — this is not a failure.
    assert BACKEND_FAILURE_HEADER not in resp.headers
    assert resp.json()["step_name"] == "fastq"
    assert "contains no records" in resp.json()["reason"]


def test_step_result_serializes_step_no_data(http_client, cp_to_co_token):
    """A StepNoData from result_step (SLURM's deferred empty-well path)
    serializes through the route with the no-data header."""
    from qiita_common.api_paths import URL_STEP_RESULT
    from qiita_common.backend_failure import (
        STEP_NO_DATA_HEADER,
        STEP_NO_DATA_HTTP_STATUS,
        StepNoData,
    )

    client, backend = http_client

    async def empty(*args, **kwargs):
        raise StepNoData(step_name="fastq", reason="FASTQ file contains no records: x.fastq")

    backend.result_step = empty  # type: ignore[method-assign]
    resp = client.post(
        URL_STEP_RESULT,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "handle": {
                "compute_target": "slurm",
                "step_name": "fastq",
                "slurm_job_id": 1,
                "output_path": "/scratch/ws/output",
                "logs_path": "/scratch/ws/logs",
            },
            "status": {"status": "failed", "raw_state": "FAILED", "exit_code": 1},
        },
    )
    assert resp.status_code == STEP_NO_DATA_HTTP_STATUS
    assert resp.headers[STEP_NO_DATA_HEADER] == "1"
    assert resp.json()["step_name"] == "fastq"


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


# ============================================================================
# /step/plan — the submit-time resource-sizing hint (backend-agnostic)
# ============================================================================

_QC_MODULE = "qiita_compute_orchestrator.jobs.qc"


def _write_reads(path: Path, n_rows: int) -> Path:
    """Minimal fastq_to_parquet-shaped reads.parquet with `n_rows` rows — enough
    for qc.plan()'s footer count(*). Sequences/quals are irrelevant to plan."""
    import duckdb

    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT i AS sequence_idx, CAST(i AS VARCHAR) AS read_id, "
            "'AAAA' AS sequence1, CAST(NULL AS UTINYINT[]) AS qual1, "
            "CAST(NULL AS VARCHAR) AS sequence2, CAST(NULL AS UTINYINT[]) AS qual2 "
            f"FROM range({n_rows}) t(i)) TO '{path}' (FORMAT PARQUET)"
        )
    return path


def test_step_plan_requires_bearer_token(http_client):
    from qiita_common.api_paths import URL_STEP_PLAN

    client, _ = http_client
    resp = client.post(
        URL_STEP_PLAN,
        json={
            "step_name": "qc",
            "inputs": {},
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": 1},
            "work_ticket_idx": 1,
            "module": _QC_MODULE,
        },
    )
    assert resp.status_code == 401


def test_step_plan_requires_module(http_client, cp_to_co_token):
    """module is native-only and required — a body without it is a 422."""
    from qiita_common.api_paths import URL_STEP_PLAN

    client, _ = http_client
    resp = client.post(
        URL_STEP_PLAN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "qc",
            "inputs": {},
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": 1},
            "work_ticket_idx": 1,
        },
    )
    assert resp.status_code == 422


def test_step_plan_returns_qc_walltime_hint(http_client, cp_to_co_token, tmp_path):
    """End-to-end: the route flattens inputs, runs the real qc.plan(), and
    returns its walltime hint (memory/cpu left to the baseline)."""
    from qiita_common.api_paths import URL_STEP_PLAN

    client, _ = http_client
    reads = _write_reads(tmp_path / "reads.parquet", 3)
    resp = client.post(
        URL_STEP_PLAN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "qc",
            "inputs": {"reads": str(reads), "adapter_parquet": str(tmp_path / "a.parquet")},
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": 42},
            "work_ticket_idx": 9,
            "module": _QC_MODULE,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # 3 reads: base 300s + ceil(3/1e6 * 30) = 301s. mem/cpu untouched (None).
    assert body["walltime_seconds"] == 301
    assert body["mem_gb"] is None
    assert body["cpu"] is None


def test_step_plan_advisory_degrade_on_broken_module(http_client, cp_to_co_token):
    """A native module path that can't be imported is a CONTRACT_VIOLATION in
    the dispatcher, but the route degrades it to an EMPTY hint (200, all None)
    so the CP falls back to the baseline — plan is never allowed to fail a step."""
    from qiita_common.api_paths import URL_STEP_PLAN

    client, _ = http_client
    resp = client.post(
        URL_STEP_PLAN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "ghost",
            "inputs": {},
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": 1},
            "work_ticket_idx": 1,
            "module": "qiita_compute_orchestrator.jobs.definitely_not_real",
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"cpu": None, "mem_gb": None, "walltime_seconds": None}


def test_step_plan_rejects_non_native_module(http_client, cp_to_co_token):
    """Defense in depth: a module outside NATIVE_MODULE_PREFIX is a 422 (same
    guard as submit), not an advisory degrade."""
    from qiita_common.api_paths import URL_STEP_PLAN

    client, _ = http_client
    resp = client.post(
        URL_STEP_PLAN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "evil",
            "inputs": {},
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": 1},
            "work_ticket_idx": 1,
            "module": "os.system",
        },
    )
    assert resp.status_code == 422


def test_step_plan_degenerate_hint_degrades_not_500(http_client, cp_to_co_token, monkeypatch):
    """A plan() whose resources violate StepPlanResponse's gt=0 (e.g. cpu=0, or a
    sub-second walltime that truncates to 0) must NOT surface as HTTP 500 — the
    resources->StepPlanResponse mapping lives inside the advisory try, so a
    degenerate hint degrades to an empty 200 like any other plan failure."""
    import sys
    import types

    from pydantic import BaseModel
    from qiita_common.api_paths import URL_STEP_PLAN

    from qiita_compute_orchestrator.jobs import JobPlan, JobResourcePlan

    modname = "qiita_compute_orchestrator.jobs.zero_hint_stub"
    mod = types.ModuleType(modname)

    class Inputs(BaseModel):
        pass

    async def execute(inputs, workspace):
        return {}

    def plan(inputs):
        # cpu=0 is a valid JobResourcePlan but violates StepPlanResponse(gt=0).
        return JobPlan(resources=JobResourcePlan(cpu=0))

    mod.Inputs = Inputs
    mod.execute = execute
    mod.plan = plan
    monkeypatch.setitem(sys.modules, modname, mod)

    client, _ = http_client
    resp = client.post(
        URL_STEP_PLAN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "zh",
            "inputs": {},
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": 1},
            "work_ticket_idx": 1,
            "module": modname,
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"cpu": None, "mem_gb": None, "walltime_seconds": None}


def test_step_submit_forwards_derived_inputs(http_client, cp_to_co_token, tmp_path):
    """`derived_inputs` survives the CP->CO wire hop and reaches the backend.
    It rides as a RELATIVE path — the CP never names a compute-node absolute
    path; the orchestrator joins it against its own PATH_DERIVED at submit."""
    from qiita_common.api_paths import URL_STEP_SUBMIT

    client, backend = http_client
    resp = client.post(
        URL_STEP_SUBMIT,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "checkm",
            "inputs": {},
            "workspace": str(tmp_path),
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": 7},
            "work_ticket_idx": 99,
            "container": REFERENCE_HASH_CONTAINER,
            "derived_inputs": {"QIITA_CHECKM_DB": "checkm_data"},
        },
    )
    assert resp.status_code == 200, resp.text
    assert backend.calls[-1].derived_inputs == {"QIITA_CHECKM_DB": "checkm_data"}


def test_step_submit_rejects_absolute_derived_input(http_client, cp_to_co_token, tmp_path):
    """The wire validator refuses an absolute derived_inputs path — otherwise a
    workflow could name any host directory for the orchestrator to bind into a
    container. 422 at the boundary, never reaching the backend."""
    from qiita_common.api_paths import URL_STEP_SUBMIT

    client, backend = http_client
    before = len(backend.calls)
    resp = client.post(
        URL_STEP_SUBMIT,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "checkm",
            "inputs": {},
            "workspace": str(tmp_path),
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": 7},
            "work_ticket_idx": 99,
            "container": REFERENCE_HASH_CONTAINER,
            "derived_inputs": {"QIITA_CHECKM_DB": "/etc"},
        },
    )
    assert resp.status_code == 422, resp.text
    assert len(backend.calls) == before
