"""Tests for POST /api/v1/step/run.

Bearer-token enforcement, request-shape validation, and dispatch wiring
are exercised; the backend itself is stubbed so we don't need DuckDB or
miint to test the route surface.
"""

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from qiita_common.api_paths import URL_STEP_RUN

from qiita_compute_orchestrator.backend import ComputeBackend
from qiita_compute_orchestrator.main import app


class _RecordingBackend(ComputeBackend):
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict, Path, int]] = []

    async def run_step(
        self,
        name: str,
        inputs: dict[str, Path],
        workspace: Path,
        *,
        reference_idx: int,
        work_ticket_idx: int,
        container: str | None = None,
        module: str | None = None,
        entrypoint: str | None = None,
        baseline_resources=None,
    ) -> dict[str, Path]:
        # Existing tests don't exercise every field; they get captured
        # into the calls log as a tuple so future tests that DO
        # exercise them can assert on the values.
        self.calls.append(
            (
                name,
                dict(inputs),
                workspace,
                reference_idx,
                work_ticket_idx,
                container,
                module,
                entrypoint,
                baseline_resources,
            )
        )
        return {"manifest": workspace / "manifest.parquet"}


@pytest.fixture
def http_client():
    """A TestClient with a recording backend swapped in. The stock
    LocalBackend would try to install miint on every test."""
    with TestClient(app) as client:
        backend = _RecordingBackend()
        app.state.backend = backend
        yield client, backend


def test_step_run_requires_bearer_token(http_client):
    client, _ = http_client
    resp = client.post(
        URL_STEP_RUN,
        json={
            "step_name": "hash",
            "inputs": {"fasta_path": "/tmp/x.fa"},
            "workspace": "/tmp/ws",
            "reference_idx": 1,
            "work_ticket_idx": 1,
        },
    )
    assert resp.status_code == 401


def test_step_run_rejects_wrong_token(http_client, cp_to_co_token):
    client, _ = http_client
    resp = client.post(
        URL_STEP_RUN,
        headers={"Authorization": "Bearer not-the-right-token"},
        json={
            "step_name": "hash",
            "inputs": {"fasta_path": "/tmp/x.fa"},
            "workspace": "/tmp/ws",
            "reference_idx": 1,
            "work_ticket_idx": 1,
        },
    )
    assert resp.status_code == 401
    assert cp_to_co_token  # fixture used to assert env-driven config is present


def test_step_run_dispatches_to_backend(http_client, cp_to_co_token, tmp_path):
    client, backend = http_client
    fasta = tmp_path / "x.fa"
    fasta.write_text(">seq\nACGT\n")
    resp = client.post(
        URL_STEP_RUN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "hash",
            "inputs": {"fasta_path": str(fasta)},
            "workspace": str(tmp_path),
            "reference_idx": 7,
            "work_ticket_idx": 99,
            # Required by StepRunRequest's exactly-one(container, module)
            # validator. The route test doesn't care which runtime drives
            # the recording backend; container is the simpler choice.
            "container": "qiita/reference-hash:1.0.0",
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "outputs" in body
    assert body["outputs"]["manifest"].endswith("manifest.parquet")

    assert len(backend.calls) == 1
    (
        name,
        inputs,
        workspace,
        reference_idx,
        work_ticket_idx,
        container,
        module,
        entrypoint,
        baseline,
    ) = backend.calls[0]
    assert name == "hash"
    assert inputs == {"fasta_path": fasta}
    assert workspace == tmp_path
    assert reference_idx == 7
    assert work_ticket_idx == 99
    assert container == "qiita/reference-hash:1.0.0"
    assert module is None
    assert entrypoint is None
    assert baseline is None


def test_step_run_forwards_module_to_backend(http_client, cp_to_co_token, tmp_path):
    """The module form on the wire must reach backend.run_step verbatim.
    Catches dropped-on-the-floor regressions where the route accepts module
    in the payload but doesn't forward it through."""
    client, backend = http_client
    resp = client.post(
        URL_STEP_RUN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "fastq",
            "inputs": {},
            "workspace": str(tmp_path),
            "reference_idx": 1,
            "work_ticket_idx": 1,
            "module": "qiita_compute_orchestrator.jobs.fastq_to_parquet",
        },
    )
    assert resp.status_code == 200, resp.text
    assert len(backend.calls) == 1
    (_, _, _, _, _, container, module, _, _) = backend.calls[0]
    assert container is None
    assert module == "qiita_compute_orchestrator.jobs.fastq_to_parquet"


def test_step_run_translates_backend_value_error(http_client, cp_to_co_token, tmp_path):
    """ValueError from the backend (e.g. unknown step name) → 422."""
    client, backend = http_client

    async def boom(*args, **kwargs):
        raise ValueError("unknown step")

    backend.run_step = boom  # type: ignore[method-assign]
    resp = client.post(
        URL_STEP_RUN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "nope",
            "inputs": {},
            "workspace": str(tmp_path),
            "reference_idx": 1,
            "work_ticket_idx": 1,
            "container": "qiita/test:1.0.0",
        },
    )
    assert resp.status_code == 422
    # ValueError → FastAPI's HTTPException shape, no discriminator header.
    # The header is reserved for BackendFailure (see test below).
    from qiita_common.backend_failure import BACKEND_FAILURE_HEADER

    assert BACKEND_FAILURE_HEADER not in resp.headers
    assert resp.json() == {"detail": "unknown step"}


def test_step_run_serializes_backend_failure(http_client, cp_to_co_token, tmp_path):
    """BackendFailure from the backend → structured response the
    runner can reconstruct into a typed BackendFailure for retry
    classification."""
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
            kind=FailureKind.NODE_FAIL,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name="hash",
            reason="slurm reported node n01 lost mid-step",
        )

    backend.run_step = boom  # type: ignore[method-assign]
    resp = client.post(
        URL_STEP_RUN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "hash",
            "inputs": {},
            "workspace": str(tmp_path),
            "reference_idx": 1,
            "work_ticket_idx": 1,
            "container": "qiita/reference-hash:1.0.0",
        },
    )
    assert resp.status_code == BACKEND_FAILURE_HTTP_STATUS
    assert resp.headers[BACKEND_FAILURE_HEADER] == "1"
    assert resp.json() == {
        "kind": "node_fail",
        "stage": "step_run",
        "step_name": "hash",
        "reason": "slurm reported node n01 lost mid-step",
    }


def test_step_run_rejects_payload_without_runtime(http_client, cp_to_co_token, tmp_path):
    """A request body with neither `container` nor `module` is rejected at
    the wire boundary by the StepRunRequest validator; the route never
    reaches the backend dispatch."""
    client, backend = http_client
    resp = client.post(
        URL_STEP_RUN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "hash",
            "inputs": {"fasta_path": "/tmp/x.fa"},
            "workspace": str(tmp_path),
            "reference_idx": 1,
            "work_ticket_idx": 1,
        },
    )
    assert resp.status_code == 422
    assert "exactly one" in resp.text
    assert backend.calls == []


def test_step_run_rejects_payload_with_both_runtimes(http_client, cp_to_co_token, tmp_path):
    """A request body with both `container` AND `module` is rejected at
    the wire boundary — runtime must be unambiguous."""
    client, backend = http_client
    resp = client.post(
        URL_STEP_RUN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "hash",
            "inputs": {"fasta_path": "/tmp/x.fa"},
            "workspace": str(tmp_path),
            "reference_idx": 1,
            "work_ticket_idx": 1,
            "container": "qiita/reference-hash:1.0.0",
            "module": "qiita_compute_orchestrator.jobs.fastq_to_parquet",
        },
    )
    assert resp.status_code == 422
    assert "exactly one" in resp.text
    assert backend.calls == []


def test_settings_resolves_token_from_env():
    """Sanity-check Settings.from_env reads the dev-mode env override."""
    from qiita_compute_orchestrator.config import Settings

    assert os.environ.get("QIITA_ALLOW_TOKEN_ENV") == "true"
    s = Settings.from_env()
    assert s.cp_to_co_token == os.environ["CP_TO_CO_TOKEN"]
