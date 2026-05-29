"""Tests for POST /api/v1/step/run.

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
from qiita_common.api_paths import URL_STEP_RUN
from qiita_common.testing.containers import REFERENCE_HASH_CONTAINER
from qiita_common.testing.native_steps import FASTQ_TO_PARQUET_MODULE

from qiita_compute_orchestrator.backend import ComputeBackend
from qiita_compute_orchestrator.main import app


@dataclass(frozen=True)
class _RecordedCall:
    """One recorded call into `_RecordingBackend.run_step`. Per-attribute
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

    async def run_step(
        self,
        name: str,
        inputs: dict[str, Path],
        workspace: Path,
        *,
        scope_target: dict[str, Any],
        work_ticket_idx: int,
        container: str | None = None,
        module: str | None = None,
        entrypoint: str | None = None,
        baseline_resources=None,
    ) -> dict[str, Path]:
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
            "scope_target": {"kind": "reference", "reference_idx": 1},
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
            "scope_target": {"kind": "reference", "reference_idx": 1},
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
            "scope_target": {"kind": "reference", "reference_idx": 7},
            "work_ticket_idx": 99,
            # Required by StepRunRequest's exactly-one(container, module)
            # validator. The route test doesn't care which runtime drives
            # the recording backend; container is the simpler choice.
            "container": REFERENCE_HASH_CONTAINER,
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "outputs" in body
    assert body["outputs"]["manifest"].endswith("manifest.parquet")

    assert len(backend.calls) == 1
    call = backend.calls[0]
    assert call.name == "hash"
    assert call.inputs == {"fasta_path": fasta}
    assert call.workspace == tmp_path
    assert call.scope_target == {"kind": "reference", "reference_idx": 7}
    assert call.work_ticket_idx == 99
    assert call.container == REFERENCE_HASH_CONTAINER
    assert call.module is None
    assert call.entrypoint is None
    assert call.baseline_resources is None


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
            "scope_target": {"kind": "reference", "reference_idx": 1},
            "work_ticket_idx": 1,
            "module": FASTQ_TO_PARQUET_MODULE,
        },
    )
    assert resp.status_code == 200, resp.text
    assert len(backend.calls) == 1
    call = backend.calls[0]
    assert call.container is None
    assert call.module == FASTQ_TO_PARQUET_MODULE


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
            "scope_target": {"kind": "reference", "reference_idx": 1},
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
            "scope_target": {"kind": "reference", "reference_idx": 1},
            "work_ticket_idx": 1,
            "container": REFERENCE_HASH_CONTAINER,
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


def test_step_run_rejects_wrong_prefix_module(http_client, cp_to_co_token, tmp_path):
    """Defense in depth: a module path outside NATIVE_MODULE_PREFIX is
    rejected at the route boundary before the backend tries to import
    it. The wire validator only checks shape (exactly-one runtime); the
    prefix check lives in the handler."""
    client, backend = http_client
    resp = client.post(
        URL_STEP_RUN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "x",
            "inputs": {},
            "workspace": str(tmp_path),
            "scope_target": {"kind": "reference", "reference_idx": 1},
            "work_ticket_idx": 1,
            "module": "os.system",  # bad prefix
        },
    )
    assert resp.status_code == 422
    assert "qiita_compute_orchestrator.jobs." in resp.text
    assert backend.calls == []


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
            "scope_target": {"kind": "reference", "reference_idx": 1},
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
            "scope_target": {"kind": "reference", "reference_idx": 1},
            "work_ticket_idx": 1,
            "container": REFERENCE_HASH_CONTAINER,
            "module": FASTQ_TO_PARQUET_MODULE,
        },
    )
    assert resp.status_code == 422
    assert "exactly one" in resp.text
    assert backend.calls == []


def test_step_run_dispatches_prep_sample_scope_target(http_client, cp_to_co_token, tmp_path):
    """prep_sample-scoped wire traffic round-trips: the validator accepts
    the discriminated-union shape and the route forwards the dict verbatim
    to backend.run_step. Module form because the natural pairing is
    native step (fastq_to_parquet) + prep_sample scope."""
    client, backend = http_client
    resp = client.post(
        URL_STEP_RUN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "fastq",
            "inputs": {"fastq_path": "/scratch/in.fastq"},
            "workspace": str(tmp_path),
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": 42},
            "work_ticket_idx": 1,
            "module": FASTQ_TO_PARQUET_MODULE,
        },
    )
    assert resp.status_code == 200, resp.text
    assert len(backend.calls) == 1
    call = backend.calls[0]
    assert call.scope_target == {"kind": "prep_sample", "prep_sample_idx": 42}
    assert call.module == FASTQ_TO_PARQUET_MODULE
    assert call.container is None


def test_step_run_dispatches_study_prep_scope_target(http_client, cp_to_co_token, tmp_path):
    """study_prep-scoped wire traffic round-trips. Container form here
    because there's no native step pinned to study_prep today; a future
    container action (e.g. per-(study,prep) sample processing) would
    naturally take this shape."""
    client, backend = http_client
    resp = client.post(
        URL_STEP_RUN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "process",
            "inputs": {},
            "workspace": str(tmp_path),
            "scope_target": {
                "kind": "study_prep",
                "study_idx": 7,
                "prep_idx": 11,
            },
            "work_ticket_idx": 1,
            "container": "qiita/process:1.0.0",
        },
    )
    assert resp.status_code == 200, resp.text
    assert len(backend.calls) == 1
    call = backend.calls[0]
    assert call.scope_target == {
        "kind": "study_prep",
        "study_idx": 7,
        "prep_idx": 11,
    }


def test_step_run_serializes_container_on_unsupported_scope_guard(
    http_client, cp_to_co_token, tmp_path
):
    """End-to-end at the HTTP boundary: a container step whose
    scope_target.kind is not one the backends can dispatch must surface
    as a CONTRACT_VIOLATION BackendFailure, serialized through the route
    into the wire shape the runner consumes.

    The stub backend calls the REAL shared guard
    (`assert_container_scope_supported`, the same function LocalBackend
    and SlurmBackend invoke) rather than reimplementing its predicate or
    error string — so this test tracks the guard if the supported-kind
    set or wording changes, instead of asserting a copy that can silently
    rot. We still stub at the `run_step` boundary so we don't spin up
    either real backend (LocalBackend triggers miint install, SlurmBackend
    needs slurmrestd config); the round-trip we care about is the wire
    serialization of the guard's BackendFailure.

    `prep_sample` is the rejected kind here: the guard supports `reference`
    and `sequenced_pool` (bcl-convert), so a prep_sample-scoped container
    step is the contract violation."""
    from qiita_common.backend_failure import (
        BACKEND_FAILURE_HEADER,
        BACKEND_FAILURE_HTTP_STATUS,
    )

    from qiita_compute_orchestrator.backend import assert_container_scope_supported

    client, backend = http_client

    async def _container_guard(name, inputs, workspace, *, scope_target, container=None, **_):
        # Mirror the backends: the guard only applies to container steps.
        if container is not None:
            assert_container_scope_supported(step_name=name, scope_target=scope_target)
        return {}

    backend.run_step = _container_guard  # type: ignore[method-assign]

    resp = client.post(
        URL_STEP_RUN,
        headers={"Authorization": f"Bearer {cp_to_co_token}"},
        json={
            "step_name": "hash",
            "inputs": {"fasta_path": "/tmp/x.fa"},
            "workspace": str(tmp_path),
            "scope_target": {"kind": "prep_sample", "prep_sample_idx": 42},
            "work_ticket_idx": 1,
            "container": REFERENCE_HASH_CONTAINER,
        },
    )
    assert resp.status_code == BACKEND_FAILURE_HTTP_STATUS
    assert resp.headers[BACKEND_FAILURE_HEADER] == "1"
    body = resp.json()
    assert body["kind"] == "contract_violation"
    assert body["stage"] == "step_run"
    assert body["step_name"] == "hash"
    # Assert against the real guard's current wording, not a stale copy.
    assert "scope_target with kind in" in body["reason"]
    assert "prep_sample" in body["reason"]


def test_settings_resolves_token_from_env():
    """Sanity-check Settings.from_env reads the dev-mode env override."""
    from qiita_compute_orchestrator.config import Settings

    assert os.environ.get("QIITA_ALLOW_TOKEN_ENV") == "true"
    s = Settings.from_env()
    assert s.cp_to_co_token == os.environ["CP_TO_CO_TOKEN"]
