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
    ) -> dict[str, Path]:
        self.calls.append((name, dict(inputs), workspace, reference_idx))
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
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "outputs" in body
    assert body["outputs"]["manifest"].endswith("manifest.parquet")

    assert len(backend.calls) == 1
    name, inputs, workspace, reference_idx = backend.calls[0]
    assert name == "hash"
    assert inputs == {"fasta_path": fasta}
    assert workspace == tmp_path
    assert reference_idx == 7


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
        },
    )
    assert resp.status_code == 422


def test_settings_resolves_token_from_env():
    """Sanity-check Settings.from_env reads the dev-mode env override."""
    from qiita_compute_orchestrator.config import Settings

    assert os.environ.get("QIITA_ALLOW_TOKEN_ENV") == "true"
    s = Settings.from_env()
    assert s.cp_to_co_token == os.environ["CP_TO_CO_TOKEN"]
