"""Tests for compute backend abstraction."""

import base64
import json
from abc import ABC
from pathlib import Path

import pytest


def _make_jwt(sun: str) -> str:
    """Minimal JWT-shaped string with the given `sun` claim. Used by
    the SLURM-backend constructor test below; SlurmrestdClient refuses
    to load a token whose sun doesn't match user_name."""

    def _b64url(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    return f"{_b64url({'alg': 'HS256'})}.{_b64url({'sun': sun})}.sig"


def test_compute_backend_is_abstract():
    """ComputeBackend must be an ABC with abstract methods."""
    from qiita_compute_orchestrator.backend import ComputeBackend

    assert issubclass(ComputeBackend, ABC)
    with pytest.raises(TypeError):
        ComputeBackend()


def test_local_backend_is_concrete():
    """LocalBackend must be a concrete implementation of ComputeBackend."""
    from qiita_compute_orchestrator.backend import ComputeBackend
    from qiita_compute_orchestrator.backends.local import LocalBackend

    assert issubclass(LocalBackend, ComputeBackend)


async def test_slurm_backend_constructor_accepts_config(tmp_path):
    """SlurmBackend now requires a SlurmrestdClient + partition /
    account / poll & timeout config. Verifies the constructor accepts
    those args. Functional behavior tests live in test_slurm_backend.py."""
    import httpx

    from qiita_compute_orchestrator.backends.slurm import SlurmBackend
    from qiita_compute_orchestrator.slurm import SlurmrestdClient

    jwt = tmp_path / "jwt"
    jwt.write_text(_make_jwt("qiita-orch"))
    client = SlurmrestdClient(
        base_url="http://x",
        jwt_path=jwt,
        user_name="qiita-orch",
        http_client=httpx.AsyncClient(
            base_url="http://x",
            transport=httpx.MockTransport(lambda req: httpx.Response(200, json={"job_id": 1})),
        ),
    )
    backend = SlurmBackend(
        client=client,
        partition="qiita",
        account="qiita-prod",
        poll_interval_seconds=1,
        job_timeout_seconds=60,
    )
    assert backend is not None


async def test_local_backend_rejects_container_step():
    """LocalBackend no longer supports container steps — every workflow
    step must declare `module:`. A container-shaped request is a
    contract violation (CONTRACT_VIOLATION, permanent) rather than a
    silent no-op so a stale YAML that escaped review surfaces here. The
    container path still lives on SlurmBackend (production runtime); a
    deploy that needs to test container behavior must run against SLURM."""
    from qiita_common.backend_failure import BackendFailure, FailureKind
    from qiita_common.models import WorkTicketFailureStage

    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step(
            "legacy_hash",
            {},
            Path("/fake"),
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=1,
            container="qiita/test:1.0.0",
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert ei.value.stage == WorkTicketFailureStage.STEP_RUN
    assert ei.value.step_name == "legacy_hash"
    assert not ei.value.transient
    assert "container" in ei.value.reason.lower()
