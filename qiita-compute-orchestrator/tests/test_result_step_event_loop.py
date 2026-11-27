"""`SlurmBackend.result_step` checks a step's output tree off the event loop (see
its docstring for why). `verify_container_output` is replaced here by a wrapper that
records its thread and sleeps `_STALL_SECONDS` before running the real check,
standing in for a slow file system.
"""

import asyncio
import base64
import json
import threading
import time
from pathlib import Path

import httpx
import pytest
from qiita_common.api_paths import URL_STEP_RESULT
from qiita_common.models import StepStatus

from qiita_compute_orchestrator.backend import SlurmStepHandle, StepStatusInfo
from qiita_compute_orchestrator.backends import slurm as slurm_backend_module
from qiita_compute_orchestrator.backends.slurm import SlurmBackend
from qiita_compute_orchestrator.main import app
from qiita_compute_orchestrator.slurm import SlurmrestdClient

_STALL_SECONDS = 0.5
# A loop that is free answers within a few milliseconds; a blocked one waits out
# the whole stall.
_FREE_LOOP_BOUND_SECONDS = _STALL_SECONDS / 2


def _jwt(sun: str) -> str:
    def b64url(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    return f"{b64url({'alg': 'none'})}.{b64url({'sun': sun, 'exp': 4102444800})}.sig"


def _no_slurmrestd(request: httpx.Request) -> httpx.Response:
    raise AssertionError(f"result_step made a slurmrestd call: {request.method} {request.url}")


@pytest.fixture
async def backend(tmp_path):
    jwt_path = tmp_path / "jwt"
    jwt_path.write_text(_jwt("qiita-orch"))
    client = SlurmrestdClient(
        base_url="http://slurm-test:6820",
        jwt_path=jwt_path,
        user_name="qiita-orch",
        http_client=httpx.AsyncClient(
            base_url="http://slurm-test:6820",
            transport=httpx.MockTransport(_no_slurmrestd),
        ),
    )
    backend = SlurmBackend(client=client, partition="qiita", account="qiita-prod")
    yield backend
    await backend.aclose()


@pytest.fixture
def workspace(tmp_path) -> Path:
    """A completed step's output tree that passes verification."""
    out = tmp_path / "ws" / "output"
    out.mkdir(parents=True)
    (out / "result.parquet").write_bytes(b"abc")
    (out / "result.parquet").chmod(0o440)
    manifest = {
        "files": [{"path": "result.parquet", "size_bytes": 3}],
        "outputs": {"manifest": "result.parquet"},
    }
    (out / "manifest.json").write_text(json.dumps(manifest))
    (out / "manifest.json").chmod(0o440)
    (tmp_path / "ws" / "logs").mkdir()
    return tmp_path / "ws"


@pytest.fixture
def slow_verify(monkeypatch) -> list[int]:
    """Thread idents `verify_container_output` ran on, one per call."""
    real = slurm_backend_module.verify_container_output
    threads: list[int] = []

    def slow(*args, **kwargs):
        threads.append(threading.get_ident())
        time.sleep(_STALL_SECONDS)
        return real(*args, **kwargs)

    monkeypatch.setattr(slurm_backend_module, "verify_container_output", slow)
    return threads


async def test_result_step_verifies_off_the_event_loop_thread(backend, workspace, slow_verify):
    handle = SlurmStepHandle(
        step_name="gene_call",
        slurm_job_id=1,
        job_name="qiita-wt1-gene_call-a0",
        output_path=workspace / "output",
        logs_path=workspace / "logs",
    )
    outputs = await backend.result_step(handle, StepStatusInfo(status=StepStatus.COMPLETED))

    assert outputs == {"manifest": (workspace / "output" / "result.parquet").resolve()}
    assert len(slow_verify) == 1
    assert slow_verify[0] != threading.get_ident()


async def test_health_answers_while_step_result_verifies(
    backend, workspace, slow_verify, cp_to_co_token, monkeypatch
):
    """The production symptom: the CP's 1 s `/health` probe of the CO timed out
    while the CO was answering a `step/result`."""
    monkeypatch.setattr(app.state, "backend", backend, raising=False)
    monkeypatch.setattr(app.state, "cp_to_co_token", cp_to_co_token, raising=False)
    body = {
        "handle": {
            "compute_target": "slurm",
            "step_name": "gene_call",
            "slurm_job_id": 1,
            "job_name": "qiita-wt1-gene_call-a0",
            "output_path": str(workspace / "output"),
            "logs_path": str(workspace / "logs"),
        },
        "status": {"status": "completed"},
    }
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://co") as client:

        async def timed_health() -> tuple[httpx.Response, float]:
            # Due 50 ms after step/result is sent, which is by then in the verifier.
            # Timed from when it was due, not from when the loop got round to
            # sending it: a blocked loop delays the send itself.
            delay = 0.05
            due = time.perf_counter() + delay
            await asyncio.sleep(delay)
            resp = await client.get("/health")
            return resp, time.perf_counter() - due

        result, (health, health_seconds) = await asyncio.gather(
            client.post(
                URL_STEP_RESULT,
                headers={"Authorization": f"Bearer {cp_to_co_token}"},
                json=body,
            ),
            timed_health(),
        )

    assert result.status_code == 200, result.text
    assert health.status_code == 200, health.text
    assert health_seconds < _FREE_LOOP_BOUND_SECONDS, (
        f"/health took {health_seconds:.3f}s while step/result verified the output"
    )
