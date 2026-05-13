"""Tests for ComputeBackendClient (auth params + run_step wire shape)."""

import json
from pathlib import Path

import httpx
import pytest


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


async def test_run_step_posts_to_step_run_endpoint(tmp_path):
    """run_step must POST /api/v1/step/run with the StepRunRequest envelope
    and return the parsed StepRunResponse outputs as Path objects."""
    from qiita_common.api_paths import URL_STEP_RUN
    from qiita_common.compute_backend_client import ComputeBackendClient

    captured: list[httpx.Request] = []
    transport = _capture_transport(
        captured,
        {"outputs": {"manifest": "/workspace/manifest.parquet"}},
    )
    custom = httpx.AsyncClient(
        base_url="http://localhost:8081",
        transport=transport,
        headers={"Authorization": "Bearer xx"},
    )
    client = ComputeBackendClient(
        "http://localhost:8081",
        api_token="unused",
        http_client=custom,
    )

    outputs = await client.run_step(
        step_name="hash",
        inputs={"fasta_path": Path("/data/in.fa")},
        workspace=Path("/workspace"),
        reference_idx=42,
        work_ticket_idx=99,
        # container is required by the StepRunRequest exactly-one(container, module)
        # validator. The test exercises the wire shape; either runtime works.
        container="qiita/reference-hash:1.0.0",
    )

    assert len(captured) == 1
    req = captured[0]
    assert req.method == "POST"
    assert req.url.path == URL_STEP_RUN
    body = json.loads(req.content)
    assert body["step_name"] == "hash"
    assert body["inputs"] == {"fasta_path": "/data/in.fa"}
    assert body["workspace"] == "/workspace"
    assert body["reference_idx"] == 42
    assert body["work_ticket_idx"] == 99
    # Outputs come back as Paths the runner can plumb into downstream entries.
    assert outputs == {"manifest": Path("/workspace/manifest.parquet")}


async def test_run_step_reconstructs_backend_failure(tmp_path):
    """When the orchestrator returns a structured BackendFailureBody
    (header set), the client must re-raise BackendFailure so the
    runner's retry classification sees the typed surface — not a
    generic HTTPStatusError."""
    from qiita_common.backend_failure import (
        BACKEND_FAILURE_HEADER,
        BACKEND_FAILURE_HTTP_STATUS,
        BackendFailure,
        FailureKind,
    )
    from qiita_common.compute_backend_client import ComputeBackendClient
    from qiita_common.models import WorkTicketFailureStage

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            BACKEND_FAILURE_HTTP_STATUS,
            content=json.dumps(
                {
                    "kind": "oom_killed",
                    "stage": "step_run",
                    "step_name": "load",
                    "reason": "step exceeded 32GB mem cap",
                }
            ).encode(),
            headers={
                "content-type": "application/json",
                BACKEND_FAILURE_HEADER: "1",
            },
        )

    custom = httpx.AsyncClient(
        base_url="http://localhost:8081",
        transport=httpx.MockTransport(handler),
        headers={"Authorization": "Bearer xx"},
    )
    client = ComputeBackendClient(
        "http://localhost:8081",
        api_token="unused",
        http_client=custom,
    )

    with pytest.raises(BackendFailure) as ei:
        await client.run_step(
            step_name="load",
            inputs={},
            workspace=Path("/workspace"),
            reference_idx=1,
            work_ticket_idx=1,
            container="qiita/reference-load:1.0.0",
        )
    exc = ei.value
    assert exc.kind is FailureKind.OOM_KILLED
    assert exc.stage is WorkTicketFailureStage.STEP_RUN
    assert exc.step_name == "load"
    assert exc.reason == "step exceeded 32GB mem cap"
    # Retry classification round-trips: OOM_KILLED is transient.
    assert exc.transient is True


async def test_run_step_without_header_falls_through_to_raise_for_status(tmp_path):
    """A non-2xx response *without* the discriminator header is a real
    HTTP error (auth failure, 5xx infra) and must surface as
    HTTPStatusError so the runner classifies it generically — not as a
    typed BackendFailure."""
    from qiita_common.compute_backend_client import ComputeBackendClient

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"internal error")

    custom = httpx.AsyncClient(
        base_url="http://localhost:8081",
        transport=httpx.MockTransport(handler),
        headers={"Authorization": "Bearer xx"},
    )
    client = ComputeBackendClient(
        "http://localhost:8081",
        api_token="unused",
        http_client=custom,
    )

    with pytest.raises(httpx.HTTPStatusError):
        await client.run_step(
            step_name="x",
            inputs={},
            workspace=Path("/workspace"),
            reference_idx=1,
            work_ticket_idx=1,
            container="qiita/test:1.0.0",
        )
