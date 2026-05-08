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
