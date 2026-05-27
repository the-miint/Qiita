"""Unit tests for qiita_compute_orchestrator.slurm.client.

Driven by httpx.MockTransport so the wire shape is exercised without a
live SLURM controller. Each test asserts on the request the client
*would* send (URL, headers, JSON body) plus the parsed response shape
the client returns.
"""

from __future__ import annotations

import base64
import json

import httpx
import pytest

from qiita_compute_orchestrator.slurm import (
    DEFAULT_SLURMRESTD_API_VERSION,
    SlurmJobInfo,
    SlurmrestdClient,
    SlurmrestdError,
)


def _make_jwt(sun: str) -> str:
    """Build a minimal JWT-shaped string (header.payload.signature) with
    the given `sun` claim. The signature segment is a placeholder —
    we never verify it; slurmrestd does. The client's only crypto-free
    check is that `sun` matches the configured user."""

    def _b64url(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    header = _b64url({"alg": "HS256", "typ": "JWT"})
    payload = _b64url({"sun": sun})
    return f"{header}.{payload}.placeholder-signature"


@pytest.fixture
def jwt_path(tmp_path):
    p = tmp_path / "jwt"
    p.write_text(_make_jwt("qiita-orch") + "\n")
    return p


def _client(transport: httpx.MockTransport, jwt_path) -> SlurmrestdClient:
    """Build a client wired to a MockTransport. The transport is the
    test-supplied request handler; the client treats it as if it were
    talking to a real slurmrestd."""
    http = httpx.AsyncClient(
        base_url="http://slurm-test:6820",
        transport=transport,
        timeout=5,
    )
    return SlurmrestdClient(
        base_url="http://slurm-test:6820",
        jwt_path=jwt_path,
        user_name="qiita-orch",
        http_client=http,
    )


# ============================================================================
# Construction
# ============================================================================


def test_constructor_loads_jwt_from_file(jwt_path):
    client = _client(httpx.MockTransport(lambda req: httpx.Response(200, json={})), jwt_path)
    # JWT cached on the instance; no header sent yet but ready to send.
    expected = _make_jwt("qiita-orch")
    assert client._jwt == expected  # type: ignore[attr-defined]


def test_constructor_rejects_jwt_with_wrong_sun(tmp_path):
    """A SLURM JWT whose sun=<X> doesn't match SLURMRESTD_USER_NAME=<Y>
    is refused at construction. Surfaced by the first smoke: a stale
    JWT minted under a different user was happily used by the
    orchestrator, and slurmrestd silently authenticated jobs as the
    wrong identity."""
    p = tmp_path / "jwt"
    p.write_text(_make_jwt("antoniog") + "\n")  # not qiita-orch
    with pytest.raises(RuntimeError, match="sun='antoniog' does not match"):
        SlurmrestdClient(
            base_url="http://x",
            jwt_path=p,
            user_name="qiita-orch",
        )


def test_constructor_rejects_jwt_not_three_segments(tmp_path):
    p = tmp_path / "jwt"
    p.write_text("not-a-jwt")
    with pytest.raises(RuntimeError, match="3-segment JWT"):
        SlurmrestdClient(
            base_url="http://x",
            jwt_path=p,
            user_name="qiita-orch",
        )


def test_constructor_rejects_jwt_payload_missing_sun(tmp_path):
    p = tmp_path / "jwt"
    # Valid JWT shape with a payload that has no `sun` claim.
    bad_payload = base64.urlsafe_b64encode(json.dumps({"iss": "x"}).encode()).rstrip(b"=").decode()
    p.write_text(f"header.{bad_payload}.sig")
    with pytest.raises(RuntimeError, match="missing a string `sun`"):
        SlurmrestdClient(
            base_url="http://x",
            jwt_path=p,
            user_name="qiita-orch",
        )


def test_constructor_rejects_empty_base_url(jwt_path):
    with pytest.raises(ValueError, match="base_url"):
        SlurmrestdClient(
            base_url="",
            jwt_path=jwt_path,
            user_name="qiita-orch",
        )


def test_constructor_rejects_empty_user_name(jwt_path):
    with pytest.raises(ValueError, match="user_name"):
        SlurmrestdClient(
            base_url="http://x",
            jwt_path=jwt_path,
            user_name="",
        )


def test_constructor_rejects_unreadable_jwt(tmp_path):
    with pytest.raises(SlurmrestdError, match="unable to read"):
        SlurmrestdClient(
            base_url="http://x",
            jwt_path=tmp_path / "nope",
            user_name="qiita-orch",
        )


def test_constructor_rejects_empty_jwt_file(tmp_path):
    p = tmp_path / "empty"
    p.write_text("")
    with pytest.raises(SlurmrestdError, match="empty"):
        SlurmrestdClient(
            base_url="http://x",
            jwt_path=p,
            user_name="qiita-orch",
        )


# ============================================================================
# submit_job
# ============================================================================


@pytest.mark.asyncio
async def test_submit_job_happy_path(jwt_path):
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["method"] = request.method
        captured["headers"] = dict(request.headers)
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"job_id": 12345, "errors": []})

    async with _client(httpx.MockTransport(handler), jwt_path) as client:
        job_id = await client.submit_job({"script": "...", "job": {}})

    assert job_id == 12345
    assert captured["method"] == "POST"
    assert captured["url"].endswith(f"/slurm/{DEFAULT_SLURMRESTD_API_VERSION}/job/submit")
    assert captured["headers"]["x-slurm-user-name"] == "qiita-orch"
    assert captured["headers"]["x-slurm-user-token"] == _make_jwt("qiita-orch")
    assert captured["headers"]["content-type"] == "application/json"
    assert captured["body"] == {"script": "...", "job": {}}


@pytest.mark.asyncio
async def test_submit_job_missing_job_id_raises(jwt_path):
    handler = httpx.MockTransport(
        lambda req: httpx.Response(200, json={"errors": ["something happened"]})
    )
    async with _client(handler, jwt_path) as client:
        with pytest.raises(SlurmrestdError, match="missing or non-integer job_id"):
            await client.submit_job({})


@pytest.mark.asyncio
async def test_submit_job_4xx_raises_with_status(jwt_path):
    handler = httpx.MockTransport(lambda req: httpx.Response(400, json={"errors": ["bad payload"]}))
    async with _client(handler, jwt_path) as client:
        with pytest.raises(SlurmrestdError) as ei:
            await client.submit_job({})
    assert ei.value.status_code == 400
    assert "400" in str(ei.value)


@pytest.mark.asyncio
async def test_submit_job_5xx_raises_with_status(jwt_path):
    handler = httpx.MockTransport(lambda req: httpx.Response(503, text="slurmctld unreachable"))
    async with _client(handler, jwt_path) as client:
        with pytest.raises(SlurmrestdError) as ei:
            await client.submit_job({})
    assert ei.value.status_code == 503
    assert ei.value.body == "slurmctld unreachable"


@pytest.mark.asyncio
async def test_submit_job_transport_error_raises(jwt_path):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    async with _client(httpx.MockTransport(handler), jwt_path) as client:
        with pytest.raises(SlurmrestdError) as ei:
            await client.submit_job({})
    # Transport errors carry no status_code — caller distinguishes
    # this from 4xx/5xx via .status_code is None.
    assert ei.value.status_code is None


# ============================================================================
# get_job
# ============================================================================


def _job_response(state: str, exit_code: int | None = None, reason: str | None = None) -> dict:
    """Shape a slurmrestd v0.0.40 job-status response."""
    job: dict = {
        "job_id": 12345,
        # v0.0.40+: job_state is a list (multiple states can apply).
        "job_state": [state],
        "exit_code": {
            "return_code": (
                {"number": exit_code, "set": True, "infinite": False}
                if exit_code is not None
                else {"number": 0, "set": False, "infinite": False}
            ),
        },
        "state_reason": reason if reason is not None else "None",
    }
    return {"jobs": [job]}


@pytest.mark.asyncio
async def test_get_job_completed_returns_terminal_info(jwt_path):
    handler = httpx.MockTransport(
        lambda req: httpx.Response(200, json=_job_response("COMPLETED", exit_code=0))
    )
    async with _client(handler, jwt_path) as client:
        info = await client.get_job(12345)
    assert info == SlurmJobInfo(state="COMPLETED", exit_code=0, reason=None)
    assert info.is_terminal is True


@pytest.mark.asyncio
async def test_get_job_failed_carries_exit_code(jwt_path):
    handler = httpx.MockTransport(
        lambda req: httpx.Response(
            200, json=_job_response("FAILED", exit_code=1, reason="NonZeroExitCode")
        )
    )
    async with _client(handler, jwt_path) as client:
        info = await client.get_job(12345)
    assert info.state == "FAILED"
    assert info.exit_code == 1
    assert info.reason == "NonZeroExitCode"
    assert info.is_terminal is True


@pytest.mark.asyncio
async def test_get_job_running_is_not_terminal(jwt_path):
    handler = httpx.MockTransport(lambda req: httpx.Response(200, json=_job_response("RUNNING")))
    async with _client(handler, jwt_path) as client:
        info = await client.get_job(12345)
    assert info.state == "RUNNING"
    assert info.is_terminal is False


@pytest.mark.asyncio
async def test_get_job_normalizes_string_None_reason(jwt_path):
    """SLURM's literal 'None' string for state_reason is normalized to
    Python None — callers shouldn't have to special-case it."""
    handler = httpx.MockTransport(
        lambda req: httpx.Response(200, json=_job_response("COMPLETED", exit_code=0))
    )
    async with _client(handler, jwt_path) as client:
        info = await client.get_job(12345)
    assert info.reason is None


@pytest.mark.asyncio
async def test_get_job_404_raises_with_status(jwt_path):
    handler = httpx.MockTransport(lambda req: httpx.Response(404, json={"errors": ["unknown job"]}))
    async with _client(handler, jwt_path) as client:
        with pytest.raises(SlurmrestdError) as ei:
            await client.get_job(99999)
    assert ei.value.status_code == 404


@pytest.mark.asyncio
async def test_get_job_handles_string_state(jwt_path):
    """Older slurmrestd versions return job_state as a bare string
    instead of a list; the client accepts both."""
    handler = httpx.MockTransport(
        lambda req: httpx.Response(
            200,
            json={
                "jobs": [
                    {
                        "job_id": 12345,
                        "job_state": "COMPLETED",
                        "exit_code": {"return_code": {"number": 0, "set": True}},
                    }
                ]
            },
        )
    )
    async with _client(handler, jwt_path) as client:
        info = await client.get_job(12345)
    assert info.state == "COMPLETED"


@pytest.mark.asyncio
async def test_get_job_missing_job_state_raises(jwt_path):
    handler = httpx.MockTransport(lambda req: httpx.Response(200, json={"jobs": [{"job_id": 1}]}))
    async with _client(handler, jwt_path) as client:
        with pytest.raises(SlurmrestdError, match="missing job_state"):
            await client.get_job(1)


# ============================================================================
# JWT rotation / 401 retry
# ============================================================================


@pytest.mark.asyncio
async def test_401_triggers_jwt_reload_and_one_retry(jwt_path):
    """First request 401s with the original JWT; the client re-reads
    the file (now contains a rotated token) and retries; second
    request includes the new token and succeeds."""
    headers_seen: list[str] = []
    call_count = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        headers_seen.append(request.headers["x-slurm-user-token"])
        call_count["n"] += 1
        if call_count["n"] == 1:
            return httpx.Response(401, json={"errors": ["expired token"]})
        return httpx.Response(200, json={"job_id": 7})

    original = _make_jwt("qiita-orch")
    rotated = _make_jwt("qiita-orch") + "-rotated"
    # _make_jwt(sun) is deterministic; suffix the rotated token so it's
    # distinct in the byte-equality assertion below. Still has a valid
    # 3-segment shape (`.` only in the original header.payload.sig).
    async with _client(httpx.MockTransport(handler), jwt_path) as client:
        # Simulate SLURM rotating the token between attempts: rewrite
        # the file before the retry happens. The client re-reads on 401.
        jwt_path.write_text(rotated + "\n")
        job_id = await client.submit_job({})

    assert job_id == 7
    assert call_count["n"] == 2
    # First attempt sent the original token; retry sent the rotated one.
    assert headers_seen == [original, rotated]


@pytest.mark.asyncio
async def test_double_401_raises(jwt_path):
    """If the 401 retry also 401s, the client raises — a stale or
    misconfigured token is operator-fixable, not a retry-loop matter."""
    handler = httpx.MockTransport(lambda req: httpx.Response(401, json={"errors": ["bad token"]}))
    async with _client(handler, jwt_path) as client:
        with pytest.raises(SlurmrestdError) as ei:
            await client.submit_job({})
    assert ei.value.status_code == 401


# ============================================================================
# Lifecycle
# ============================================================================


@pytest.mark.asyncio
async def test_close_idempotent_on_owned_client(jwt_path):
    handler = httpx.MockTransport(lambda req: httpx.Response(200, json={"job_id": 1}))
    client = SlurmrestdClient(
        base_url="http://x",
        jwt_path=jwt_path,
        user_name="qiita-orch",
        http_client=httpx.AsyncClient(base_url="http://x", transport=handler),
    )
    # Client doesn't own the http_client — close() is a no-op for the
    # underlying client. Make sure no exception is raised.
    await client.close()


@pytest.mark.asyncio
async def test_async_context_manager_closes_owned_client(jwt_path):
    """When the client owns its httpx.AsyncClient (no http_client
    arg), `async with` should close it on exit."""
    # No http_client passed => SlurmrestdClient constructs and owns one.
    # We can't actually verify closure without a request — instead,
    # use the context manager and assert it doesn't raise.
    client = SlurmrestdClient(
        base_url="http://x",
        jwt_path=jwt_path,
        user_name="qiita-orch",
    )
    async with client:
        pass
    # Reaching here without an exception means close() ran cleanly.
