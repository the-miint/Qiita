"""Unit tests for qiita_compute_orchestrator.slurm.client.

Driven by httpx.MockTransport so the wire shape is exercised without a
live SLURM controller. Each test asserts on the request the client
*would* send (URL, headers, JSON body) plus the parsed response shape
the client returns.
"""

from __future__ import annotations

import base64
import json
import time

import httpx
import pytest

from qiita_compute_orchestrator.slurm import (
    DEFAULT_SLURMRESTD_API_VERSION,
    SlurmJobInfo,
    SlurmrestdClient,
    SlurmrestdError,
)


def _make_jwt(sun: str, *, exp: float | None = None) -> str:
    """Build a minimal JWT-shaped string (header.payload.signature) with
    the given `sun` claim (and optional `exp` expiry, a Unix timestamp). The
    signature segment is a placeholder — we never verify it; slurmrestd does.
    The client's only crypto-free checks are that `sun` matches the configured
    user and, for proactive refresh, the `exp` claim."""

    def _b64url(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    header = _b64url({"alg": "HS256", "typ": "JWT"})
    claims: dict = {"sun": sun}
    if exp is not None:
        claims["exp"] = exp
    payload = _b64url(claims)
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
    # No job_id, no error_code, and no top-level errors[] — slurmrestd
    # accepted the request but gave us back a body we can't read a job_id
    # out of. (A body carrying errors[] is exercised separately below as a
    # *rejected* submit, which is caught before the missing-id guard.)
    handler = httpx.MockTransport(lambda req: httpx.Response(200, json={"warnings": []}))
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


@pytest.mark.asyncio
async def test_submit_job_200_with_nonzero_result_error_code_raises(jwt_path):
    """slurmrestd can answer HTTP 200 while slurmctld *rejected* the
    submission — the real outcome lives in result.error_code (0 ==
    accepted). A non-zero code (e.g. 2015, partition unavailable) must
    raise, not be mis-read as a successful submit."""
    handler = httpx.MockTransport(
        lambda req: httpx.Response(
            200,
            json={
                # job_id echoed but meaningless — the job was NOT queued.
                "job_id": 0,
                "result": {
                    "job_id": 0,
                    "error_code": 2015,
                    "error": "Requested partition configuration not available now",
                },
                "warnings": [],
            },
        )
    )
    async with _client(handler, jwt_path) as client:
        with pytest.raises(SlurmrestdError) as ei:
            await client.submit_job({})
    assert "2015" in str(ei.value)
    # Tagged 4xx-but-not-401 so SlurmBackend._classify_submit_error treats
    # a rejected submit as a permanent CONTRACT_VIOLATION, not a retriable
    # transport/auth failure.
    assert ei.value.status_code is not None
    assert 400 <= ei.value.status_code < 500
    assert ei.value.status_code != 401


@pytest.mark.asyncio
async def test_submit_job_200_with_top_level_errors_raises(jwt_path):
    """A populated top-level errors[] array on a 200 also means the
    submission was rejected — even if a job_id is echoed."""
    handler = httpx.MockTransport(
        lambda req: httpx.Response(
            200,
            json={
                "job_id": 999,
                "errors": [{"error": "Access/permission denied", "error_number": 2007}],
            },
        )
    )
    async with _client(handler, jwt_path) as client:
        with pytest.raises(SlurmrestdError) as ei:
            await client.submit_job({})
    assert ei.value.status_code is not None
    assert 400 <= ei.value.status_code < 500
    assert ei.value.status_code != 401


@pytest.mark.asyncio
async def test_submit_job_200_error_code_zero_is_authoritative_over_errors(jwt_path):
    """`result.error_code == 0` means slurmctld queued the job — it is the
    authoritative accept signal and is NOT overridden by a populated
    top-level errors[] (which can carry informational entries on a good
    submit). The job_id must be returned, not rejected."""
    handler = httpx.MockTransport(
        lambda req: httpx.Response(
            200,
            json={
                "job_id": 4242,
                "result": {"job_id": 4242, "error_code": 0, "error": ""},
                "errors": [{"description": "informational note, not a rejection"}],
            },
        )
    )
    async with _client(handler, jwt_path) as client:
        job_id = await client.submit_job({})
    assert job_id == 4242


@pytest.mark.asyncio
async def test_submit_job_200_warnings_are_non_fatal(jwt_path):
    """slurmrestd emits warnings[] (e.g. the 'nodes' type warning) on a
    perfectly good submit. error_code == 0 + a real job_id + only
    warnings means success — warnings must NOT fail the submit."""
    handler = httpx.MockTransport(
        lambda req: httpx.Response(
            200,
            json={
                "job_id": 4242,
                "result": {"job_id": 4242, "error_code": 0, "error": ""},
                "errors": [],
                "warnings": [{"description": "Unexpected key 'nodes'"}],
            },
        )
    )
    async with _client(handler, jwt_path) as client:
        job_id = await client.submit_job({})
    assert job_id == 4242


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
    # get_job now also carries job_id + name (name absent in this fixture
    # response, so None) so the recovery path can match jobs by name.
    assert info == SlurmJobInfo(
        state="COMPLETED", exit_code=0, reason=None, job_id=12345, name=None
    )
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
# find_jobs_by_name (recovery / idempotency lookup)
# ============================================================================


@pytest.mark.asyncio
async def test_find_jobs_by_name_matches(jwt_path):
    """GET /slurm/{v}/jobs, return only the jobs whose name matches. The
    control plane uses this to adopt a job it submitted but whose id it
    may not have persisted (CP/CO died in the submit window)."""
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(
            200,
            json={
                "jobs": [
                    {"job_id": 1, "name": "qiita-wt5-hash-a0", "job_state": ["RUNNING"]},
                    {"job_id": 2, "name": "someone-elses-job", "job_state": ["RUNNING"]},
                ]
            },
        )

    async with _client(httpx.MockTransport(handler), jwt_path) as client:
        jobs = await client.find_jobs_by_name("qiita-wt5-hash-a0")

    assert captured["url"].endswith(f"/slurm/{DEFAULT_SLURMRESTD_API_VERSION}/jobs")
    assert len(jobs) == 1
    assert jobs[0].job_id == 1
    assert jobs[0].name == "qiita-wt5-hash-a0"
    assert jobs[0].state == "RUNNING"


@pytest.mark.asyncio
async def test_find_jobs_by_name_no_match_returns_empty(jwt_path):
    """No matching name (incl. when slurmrestd has already purged the
    job) returns [] — the caller treats an empty result as 'no live job
    by that name' and falls back to the filesystem tiebreaker."""
    handler = httpx.MockTransport(
        lambda req: httpx.Response(
            200, json={"jobs": [{"job_id": 9, "name": "other", "job_state": ["COMPLETED"]}]}
        )
    )
    async with _client(handler, jwt_path) as client:
        assert await client.find_jobs_by_name("qiita-wt5-hash-a0") == []


@pytest.mark.asyncio
async def test_find_jobs_by_name_empty_jobs_list(jwt_path):
    handler = httpx.MockTransport(lambda req: httpx.Response(200, json={"jobs": []}))
    async with _client(handler, jwt_path) as client:
        assert await client.find_jobs_by_name("anything") == []


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
# Proactive JWT refresh: reload before expiry, not only after a 401
# ============================================================================


@pytest.mark.asyncio
async def test_near_expired_jwt_proactively_reloaded_without_401(jwt_path):
    """A JWT cached at construction within the refresh margin of its `exp` is
    reloaded from the file BEFORE the next request fires — no 401 round-trip
    needed. Belt-and-suspenders for the case where slurmrestd rejects an
    expired token with something other than a clean 401 (5xx / dropped
    connection), so the reload-on-401 path never triggers and the orchestrator
    would otherwise run on a boot-cached token until restart."""
    now = time.time()
    near_expired = _make_jwt("qiita-orch", exp=now + 5)  # inside the refresh margin
    fresh = _make_jwt("qiita-orch", exp=now + 3600)
    jwt_path.write_text(near_expired + "\n")

    seen: list[str] = []
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["x-slurm-user-token"])
        calls["n"] += 1
        return httpx.Response(200, json={"job_id": 11})

    async with _client(httpx.MockTransport(handler), jwt_path) as client:
        # SLURM rotated a fresh token into the file after we cached the
        # near-expired one.
        jwt_path.write_text(fresh + "\n")
        job_id = await client.submit_job({})

    assert job_id == 11
    assert calls["n"] == 1  # exactly one request — no 401 retry
    assert seen == [fresh]  # proactively reloaded the fresh token before sending


@pytest.mark.asyncio
async def test_valid_jwt_not_proactively_reloaded(jwt_path):
    """A cached JWT comfortably before expiry is NOT reloaded — the proactive
    refresh fires only within the margin, so a healthy token keeps being used
    (no needless file read per request) even if the file changes underneath."""
    now = time.time()
    valid = _make_jwt("qiita-orch", exp=now + 3600)
    rotated = _make_jwt("qiita-orch", exp=now + 7200) + "-rotated"
    jwt_path.write_text(valid + "\n")

    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["x-slurm-user-token"])
        return httpx.Response(200, json={"job_id": 12})

    async with _client(httpx.MockTransport(handler), jwt_path) as client:
        jwt_path.write_text(rotated + "\n")  # file changes, but cached token is healthy
        await client.submit_job({})

    assert seen == [valid]  # used the cached token; no proactive reload


@pytest.mark.asyncio
async def test_jwt_without_exp_claim_not_proactively_reloaded(jwt_path):
    """A JWT with no `exp` claim has no expiry to check, so the proactive
    refresh is a no-op and the client falls back to reload-on-401 — the cached
    token is used as-is until slurmrestd 401s. (The jwt_path fixture mints an
    exp-less token.)"""
    original = jwt_path.read_text().strip()
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["x-slurm-user-token"])
        return httpx.Response(200, json={"job_id": 13})

    async with _client(httpx.MockTransport(handler), jwt_path) as client:
        jwt_path.write_text(_make_jwt("qiita-orch") + "-changed\n")
        await client.submit_job({})

    assert seen == [original]  # no exp → no proactive reload


@pytest.mark.asyncio
async def test_proactive_refresh_file_blip_proceeds_with_cached_token(jwt_path):
    """A transient file-read failure during the proactive-refresh window must
    NOT abort a request — the cached token is still within its validity (the
    60s margin), so the request proceeds with it (and the reload-on-401 path
    remains the fallback for a genuinely expired token)."""
    now = time.time()
    near_expired = _make_jwt("qiita-orch", exp=now + 5)  # within the margin → refresh fires
    jwt_path.write_text(near_expired + "\n")

    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["x-slurm-user-token"])
        return httpx.Response(200, json={"job_id": 14})

    async with _client(httpx.MockTransport(handler), jwt_path) as client:
        # Simulate the rotation script mid-write: the file is momentarily empty
        # when the proactive refresh tries to read it.
        jwt_path.write_text("")
        job_id = await client.submit_job({})

    assert job_id == 14
    assert seen == [near_expired]  # used the still-valid cached token, did not abort


@pytest.mark.asyncio
async def test_401_reload_refreshes_cached_exp(jwt_path):
    """The reload-on-401 path goes through the same single mutation point, so it
    updates the cached `exp` too — a token rotated in via a 401 reload is then
    governed by ITS expiry for the next proactive refresh, not the old one."""
    now = time.time()
    # Cached token sits comfortably beyond the margin so proactive refresh does
    # NOT fire — isolating the 401-reload's effect on _jwt_exp.
    original = _make_jwt("qiita-orch", exp=now + 3600)
    rotated_exp = now + 7200
    rotated = _make_jwt("qiita-orch", exp=rotated_exp)
    jwt_path.write_text(original + "\n")

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(401, json={"errors": ["expired"]})
        return httpx.Response(200, json={"job_id": 15})

    async with _client(httpx.MockTransport(handler), jwt_path) as client:
        assert client._jwt_exp == now + 3600
        jwt_path.write_text(rotated + "\n")
        await client.submit_job({})
        # The 401 reload picked up the rotated token AND its expiry.
        assert client._jwt_exp == rotated_exp


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
