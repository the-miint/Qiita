"""Async httpx wrapper around slurmrestd's submit / get-job routes.

Thin by design: the client knows about HTTP and the SLURM JWT. State
classification (mapping SLURM job states to FailureKind) lives in
SlurmBackend.run_step where the workflow context is available.

Auth model:
  - SLURM JWT is read from a file at construction time and cached on
    the instance. Each request sends it in `X-SLURM-USER-TOKEN`. The
    SLURM user identity rides in `X-SLURM-USER-NAME` — the SLURM
    job-execution user (e.g. `qiita-job`), distinct from the
    orchestrator's own system user.
  - SLURM rotates JWTs periodically. On a 401, the client reloads the
    file and retries once. If the retry also 401s, it raises — a stale
    or unreadable token is operator-fixable, not retriable internally.

Errors:
  - `SlurmrestdError` wraps every non-2xx response and every transport
    error (DNS / connection / timeout). The caller — SlurmBackend
    today, anything else later — decides whether the failure is
    transient (5xx, network) or permanent (4xx).

API version pinning:
  - Default `v0.0.40` (current LTS at time of writing). Operators
    override via `SLURMRESTD_API_VERSION`. The two routes used here
    (job/submit and job/{id}) have stable shapes across recent
    versions; if a breaking schema bump arrives, branch on
    `self.api_version` here rather than in the payload builder.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

import httpx

# Default API version. Override via the SLURMRESTD_API_VERSION env var
# in Settings.from_env(). Routes used here (job/submit, job/{id}) are
# stable across v0.0.39 → v0.0.41; if slurmrestd ever breaks them, the
# branching point is `self.api_version` rather than the payload module.
DEFAULT_SLURMRESTD_API_VERSION = "v0.0.40"

# Generous timeout: a one-shot HTTP exchange against slurmrestd should
# return in milliseconds; if it doesn't, slurmrestd is wedged and we
# want SLURMRESTD_UNREACHABLE retry semantics rather than a runaway
# request hanging the dispatch task.
_HTTP_TIMEOUT_SECONDS = 30


class SlurmrestdError(RuntimeError):
    """Wraps slurmrestd HTTP failures (non-2xx responses, transport
    errors). Carries enough context for SlurmBackend to classify into
    a BackendFailure: status code (None for transport-level errors),
    URL, response body when available."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        url: str | None = None,
        body: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.url = url
        self.body = body


@dataclass(frozen=True, slots=True)
class SlurmJobInfo:
    """Minimal snapshot of one SLURM job's status. The string state
    follows SLURM's own naming (PENDING / RUNNING / COMPLETED /
    FAILED / NODE_FAIL / OUT_OF_MEMORY / PREEMPTED / TIMEOUT /
    CANCELLED / ...) so the SlurmBackend mapping is faithful to what
    SLURM reports."""

    state: str
    exit_code: int | None
    reason: str | None

    @property
    def is_terminal(self) -> bool:
        return self.state in TerminalSlurmState


# SLURM job-state names that mean the job is no longer in flight. These
# are wire values, not symbols we coined — spellings must match SLURM's
# own. StrEnum so members compare equal to the raw strings parsed out of
# slurmrestd JSON (no conversion at the boundary). Non-terminal states
# (PENDING / RUNNING / COMPLETING / ...) intentionally aren't enumerated
# — nothing branches on them, the poll loop just waits for is_terminal.
# Add members here if a future slurmrestd version introduces a new
# terminal state.
class TerminalSlurmState(StrEnum):
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    NODE_FAIL = "NODE_FAIL"
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    PREEMPTED = "PREEMPTED"
    BOOT_FAIL = "BOOT_FAIL"
    DEADLINE = "DEADLINE"
    SPECIAL_EXIT = "SPECIAL_EXIT"


def decode_jwt_payload(token: str, jwt_path: Path) -> dict[str, Any]:
    """Decode a JWT's payload segment to a Python dict.

    Stdlib only — signature verification is slurmrestd's job, and we
    don't want to pull in a cryptographic JWT library just for a
    claim read.

    Raises `SlurmrestdError` (not `RuntimeError`) so callers that hit
    this via the 401-retry rotation path (`_load_jwt` → `_headers` →
    `_request_with_jwt_retry`) get a typed error that
    `SlurmBackend._classify_submit_error` can handle uniformly; the
    boot-time path (construction) still surfaces it as an
    `__init__`-fatal because `SlurmrestdError` is a `RuntimeError`
    subclass.
    """
    parts = token.split(".")
    if len(parts) != 3:
        raise SlurmrestdError(
            f"SLURM JWT at {jwt_path} is not a 3-segment JWT (header.payload.signature)",
            url=str(jwt_path),
        )
    payload_segment = parts[1]
    # JWT uses unpadded base64-urlsafe; pad before decoding.
    padding = "=" * (-len(payload_segment) % 4)
    try:
        payload_bytes = base64.urlsafe_b64decode(payload_segment + padding)
    except (ValueError, TypeError) as exc:
        raise SlurmrestdError(
            f"SLURM JWT at {jwt_path} payload is not valid base64-urlsafe: {exc}",
            url=str(jwt_path),
        ) from exc
    try:
        payload = json.loads(payload_bytes)
    except ValueError as exc:
        raise SlurmrestdError(
            f"SLURM JWT at {jwt_path} payload is not valid JSON: {exc}",
            url=str(jwt_path),
        ) from exc
    if not isinstance(payload, dict):
        raise SlurmrestdError(
            f"SLURM JWT at {jwt_path} payload is not a JSON object: {payload!r}",
            url=str(jwt_path),
        )
    return payload


def _verify_jwt_sun_matches(token: str, expected_user: str, jwt_path: Path) -> None:
    """Refuse to start with a SLURM JWT whose `sun` claim names a
    different user than SLURMRESTD_USER_NAME — otherwise slurmrestd
    will authenticate jobs as whoever the JWT was minted for, not the
    orchestrator's configured user.

    Reached at boot (construction) and during 401-retry rotation
    (`_load_jwt` → `_headers`). Both paths want the same shape of
    failure: `SlurmrestdError` typed so the retry classifier
    distinguishes a sun-mismatch from other 401 causes, and a
    `RuntimeError` subtype so boot-time mismatches still crash
    `__init__`.
    """
    payload = decode_jwt_payload(token, jwt_path)
    sun = payload.get("sun")
    if not isinstance(sun, str):
        raise SlurmrestdError(
            f"SLURM JWT at {jwt_path} payload is missing a string `sun` claim: {payload!r}",
            url=str(jwt_path),
        )
    if sun != expected_user:
        raise SlurmrestdError(
            f"JWT sun={sun!r} does not match SLURMRESTD_USER_NAME={expected_user!r} —"
            " refusing to start with stale JWT (was this JWT minted by the wrong user,"
            " or before the qiita-slurm-jwt-refresh.timer was provisioned?)",
            url=str(jwt_path),
        )


class SlurmrestdClient:
    """Async HTTP client for slurmrestd's job-submit + job-status routes.

    Usage:

        async with SlurmrestdClient(
            base_url="http://slurmrestd-host:6820",   # the slurmrestd host, NOT slurmctld
            jwt_path=Path("/var/spool/slurm/jwt-token"),
            user_name="qiita-job",
        ) as client:
            job_id = await client.submit_job(payload)
            info = await client.get_job(job_id)

    Constructor accepts an optional `http_client` for tests — pass an
    `httpx.AsyncClient` with a `httpx.MockTransport` to drive the unit
    tests without a live slurmrestd.
    """

    def __init__(
        self,
        *,
        base_url: str,
        jwt_path: Path,
        user_name: str,
        api_version: str = DEFAULT_SLURMRESTD_API_VERSION,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        if not base_url:
            raise ValueError("base_url must be a non-empty URL")
        if not user_name:
            raise ValueError("user_name must be a non-empty string")
        if not api_version:
            raise ValueError("api_version must be a non-empty string")

        self._base_url = base_url.rstrip("/")
        self._jwt_path = jwt_path
        self._user_name = user_name
        self._api_version = api_version
        self._jwt = self._load_jwt()

        if http_client is None:
            self._http = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=_HTTP_TIMEOUT_SECONDS,
            )
            self._owns_http = True
        else:
            self._http = http_client
            self._owns_http = False

    @property
    def api_version(self) -> str:
        return self._api_version

    def _load_jwt(self) -> str:
        try:
            token = self._jwt_path.read_text().strip()
        except OSError as exc:
            raise SlurmrestdError(
                f"unable to read SLURM JWT from {self._jwt_path}: {exc}",
            ) from exc
        if not token:
            raise SlurmrestdError(
                f"SLURM JWT file is empty: {self._jwt_path}",
            )
        _verify_jwt_sun_matches(token, self._user_name, self._jwt_path)
        return token

    def _headers(self) -> dict[str, str]:
        return {
            "X-SLURM-USER-NAME": self._user_name,
            "X-SLURM-USER-TOKEN": self._jwt,
            "Content-Type": "application/json",
        }

    async def __aenter__(self) -> SlurmrestdClient:
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    async def submit_job(self, payload: dict[str, Any]) -> int:
        """POST /slurm/{api_version}/job/submit. Returns SLURM job_id.

        Raises SlurmrestdError on any non-2xx or transport failure. The
        caller (SlurmBackend) classifies into a BackendFailure based on
        status_code (5xx / transport => SLURMRESTD_UNREACHABLE
        retriable; 4xx => CONTRACT_VIOLATION permanent unless 401)."""
        url = f"/slurm/{self._api_version}/job/submit"
        body = await self._post_with_jwt_retry(url, json=payload)
        # slurmrestd returns {"job_id": <int>, ...} (plus a possible
        # "errors" array even on 2xx for warnings); pull the id out.
        job_id = body.get("job_id")
        if not isinstance(job_id, int):
            raise SlurmrestdError(
                f"slurmrestd response missing or non-integer job_id: {body!r}",
                url=url,
            )
        return job_id

    async def get_job(self, job_id: int) -> SlurmJobInfo:
        """GET /slurm/{api_version}/job/{job_id}. Returns a SlurmJobInfo
        snapshot — state, exit_code (when terminal), reason.

        Raises SlurmrestdError if the response is unparseable or the
        job is not found (404). Caller distinguishes 404 from transport
        failures via .status_code on the exception."""
        url = f"/slurm/{self._api_version}/job/{job_id}"
        body = await self._request_with_jwt_retry("GET", url)
        jobs = body.get("jobs")
        if not isinstance(jobs, list) or not jobs:
            raise SlurmrestdError(
                f"slurmrestd job/{job_id} response missing 'jobs' array: {body!r}",
                url=url,
            )
        job = jobs[0]
        # job_state in v0.0.40+ is a list (multiple states can apply at
        # once, e.g. ["RUNNING", "COMPLETING"]). Pick the first one as
        # the "primary" — sufficient for terminal vs non-terminal
        # decisions; SlurmBackend's mapping handles the canonical states.
        raw_state = job.get("job_state")
        if isinstance(raw_state, list) and raw_state:
            state = str(raw_state[0])
        elif isinstance(raw_state, str):
            state = raw_state
        else:
            raise SlurmrestdError(
                f"slurmrestd job/{job_id} response missing job_state: {job!r}",
                url=url,
            )
        # exit_code shape varies by SLURM version; v0.0.40+ wraps
        # return_code in the typed-numeric envelope (number/set/infinite).
        exit_code: int | None = None
        rc = job.get("exit_code", {}).get("return_code")
        if isinstance(rc, dict) and rc.get("set"):
            exit_code = rc.get("number")
        elif isinstance(rc, int):
            exit_code = rc
        reason = job.get("state_reason") or None
        if reason == "None":
            # SLURM uses the literal string "None" for "no reason set",
            # not the JSON null. Normalize so callers don't treat it as
            # a real reason string.
            reason = None
        return SlurmJobInfo(state=state, exit_code=exit_code, reason=reason)

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _post_with_jwt_retry(self, url: str, *, json: dict[str, Any]) -> dict[str, Any]:
        return await self._request_with_jwt_retry("POST", url, json=json)

    async def _request_with_jwt_retry(
        self,
        method: str,
        url: str,
        *,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Issue one HTTP request. If the response is 401, re-read the
        JWT file (handling SLURM's periodic rotation) and retry once;
        if that also 401s, raise. Any other non-2xx raises immediately."""
        for attempt in (1, 2):
            try:
                response = await self._http.request(
                    method,
                    url,
                    json=json,
                    headers=self._headers(),
                )
            except httpx.RequestError as exc:
                # Transport-level failure (DNS, connection, timeout).
                # Caller treats as SLURMRESTD_UNREACHABLE retriable.
                raise SlurmrestdError(
                    f"slurmrestd transport error: {exc}",
                    url=url,
                ) from exc

            if response.status_code == 401 and attempt == 1:
                # JWT may have rotated since startup. Re-read and retry
                # once. Don't loop — a second 401 means the file is
                # actually wrong (mis-installed, permissions, etc).
                self._jwt = self._load_jwt()
                continue

            if response.status_code >= 400:
                raise SlurmrestdError(
                    f"slurmrestd {method} {url} returned {response.status_code}",
                    status_code=response.status_code,
                    url=url,
                    body=response.text,
                )

            try:
                return response.json()
            except ValueError as exc:
                raise SlurmrestdError(
                    f"slurmrestd {method} {url} returned non-JSON body",
                    status_code=response.status_code,
                    url=url,
                    body=response.text,
                ) from exc

        # Unreachable: the for-loop either returned or raised.
        raise SlurmrestdError(  # pragma: no cover
            "slurmrestd retry loop exited without raising or returning",
            url=url,
        )
