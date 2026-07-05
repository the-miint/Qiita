"""Async httpx wrapper around slurmrestd's submit / get-job routes.

Thin by design: the client knows about HTTP and the SLURM JWT. State
classification (mapping SLURM job states to FailureKind) lives in
SlurmBackend's status_step / result_step where the workflow context is
available.

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
import logging
import time
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

import httpx

_log = logging.getLogger(__name__)

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

# Proactively reload the SLURM JWT from its file once the cached token is
# within this many seconds of its `exp` claim, BEFORE a request — rather than
# waiting for slurmrestd to reject it with a 401. slurmrestd does not
# always answer an expired token with a clean 401 (it may 5xx or drop the
# connection), and the reload-on-401 path only fires on a 401; without this a
# long-lived orchestrator can run on a boot-cached token past its expiry until
# a restart. The margin only needs to cover one request's round-trip plus
# clock skew; a token with no `exp` claim is never proactively refreshed.
_JWT_REFRESH_MARGIN_SECONDS = 60.0


class SlurmrestdError(RuntimeError):
    """Wraps slurmrestd HTTP failures (non-2xx responses, transport
    errors). Carries enough context for SlurmBackend to classify into
    a BackendFailure: status code (None for transport-level errors),
    URL, response body when available.

    One case carries a *synthetic* status code: `submit_job` raises with
    `status_code=422` when slurmrestd answered HTTP 200 but slurmctld
    logically rejected the submission (`result.error_code != 0`). The
    wire status was 200; the 422 exists only to route the failure to the
    permanent-CONTRACT_VIOLATION bucket in `_classify_submit_error`. The
    message and `body` make the real HTTP 200 explicit for log readers."""

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
    SLURM reports.

    `job_id` / `name` are carried so the recovery path (`find_jobs_by_name`)
    can match a job by its deterministic name and recover its id. They
    default to None for the few call sites that construct a SlurmJobInfo
    directly without them."""

    state: str
    exit_code: int | None
    reason: str | None
    job_id: int | None = None
    name: str | None = None

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
        # Cached token + its decoded `exp` (None when the JWT carries no expiry
        # claim → proactive refresh disabled, reload-on-401 still applies).
        self._jwt: str = ""
        self._jwt_exp: float | None = None
        self._reload_jwt_from_file()

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

    def _reload_jwt_from_file(self) -> None:
        """Read the JWT file (validating `sun`), then cache the token and its
        decoded `exp`. The single mutation point for `self._jwt` so the cached
        token and its expiry never drift apart. Raises `SlurmrestdError` on an
        unreadable/empty/wrong-`sun` file (boot-fatal; on the 401/refresh paths
        the caller surfaces it the same as any other slurmrestd error)."""
        # Decode `exp` BEFORE mutating either field: if `_extract_exp` ever
        # raises (a malformed half-written file that survived `_load_jwt`'s
        # shape check), the cached token and its expiry stay consistent rather
        # than leaving `self._jwt` updated against a stale `self._jwt_exp`.
        token = self._load_jwt()
        exp = self._extract_exp(token)
        self._jwt = token
        self._jwt_exp = exp

    def _extract_exp(self, token: str) -> float | None:
        """Decode the JWT's `exp` (Unix-seconds expiry) for proactive refresh,
        or None if absent / non-numeric. Stdlib decode only — `_load_jwt`
        already validated the token's shape via `_verify_jwt_sun_matches`."""
        exp = decode_jwt_payload(token, self._jwt_path).get("exp")
        return float(exp) if isinstance(exp, (int, float)) else None

    def _maybe_refresh_expiring_jwt(self) -> None:
        """Reload the JWT from its file if the cached token is within
        `_JWT_REFRESH_MARGIN_SECONDS` of its `exp` — before sending the
        request, so we never depend on slurmrestd answering an expired token
        with a clean 401. No-op when the token has no `exp` claim."""
        if self._jwt_exp is None:
            return
        if time.time() < self._jwt_exp - _JWT_REFRESH_MARGIN_SECONDS:
            return
        _log.info(
            "SLURM JWT within %.0fs of expiry (exp=%s); proactively reloading from %s",
            _JWT_REFRESH_MARGIN_SECONDS,
            self._jwt_exp,
            self._jwt_path,
        )
        try:
            self._reload_jwt_from_file()
        except SlurmrestdError as exc:
            # A transient file-read blip (NFS hiccup, rotation script mid-write)
            # must NOT abort a request the still-valid cached token can serve —
            # the margin means the cached token hasn't expired yet. Proceed with
            # it; if it has in fact expired, the reload-on-401 path retries the
            # reload. Only the *proactive* head-start is skipped this once.
            _log.warning(
                "proactive SLURM JWT reload from %s failed (%s); proceeding with the"
                " still-cached token (will fall back to reload-on-401 if expired)",
                self._jwt_path,
                exc,
            )

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
        retriable; 4xx => CONTRACT_VIOLATION permanent unless 401).

        Note: a successful HTTP exchange does NOT mean the job was
        queued. slurmrestd answers HTTP 200 even when slurmctld rejected
        the submission (bad partition, QOS limit, ...) — the real outcome
        rides in `result.error_code` (0 == accepted) and the top-level
        `errors` array. We surface those as a *permanent* failure before
        trusting any echoed job_id."""
        url = f"/slurm/{self._api_version}/job/submit"
        body = await self._post_with_jwt_retry(url, json=payload)
        # Rejection check FIRST: on a rejected submit slurmrestd still
        # echoes a (meaningless, usually 0) job_id, so trusting the id
        # before checking the outcome is exactly the bug.
        self._raise_if_submit_rejected(body, url)
        # slurmrestd returns {"job_id": <int>, ...}; pull the id out.
        job_id = body.get("job_id")
        if not isinstance(job_id, int):
            raise SlurmrestdError(
                f"slurmrestd response missing or non-integer job_id: {body!r}",
                url=url,
            )
        return job_id

    @staticmethod
    def _raise_if_submit_rejected(body: dict[str, Any], url: str) -> None:
        """Detect a job/submit that slurmrestd answered HTTP 200 but
        slurmctld rejected.

        `result.error_code` is slurmrestd's authoritative accept/reject
        signal — `0` means queued. When it is present we trust it: a
        non-zero code is a rejection, and a `0` is an accept even if the
        top-level `errors` array is populated (that array can carry
        informational entries on a good submit). Only when `result` is
        absent (older slurmrestd) do we fall back to treating a populated
        top-level `errors` array as the rejection signal.

        Warnings (`warnings` array — e.g. the benign "nodes" type warning
        slurmrestd emits on a valid submit) are intentionally NEVER
        treated as fatal.

        On rejection we raise tagged with a synthetic 4xx status (not
        401) so `SlurmBackend._classify_submit_error` maps it to a
        permanent CONTRACT_VIOLATION: re-submitting the same payload to an
        unavailable partition / over a QOS limit won't succeed, so it must
        not be retried like a transient transport failure."""
        result = body.get("result")
        error_code = result.get("error_code") if isinstance(result, dict) else None
        errors = body.get("errors")
        if isinstance(error_code, int):
            rejected = error_code != 0
        else:
            # No usable result.error_code — fall back to the errors[] array.
            rejected = isinstance(errors, list) and len(errors) > 0
        if not rejected:
            return
        detail = result.get("error") if isinstance(result, dict) else None
        raise SlurmrestdError(
            "slurmrestd accepted the request (HTTP 200) but slurmctld rejected the "
            f"submission: error_code={error_code} error={detail!r} errors={errors!r}",
            # Synthetic — the HTTP layer said 200; this normalizes a
            # logical rejection into the permanent-4xx classification bucket.
            status_code=422,
            url=url,
            body=json.dumps(body),
        )

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
        return self._parse_job(jobs[0], url=url)

    async def find_jobs_by_name(self, name: str) -> list[SlurmJobInfo]:
        """GET /slurm/{api_version}/jobs and return only the jobs whose
        `name` equals `name`. Returns [] when none match (including when
        slurmrestd has already purged the job).

        Used by the control plane's idempotency / restart-recovery path:
        a job submitted under the deterministic
        `qiita-wt{idx}-{step}-a{attempt}` name can be re-found here even
        if the control plane died before persisting its id. Filtering
        happens client-side because slurmrestd's job-list route doesn't
        take a name filter; we only parse the matching entries so an
        unrelated job with a malformed shape can't break the lookup."""
        url = f"/slurm/{self._api_version}/jobs"
        body = await self._request_with_jwt_retry("GET", url)
        jobs = body.get("jobs")
        if not isinstance(jobs, list):
            raise SlurmrestdError(
                f"slurmrestd jobs response missing 'jobs' array: {body!r}",
                url=url,
            )
        return [
            self._parse_job(job, url=url)
            for job in jobs
            if isinstance(job, dict) and job.get("name") == name
        ]

    def _parse_job(self, job: dict[str, Any], *, url: str) -> SlurmJobInfo:
        """Parse one slurmrestd job object into a SlurmJobInfo. Shared by
        get_job (single) and find_jobs_by_name (filtered list)."""
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
                f"slurmrestd job response missing job_state: {job!r}",
                url=url,
            )
        # exit_code shape varies by SLURM version; v0.0.40+ wraps
        # return_code in the typed-numeric envelope (number/set/infinite).
        exit_code: int | None = None
        # exit_code may be absent, JSON null (key present, value None), or the
        # typed envelope dict. `.get("exit_code", {})` only defaults on absence,
        # so a null value would make `.get("return_code")` raise AttributeError
        # and escape the SlurmrestdError contract — guard the type explicitly.
        exit_code_obj = job.get("exit_code")
        rc = exit_code_obj.get("return_code") if isinstance(exit_code_obj, dict) else None
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
        job_id = job.get("job_id") if isinstance(job.get("job_id"), int) else None
        name = job.get("name") if isinstance(job.get("name"), str) else None
        return SlurmJobInfo(
            state=state, exit_code=exit_code, reason=reason, job_id=job_id, name=name
        )

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
        """Issue one HTTP request. Proactively reload the JWT if it is near
        expiry (before the request). If the response is still 401, re-read the
        JWT file (handling SLURM's periodic rotation) and retry once; if that
        also 401s, raise. Any other non-2xx raises immediately."""
        # Refresh an about-to-expire token BEFORE sending, so recovery
        # never hinges on slurmrestd returning a clean 401 for an expired JWT.
        self._maybe_refresh_expiring_jwt()
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
                _log.warning(
                    "slurmrestd %s %s returned 401; reloading JWT from %s and retrying once",
                    method,
                    url,
                    self._jwt_path,
                )
                self._reload_jwt_from_file()
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
