"""HTTP client used by the control-plane runner to dispatch a workflow
`step:` entry to the orchestrator's ComputeBackend.

Auth is a shared bearer token configured on both ends. The orchestrator
reads it from `/etc/qiita/cp-to-co.token` (default; override with
`CP_TO_CO_TOKEN_PATH`); the control plane reads from the path in
`Settings.cp_to_co_token_path` (same default). This is a private path
between two services on the same network — full PAT/JWT machinery is
overkill.

The runner drives the decoupled `submit_step` / `status_step` /
`result_step` trio so a long SLURM job never holds the CP→CO connection
open: submit returns a handle immediately, the runner polls status until
terminal, then asks for the verified result. `find_jobs_by_name` closes
the write-ahead idempotency gap (adopt a job whose id was never persisted).
"""

from pathlib import Path
from typing import Any

import httpx

from .api_paths import (
    URL_STEP_FIND_BY_NAME,
    URL_STEP_RESULT,
    URL_STEP_STATUS,
    URL_STEP_SUBMIT,
)
from .backend_failure import (
    BACKEND_FAILURE_HEADER,
    BackendFailure,
    BackendFailureBody,
    FailureKind,
)
from .models import (
    FoundJobWire,
    StepBaselineResources,
    StepFindByNameRequest,
    StepFindByNameResponse,
    StepHandleWire,
    StepResultRequest,
    StepResultResponse,
    StepStatusRequest,
    StepStatusWire,
    StepSubmitRequest,
    WorkTicketFailureStage,
)

# Per-call timeout. Each call in the decoupled trio (submit / status /
# result / find-by-name) returns promptly — the runner owns the poll loop,
# so no single call blocks for a job's duration. Still generous: an
# individual submit can be slow under slurmrestd load, but bounded so a hung
# orchestrator doesn't wedge the runner. A CO that's down (not slow) surfaces
# as a transport error → ORCHESTRATOR_UNREACHABLE well before this.
_HTTP_TIMEOUT_SECONDS = 600


class ComputeBackendClient:
    """Async HTTP client for the orchestrator's `/step/*` routes — the
    decoupled submit / status / result trio plus find-by-name.

    Use as an async context manager so the underlying httpx client is
    closed:

        async with ComputeBackendClient(
            "http://orchestrator.internal:8081",
            api_token_path=Path("/etc/qiita/cp-to-co.token"),
        ) as client:
            handle = await client.submit_step(...)
    """

    def __init__(
        self,
        base_url: str,
        *,
        api_token: str | None = None,
        api_token_path: Path | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        if api_token is not None and api_token_path is not None:
            raise ValueError(
                "ComputeBackendClient: pass either api_token or api_token_path,"
                " not both — they are mutually exclusive"
            )
        if api_token is None and api_token_path is None:
            raise ValueError(
                "ComputeBackendClient: exactly one of api_token or api_token_path must be provided"
            )

        token = api_token_path.read_text().strip() if api_token_path else api_token.strip()
        self._token = token

        if http_client is not None:
            self._http = http_client
            self._owns_http = False
        else:
            self._http = httpx.AsyncClient(
                base_url=base_url.rstrip("/"),
                timeout=_HTTP_TIMEOUT_SECONDS,
                headers={"Authorization": f"Bearer {token}"},
            )
            self._owns_http = True

    def __repr__(self) -> str:
        return f"ComputeBackendClient(base_url={self._http.base_url!r}, api_token=<redacted>)"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def close(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def submit_step(
        self,
        *,
        step_name: str,
        inputs: dict[str, Path],
        workspace: Path,
        scope_target: dict[str, Any],
        work_ticket_idx: int,
        attempt: int = 0,
        container: str | None = None,
        module: str | None = None,
        entrypoint: str | None = None,
        baseline_resources: StepBaselineResources | None = None,
    ) -> StepHandleWire:
        """Submit a step and return its handle immediately (does NOT block
        on completion). The caller persists the handle's fields and polls
        `status_step` until terminal, then calls `result_step`."""
        body = StepSubmitRequest(
            step_name=step_name,
            inputs={k: str(v) for k, v in inputs.items()},
            workspace=str(workspace),
            scope_target=scope_target,
            work_ticket_idx=work_ticket_idx,
            attempt=attempt,
            container=container,
            module=module,
            entrypoint=entrypoint,
            baseline_resources=baseline_resources,
        )
        resp = await self._post(URL_STEP_SUBMIT, body, step_name=step_name)
        self._raise_if_backend_failure(resp)
        resp.raise_for_status()
        return StepHandleWire.model_validate(resp.json())

    async def status_step(self, handle: StepHandleWire) -> StepStatusWire:
        """Read the live status of a submitted step (single, non-blocking).
        Raises a typed BackendFailure on a classified backend error (e.g.
        SLURMRESTD_UNREACHABLE, which the runner treats as transient)."""
        resp = await self._post(
            URL_STEP_STATUS, StepStatusRequest(handle=handle), step_name=handle.step_name
        )
        self._raise_if_backend_failure(resp)
        resp.raise_for_status()
        return StepStatusWire.model_validate(resp.json())

    async def result_step(self, handle: StepHandleWire, status: StepStatusWire) -> dict[str, Path]:
        """Finalize a terminal step and return its named output paths, or
        raise the classified BackendFailure on failure."""
        resp = await self._post(
            URL_STEP_RESULT,
            StepResultRequest(handle=handle, status=status),
            step_name=handle.step_name,
        )
        self._raise_if_backend_failure(resp)
        resp.raise_for_status()
        parsed = StepResultResponse.model_validate(resp.json())
        return {k: Path(v) for k, v in parsed.outputs.items()}

    async def find_jobs_by_name(self, job_name: str) -> list[FoundJobWire]:
        """Look up live SLURM jobs by their deterministic name. Returns the
        matches (empty when none / purged / in-process backend).

        The control-plane runner calls this during restart recovery to adopt
        a job it submitted but whose id it never persisted (the write-ahead
        gap), instead of re-submitting a duplicate. Raises a typed
        BackendFailure on a classified backend error (e.g.
        SLURMRESTD_UNREACHABLE), which the runner's recovery path treats as
        transient and retries."""
        resp = await self._post(
            URL_STEP_FIND_BY_NAME,
            StepFindByNameRequest(job_name=job_name),
            step_name=job_name,
        )
        self._raise_if_backend_failure(resp)
        resp.raise_for_status()
        return StepFindByNameResponse.model_validate(resp.json()).jobs

    async def _post(self, url: str, body, *, step_name: str) -> httpx.Response:
        """POST a wire model, converting an *unreachable* orchestrator into a
        typed `BackendFailure(ORCHESTRATOR_UNREACHABLE)` the runner's poll loop
        retries — instead of a raw httpx error that would fall through to the
        runner's outer handler and mark a still-running ticket FAILED (the old
        600s bug).

        Two unreachable shapes are converted:
          * `httpx.TransportError` — connect/read/write/pool timeout or network
            error (CO process down, connection dropped mid-job).
          * an HTTP **5xx** status — the CO (or the nginx in front of it) is up
            but borked: restarting behind the proxy, overloaded. A 502/503/504
            during a deploy is the canonical case.

        A **4xx** is left for `raise_for_status` in the caller: it's a permanent
        contract / auth problem (bad token, missing route, malformed request)
        that must fail loudly, not loop forever. BackendFailure responses carry
        the discriminator header at status 422 and are reconstructed by
        `_raise_if_backend_failure` *before* `raise_for_status`, so they never
        reach that 4xx path."""
        try:
            resp = await self._http.post(url, json=body.model_dump(mode="json"))
        except httpx.TransportError as exc:
            raise BackendFailure(
                kind=FailureKind.ORCHESTRATOR_UNREACHABLE,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=step_name,
                reason=f"orchestrator unreachable: {type(exc).__name__}: {exc}",
            ) from exc
        if resp.status_code >= 500:
            raise BackendFailure(
                kind=FailureKind.ORCHESTRATOR_UNREACHABLE,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=step_name,
                reason=f"orchestrator returned HTTP {resp.status_code}",
            )
        return resp

    @staticmethod
    def _raise_if_backend_failure(resp: httpx.Response) -> None:
        """Reconstruct and raise the typed BackendFailure the orchestrator
        deliberately structured (header set), so the runner sees the same
        surface and retry classification it would for an in-process
        backend. A validation error here is a real bug — the orchestrator
        promised a BackendFailureBody and shipped something else."""
        if resp.headers.get(BACKEND_FAILURE_HEADER):
            raise BackendFailureBody.model_validate(resp.json()).to_exception()
