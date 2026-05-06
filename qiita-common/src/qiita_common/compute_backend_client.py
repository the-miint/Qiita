"""HTTP client used by the control-plane runner to dispatch a workflow
`step:` entry to the orchestrator's ComputeBackend.

Auth is a shared bearer token configured on both ends (env var
`QIITA_CP_TO_CO_TOKEN` in production). This is a private path between two
services on the same network — full PAT/JWT machinery is overkill.

Synchronous in v1: the request blocks for the duration of the backend
step. LocalBackend completes in milliseconds; SlurmBackend will need an
async + callback model — when that lands, this client grows a
`submit` / `poll` pair and the route splits accordingly.
"""

from pathlib import Path

import httpx

from .api_paths import URL_STEP_RUN
from .models import StepRunRequest, StepRunResponse

# Generous so a slow step doesn't get prematurely cancelled, but bounded
# so a hung orchestrator doesn't block the runner indefinitely. Larger
# than ControlPlaneClient's 30s because step dispatch is synchronous and
# real workloads run for minutes.
_HTTP_TIMEOUT_SECONDS = 600


class ComputeBackendClient:
    """Async HTTP client for the orchestrator's `/step/run` route.

    Use as an async context manager so the underlying httpx client is
    closed:

        async with ComputeBackendClient(
            "http://orchestrator.internal:8081",
            api_token_path=Path("/etc/qiita/cp-to-co.token"),
        ) as client:
            outputs = await client.run_step(...)
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

    async def run_step(
        self,
        *,
        step_name: str,
        inputs: dict[str, Path],
        workspace: Path,
        reference_idx: int,
    ) -> dict[str, Path]:
        """Dispatch one step to the orchestrator. Blocks until the
        backend returns. Returns the step's named output paths."""
        body = StepRunRequest(
            step_name=step_name,
            inputs={k: str(v) for k, v in inputs.items()},
            workspace=str(workspace),
            reference_idx=reference_idx,
        )
        resp = await self._http.post(URL_STEP_RUN, json=body.model_dump())
        resp.raise_for_status()
        parsed = StepRunResponse.model_validate(resp.json())
        return {k: Path(v) for k, v in parsed.outputs.items()}
