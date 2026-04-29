"""REST client for service-to-service calls to the control plane."""

from pathlib import Path

import httpx

from .auth_constants import API_PREFIX
from .models import (
    DoGetTicketResponse,
    FeatureHashEntry,
    FeatureMintResponse,
    ReferenceResponse,
    ReferenceStatus,
    RegisterFilesResponse,
)

# Default per-request HTTP timeout. Generous so a slow control-plane reply
# doesn't get prematurely cancelled, but bounded so a hung server doesn't
# block the orchestrator indefinitely.
_CLIENT_HTTP_TIMEOUT_SECONDS = 30


class ControlPlaneClient:
    """Async HTTP client for the control plane REST API.

    Authentication is required: callers must pass either `api_token` (the
    plaintext qk_... PAT or service-account token) or `api_token_path` (the
    filesystem path to a file containing the token, mode 0400 in production).
    The two are mutually exclusive — passing both raises ValueError.

    Use as an async context manager so the underlying httpx client is closed:

        async with ControlPlaneClient(
            "http://localhost:8080",
            api_token_path=Path("/etc/qiita/orchestrator.token"),
        ) as client:
            ref = await client.create_reference(...)

    The plaintext token is loaded once at construction time and stored
    internally. `__repr__` redacts the token; the logging filter at
    `qiita_common.log.AuthorizationScrubFilter` scrubs `Authorization`
    headers from any log record that includes them.
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
                "ControlPlaneClient: pass either api_token or api_token_path,"
                " not both — they are mutually exclusive"
            )
        if api_token is None and api_token_path is None:
            raise ValueError(
                "ControlPlaneClient: exactly one of api_token or api_token_path"
                " must be provided (auth is required for every endpoint except"
                " GET /references/{id})"
            )

        if api_token_path is not None:
            token = api_token_path.read_text().strip()
        else:
            assert api_token is not None  # type narrowing
            token = api_token.strip()

        self._token = token

        if http_client is not None:
            self._http = http_client
            self._owns_http = False
            # Caller-supplied client: trust their auth setup; don't override
            # the Authorization header.
        else:
            self._http = httpx.AsyncClient(
                base_url=base_url.rstrip("/"),
                timeout=_CLIENT_HTTP_TIMEOUT_SECONDS,
                headers={"Authorization": f"Bearer {token}"},
            )
            self._owns_http = True

    def __repr__(self) -> str:
        return f"ControlPlaneClient(base_url={self._http.base_url!r}, api_token=<redacted>)"

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def close(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def create_reference(self, name: str, version: str, kind: str) -> ReferenceResponse:
        resp = await self._http.post(
            f"{API_PREFIX}/references",
            json={"name": name, "version": version, "kind": kind},
        )
        resp.raise_for_status()
        return ReferenceResponse.model_validate(resp.json())

    async def update_reference_status(
        self, reference_idx: int, status: ReferenceStatus
    ) -> ReferenceResponse:
        resp = await self._http.patch(
            f"{API_PREFIX}/references/{reference_idx}/status",
            json={"status": str(status)},
        )
        resp.raise_for_status()
        return ReferenceResponse.model_validate(resp.json())

    async def mint_features(
        self, reference_idx: int, entries: list[FeatureHashEntry]
    ) -> FeatureMintResponse:
        resp = await self._http.post(
            f"{API_PREFIX}/references/{reference_idx}/features/mint",
            json={"entries": [e.model_dump(mode="json") for e in entries]},
        )
        resp.raise_for_status()
        return FeatureMintResponse.model_validate(resp.json())

    async def register_files(
        self,
        reference_idx: int,
        staging_dir: str,
        files: dict[str, str],
    ) -> RegisterFilesResponse:
        resp = await self._http.post(
            f"{API_PREFIX}/references/{reference_idx}/register",
            json={"staging_dir": staging_dir, "files": files},
        )
        resp.raise_for_status()
        return RegisterFilesResponse.model_validate(resp.json())

    async def get_doget_ticket(self, reference_idx: int, table: str) -> DoGetTicketResponse:
        resp = await self._http.post(
            f"{API_PREFIX}/references/{reference_idx}/tickets/doget",
            json={"table": table},
        )
        resp.raise_for_status()
        return DoGetTicketResponse.model_validate(resp.json())
