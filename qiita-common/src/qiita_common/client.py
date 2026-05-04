"""REST client for service-to-service calls to the control plane."""

from pathlib import Path
from typing import Any

import httpx

from .api_paths import URL_LIBRARY_NAME, LibraryPrimitive
from .auth_constants import API_PREFIX
from .models import (
    DoGetTicketResponse,
    FeatureMintResponse,
    ReferenceMembershipResponse,
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
                " GET /reference/{id})"
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
            f"{API_PREFIX}/reference",
            json={"name": name, "version": version, "kind": kind},
        )
        resp.raise_for_status()
        return ReferenceResponse.model_validate(resp.json())

    async def update_reference_status(
        self, reference_idx: int, status: ReferenceStatus
    ) -> ReferenceResponse:
        resp = await self._http.patch(
            f"{API_PREFIX}/reference/{reference_idx}/status",
            json={"status": str(status)},
        )
        resp.raise_for_status()
        return ReferenceResponse.model_validate(resp.json())

    async def mint_features(
        self,
        reference_idx: int,
        manifest_path: Path,
        output_dir: Path,
    ) -> FeatureMintResponse:
        """Invoke the mint-features library primitive.

        `manifest_path` points to a Parquet manifest with a sequence_hash
        column (typically produced by the workflow's hash step).
        `output_dir` is where the primitive will write feature_map.parquet.
        Both must live on a workspace shared with the control plane.

        Returns FeatureMintResponse(feature_map_path, minted, reused).
        Reference-agnostic at the library level; reference_idx flows into
        the dispatch envelope's scope_target only so the control plane
        can attribute the call.
        """
        outputs = await self._invoke_library(
            name=LibraryPrimitive.MINT_FEATURES,
            reference_idx=reference_idx,
            inputs={
                "manifest_path": str(manifest_path),
                "output_dir": str(output_dir),
            },
        )
        return FeatureMintResponse.model_validate(outputs)

    async def write_membership(
        self, reference_idx: int, feature_map_path: Path
    ) -> ReferenceMembershipResponse:
        """Invoke the write-membership library primitive — link the
        feature_idx values from a feature_map Parquet file to a reference.
        Idempotent."""
        outputs = await self._invoke_library(
            name=LibraryPrimitive.WRITE_MEMBERSHIP,
            reference_idx=reference_idx,
            inputs={"feature_map_path": str(feature_map_path)},
        )
        return ReferenceMembershipResponse.model_validate(outputs)

    async def register_files(
        self,
        reference_idx: int,
        staging_dir: str,
        files: dict[str, str],
    ) -> RegisterFilesResponse:
        """Invoke the register-files library primitive — register staged
        Parquet files into DuckLake via the data plane's DoAction."""
        outputs = await self._invoke_library(
            name=LibraryPrimitive.REGISTER_FILES,
            reference_idx=reference_idx,
            inputs={"staging_dir": staging_dir, "files": files},
        )
        return RegisterFilesResponse.model_validate(outputs)

    async def get_doget_ticket(self, reference_idx: int, table: str) -> DoGetTicketResponse:
        resp = await self._http.post(
            f"{API_PREFIX}/reference/{reference_idx}/ticket/doget",
            json={"table": table},
        )
        resp.raise_for_status()
        return DoGetTicketResponse.model_validate(resp.json())

    async def _invoke_library(
        self,
        *,
        name: str,
        reference_idx: int,
        inputs: dict[str, Any],
    ) -> dict[str, Any]:
        """POST /api/v1/library/{name} with the standard envelope; return
        the unwrapped `outputs` dict for the caller's per-primitive parser.

        Today every library primitive targets a reference; if/when a
        primitive needs a different scope_target.kind this helper grows
        a `scope_target: ScopeTarget` parameter.
        """
        resp = await self._http.post(
            URL_LIBRARY_NAME.format(name=name),
            json={
                "scope_target": {"kind": "reference", "reference_idx": reference_idx},
                "inputs": inputs,
            },
        )
        resp.raise_for_status()
        return resp.json()["outputs"]
