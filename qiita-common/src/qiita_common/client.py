"""REST client for service-to-service calls to the control plane."""

import httpx

from .models import (
    FeatureHashEntry,
    FeatureMintResponse,
    PhylogenyTipEntry,
    PhylogenyTipResponse,
    ReferenceResponse,
    ReferenceStatus,
)


class ControlPlaneClient:
    """Async HTTP client for the control plane REST API.

    Use as an async context manager to ensure the underlying httpx client is closed:

        async with ControlPlaneClient("http://localhost:8080") as client:
            ref = await client.create_reference(...)
    """

    def __init__(self, base_url: str, *, http_client: httpx.AsyncClient | None = None) -> None:
        if http_client is not None:
            self._http = http_client
            self._owns_http = False
        else:
            self._http = httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=30)
            self._owns_http = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def close(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def create_reference(self, name: str, version: str, kind: str) -> ReferenceResponse:
        resp = await self._http.post(
            "/api/v1/references",
            json={"name": name, "version": version, "kind": kind},
        )
        resp.raise_for_status()
        return ReferenceResponse.model_validate(resp.json())

    async def update_reference_status(
        self, reference_idx: int, status: ReferenceStatus
    ) -> ReferenceResponse:
        resp = await self._http.patch(
            f"/api/v1/references/{reference_idx}/status",
            json={"status": str(status)},
        )
        resp.raise_for_status()
        return ReferenceResponse.model_validate(resp.json())

    async def mint_features(
        self, reference_idx: int, entries: list[FeatureHashEntry]
    ) -> FeatureMintResponse:
        resp = await self._http.post(
            f"/api/v1/references/{reference_idx}/features/mint",
            json={"entries": [e.model_dump(mode="json") for e in entries]},
        )
        resp.raise_for_status()
        return FeatureMintResponse.model_validate(resp.json())

    async def write_phylogeny_tips(
        self, reference_idx: int, entries: list[PhylogenyTipEntry]
    ) -> PhylogenyTipResponse:
        resp = await self._http.post(
            f"/api/v1/references/{reference_idx}/phylogeny-tips",
            json={"entries": [e.model_dump(mode="json") for e in entries]},
        )
        resp.raise_for_status()
        return PhylogenyTipResponse.model_validate(resp.json())
