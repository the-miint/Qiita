"""ENA Browser API client answering "is this run still available?" for a held
sequenced_sample on re-import (see `ena_import.batch`).

The Portal `/search` endpoint `MiintEnaResolver` queries never returns a
non-public record at all (`qiita_common.models.ena.EnaStatus`'s docstring), so
a run missing from a re-import's Portal response could mean anything from
"suppressed" to "never existed" -- the Browser API's `summary/{accession}` is
the source that answers which, reporting a numeric `status` regardless of
availability.

Comma-separated batching (`summary/A,B`) returns HTTP 400 (probed live against
`https://www.ebi.ac.uk/ena/browser/api/summary/PRJEB1,SRR096342`, 2026-09-30),
so `check_run` asks for one accession per request.

Tracked workaround: miint exposes no equivalent read (duckdb-miint#289); see
`docs/duckdb-miint.md`'s Open upstream gaps for the removal condition.
"""

from __future__ import annotations

import httpx
from qiita_common.models.ena import parse_ena_browser_status

BROWSER_API_BASE_URL = "https://www.ebi.ac.uk/ena/browser/api"

_TIMEOUT_SECONDS = 30.0


class EnaAvailabilityClient:
    """Checks one run accession's current ENA availability via the Browser API.

    Use as an async context manager so an owned httpx client closes:

        async with EnaAvailabilityClient() as client:
            ena_status = await client.check_run("SRR096342")

    Pass `http_client` (an already-open `httpx.AsyncClient`, e.g. for a test
    double) to reuse a caller-owned client instead -- `close()` is then a
    no-op, mirroring `qiita_common.client.ControlPlaneClient`.
    """

    def __init__(self, http_client: httpx.AsyncClient | None = None) -> None:
        if http_client is not None:
            self._http = http_client
            self._owns_http = False
        else:
            self._http = httpx.AsyncClient(base_url=BROWSER_API_BASE_URL, timeout=_TIMEOUT_SECONDS)
            self._owns_http = True

    async def __aenter__(self) -> EnaAvailabilityClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def check_run(self, run_accession: str) -> str | None:
        """Return the value `sequenced_sample.ena_status` should hold for
        `run_accession`: `None` if ENA reports it public, else ENA's own
        `statusDescription` for any other status `EnaBrowserStatus` recognizes.

        Raises `httpx.HTTPStatusError` for any non-2xx response -- including
        HTTP 500, which the Browser API returns for both a nonexistent
        accession and, per ENA's release docs, a run withdrawn after being
        imported public, indistinguishably from a transient fault. A 500 is
        not evidence the run is unavailable, so it fails the caller's item the
        same as any other HTTP error rather than writing a flag. Also raises
        `UnknownEnaBrowserStatusError` for an unrecognized `status` code, and
        `ValueError` if the response holds zero or more than one summary --
        this method always asks for exactly one accession.
        """
        response = await self._http.get(f"/summary/{run_accession}")
        response.raise_for_status()
        body = response.json()
        summaries = body.get("summaries") or []
        if len(summaries) != 1:
            raise ValueError(
                f"ENA Browser API summary/{run_accession} returned"
                f" {len(summaries)} summaries, expected exactly 1: {body!r}"
            )
        summary = summaries[0]
        return parse_ena_browser_status(
            status=summary["status"], description=summary["statusDescription"]
        )
