"""ENA Browser API client answering "is this run still available?" for a held
sequenced_sample on re-import (see `ena_import.batch`).

The Portal `/search` endpoint `MiintEnaResolver` queries never returns a
non-public record at all (`qiita_common.models.ena.EnaStatus`'s docstring), so
a run missing from a re-import's Portal response could mean anything from
"suppressed" to "never existed" -- the Browser API's `summary/{accession}` is
the source that answers which, reporting a numeric `status` regardless of
availability.

Runs batch in one comma-separated request (`summary/SRR096342,ERR000130` returns
both summaries), but one accession ENA can't serve makes the whole batch HTTP 500,
and mixing record types (`summary/PRJEB1,SRR096342`) returns 400 (probed live,
2026-10-05). So `check_runs` batches runs and re-asks a batch that 500s one run
at a time.

Tracked workaround: miint exposes no equivalent read (duckdb-miint#289); see
`docs/duckdb-miint.md`'s Open upstream gaps for the removal condition.
"""

from __future__ import annotations

import httpx
from qiita_common.models.ena import parse_ena_browser_status

BROWSER_API_BASE_URL = "https://www.ebi.ac.uk/ena/browser/api"

_TIMEOUT_SECONDS = 30.0
# Keeps the request path well under common URL-length limits.
_BATCH_SIZE = 100


class EnaAvailabilityClient:
    """Checks run accessions' current ENA availability via the Browser API.

    Use as an async context manager so an owned httpx client closes:

        async with EnaAvailabilityClient() as client:
            statuses = await client.check_runs(["SRR096342"])

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

    async def check_runs(self, run_accessions: list[str]) -> dict[str, str | None]:
        """Map each run accession to the value `sequenced_sample.ena_status`
        should hold: `None` if ENA reports it public, else ENA's own
        `statusDescription`.

        Raises `httpx.HTTPStatusError` when a run still returns non-2xx on its
        own. A 500 covers nonexistent, withdrawn and a transient fault alike, so
        it is not evidence the run is unavailable and writes no flag. Also
        raises `UnknownEnaBrowserStatusError` for an unrecognized status, and
        `ValueError` when a response omits a requested run.
        """
        statuses: dict[str, str | None] = {}
        for start in range(0, len(run_accessions), _BATCH_SIZE):
            batch = run_accessions[start : start + _BATCH_SIZE]
            response = await self._get(batch, retry_on_500=False)
            if response.status_code == httpx.codes.INTERNAL_SERVER_ERROR and len(batch) > 1:
                for run_accession in batch:
                    statuses[run_accession] = await self.check_run(run_accession)
                continue
            response.raise_for_status()
            statuses.update(_parse_summaries(response, batch))
        return statuses

    async def check_run(self, run_accession: str) -> str | None:
        """`check_runs` for one accession."""
        response = await self._get([run_accession], retry_on_500=True)
        response.raise_for_status()
        return _parse_summaries(response, [run_accession])[run_accession]

    async def _get(self, run_accessions: list[str], *, retry_on_500: bool) -> httpx.Response:
        """GET the summaries, retrying once on a transport error or a 5xx. A
        batch's 500 is returned unretried: it usually means one run ENA can't
        serve, which the caller resolves run by run."""
        path = f"/summary/{','.join(run_accessions)}"
        try:
            response = await self._http.get(path)
        except httpx.TransportError:
            return await self._http.get(path)
        if response.is_server_error and (
            retry_on_500 or response.status_code != httpx.codes.INTERNAL_SERVER_ERROR
        ):
            return await self._http.get(path)
        return response


def _parse_summaries(response: httpx.Response, run_accessions: list[str]) -> dict[str, str | None]:
    body = response.json()
    summaries = body.get("summaries") or []
    if len(run_accessions) == 1 and len(summaries) == 1:
        by_accession = {run_accessions[0]: summaries[0]}
    else:
        by_accession = {summary.get("accession"): summary for summary in summaries}
    missing = [a for a in run_accessions if a not in by_accession]
    if missing or len(summaries) != len(run_accessions):
        raise ValueError(
            f"ENA Browser API summary returned {len(summaries)} summaries for"
            f" {len(run_accessions)} requested runs, expected exactly 1 each"
            f" (missing: {', '.join(missing) or 'none'}): {body!r}"
        )
    return {
        accession: parse_ena_browser_status(
            status=summary["status"], description=summary["statusDescription"]
        )
        for accession, summary in by_accession.items()
    }
