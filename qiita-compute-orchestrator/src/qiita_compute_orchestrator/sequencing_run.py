"""Orchestrator-side client for sequencing-run / sequenced-pool reads
on the control plane.

Covers the GET /sequencing-run/{R}/sequenced-pool/{P}/preflight read.
The module is named to mirror its CP-side twin (`sequencing_run` on the
control plane) so the read path carries the same name on both ends of
the wire.

The authed httpx client is built by `cp_client.make_cp_client`; callers
own its lifetime (one per `execute()` invocation).
"""

from __future__ import annotations

import httpx
from qiita_common.api_paths import URL_SEQUENCED_POOL_PREFLIGHT
from qiita_common.models import SequencedPoolPreflightResponse


class SequencedPoolPreflightNotFound(Exception):
    """Raised when GET /sequencing-run/{R}/sequenced-pool/{P}/preflight
    returns 404 — either the pool doesn't exist under the named run, or
    the pool exists but its preflight blob isn't populated.

    The route distinguishes the two cases in the response body's
    ``detail`` field; callers that need to discriminate can read
    ``self.detail``. The bcl_convert_prep step treats both as
    BackendFailure(BAD_INPUT) — neither is a transient infra issue —
    so the runner side maps it to a PERMANENT work_ticket failure.
    """

    def __init__(
        self,
        *,
        sequencing_run_idx: int,
        sequenced_pool_idx: int,
        detail: str | None = None,
    ) -> None:
        msg = (
            f"sequenced_pool {sequenced_pool_idx} preflight not available"
            f" under sequencing_run {sequencing_run_idx}"
        )
        if detail:
            msg = f"{msg}: {detail}"
        super().__init__(msg)
        self.sequencing_run_idx = sequencing_run_idx
        self.sequenced_pool_idx = sequenced_pool_idx
        self.detail = detail


async def fetch_sequenced_pool_preflight(
    *,
    http: httpx.AsyncClient,
    sequencing_run_idx: int,
    sequenced_pool_idx: int,
) -> SequencedPoolPreflightResponse:
    """GET /sequencing-run/{R}/sequenced-pool/{P}/preflight.

    ``http`` is the authed httpx client (Bearer with the compute SA PAT,
    base_url=the CP) returned by ``cp_client.make_cp_client``. The caller
    manages its lifetime — one client per ``execute()`` invocation matches
    the pattern ``sequence_range.mint_sequence_range`` uses.

    Returns the populated SequencedPoolPreflightResponse (raw bytes after
    Pydantic's Base64Bytes decoding).

    Raises:
      SequencedPoolPreflightNotFound: 404 (pool not in run / no preflight).
      httpx.HTTPStatusError: anything else (401/403, 5xx). The caller
        maps to BackendFailure based on the status.
    """
    url = URL_SEQUENCED_POOL_PREFLIGHT.format(
        sequencing_run_idx=sequencing_run_idx,
        sequenced_pool_idx=sequenced_pool_idx,
    )
    resp = await http.get(url)
    if resp.status_code == 404:
        detail: str | None = None
        try:
            detail = resp.json().get("detail")
        except ValueError, AttributeError:
            pass
        raise SequencedPoolPreflightNotFound(
            sequencing_run_idx=sequencing_run_idx,
            sequenced_pool_idx=sequenced_pool_idx,
            detail=detail if isinstance(detail, str) else None,
        )
    resp.raise_for_status()
    return SequencedPoolPreflightResponse.model_validate_json(resp.content)
