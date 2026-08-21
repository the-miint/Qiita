"""Assembly DoGet ticket route.

``POST /assembly/ticket/doget`` signs an Ed25519 Flight DoGet ticket for the
contig sequences ONE assembly run produced — a ``(prep_sample_idx,
processing_idx)`` pair — on the data plane's ``assembled_sequence`` /
``assembled_sequence_chunks`` tables.

The body names the pair; the ticket carries a ``feature_idx`` set. Neither lake
table has a ``prep_sample_idx`` column — a contig is stored once, keyed by the
content-deduped ``feature_idx`` it shares with every other run that produced the
same bytes — so "this run's assembly" is not a filter a Flight ticket can
express. ``qiita.assembly_membership`` is where the pair maps to a feature set,
it lives in Postgres, and the data plane has no database, so the route reads the
roster and signs it. The caller never names features; unlike the block-read
route, the resolved set does ride the ticket, because the data plane has nothing
else to match on.
"""

import base64

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from qiita_common.api_paths import PATH_ASSEMBLY_DOGET, PATH_ASSEMBLY_PREFIX
from qiita_common.assembly_constants import (
    ASSEMBLED_SEQUENCE_CHUNKS_TABLE,
    ASSEMBLED_SEQUENCE_TABLE,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import AssemblyDoGetTicketRequest, DoGetTicketResponse
from qiita_common.models.step import MAX_DOGET_FEATURE_IDX

from ..auth.guards import require_service_with_scope
from ..auth.principal import ServiceAccount
from ..auth.tickets import sign_ticket
from ..deps import get_db_pool, get_flight_signing_key

ASSEMBLY_DOGET_TABLES = frozenset({ASSEMBLED_SEQUENCE_TABLE, ASSEMBLED_SEQUENCE_CHUNKS_TABLE})

assembly_router = APIRouter(prefix=PATH_ASSEMBLY_PREFIX, tags=["assembly"])


@assembly_router.post(PATH_ASSEMBLY_DOGET, status_code=201)
async def create_assembly_doget_ticket(
    body: AssemblyDoGetTicketRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    # Rides the generic `ticket:doget` rather than a scope of its own. What that
    # reuse grants: the scope is on SERVICE_ACCOUNT_SCOPE_CEILING and on no human
    # role ceiling, so the principals that gain contig read-back are exactly the
    # service accounts already holding it, and a human PAT cannot carry it at
    # all. What it newly exposes: `assembled_sequence` is the first
    # sample-derived sequence surface that scope reaches — every other table it
    # opens is reference data or the derived per-read `alignment` slice. The
    # privacy-sensitive neighbours (Scope.READ_MASKED_DOGET, Scope.READ_DOGET)
    # are for READ surfaces; these bytes are assembled from the `read_masked`
    # pass-set, which is the assembly workflow's only read input.
    _sa: ServiceAccount = Depends(require_service_with_scope(Scope.TICKET_DOGET)),
) -> DoGetTicketResponse:
    """Sign a DoGet ticket scoped to one assembly run's contig features.

    Service-account-only (``ticket:doget``) — minted at job RUNTIME, like every
    other DoGet mint route (short TTL; a SLURM queue can outlive a submit-time
    ticket).

    Refusals, each of which would otherwise stream a different run's contigs
    under this run's name, or none at all:

    * a table outside the two assembly surfaces → 422 (the reference / alignment
      / read surfaces each have their own route and their own scope rule);
    * no ``qiita.assembly_membership`` row for the pair → 404, never a
      whole-table ticket. An unknown ``prep_sample_idx``, an unknown
      ``processing_idx``, a pair whose run has not loaded yet, and a pair that
      never assembled are one answer here: there are no contigs to stream;
    * a roster past ``MAX_DOGET_FEATURE_IDX`` → 422 naming the count. The list
      rides the signed ticket payload and becomes one ``feature_idx IN (...)`` on
      the data plane, which is the bound that constant already states.

    Authorization is scope-only at this layer, matching the reference /
    read_masked / alignment / read doget routes: any service account holding
    ``ticket:doget`` can request a ticket; row-level visibility is not enforced
    here.
    """
    if body.table not in ASSEMBLY_DOGET_TABLES:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown table {body.table!r}; allowed: {sorted(ASSEMBLY_DOGET_TABLES)}",
        )

    feature_idx = await _fetch_assembly_roster(
        pool,
        prep_sample_idx=body.prep_sample_idx,
        processing_idx=body.processing_idx,
    )
    if not feature_idx:
        raise HTTPException(
            status_code=404,
            detail=(
                "no assembled contigs for prep_sample_idx="
                f"{body.prep_sample_idx}, processing_idx={body.processing_idx}"
            ),
        )
    if len(feature_idx) > MAX_DOGET_FEATURE_IDX:
        raise HTTPException(
            status_code=422,
            detail=(
                f"assembly roster is {len(feature_idx)} features, over the "
                f"{MAX_DOGET_FEATURE_IDX} a DoGet ticket carries"
            ),
        )

    ticket_bytes = sign_ticket(
        table=body.table,
        filter={"feature_idx": feature_idx},
        secret=signing_key,
    )
    return DoGetTicketResponse(ticket=base64.b64encode(ticket_bytes).decode())


async def _fetch_assembly_roster(
    pool: asyncpg.Pool,
    *,
    prep_sample_idx: int,
    processing_idx: int,
) -> list[int]:
    """The contig features one assembly run produced, ascending.

    DISTINCT because ``assembly_membership``'s key includes ``(kind, bin_id)``: a
    contig a refined bin claims is also carried under whichever other kind the
    run emitted it as, so the same ``feature_idx`` appears on more than one row.
    Every bin of the run is included — the ticket is the run's whole assembly,
    not one bin's.
    """
    rows = await pool.fetch(
        "SELECT DISTINCT feature_idx FROM qiita.assembly_membership"
        " WHERE prep_sample_idx = $1 AND processing_idx = $2"
        " ORDER BY feature_idx",
        prep_sample_idx,
        processing_idx,
    )
    return [row["feature_idx"] for row in rows]
