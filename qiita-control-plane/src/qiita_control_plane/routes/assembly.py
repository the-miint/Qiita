"""Assembly DoGet ticket route.

``POST /assembly/ticket/doget`` signs an Ed25519 Flight DoGet ticket for the
contig sequences ONE assembly run produced — a ``(prep_sample_idx,
processing_idx)`` pair — on the data plane's ``assembled_sequence`` /
``assembled_sequence_chunks`` tables.

The pair itself is what rides the ticket. Neither lake table has a
``prep_sample_idx`` column — a contig is stored once, keyed by the
content-deduped ``feature_idx`` it shares with every other run that produced the
same bytes — but ``assembly_membership`` is a DuckLake table as well as a
Postgres one, so "this run's contigs" is a fact the data plane can resolve for
itself, as a semi join, exactly the way a ``reference_idx`` filter resolves
through ``reference_membership``. Both identifiers are CP-minted and signed; the
data plane treats them as the opaque integers it treats every other identifier
as.

What the data plane reads is therefore the DuckLake copy of the junction, which
is replace-keyed on this same pair — so a re-run's ticket names that re-run's
contigs. The Postgres copy upserts on the natural key and keeps
superseded rows (see ``DEPLOY_CHECKLIST.md``), which is why it is used below only
to answer "did this run assemble at all", never to bound what streams.
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
    """Sign a DoGet ticket scoped to one assembly run.

    Service-account-only (``ticket:doget``) — minted at job RUNTIME, like every
    other DoGet mint route (short TTL; a SLURM queue can outlive a submit-time
    ticket).

    The signed filter is the pair and nothing else. A caller cannot name a
    contig: ``extra="forbid"`` on the body leaves no field that reaches the
    filter, and the data plane refuses a ``feature_idx`` on these two tables
    outright, so neither end of the wire can widen or narrow a run.

    Refusals:

    * a table outside the two assembly surfaces → 422 (the reference / alignment
      / read surfaces each have their own route and their own scope rule);
    * a pair that never assembled → 404, never a ticket. An unknown
      ``prep_sample_idx``, an unknown ``processing_idx``, and a real pair that
      never assembled are one answer here: there are no contigs to stream.

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

    if not await _assembly_run_exists(
        pool,
        prep_sample_idx=body.prep_sample_idx,
        processing_idx=body.processing_idx,
    ):
        raise HTTPException(
            status_code=404,
            detail=(
                "no assembled contigs for prep_sample_idx="
                f"{body.prep_sample_idx}, processing_idx={body.processing_idx}"
            ),
        )

    ticket_bytes = sign_ticket(
        table=body.table,
        filter={
            "prep_sample_idx": [body.prep_sample_idx],
            "processing_idx": [body.processing_idx],
        },
        secret=signing_key,
    )
    return DoGetTicketResponse(ticket=base64.b64encode(ticket_bytes).decode())


async def _assembly_run_exists(
    pool: asyncpg.Pool,
    *,
    prep_sample_idx: int,
    processing_idx: int,
) -> bool:
    """Whether this run linked any contig at all — the 404's only question.

    A diagnostic, not the read boundary: the data plane resolves which contigs
    stream from its own copy of the junction. The two copies can disagree on
    CONTENT after a re-run (Postgres keeps superseded rows, DuckLake replaces
    them), but not on existence in the direction this gate acts: the workflow
    runs ``write-assembly-membership`` before ``register-files``, so a run whose
    contigs are in the lake has rows here too, and the gate cannot refuse a run
    the data plane could serve.
    """
    return await pool.fetchval(
        "SELECT EXISTS (SELECT 1 FROM qiita.assembly_membership"
        " WHERE prep_sample_idx = $1 AND processing_idx = $2)",
        prep_sample_idx,
        processing_idx,
    )
