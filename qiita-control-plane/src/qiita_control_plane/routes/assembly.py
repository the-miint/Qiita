"""Assembly read routes: the DoGet tickets, and the run's contig -> genome map.

``POST /assembly/ticket/doget`` and
``POST /assembly/{prep_sample_idx}/{processing_idx}/ticket/doget`` both sign an
Ed25519 Flight DoGet ticket for the contig sequences ONE assembly run produced — a
``(prep_sample_idx, processing_idx)`` pair — on the data plane's
``assembled_sequence`` / ``assembled_sequence_chunks`` tables. Same signed filter,
same surfaces; they differ in who may ask and how the run is authorized, the way
``/alignment``'s two mints do (``Scope.ASSEMBLY_DOGET`` carries the argument).

``GET /assembly/{prep_sample_idx}/{processing_idx}/genome-map`` is not a ticket:
``genome_idx`` lives only in Postgres, so there is nothing for the data plane to
serve, and it is a control-plane read like its reference twin.

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
from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from qiita_common.api_paths import (
    PATH_ASSEMBLY_DOGET,
    PATH_ASSEMBLY_GENOME_MAP,
    PATH_ASSEMBLY_PREFIX,
    PATH_ASSEMBLY_RUN_DOGET,
)
from qiita_common.assembly_constants import (
    ASSEMBLED_SEQUENCE_CHUNKS_TABLE,
    ASSEMBLED_SEQUENCE_TABLE,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    AssemblyDoGetTicketRequest,
    AssemblyGenomeMapResponse,
    AssemblyRunDoGetTicketRequest,
    DoGetTicketResponse,
    GenomeMapEntry,
)

from ..auth.guards import (
    COHORT_MIN_TIER,
    require_complete_profile,
    require_scope,
    require_service_with_scope,
)
from ..auth.principal import HumanUser, Principal, ServiceAccount
from ..auth.tickets import sign_ticket
from ..deps import get_db_pool, get_flight_signing_key
from ..repositories.assembly import (
    ASSEMBLY_SAMPLE_COMPLETED,
    ASSEMBLY_SAMPLE_NO_DATA,
    count_assembly_genome_map,
    count_assembly_membership_without_genome,
    fetch_assembly_genome_map,
    fetch_assembly_sample_state,
)
from ._helpers import GENOME_MAP_HARD_CAP, authorize_prep_sample_cohort

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
    return await _sign_assembly_ticket(
        pool,
        prep_sample_idx=body.prep_sample_idx,
        processing_idx=body.processing_idx,
        table=body.table,
        signing_key=signing_key,
    )


def _no_contigs_detail(prep_sample_idx: int, processing_idx: int) -> str:
    """The one wording for "this pair has no contigs", shared by every route that
    404s on it — an unknown prep_sample, an unknown processing_idx, and a real pair
    that never assembled are deliberately one answer."""
    return (
        f"no assembled contigs for prep_sample_idx={prep_sample_idx},"
        f" processing_idx={processing_idx}"
    )


async def _sign_assembly_ticket(
    pool: asyncpg.Pool,
    *,
    prep_sample_idx: int,
    processing_idx: int,
    table: str,
    signing_key: bytes,
) -> DoGetTicketResponse:
    """Sign the assembly DoGet ticket both mint routes return.

    Shared so the allowed table set, the 404 on a run with no contigs, and the
    signed filter's shape have one definition across the two mint routes — the same
    device `_sign_alignment_ticket` is. What they do not share is authorization,
    which is the whole difference between them.
    """
    if table not in ASSEMBLY_DOGET_TABLES:
        raise HTTPException(
            status_code=422,
            detail=f"Unknown table {table!r}; allowed: {sorted(ASSEMBLY_DOGET_TABLES)}",
        )

    if not await _assembly_run_exists(
        pool, prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
    ):
        raise HTTPException(
            status_code=404, detail=_no_contigs_detail(prep_sample_idx, processing_idx)
        )

    ticket_bytes = sign_ticket(
        table=table,
        filter={
            "prep_sample_idx": [prep_sample_idx],
            "processing_idx": [processing_idx],
        },
        secret=signing_key,
    )
    return DoGetTicketResponse(ticket=base64.b64encode(ticket_bytes).decode())


@assembly_router.post(PATH_ASSEMBLY_RUN_DOGET, status_code=201)
async def create_assembly_run_doget_ticket(
    prep_sample_idx: Annotated[int, Field(gt=0)],
    processing_idx: Annotated[int, Field(gt=0)],
    body: AssemblyRunDoGetTicketRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    caller: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.ASSEMBLY_DOGET)),
) -> DoGetTicketResponse:
    """Sign a DoGet ticket for an assembly run the CALLER names — the
    scientist-facing counterpart of the work-ticket mint above.

    Human-callable (``assembly:doget``, on every role ceiling and on no service
    ceiling — that scope carries why, and why it is not a widening of what an
    assembly ticket returns). The caller must hold ``Tier.VIEWER`` on every study
    the run's prep_sample is still linked to.

    **Access is checked BEFORE existence, which inverts the alignment mint's
    ladder.** There the 404 is about an ``alignment_definition`` — a global object
    whose existence discloses nothing about a caller's samples. Here the thing that
    may or may not exist is ``(this prep_sample, this run)``, so answering 404 first
    would tell an unauthorized caller whether a prep_sample they cannot read was
    assembled. The signed run is the authorization boundary either way: the data
    plane serves exactly the pair this ticket carries and knows nothing about
    studies or users.
    """
    await authorize_prep_sample_cohort(
        pool, caller=caller, prep_sample_idx=[prep_sample_idx], min_tier=COHORT_MIN_TIER
    )
    await _require_completed_assembly_run(
        pool, prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
    )
    return await _sign_assembly_ticket(
        pool,
        prep_sample_idx=prep_sample_idx,
        processing_idx=processing_idx,
        table=body.table,
        signing_key=signing_key,
    )


@assembly_router.get(PATH_ASSEMBLY_GENOME_MAP)
async def get_assembly_genome_map(
    prep_sample_idx: Annotated[int, Field(gt=0)],
    processing_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> AssemblyGenomeMapResponse:
    """One assembly run's contig → genome lookup: one entry per (feature, genome)
    pair with the genome's ``source`` / ``source_id``, ordered by (feature_idx,
    genome_idx).

    The de novo arm's twin of ``GET /reference/{idx}/genome-map``, and a
    control-plane read for the same reason: ``genome_idx`` and a genome's
    provenance exist only in Postgres. The client-side feature-table recipe rolls
    this run's contigs up to genomes through it — per-genome length, breadth, the
    survivor join, the relabel — exactly as it does the reference map.

    ``Scope.PREP_SAMPLE_READ`` rather than the DoGet scope beside it: this returns
    identifiers and provenance, no sequence and no ticket, so it sits with the
    other per-sample metadata reads. Access is still per-study and still checked
    first, for the reason the mint above gives.

    422 — not a short 200 — when any of the run's memberships carries no
    ``genome_idx``. Those contigs are simply absent from the map, and the absence
    is invisible downstream: the genomes they belong to keep their other contigs,
    so their length denominators come back short and their breadth comes back
    high. The refusal names the backfill that fixes it.

    413 above the hard cap, naming the real size, and no ``truncated`` field — a
    silently short lookup table yields a WRONG feature table rather than a partial
    one, which is the reference twin's reasoning unchanged.
    """
    await authorize_prep_sample_cohort(
        pool, caller=caller, prep_sample_idx=[prep_sample_idx], min_tier=COHORT_MIN_TIER
    )
    await _require_completed_assembly_run(
        pool, prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
    )

    unminted = await count_assembly_membership_without_genome(
        pool, prep_sample_idx=[prep_sample_idx], processing_idx=processing_idx
    )
    if unminted:
        raise HTTPException(
            status_code=422,
            detail=(
                f"{unminted[prep_sample_idx]} membership row(s) of"
                f" prep_sample_idx={prep_sample_idx},"
                f" processing_idx={processing_idx} carry no genome_idx, so this map"
                " would silently omit their contigs and shorten their genomes'"
                " length denominators. An operator has to run the assembly-genome"
                " backfill on the host before this run can be used as a de novo arm"
                " — `qiita-admin` is host-side and reads DATABASE_URL, so it is not"
                " something this caller can run."
            ),
        )

    rows = await fetch_assembly_genome_map(
        pool,
        prep_sample_idx=prep_sample_idx,
        processing_idx=processing_idx,
        limit=GENOME_MAP_HARD_CAP + 1,
    )
    if len(rows) > GENOME_MAP_HARD_CAP:
        total = await count_assembly_genome_map(
            pool, prep_sample_idx=prep_sample_idx, processing_idx=processing_idx
        )
        raise HTTPException(
            status_code=413,
            detail=(
                f"Genome map for prep_sample_idx={prep_sample_idx},"
                f" processing_idx={processing_idx} has {total} entries, over the"
                f" {GENOME_MAP_HARD_CAP} maximum this endpoint serves."
            ),
        )
    return AssemblyGenomeMapResponse(
        prep_sample_idx=prep_sample_idx,
        processing_idx=processing_idx,
        entries=[GenomeMapEntry.model_validate(dict(r)) for r in rows],
        count=len(rows),
    )


async def _require_completed_assembly_run(
    pool: asyncpg.Pool, *, prep_sample_idx: int, processing_idx: int
) -> None:
    """Refuse unless this run's gate reads ``'completed'`` for this prep_sample.

    **Completion is the gate's value, never the presence of membership rows** — the
    schema says so on the column itself, because the assembly tail writes membership
    across several workflow entries, so a partial footprint is indistinguishable from
    a finished one. `_assembly_run_exists` answers a different question and is the
    right gate for the routes that only sign a run they can serve.

    The two human reads need the stronger one. They feed the client-side combined
    feature table, and its server-side counterpart refuses exactly these states at
    submit (`runner/_feature_table.py`'s arm gate): a ``'pending'`` run would give a
    table that changes underneath it, and an ``'invalidated'`` one would carry
    withdrawn contigs into a published result. A client cannot re-derive either from
    a 404, which is deliberately three answers in one.

    ``'no_data'`` keeps the 404 the caller already handles as "no de novo arm for
    this prep_sample", so the graceful path is unchanged.
    """
    state = await fetch_assembly_sample_state(
        pool, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
    )
    if state == ASSEMBLY_SAMPLE_COMPLETED:
        return
    if state in (None, ASSEMBLY_SAMPLE_NO_DATA):
        raise HTTPException(
            status_code=404, detail=_no_contigs_detail(prep_sample_idx, processing_idx)
        )
    raise HTTPException(
        status_code=409,
        detail=(
            f"assembly run {processing_idx} for prep_sample_idx={prep_sample_idx} reads"
            f" {state!r} in qiita.assembly_sample, not 'completed', so its contigs are"
            " not to be consumed. A run still going would change the answer underneath"
            " you; a withdrawn one was judged untrustworthy by a person."
        ),
    )


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
