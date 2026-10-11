"""An amplicon run's feature table over REST.

`GET /amplicon/{processing_idx}/feature-table?sequenced_pool_idx=` serves, as TSV,
the ASV counts one amplicon run produced over one pool's prep_samples. A REST route
rather than a ticket mint because its consumers include the web UI, which cannot
speak Flight, and because the table is small: a real rapid-16S pool of 192 prep_samples
gave 4,028 ASVs over 173 counted prep_samples, about 2 MB of TSV.

A processing_idx is the run's parameters, shared by every pool denoised with
them, so the pool is part of the request: the cohort is that pool's population as
the other pool rollups count it (`list_pool_prep_sample_idxs`), narrowed by
`study_idx` to those linked to one study when the caller asks — which is how a
reader of one study in a multi-study pool gets its share. Access is
`authorize_prep_sample_cohort`, all-or-nothing over that cohort, at the tier the
other cohort reads use. The data plane serves exactly the cohort the ticket
carries, so that gate is the authorization boundary.

Columns are the prep_samples the run counted, named by their biosample's
accession — see `_sample_labels`.
"""

import re
from typing import Annotated

import asyncpg
import pyarrow.compute as pc
import pyarrow.flight as flight
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pydantic import Field
from qiita_common.actions import AMPLICON_ACTION_ID
from qiita_common.api_paths import PATH_AMPLICON_FEATURE_TABLE, PATH_AMPLICON_PREFIX
from qiita_common.auth_constants import Scope

from ..amplicon_feature_table import fetch_amplicon_run, render_feature_table_tsv
from ..auth.guards import COHORT_MIN_TIER, require_complete_profile, require_scope
from ..auth.principal import HumanUser, Principal
from ..deps import get_data_plane_url, get_db_pool, get_flight_signing_key
from ..repositories.alignment_definition import list_pool_prep_sample_idxs
from ..repositories.processing import fetch_processing_by_idx
from ._helpers import authorize_prep_sample_cohort, first_few

router = APIRouter(prefix=PATH_AMPLICON_PREFIX, tags=["amplicon"])

TSV_MEDIA_TYPE = "text/tab-separated-values"
# What an accession may hold to name a TSV column: no tab, newline or other
# character a reader would split or trip on.
_SAFE_LABEL = re.compile(r"[A-Za-z0-9._-]+")


async def _pool_cohort(
    pool: asyncpg.Pool, sequenced_pool_idx: int, study_idx: int | None
) -> list[int]:
    """The pool's population, optionally only the prep_samples linked (unretired)
    to `study_idx`, ascending."""
    population = await list_pool_prep_sample_idxs(pool, sequenced_pool_idx)
    if study_idx is None:
        return population
    rows = await pool.fetch(
        "SELECT prep_sample_idx FROM qiita.prep_sample_to_study"
        " WHERE study_idx = $1 AND NOT retired AND prep_sample_idx = ANY($2::bigint[])"
        " ORDER BY prep_sample_idx",
        study_idx,
        population,
    )
    return [row["prep_sample_idx"] for row in rows]


async def _sample_labels(pool: asyncpg.Pool, prep_sample_idxs: list[int]) -> dict[int, str]:
    """prep_sample_idx -> the public name of its column, ascending.

    The name is the biosample's accession: its BioSample accession, else its ENA
    sample accession. Refused rather than renamed: a prep_sample with neither, one
    whose accession is not safe to print as a column name, and two that would
    share a name (one biosample sequenced twice in the pool) — two columns with
    one name cannot say which prep_sample a count came from.

    TODO: once Qiita-minted biosample identifiers (QB) land, settle this label on
    the convention chosen for them, so a biosample not yet deposited is still named.
    """
    rows = await pool.fetch(
        "SELECT ps.idx AS prep_sample_idx,"
        "       COALESCE(b.biosample_accession, b.ena_sample_accession) AS label"
        "  FROM qiita.prep_sample ps JOIN qiita.biosample b ON b.idx = ps.biosample_idx"
        " WHERE ps.idx = ANY($1::bigint[])"
        " ORDER BY ps.idx",
        prep_sample_idxs,
    )
    unnamed = [row["prep_sample_idx"] for row in rows if row["label"] is None]
    if unnamed:
        raise HTTPException(
            status_code=409,
            detail=(
                f"{len(unnamed)} counted prep_sample(s) have no biosample accession to"
                f" name them by (prep_sample_idx {first_few(unnamed)})"
            ),
        )
    unsafe = [row["prep_sample_idx"] for row in rows if not _SAFE_LABEL.fullmatch(row["label"])]
    if unsafe:
        raise HTTPException(
            status_code=409,
            detail=(
                f"{len(unsafe)} counted prep_sample(s) have a biosample accession outside"
                f" [A-Za-z0-9._-] (prep_sample_idx {first_few(unsafe)})"
            ),
        )
    owners: dict[str, list[int]] = {}
    for row in rows:
        owners.setdefault(row["label"], []).append(row["prep_sample_idx"])
    shared = {label: idxs for label, idxs in owners.items() if len(idxs) > 1}
    if shared:
        label, idxs = min(shared.items())
        raise HTTPException(
            status_code=409,
            detail=(
                f"{len(shared)} biosample accession(s) would name more than one column,"
                f" e.g. {label!r} (prep_sample_idx {first_few(idxs)})"
            ),
        )
    return {row["prep_sample_idx"]: row["label"] for row in rows}


@router.get(PATH_AMPLICON_FEATURE_TABLE, response_class=PlainTextResponse)
async def get_amplicon_feature_table(
    processing_idx: Annotated[int, Field(gt=0)],
    sequenced_pool_idx: Annotated[int, Query(gt=0)],
    study_idx: Annotated[int | None, Query(gt=0)] = None,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    data_plane_url: str = Depends(get_data_plane_url),
    caller: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> PlainTextResponse:
    """The run's feature table over the pool as TSV (`#OTU ID`, then one column
    per counted prep_sample)."""
    run = await fetch_processing_by_idx(pool, processing_idx)
    if run is None or run["workflow"] != AMPLICON_ACTION_ID:
        raise HTTPException(status_code=404, detail="amplicon run not found")

    candidates = await _pool_cohort(pool, sequenced_pool_idx, study_idx)
    if not candidates:
        detail = (
            "the sequenced pool has no prep_samples"
            if study_idx is None
            else "the sequenced pool has no prep_samples in that study"
        )
        raise HTTPException(status_code=404, detail=detail)

    cohort = await authorize_prep_sample_cohort(
        pool, caller=caller, prep_sample_idx=candidates, min_tier=COHORT_MIN_TIER
    )
    try:
        membership, chunks = await fetch_amplicon_run(
            data_plane_url=data_plane_url,
            signing_key=signing_key,
            processing_idx=processing_idx,
            prep_sample_idxs=cohort,
        )
    except flight.FlightError as exc:
        raise HTTPException(
            status_code=502, detail=f"data plane read of the amplicon run failed: {exc}"
        ) from exc
    counted = sorted(pc.unique(membership["prep_sample_idx"]).to_pylist())
    if not counted:
        detail = (
            "the amplicon run counted none of the pool's prep_samples"
            if study_idx is None
            else "the amplicon run counted none of the pool's prep_samples in that study"
        )
        raise HTTPException(status_code=404, detail=detail)
    labels = await _sample_labels(pool, counted)
    tsv = render_feature_table_tsv(membership, chunks, labels)
    return PlainTextResponse(
        tsv,
        media_type=TSV_MEDIA_TYPE,
        headers={"Content-Disposition": 'attachment; filename="amplicon-feature-table.tsv"'},
    )
