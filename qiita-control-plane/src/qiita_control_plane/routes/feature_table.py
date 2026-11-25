"""A feature table of stored values over REST: one cohort request, three answers.

`POST /feature-table/{sample,value,feature}` all take the same body
(`FeatureTableRequest`: runs, and a study or a prep_sample list) and answer:

  * samples — the table's columns as JSON: each processed sample's QM (its column
    name) and its biosample's public name (to join to metadata); with the runs' method
    and what their values are;
  * values — sparse COO `(sample, feature, value)` as an Arrow IPC stream;
  * features — `(feature, sequence, length, md5)` for every feature the values name:
    its handle, its sequence and length, and the sequence's md5, as an Arrow IPC stream.

What a feature table is, which runs it can merge, and why COO, is in
`stored_feature_table`. Arrow IPC keeps the same columnar rows a Flight reader gets.

Access is all-or-nothing over the cohort, at the tier the other cohort reads use,
checked before anything is read from the data plane (`_authorized_cohort`). The
data plane serves exactly the cohort the ticket carries, so that gate is the
authorization boundary.
"""

import asyncio

import asyncpg
import pyarrow as pa
import pyarrow.flight as flight
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from qiita_common.api_paths import (
    PATH_FEATURE_TABLE_FEATURE,
    PATH_FEATURE_TABLE_PREFIX,
    PATH_FEATURE_TABLE_SAMPLE,
    PATH_FEATURE_TABLE_VALUE,
)
from qiita_common.auth_constants import Scope
from qiita_common.flight_stream import StreamRowCountMismatch
from qiita_common.models.feature_table import FeatureTableRequest, FeatureTableSamples

from ..auth.guards import (
    COHORT_MIN_TIER,
    require_caller_has_tier_on_all_studies,
    require_complete_profile,
    require_scope,
)
from ..auth.principal import HumanUser, Principal
from ..deps import get_data_plane_url, get_db_pool, get_flight_signing_key
from ..stored_feature_table import (
    ARROW_STREAM_MEDIA_TYPE,
    Cohort,
    CohortError,
    arrow_stream_bytes,
    build_features,
    build_values,
    fetch_table,
    mint_feature_labels,
    resolve_cohort,
)
from ._helpers import authorize_prep_sample_cohort

router = APIRouter(prefix=PATH_FEATURE_TABLE_PREFIX, tags=["feature-table"])


async def _authorized_cohort(
    pool: asyncpg.Pool, caller: HumanUser, request: FeatureTableRequest
) -> Cohort:
    # Authorize what the request names before resolving it: resolve_cohort's
    # refusals describe the cohort (which named prep_samples the runs did not
    # process, how many prep_samples a study holds), so a caller who may not read it
    # is refused first. A study cohort's prep_samples are authorized again once
    # resolved, because one may also be linked to a study the caller cannot read.
    if request.study_idx is not None:
        await require_caller_has_tier_on_all_studies(
            pool, caller=caller, study_idxs=[request.study_idx], min_tier=COHORT_MIN_TIER
        )
    else:
        await authorize_prep_sample_cohort(
            pool, caller=caller, prep_sample_idx=request.prep_sample_idx, min_tier=COHORT_MIN_TIER
        )
    try:
        cohort = await resolve_cohort(pool, request)
    except CohortError as exc:
        raise HTTPException(status_code=exc.status, detail=exc.detail) from exc
    if request.study_idx is not None:
        await authorize_prep_sample_cohort(
            pool, caller=caller, prep_sample_idx=cohort.prep_sample_idxs, min_tier=COHORT_MIN_TIER
        )
    return cohort


async def _read(cohort: Cohort, table: str, data_plane_url: str, signing_key: bytes) -> pa.Table:
    try:
        return await asyncio.to_thread(
            lambda: fetch_table(
                data_plane_url=data_plane_url,
                signing_key=signing_key,
                cohort=cohort,
                table=table,
            )
        )
    except (flight.FlightError, StreamRowCountMismatch) as exc:
        raise HTTPException(
            status_code=502,
            detail=f"data plane read of the {cohort.source.method} runs failed: {exc}",
        ) from exc


@router.post(PATH_FEATURE_TABLE_SAMPLE)
async def feature_table_samples(
    request: FeatureTableRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> FeatureTableSamples:
    """The table's columns: one per processed sample the request names."""
    cohort = await _authorized_cohort(pool, caller, request)
    return FeatureTableSamples(
        method=cohort.source.method,
        value_kind=cohort.source.value_kind,
        samples=[s.column for s in cohort.samples],
    )


@router.post(PATH_FEATURE_TABLE_VALUE, response_class=Response)
async def feature_table_values(
    request: FeatureTableRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    data_plane_url: str = Depends(get_data_plane_url),
    caller: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> Response:
    """The values as sparse COO `(sample, feature, value)`, an Arrow IPC stream."""
    cohort = await _authorized_cohort(pool, caller, request)
    values = await _read(cohort, cohort.source.values_table, data_plane_url, signing_key)
    labels = await mint_feature_labels(
        pool, source=cohort.source, created_by_idx=caller.principal_idx, features=values
    )
    table = build_values(cohort, values, labels)
    return Response(arrow_stream_bytes(table), media_type=ARROW_STREAM_MEDIA_TYPE)


@router.post(PATH_FEATURE_TABLE_FEATURE, response_class=Response)
async def feature_table_features(
    request: FeatureTableRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    data_plane_url: str = Depends(get_data_plane_url),
    caller: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> Response:
    """Every feature the values name, `(feature, sequence, length, md5)`, an Arrow IPC
    stream — the published handle mapped to its sequence and md5."""
    cohort = await _authorized_cohort(pool, caller, request)
    sequences = await _read(cohort, cohort.source.sequences_table, data_plane_url, signing_key)
    labels = await mint_feature_labels(
        pool, source=cohort.source, created_by_idx=caller.principal_idx, features=sequences
    )
    table = build_features(cohort, sequences, labels)
    return Response(arrow_stream_bytes(table), media_type=ARROW_STREAM_MEDIA_TYPE)
