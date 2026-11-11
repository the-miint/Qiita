"""The run feature table over REST: one cohort request, four answers.

`POST /feature-table/{sample,value,feature,taxonomy}` all take the same body
(`FeatureTableRequest`: runs, and a study or a sample list) and answer:

  * samples — the table's columns as JSON: each processed sample's column name,
    its QM, its biosample's public name (to join to metadata), and its run; with
    the runs' method and what their values are (`count`);
  * values — sparse COO `(sample, feature, value)` as an Arrow IPC stream;
  * features — `(feature, sequence, length)` for every feature the values name
    (an ASV's md5, its sequence and length), as an Arrow IPC stream;
  * taxonomy — `(feature, <ranks>)` for every feature the cohort observed, a
    feature's md5 and its taxonomic lineage from the reference the request names, as
    an Arrow IPC stream. On demand and separate from the table itself: the table is
    served without taxonomy, and a caller that wants lineage asks for it with this
    second request, naming the reference by its public `(name, version)` handle.

What a feature table is, which runs it can merge, and why COO, is in
`run_feature_table`. Arrow IPC keeps the same columnar rows a Flight reader gets,
for the web UI and the CLI alike.

Access is `authorize_prep_sample_cohort`, all-or-nothing over the cohort, at the
tier the other cohort reads use, checked before anything is read from the data
plane. The data plane serves exactly the cohort the ticket carries, so that gate
is the authorization boundary.
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
    PATH_FEATURE_TABLE_TAXONOMY,
    PATH_FEATURE_TABLE_VALUE,
)
from qiita_common.auth_constants import Scope
from qiita_common.models.run_feature_table import (
    FeatureTableRequest,
    FeatureTableSamples,
    FeatureTableTaxonomyRequest,
)

from ..auth.guards import COHORT_MIN_TIER, require_complete_profile, require_scope
from ..auth.principal import HumanUser, Principal
from ..deps import get_data_plane_url, get_db_pool, get_flight_signing_key
from ..run_feature_table import (
    ARROW_STREAM_MEDIA_TYPE,
    TAXONOMY_SCHEMA,
    Cohort,
    CohortError,
    arrow_stream_bytes,
    build_features,
    build_taxonomy,
    build_values,
    feature_idxs_of,
    fetch_runs,
    fetch_taxonomy,
    resolve_cohort,
    resolve_reference_idx,
)
from ._helpers import authorize_prep_sample_cohort

router = APIRouter(prefix=PATH_FEATURE_TABLE_PREFIX, tags=["feature-table"])


async def _authorized_cohort(
    pool: asyncpg.Pool, caller: HumanUser, request: FeatureTableRequest
) -> Cohort:
    try:
        cohort = await resolve_cohort(pool, request)
    except CohortError as exc:
        raise HTTPException(status_code=exc.status, detail=exc.detail) from exc
    await authorize_prep_sample_cohort(
        pool, caller=caller, prep_sample_idx=cohort.prep_sample_idxs, min_tier=COHORT_MIN_TIER
    )
    return cohort


async def _read(
    cohort: Cohort, data_plane_url: str, signing_key: bytes, *, values: bool
) -> tuple[pa.Table | None, pa.Table]:
    try:
        return await asyncio.to_thread(
            lambda: fetch_runs(
                data_plane_url=data_plane_url,
                signing_key=signing_key,
                cohort=cohort,
                values=values,
            )
        )
    except flight.FlightError as exc:
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
    values, sequences = await _read(cohort, data_plane_url, signing_key, values=True)
    table = build_values(cohort, values, sequences)
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
    """Every feature the values name, `(feature, sequence, length)`, an Arrow IPC
    stream. Reads only the ASV sequences: the ticket already scopes them to the ASVs
    the cohort observed."""
    cohort = await _authorized_cohort(pool, caller, request)
    _, sequences = await _read(cohort, data_plane_url, signing_key, values=False)
    table = build_features(cohort, sequences)
    return Response(arrow_stream_bytes(table), media_type=ARROW_STREAM_MEDIA_TYPE)


@router.post(PATH_FEATURE_TABLE_TAXONOMY, response_class=Response)
async def feature_table_taxonomy(
    request: FeatureTableTaxonomyRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    data_plane_url: str = Depends(get_data_plane_url),
    caller: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> Response:
    """Each feature the cohort observed, `(feature, <ranks>)`, an Arrow IPC stream:
    a feature's md5 and its taxonomic lineage, looked up by joining the feature's
    feature_idx to the taxonomy of the reference the request names by its public
    `(name, version)` handle. A feature the reference does not place keeps its row with
    all-NULL ranks — a lookup, not a filter.

    Refuses 404 when no reference carries that `(name, version)`, and 409 when the
    reference exists but carries no taxonomy at all — a sequence reference, or one still
    loading — told apart from "loaded but places nothing" by the readiness probe in
    `fetch_taxonomy`."""
    cohort = await _authorized_cohort(pool, caller, request)
    reference_idx = await resolve_reference_idx(
        pool, name=request.reference_name, version=request.reference_version
    )
    if reference_idx is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"no reference named ({request.reference_name!r}, {request.reference_version!r})"
            ),
        )
    _, sequences = await _read(cohort, data_plane_url, signing_key, values=False)
    roster = feature_idxs_of(sequences)
    if not roster:
        # No observed features: the answer is trivially empty, and the data plane
        # rejects a ticket whose feature_idx list is empty. Answer an empty stream — as
        # the features route does for the same cohort — rather than letting the read 502.
        return Response(
            arrow_stream_bytes(TAXONOMY_SCHEMA.empty_table()), media_type=ARROW_STREAM_MEDIA_TYPE
        )
    try:
        taxonomy = await asyncio.to_thread(
            lambda: fetch_taxonomy(
                data_plane_url=data_plane_url,
                signing_key=signing_key,
                reference_idx=reference_idx,
                feature_idxs=roster,
            )
        )
    except flight.FlightError as exc:
        raise HTTPException(
            status_code=502, detail=f"data plane read of reference taxonomy failed: {exc}"
        ) from exc
    if taxonomy is None:
        raise HTTPException(status_code=409, detail="that reference carries no taxonomy")
    table = build_taxonomy(sequences, taxonomy)
    return Response(arrow_stream_bytes(table), media_type=ARROW_STREAM_MEDIA_TYPE)
