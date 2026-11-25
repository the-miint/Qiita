"""A feature table of stored values over REST: `POST /feature-table`.

The body (`FeatureTableRequest`) names a method, its parameters, and a study or a list
of biosamples, each by any of its identifiers; the answer is one zip (`pack_feature_table`) of:

  * the table's columns as JSON: each processed sample's QM (its column name) and its
    biosample's public name (to join to metadata); with the method, what its values
    are, and the settings its runs share;
  * the values, sparse COO `(sample, feature, value)`, as an Arrow IPC stream;
  * the features, `(feature, sequence, length, md5)` for every feature the values
    name, as an Arrow IPC stream;
  * when the request names a reference (`taxonomy`), each of those features' lineage
    under it, `(feature, <ranks>)`, as an Arrow IPC stream.

What a feature table is, which runs answer it, and why COO, is in
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
from qiita_common.api_paths import PATH_FEATURE_TABLE_PREFIX, PATH_FEATURE_TABLE_ROOT
from qiita_common.auth_constants import Scope
from qiita_common.flight_stream import StreamRowCountMismatch
from qiita_common.models.feature_table import FeatureTableRequest

from ..auth.guards import (
    COHORT_MIN_TIER,
    require_caller_has_tier_on_all_studies,
    require_complete_profile,
    require_scope,
)
from ..auth.principal import HumanUser, Principal
from ..deps import get_data_plane_url, get_db_pool, get_flight_signing_key
from ..stored_feature_table import (
    FEATURE_TABLE_MEDIA_TYPE,
    TAXONOMY_SCHEMA,
    Cohort,
    CohortError,
    build_features,
    build_taxonomy,
    build_values,
    fetch_table,
    fetch_taxonomy,
    mint_feature_labels,
    pack_feature_table,
    resolve_cohort,
    resolve_reference,
    resolve_scope,
    source_of,
)
from ._helpers import authorize_prep_sample_cohort

router = APIRouter(prefix=PATH_FEATURE_TABLE_PREFIX, tags=["feature-table"])


async def _authorized_cohort(
    pool: asyncpg.Pool, caller: HumanUser, request: FeatureTableRequest
) -> Cohort:
    # Authorize the cohort before resolving its processed samples: resolve_cohort's
    # refusals describe it (which named biosamples no matching run processed, how many
    # prep_samples it spans), so a caller who may not read it is refused first. A study
    # is authorized by its tier, then its resolved prep_samples, because one may also be
    # linked to a study the caller cannot read. Which study or biosamples a value names
    # is answered before any of that, as the study and biosample lookups answer it.
    try:
        source = source_of(request)
        scope = await resolve_scope(pool, request)
        if scope.study_idx is not None:
            await require_caller_has_tier_on_all_studies(
                pool, caller=caller, study_idxs=[scope.study_idx], min_tier=COHORT_MIN_TIER
            )
        else:
            await authorize_prep_sample_cohort(
                pool,
                caller=caller,
                prep_sample_idx=scope.prep_sample_idxs,
                min_tier=COHORT_MIN_TIER,
            )
        cohort = await resolve_cohort(pool, source, request, scope)
    except CohortError as exc:
        raise HTTPException(status_code=exc.status, detail=exc.detail) from exc
    if scope.study_idx is not None:
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


@router.post(PATH_FEATURE_TABLE_ROOT, response_class=Response)
async def feature_table(
    request: FeatureTableRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    signing_key: bytes = Depends(get_flight_signing_key),
    data_plane_url: str = Depends(get_data_plane_url),
    caller: HumanUser = Depends(require_complete_profile),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> Response:
    """The cohort's feature table: its columns, values and features, and each feature's
    lineage when the request names a reference, in one answer."""
    cohort = await _authorized_cohort(pool, caller, request)
    reference_idx = None
    if request.taxonomy is not None:
        # Before any read: a reference that cannot classify refuses the whole request.
        try:
            reference_idx = await resolve_reference(pool, request.taxonomy)
        except CohortError as exc:
            raise HTTPException(status_code=exc.status, detail=exc.detail) from exc
    values, sequences = await asyncio.gather(
        _read(cohort, cohort.source.values_table, data_plane_url, signing_key),
        _read(cohort, cohort.source.sequences_table, data_plane_url, signing_key),
    )
    labels = await mint_feature_labels(
        pool, source=cohort.source, created_by_idx=caller.principal_idx, features=values
    )
    reference_taxonomy = None
    # A table with no features has an empty taxonomy, unread: the data plane refuses a
    # ticket with an empty feature list.
    if request.taxonomy is not None and labels.num_rows:
        reference_taxonomy = await _read_taxonomy(
            labels, reference_idx, data_plane_url, signing_key
        )

    def build() -> bytes:
        taxonomy = None
        if request.taxonomy is not None:
            lineage = (
                TAXONOMY_SCHEMA.empty_table()
                if reference_taxonomy is None
                else build_taxonomy(labels, reference_taxonomy)
            )
            taxonomy = (request.taxonomy, lineage)
        return pack_feature_table(
            cohort,
            build_values(cohort, values, labels),
            build_features(cohort, sequences, labels),
            taxonomy=taxonomy,
        )

    body = await asyncio.to_thread(build)
    return Response(body, media_type=FEATURE_TABLE_MEDIA_TYPE)


async def _read_taxonomy(
    labels: pa.Table, reference_idx: int, data_plane_url: str, signing_key: bytes
) -> pa.Table:
    """The reference's taxonomy rows for each labelled feature (`fetch_taxonomy`),
    refusing a reference that carries none."""
    feature_idxs = labels.column("feature_idx").to_pylist()
    try:
        taxonomy = await asyncio.to_thread(
            lambda: fetch_taxonomy(
                data_plane_url=data_plane_url,
                signing_key=signing_key,
                reference_idx=reference_idx,
                feature_idxs=feature_idxs,
            )
        )
    except (flight.FlightError, StreamRowCountMismatch) as exc:
        raise HTTPException(
            status_code=502, detail=f"data plane read of the reference taxonomy failed: {exc}"
        ) from exc
    if taxonomy is None:
        raise HTTPException(status_code=409, detail="that reference carries no taxonomy")
    return taxonomy
