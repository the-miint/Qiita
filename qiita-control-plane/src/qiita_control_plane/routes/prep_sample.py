"""Prep-sample routes.

Covers the prep-sample reads. Today: the study list
(GET /prep-sample/{idx}/study/list). The prep_sample create path runs through
the sequenced-sample composer (the only subtype today), so there is no
prep-sample POST here.

The read gates on caller scope (Scope.PREP_SAMPLE_READ) plus
require_role_at_least(WET_LAB_ADMIN), matching the sibling sequenced_sample
reads; prep_sample is that subtype's supertype.
"""

from typing import Annotated

import asyncpg
from fastapi import APIRouter, Depends
from pydantic import Field
from qiita_common.api_paths import (
    PATH_PREP_SAMPLE_PREFIX,
    PATH_PREP_SAMPLE_STUDY_LIST,
)
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import StudyListItem, StudyListResponse

from ..auth.guards import (
    require_human,
    require_prep_sample_exists,
    require_role_at_least,
    require_scope,
)
from ..auth.principal import HumanUser, Principal
from ..deps import get_db_pool
from ..repositories.prep_sample import fetch_active_studies_for_prep_sample

router = APIRouter(prefix=PATH_PREP_SAMPLE_PREFIX, tags=["prep-sample"])

# Hard cap on the study-roster read. Sized to comfortably cover any single
# prep_sample's linked-study roster while bounding per-response payload size.
# The biosample and sequenced-sample roster caps happen to share this numeric
# value, but the three bound conceptually distinct rosters and are sized
# independently; they are intentionally not factored into a shared constant.
_PREP_SAMPLE_STUDIES_HARD_CAP = 500_000


@router.get(PATH_PREP_SAMPLE_STUDY_LIST)
async def list_studies_for_prep_sample(
    prep_sample_idx: Annotated[int, Field(gt=0)],
    pool: asyncpg.Pool = Depends(get_db_pool),
    user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
    _role: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
    _exists: None = Depends(require_prep_sample_exists),
) -> StudyListResponse:
    """List the studies this prep_sample is actively linked to, ascending by
    study_idx, each with its BioProject and ENA study accessions.

    Caller must be a HumanUser with Scope.PREP_SAMPLE_READ and system_role at
    least wet_lab_admin. require_prep_sample_exists fires a 404 before the read
    runs. Retired prep_sample_to_study links are excluded. The accessions let
    an ENA-submission caller read the BioProject accession (the experiment
    study_ref) without a per-study GET. The `truncated` flag indicates the
    underlying set exceeded the hard cap.
    """
    # Fetch cap+1 rows so a count strictly greater than the cap signals
    # truncation; the route slices back to the cap before returning.
    rows = await fetch_active_studies_for_prep_sample(
        pool, prep_sample_idx, limit=_PREP_SAMPLE_STUDIES_HARD_CAP + 1
    )
    truncated = len(rows) > _PREP_SAMPLE_STUDIES_HARD_CAP
    if truncated:
        rows = rows[:_PREP_SAMPLE_STUDIES_HARD_CAP]
    return StudyListResponse(
        studies=[StudyListItem.model_validate(dict(r)) for r in rows],
        count=len(rows),
        truncated=truncated,
        caller_system_role=user.system_role,
    )
