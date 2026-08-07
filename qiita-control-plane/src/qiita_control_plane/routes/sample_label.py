"""The public sample label map.

`POST /sample-label` resolves a prep_sample cohort to the labels a published
feature table carries in place of our identifiers. A control-plane read rather
than a Flight ticket for the same reason as the genome map: the accessions it is
built from live only in Postgres, so there is nothing for the data plane to
serve.

Cohort authorization is `authorize_prep_sample_cohort`, shared with the human
alignment mint: minting a ticket for a cohort and labelling that cohort is one
workflow, and two answers to "may I read this sample" is how one surface comes to
advertise what the other refuses.
"""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from qiita_common.api_paths import PATH_SAMPLE_LABEL_PREFIX, PATH_SAMPLE_LABEL_ROOT
from qiita_common.auth_constants import Scope
from qiita_common.models import SampleLabel, SampleLabelRequest, SampleLabelResponse, Tier

from ..auth.guards import require_human, require_scope
from ..auth.principal import HumanUser, Principal
from ..deps import get_db_pool
from ..repositories.sequenced_sample import fetch_sample_labels
from ._helpers import authorize_prep_sample_cohort, first_few

router = APIRouter(prefix=PATH_SAMPLE_LABEL_PREFIX, tags=["sample-label"])

# The tier a caller needs on every study a sample is linked to. Matches the
# alignment cohort mint: this answers "what is this sample called publicly",
# which is strictly less than being able to read its rows.
_LABEL_MIN_TIER = Tier.VIEWER

# `require_human`, NOT the mint's `require_complete_profile`, and the difference
# is the rule rather than an oversight: a metadata/discovery read sits at
# require_human (as the read-mask discovery GETs do), while a route that hands
# out access to raw sequence-derived data demands a complete profile. This one
# returns accession strings the submitting archives already publish.


@router.post(PATH_SAMPLE_LABEL_ROOT)
async def resolve_sample_labels(
    body: SampleLabelRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    caller: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.PREP_SAMPLE_READ)),
) -> SampleLabelResponse:
    """Resolve a prep_sample cohort to its public labels, ascending by
    prep_sample_idx.

    A published feature table cannot carry `prep_sample_idx` — those identifiers
    are ours and mean nothing outside this system — so this map ships beside the
    table to make the artifact self-describing. Each entry carries the label AND
    the parts it was composed from, so nothing downstream has to parse a label to
    recover an accession (see `qiita_common.sample_label` for the three forms).

    Validation is ordered access → existence → labellability, and the order is
    load-bearing: each later refusal names prep_samples, so running it first would
    describe a cohort the caller has not yet been shown they may read.

    * **404** on an unknown prep_sample. Only a role-bypassed caller reaches it —
      for everyone else an unknown idx has no study link and the access gate
      already refused — but without it a typo'd identifier would vanish from an
      answer that claims to cover the whole cohort.
    * **422** on a sample with no `biosample_accession`, which cannot be labelled.
    """
    cohort = await authorize_prep_sample_cohort(
        pool, caller=caller, prep_sample_idx=body.prep_sample_idx, min_tier=_LABEL_MIN_TIER
    )

    rows = await fetch_sample_labels(pool, cohort)
    if len(rows) != len(cohort):
        found = {r["prep_sample_idx"] for r in rows}
        missing = [idx for idx in cohort if idx not in found]
        raise HTTPException(
            status_code=404,
            detail=f"{len(missing)} unknown prep_sample(s) (e.g. {first_few(missing)})",
        )

    unlabellable = [r["prep_sample_idx"] for r in rows if not r["biosample_accession"]]
    if unlabellable:
        raise HTTPException(
            status_code=422,
            detail=(
                f"{len(unlabellable)} prep_sample(s) have no biosample_accession and"
                f" cannot be labelled (e.g. {first_few(unlabellable)});"
                " submit or repair these samples first"
            ),
        )

    labels = [SampleLabel.model_validate(dict(row)) for row in rows]
    return SampleLabelResponse(labels=labels, count=len(labels))
