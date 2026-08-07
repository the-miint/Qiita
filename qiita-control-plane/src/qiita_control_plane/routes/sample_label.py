"""The public sample label map.

`POST /sample-label` resolves a prep_sample cohort to the labels a published
feature table carries in place of our identifiers. A control-plane read rather
than a Flight ticket for the same reason as the genome map: the accessions it is
built from live only in Postgres, so there is nothing for the data plane to
serve.

Sibling of the human alignment mint (routes/alignment.py) — same cohort shape,
same all-or-nothing access gate, same refusal wording — because a scientist
minting a ticket for a cohort and labelling that cohort is one workflow, and two
answers to "may I read this sample" is the drift that ends with one surface
advertising what the other refuses.
"""

import asyncpg
from fastapi import APIRouter, Depends, HTTPException
from qiita_common.api_paths import PATH_SAMPLE_LABEL_PREFIX, PATH_SAMPLE_LABEL_ROOT
from qiita_common.auth_constants import Scope
from qiita_common.models import SampleLabel, SampleLabelRequest, SampleLabelResponse, Tier
from qiita_common.sample_label import compose_sample_label

from ..auth.guards import filter_prep_samples_caller_can_read, require_human, require_scope
from ..auth.principal import HumanUser, Principal
from ..deps import get_db_pool
from ..repositories.sequenced_sample import fetch_sample_labels
from ._helpers import first_few, prep_sample_access_denied_detail

router = APIRouter(prefix=PATH_SAMPLE_LABEL_PREFIX, tags=["sample-label"])

# The tier a caller needs on every study a sample is linked to. Matches the
# alignment cohort mint: this answers "what is this sample called publicly",
# which is strictly less than being able to read its rows.
_LABEL_MIN_TIER = Tier.VIEWER


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

    Validation is ordered, and the order is load-bearing:

    1. **Access** → 403. All-or-nothing, via the same gate as the alignment
       cohort mint: the caller must hold `Tier.VIEWER` on every study each
       requested sample is still linked to. Narrowing instead of refusing would
       silently ship a label map that does not cover the table it accompanies.
    2. **Existence** → 404, naming a few of the unknown identifiers. Only a
       role-bypassed caller can reach this — for everyone else a nonexistent
       prep_sample has no study link and is already a 403 — but without it a
       typo'd idx would just vanish from an answer that claims to be complete.
    3. **Labellability** → 422, naming a few of the samples with no
       `biosample_accession`. Third, not second, for the same reason the mint
       checks completeness after access: this list would otherwise tell a caller
       which samples exist for a cohort they have no right to read.
    """
    # Sorted and deduped: the cohort is a set, and the response is ordered by
    # prep_sample_idx, so two spellings of the same request answer identically.
    cohort = sorted(set(body.prep_sample_idx))

    access = await filter_prep_samples_caller_can_read(
        pool, caller=caller, prep_sample_idxs=cohort, min_tier=_LABEL_MIN_TIER
    )
    if access.unlinked or access.blocked_by:
        raise HTTPException(
            status_code=403,
            detail=prep_sample_access_denied_detail(access, min_tier=_LABEL_MIN_TIER),
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

    labels = [
        SampleLabel(
            prep_sample_idx=row["prep_sample_idx"],
            label=compose_sample_label(
                prep_sample_idx=row["prep_sample_idx"],
                biosample_accession=row["biosample_accession"],
                ena_run_accession=row["ena_run_accession"],
                sequencing_run_idx=row["sequencing_run_idx"],
                sequenced_pool_idx=row["sequenced_pool_idx"],
            ),
            biosample_accession=row["biosample_accession"],
            ena_run_accession=row["ena_run_accession"],
            sequencing_run_idx=row["sequencing_run_idx"],
            sequenced_pool_idx=row["sequenced_pool_idx"],
        )
        for row in rows
    ]
    return SampleLabelResponse(labels=labels, count=len(labels))
