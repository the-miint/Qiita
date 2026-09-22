"""Prep-sample wire models.

prep_sample is the processing-kind supertype — a sequenced_sample is one
subtype of it — so these shapes are not specific to any single processing
kind.
"""

from typing import Annotated

from pydantic import Field

from qiita_common.models.sample_field import (
    SampleGlobalFieldResponse,
    SampleStudyFieldCreateRequest,
    SampleStudyFieldResponse,
)

# The wire spellings of the idx fields the prep-sample field shapes re-declare
# with an entity-qualified alias. Named so a caller writing or reading one of
# those payload keys resolves it here instead of retyping it.
PREP_SAMPLE_STUDY_FIELD_IDX_WIRE = "prep_sample_study_field_idx"
PREP_SAMPLE_GLOBAL_FIELD_IDX_WIRE = "prep_sample_global_field_idx"


class PrepSampleStudyFieldCreateRequest(SampleStudyFieldCreateRequest):
    """Body for POST /api/v1/study/{study_idx}/prep-sample-field."""

    global_field_idx: Annotated[int, Field(gt=0)] | None = Field(
        default=None, alias=PREP_SAMPLE_GLOBAL_FIELD_IDX_WIRE
    )


class PrepSampleStudyFieldResponse(SampleStudyFieldResponse):
    """Returned by POST /api/v1/study/{study_idx}/prep-sample-field (201 body is
    the created resource), carrying every qiita.prep_sample_study_field column.
    """

    study_field_idx: Annotated[int, Field(gt=0)] = Field(alias=PREP_SAMPLE_STUDY_FIELD_IDX_WIRE)
    global_field_idx: Annotated[int, Field(gt=0)] | None = Field(
        alias=PREP_SAMPLE_GLOBAL_FIELD_IDX_WIRE
    )


class PrepSampleGlobalFieldResponse(SampleGlobalFieldResponse):
    """One row of GET /api/v1/prep-sample-global-field, carrying every
    qiita.prep_sample_global_field column.
    """

    global_field_idx: Annotated[int, Field(gt=0)] = Field(alias=PREP_SAMPLE_GLOBAL_FIELD_IDX_WIRE)
