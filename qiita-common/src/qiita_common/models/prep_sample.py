"""Prep-sample wire models.

prep_sample is the processing-kind supertype — a sequenced_sample is one
subtype of it — so these shapes are not specific to any single processing
kind.
"""

from typing import Annotated

from pydantic import Field

from qiita_common.models.sample_field import (
    SampleStudyFieldCreateRequest,
    SampleStudyFieldResponse,
)


class PrepSampleStudyFieldCreateRequest(SampleStudyFieldCreateRequest):
    """Body for POST /api/v1/study/{study_idx}/prep-sample-field."""

    global_field_idx: Annotated[int, Field(gt=0)] | None = Field(
        default=None, alias="prep_sample_global_field_idx"
    )


class PrepSampleStudyFieldResponse(SampleStudyFieldResponse):
    """Returned by POST /api/v1/study/{study_idx}/prep-sample-field (201 body is
    the created resource), carrying every qiita.prep_sample_study_field column.
    """

    study_field_idx: Annotated[int, Field(gt=0)] = Field(alias="prep_sample_study_field_idx")
    global_field_idx: Annotated[int, Field(gt=0)] | None = Field(
        alias="prep_sample_global_field_idx"
    )
