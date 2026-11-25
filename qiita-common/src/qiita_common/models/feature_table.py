"""A feature table request: one or more runs of one method over a cohort, and the
processed samples that become its columns."""

from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from qiita_common.models._base import PrepSampleCohort

PositiveIdx = Annotated[int, Field(gt=0)]

# The most runs one feature-table request may merge.
MAX_FEATURE_TABLE_RUNS = 32

# What a table's values are.
FeatureTableValueKind = Literal["count"]


class FeatureTableRequest(BaseModel):
    """Which runs, over which prep_samples: a study's, or a named list.

    The runs merge into one table only when they are runs of one method whose features
    compare (that method's merge keys); the route refuses the rest.
    """

    model_config = ConfigDict(extra="forbid")

    processing_idx: Annotated[
        list[PositiveIdx], Field(min_length=1, max_length=MAX_FEATURE_TABLE_RUNS)
    ]
    study_idx: PositiveIdx | None = None
    prep_sample_idx: PrepSampleCohort | None = None

    @model_validator(mode="after")
    def _one_cohort(self) -> Self:
        if (self.study_idx is None) == (self.prep_sample_idx is None):
            raise ValueError("name the samples by exactly one of study_idx or prep_sample_idx")
        return self


class FeatureTableSample(BaseModel):
    """One column of the table: a processed sample (one run's view of one
    prep_sample)."""

    model_config = ConfigDict(extra="forbid")

    # The processed sample's QM: the column's name in the values.
    export_id: str
    # The biosample's public name as the study's sample listing shows it
    # (accession, else QB): the key for joining the column to its metadata.
    biosample: str


class FeatureTableSamples(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # The runs' method (`qiita.processing.workflow`) and what its values are.
    method: str
    value_kind: FeatureTableValueKind
    samples: list[FeatureTableSample]
