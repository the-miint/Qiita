"""A feature table request: a method's stored values over a cohort, and the processed
samples that become its columns."""

from typing import Annotated, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
    model_validator,
)

from qiita_common.models._base import PrepSampleCohort

PositiveIdx = Annotated[int, Field(gt=0)]

# What a table's values are.
FeatureTableValueKind = Literal["count"]

# A method parameter's value. Strict, because a parameter matches a run's stored setting
# by type as well as value: "150" is not 150.
FeatureTableParameter = StrictBool | StrictInt | StrictFloat | StrictStr


class FeatureTableRequest(BaseModel):
    """A method's stored values over a cohort: a study's prep_samples, or a named list.

    The server draws on the method's active runs that processed the cohort, narrowed to
    those whose settings match `parameters`, and refuses runs that would not compare.
    """

    model_config = ConfigDict(extra="forbid")

    method: Annotated[str, Field(min_length=1)]
    parameters: dict[str, FeatureTableParameter] = Field(default_factory=dict)
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
    # The settings the runs share, one per parameter the method takes, whether or not
    # the request named it; None where the runs store none.
    parameters: dict[str, FeatureTableParameter | None]
    samples: list[FeatureTableSample]
