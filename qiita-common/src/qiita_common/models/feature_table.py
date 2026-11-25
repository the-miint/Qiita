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

from qiita_common.models._base import (
    EXPORT_ENTITY_ID,
    MAX_COHORT_PREP_SAMPLE_IDX,
    NonBlankText,
    check_no_repeats,
)
from qiita_common.models.biosample import (
    BiosampleIdentifierKey,
    StudyLookupKey,
    check_lookup_value_pattern,
)

# What a table's values are.
FeatureTableValueKind = Literal["count"]

# A method parameter's value. Strict, because a parameter matches a run's stored setting
# by type as well as value: "150" is not 150.
FeatureTableParameter = StrictBool | StrictInt | StrictFloat | StrictStr


class FeatureTableStudy(BaseModel):
    """A study, named by one value of the identifier kind `key` names, as a study
    lookup takes it."""

    model_config = ConfigDict(extra="forbid")

    key: StudyLookupKey = EXPORT_ENTITY_ID
    value: NonBlankText


class FeatureTableBiosamples(BaseModel):
    """Biosamples, named by values of the identifier kind `key` names, as a biosample
    lookup takes them. No value may repeat."""

    model_config = ConfigDict(extra="forbid")

    key: BiosampleIdentifierKey = EXPORT_ENTITY_ID
    values: list[NonBlankText] = Field(min_length=1, max_length=MAX_COHORT_PREP_SAMPLE_IDX)

    @model_validator(mode="after")
    def _check_values(self) -> Self:
        check_lookup_value_pattern(self.key, self.values)
        check_no_repeats(self.values, field="values")
        return self


class FeatureTableRequest(BaseModel):
    """A method's stored values over a cohort: a study's prep_samples, or the
    prep_samples of named biosamples.

    The server draws on the method's active runs that processed the cohort, narrowed to
    those whose settings match `parameters`, and refuses runs that would not compare.
    """

    model_config = ConfigDict(extra="forbid")

    method: Annotated[str, Field(min_length=1)]
    parameters: dict[str, FeatureTableParameter] = Field(default_factory=dict)
    study: FeatureTableStudy | None = None
    biosample: FeatureTableBiosamples | None = None

    @model_validator(mode="after")
    def _one_cohort(self) -> Self:
        if (self.study is None) == (self.biosample is None):
            raise ValueError("name the samples by exactly one of study or biosample")
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
