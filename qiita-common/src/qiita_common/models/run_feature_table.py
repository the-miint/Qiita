"""A run feature table: one cohort request over amplicon runs, and the samples it
names. One table holds runs whose features compare — amplicon runs of one trim
length and orientation — so their ASV counts line up into one matrix."""

from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from qiita_common.models._base import PrepSampleCohort

PositiveIdx = Annotated[int, Field(gt=0)]

# The most runs one feature-table request may merge; the data plane bounds its
# tickets the same way.
MAX_FEATURE_TABLE_RUNS = 32

# What a table's values are: reads counted per feature (amplicon ASVs).
FeatureTableValueKind = Literal["count"]


class FeatureTableRequest(BaseModel):
    """Which runs, over which samples: a study's, or a named list.

    The runs merge into one table only when they are runs of one method whose
    features compare: amplicon runs of one trim length and orientation. The route
    refuses the rest.
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


class FeatureTableTaxonomyRequest(FeatureTableRequest):
    """A taxonomy query: the same cohort, plus the reference to classify its features
    against, named by its public `(name, version)` handle — what `GET /reference`
    lists and what someone reproducing the table would cite. The internal
    `reference_idx` never appears here; the route resolves `(name, version)` to it
    (`resolve_reference_idx`, which keeps the opaque idx server-side).

    The reference travels with the request — there is no server-wide default, the way an
    alignment carries the reference its OGU table is built against — so one request can
    classify one run's features against one reference release and another request against
    a newer release without a redeploy.
    """

    reference_name: Annotated[str, Field(min_length=1)]
    reference_version: Annotated[str, Field(min_length=1)]


class FeatureTableSample(BaseModel):
    """One column of the table: a processed sample (one run's view of one
    prep_sample), under its public name."""

    model_config = ConfigDict(extra="forbid")

    # The column name the values use: the biosample's deposited accession
    # (BioSample, else ENA) when no other column of the table shares it, else the
    # processed sample's QM.
    sample: str
    # The processed sample's QM, always: the stable handle, whatever `sample` is.
    export_id: str
    # The biosample's public name as the study's sample listing shows it
    # (accession, else QB): the key for joining the column to its metadata.
    biosample: str
    # Which of the requested runs this column is from (echoed from the request).
    processing_idx: int


class FeatureTableSamples(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # The runs' method (`qiita.processing.workflow`) and what its values are.
    method: str
    value_kind: FeatureTableValueKind
    samples: list[FeatureTableSample]
