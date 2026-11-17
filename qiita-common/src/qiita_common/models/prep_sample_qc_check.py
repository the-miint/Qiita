"""Wire models for `qiita.prep_sample_qc_check` and the `qiita.qc_verdict` view."""

import math
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Self

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, model_validator


class QcCheck(StrEnum):
    """Mirrors the `prep_sample_qc_check_check_values` CHECK."""

    DATA_TYPE = "data_type"
    COMPOSITION = "composition"
    HOST_FRACTION = "host_fraction"
    READ_RETENTION = "read_retention"
    AMPLICON_REGION = "amplicon_region"
    ORIENTATION = "orientation"
    SOURCE_TRACKER = "source_tracker"
    CLASSIFIER = "classifier"


class QcSeverity(StrEnum):
    """Mirrors the `prep_sample_qc_check_severity_values` CHECK."""

    INFO = "info"
    WARNING = "warning"
    FAIL = "fail"


class QcCheckState(StrEnum):
    """Mirrors the `prep_sample_qc_check_state_values` CHECK."""

    CURRENT = "current"
    INVALIDATED = "invalidated"


class QcVerdict(BaseModel):
    """Worst severity among the rows that count, and the check that set it."""

    severity: QcSeverity
    check: QcCheck


class QcCheckResult(BaseModel):
    """One entry of a step's `qc_checks.json`; the producer comes from the ticket."""

    model_config = ConfigDict(extra="forbid", strict=True)

    prep_sample_idx: int
    check: QcCheck
    severity: QcSeverity
    reason: str | None = None
    measured_value: FiniteFloat | None = None
    threshold: FiniteFloat | None = None
    metrics: dict[str, Any] | None = None


class MarkerClass(StrEnum):
    """An rRNA marker class; stored in jsonb, with no Postgres ENUM twin."""

    MARKER_16S = "marker_16s"
    MARKER_18S = "marker_18s"
    MARKER_23S_28S = "marker_23s_28s"
    MARKER_5S_5_8S = "marker_5s_5_8s"


_Fraction = Annotated[float, Field(ge=0, le=1, strict=True, allow_inf_nan=False)]
_PositiveInt = Annotated[int, Field(gt=0, strict=True)]
_FRACTION_SUM_TOLERANCE = 1e-6


class MarkerFractions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    marker_16s: _Fraction
    marker_18s: _Fraction
    marker_23s_28s: _Fraction
    marker_5s_5_8s: _Fraction
    unclassified: _Fraction


class MarkerReferenceIdx(BaseModel):
    model_config = ConfigDict(extra="forbid")

    marker_16s: _PositiveInt
    marker_18s: _PositiveInt
    marker_23s_28s: _PositiveInt
    marker_5s_5_8s: _PositiveInt


class DataTypeMetrics(BaseModel):
    """`metrics` of a `data_type` check: the share of sampled reads per marker class."""

    model_config = ConfigDict(extra="forbid")

    n_sampled: _PositiveInt
    fractions: MarkerFractions
    dominant_class: MarkerClass | None
    marker_reference_idx: MarkerReferenceIdx

    @model_validator(mode="after")
    def _fractions_are_consistent(self) -> Self:
        total = sum(self.fractions.model_dump().values())
        if not math.isclose(total, 1.0, abs_tol=_FRACTION_SUM_TOLERANCE):
            raise ValueError(f"fractions sum to {total}, expected 1")
        marker = {c: getattr(self.fractions, c) for c in MarkerClass}
        top = max(marker.values())
        if self.dominant_class is None:
            if top > 0:
                raise ValueError("dominant_class is null but a marker fraction is above 0")
        elif marker[self.dominant_class] != top or top == 0:
            raise ValueError(f"dominant_class {self.dominant_class} is not the largest fraction")
        return self


class PrepSampleQcCheck(BaseModel):
    check: QcCheck
    severity: QcSeverity
    reason: str | None
    mask_idx: int | None
    alignment_idx: int | None
    measured_value: float | None
    threshold: float | None
    metrics: dict[str, Any] | None
    work_ticket_idx: int
    created_at: datetime
    state: QcCheckState
    invalidated_at: datetime | None
    invalidation_reason: str | None


class PrepSampleQcCheckResponse(BaseModel):
    prep_sample_idx: int
    verdict: QcVerdict | None
    checks: list[PrepSampleQcCheck]
