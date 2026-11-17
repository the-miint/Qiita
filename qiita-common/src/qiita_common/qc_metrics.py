"""Per-check metric models for `prep_sample_qc_check.metrics`, keyed by `QcCheck`, and the
marker classes' binding and action_context key names."""

import math
from enum import StrEnum
from typing import Annotated, Any, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from qiita_common.models import QcCheck


class MarkerClass(StrEnum):
    """An rRNA marker class; stored in jsonb, with no Postgres ENUM twin."""

    MARKER_16S = "marker_16s"
    MARKER_18S = "marker_18s"
    MARKER_23S_28S = "marker_23s_28s"
    MARKER_5S_5_8S = "marker_5s_5_8s"


MARKER_REF_BINDINGS = {cls: f"{cls}_ref" for cls in MarkerClass}
MARKER_REFERENCE_IDX_KEYS = {cls: f"{cls}_reference_idx" for cls in MarkerClass}

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


METRIC_MODELS: dict[QcCheck, type[BaseModel]] = {QcCheck.DATA_TYPE: DataTypeMetrics}


def validate_qc_metrics(check: QcCheck, metrics: dict[str, Any]) -> dict[str, Any]:
    """Validate `metrics` against the model for `check` and return its canonical JSON dump."""
    model = METRIC_MODELS.get(check)
    if model is None:
        raise ValueError(f"{check}: no metrics model is registered for this check")
    try:
        return model.model_validate(metrics).model_dump(mode="json")
    except ValidationError as exc:
        raise ValueError(f"{check}: invalid metrics: {exc}") from exc
