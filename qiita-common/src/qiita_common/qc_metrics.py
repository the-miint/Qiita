"""Per-check metric models for `prep_sample_qc_check.metrics`, keyed by `QcCheck`."""

from typing import Any

from pydantic import BaseModel, ValidationError

from qiita_common.models import DataTypeMetrics, QcCheck

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
