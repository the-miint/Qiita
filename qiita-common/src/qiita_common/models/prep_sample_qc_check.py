"""Wire models for `qiita.prep_sample_qc_check` and the `qiita.qc_verdict` view."""

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, FiniteFloat


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


class PrepSampleQcCheck(BaseModel):
    check: QcCheck
    severity: QcSeverity
    reason: str | None
    mask_idx: int | None
    alignment_idx: int | None
    measured_value: float | None
    threshold: float | None
    work_ticket_idx: int
    created_at: datetime
    state: QcCheckState
    invalidated_at: datetime | None
    invalidation_reason: str | None


class PrepSampleQcCheckResponse(BaseModel):
    prep_sample_idx: int
    verdict: QcVerdict | None
    checks: list[PrepSampleQcCheck]
