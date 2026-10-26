"""StudyCreate: the explicit study_idx band and the unknown-key refusal."""

import pytest
from pydantic import ValidationError

from qiita_common.models import LEGACY_IDX_RESERVED_CEILING, StudyCreate


@pytest.mark.parametrize("study_idx", [1, LEGACY_IDX_RESERVED_CEILING - 1])
def test_study_create_accepts_study_idx_inside_reserved_band(study_idx):
    assert StudyCreate(title="t", study_idx=study_idx).study_idx == study_idx


@pytest.mark.parametrize("study_idx", [0, -1, LEGACY_IDX_RESERVED_CEILING])
def test_study_create_rejects_study_idx_outside_reserved_band(study_idx):
    with pytest.raises(ValidationError, match="study_idx"):
        StudyCreate(title="t", study_idx=study_idx)


def test_study_create_study_idx_defaults_to_none():
    assert StudyCreate(title="t").study_idx is None


def test_study_create_rejects_unknown_key():
    with pytest.raises(ValidationError, match="extra_forbidden"):
        StudyCreate(title="t", study_id=10317)
