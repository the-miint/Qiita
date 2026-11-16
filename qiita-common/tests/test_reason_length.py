"""The free-text `reason` an operator gives a lifecycle change or an exclusion takes
1 to MAX_REASON_LENGTH characters."""

from typing import Annotated

import pytest
from pydantic import BaseModel, TypeAdapter, ValidationError

from qiita_common import models
from qiita_common.auth_constants import MAX_REASON_LENGTH

_CAPPED = [
    models.AlignmentDefinitionStatusUpdate,
    models.AlignmentPurgeSampleRowsRequest,
    models.AlignmentRerunBlockRequest,
    models.AlignmentSampleStatusUpdate,
    models.AssemblySampleStatusUpdate,
    models.MaskDefinitionStatusUpdate,
    models.MaskSampleStatusUpdate,
    models.ProcessingStatusUpdate,
    models.ReferenceExclusionCreateRequest,
]


@pytest.mark.parametrize("model", _CAPPED, ids=lambda m: m.__name__)
def test_reason_takes_one_to_the_limit(model):
    info = model.model_fields["reason"]
    reason = TypeAdapter(Annotated[info.annotation, *info.metadata])
    assert reason.validate_python("x" * MAX_REASON_LENGTH) == "x" * MAX_REASON_LENGTH
    for bad in ("", "x" * (MAX_REASON_LENGTH + 1)):
        with pytest.raises(ValidationError):
            reason.validate_python(bad)


def test_every_capped_exported_reason_has_the_constants_value():
    """A `reason` with a length cap, on any model `qiita_common.models` exports,
    is capped at MAX_REASON_LENGTH."""
    caps = {
        name: [
            m.max_length
            for m in obj.model_fields["reason"].metadata
            if getattr(m, "max_length", None) is not None
        ]
        for name in dir(models)
        if isinstance(obj := getattr(models, name), type)
        and issubclass(obj, BaseModel)
        and "reason" in obj.model_fields
    }
    capped = {name: c for name, c in caps.items() if c}
    assert {m.__name__ for m in _CAPPED} <= set(capped)
    assert {name: c for name, c in capped.items() if c != [MAX_REASON_LENGTH]} == {}
