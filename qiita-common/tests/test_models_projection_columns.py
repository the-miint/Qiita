"""The caps on `ProjectionColumns`, the one alias that every projecting ticket body
uses: at most 128 names, each 1 to 64 characters, and never an empty list."""

import pydantic
import pytest
from pydantic import TypeAdapter

from qiita_common.models.step import ProjectionColumns

_ADAPTER = TypeAdapter(ProjectionColumns)


@pytest.mark.parametrize(
    "columns",
    [
        pytest.param(["c"], id="one-name"),
        pytest.param(["c" * 64], id="64-char-name"),
        pytest.param([f"c{i}" for i in range(128)], id="128-names"),
    ],
)
def test_a_list_inside_the_caps_is_accepted(columns):
    assert _ADAPTER.validate_python(columns) == columns


@pytest.mark.parametrize(
    "columns",
    [
        pytest.param([], id="empty-list"),
        pytest.param([""], id="empty-name"),
        pytest.param(["c" * 65], id="65-char-name"),
        pytest.param([f"c{i}" for i in range(129)], id="129-names"),
    ],
)
def test_a_list_outside_the_caps_is_refused(columns):
    with pytest.raises(pydantic.ValidationError):
        _ADAPTER.validate_python(columns)
