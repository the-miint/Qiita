"""`registered_output_step` names the step whose output register-files loads: the
re-arm checks that step's attempt directory, so a wrong answer checks the wrong
directory."""

from pathlib import Path

import pytest
import yaml
from qiita_common.actions import ActionDefinition

from qiita_control_plane.actions.align_rerun import registered_output_step

_ALIGN_YAML = Path(__file__).resolve().parents[2] / "workflows" / "align" / "1.0.0.yaml"


def test_the_shipped_align_workflow_registers_align_sharded_output():
    steps = ActionDefinition.model_validate(yaml.safe_load(_ALIGN_YAML.read_text())).steps
    assert registered_output_step(steps) == "align_sharded"


def _steps(*entries):
    from types import SimpleNamespace

    return [SimpleNamespace(name=n, inputs=i, outputs=o) for n, i, o in entries]


def test_the_producer_need_not_be_first():
    steps = _steps(
        ("prep", [], ["x"]),
        ("align", ["x"], ["staging"]),
        ("register-files", ["staging"], []),
    )
    assert registered_output_step(steps) == "align"


@pytest.mark.parametrize(
    "entries",
    [
        [("align", [], ["other"]), ("register-files", ["staging"], [])],
        [("a", [], ["staging"]), ("b", [], ["staging"]), ("register-files", ["staging"], [])],
        [("align", [], ["staging"])],
    ],
    ids=["no-producer", "two-producers", "no-register-files"],
)
def test_an_ambiguous_wiring_raises(entries):
    with pytest.raises(RuntimeError):
        registered_output_step(_steps(*entries))
