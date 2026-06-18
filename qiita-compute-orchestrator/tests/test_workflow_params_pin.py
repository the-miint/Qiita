"""Pin every workflow `params:` mapping against the real native-job `Inputs`.

A `params:` entry on a native step maps an action_context key to a field on the
job's Pydantic `Inputs` model (the runner merges it un-coerced; the model
re-coerces). The `Inputs` models do NOT set `extra="forbid"`, so a mistyped
field name would be silently ignored — the parameter would vanish and the
builder would quietly use its default. This test runs in the orchestrator tier
(the only one that can import the job modules) and walks every on-disk workflow
YAML, asserting each `params` VALUE names a real field on the target module's
`Inputs`. It is the fail-loud guard for that fail-quiet seam; the control-plane
loader test pins the YAML `params` VALUES, so the two ends are locked together.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest
import yaml
from qiita_common.actions import NATIVE_MODULE_PREFIX

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOWS_DIR = _REPO_ROOT / "workflows"


def _native_steps_with_params() -> list[tuple[str, str, dict[str, str]]]:
    """Yield (yaml_path, module, params) for every native step declaring a
    non-empty `params:` across all workflow YAMLs."""
    found: list[tuple[str, str, dict[str, str]]] = []
    for yaml_path in sorted(_WORKFLOWS_DIR.glob("*/*.yaml")):
        data = yaml.safe_load(yaml_path.read_text())
        for entry in data.get("steps", []):
            module = entry.get("module")
            params = entry.get("params")
            if module and params:
                found.append((str(yaml_path), module, params))
    return found


def test_workflows_dir_present():
    """Guard against a wrong _REPO_ROOT (the glob would silently match nothing
    and make the param pin below vacuously pass)."""
    assert _WORKFLOWS_DIR.is_dir(), f"expected workflows/ at {_WORKFLOWS_DIR}"


def test_at_least_one_params_workflow_exists():
    """host-reference-add / local-host-reference-add introduced `params:`; if
    this list goes empty the pin below stops protecting anything."""
    assert _native_steps_with_params(), "expected at least one native step with params:"


@pytest.mark.parametrize(
    "yaml_path,module,params",
    _native_steps_with_params(),
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_params_values_are_real_inputs_fields(yaml_path: str, module: str, params: dict[str, str]):
    """Every `params` value (the target Inputs field) must exist on the native
    job's `Inputs` model — otherwise the merged scalar is silently dropped."""
    assert module.startswith(NATIVE_MODULE_PREFIX), module
    mod = importlib.import_module(module)
    fields = set(mod.Inputs.model_fields)
    for ctx_key, field_name in params.items():
        assert field_name in fields, (
            f"{yaml_path}: params[{ctx_key!r}] -> {field_name!r} is not a field on"
            f" {module}.Inputs (have: {sorted(fields)})"
        )
