"""Pin every workflow's container steps against the backend's scope allowlist.

`assert_container_scope_supported` rejects a container step whose work ticket's
`scope_target.kind` is not in `_CONTAINER_SUPPORTED_SCOPES`. That check runs at
SUBMIT — on the compute path, against a live ticket — so a workflow whose
`target_kind` is outside the allowlist parses fine, syncs into `qiita.action`
fine, and only dies when someone actually runs it. Two workflows shipped that
way (long-read-assembly's four container steps and read-mask's lima step, both
`prep_sample`) and neither was caught until a real submit.

This walks the on-disk YAML and asserts the pairing statically, so the gap
closes at `make test` instead of at first run.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from qiita_compute_orchestrator.backend import _CONTAINER_SUPPORTED_SCOPES

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOWS_DIR = _REPO_ROOT / "workflows"


def _container_workflows() -> list[tuple[str, str, list[str]]]:
    """Yield (yaml_path, target_kind, [step names with `container:`]) for every
    workflow YAML that declares at least one container step."""
    found: list[tuple[str, str, list[str]]] = []
    for yaml_path in sorted(_WORKFLOWS_DIR.rglob("*.yaml")):
        data = yaml.safe_load(yaml_path.read_text())
        # Mirror the CP loader's filter exactly: a YAML without `action_id` is
        # not an action definition and never syncs (workflows/amplicon/
        # workflow.yaml is pre-schema scaffolding). Matching the loader keeps
        # this pin from flagging a file the system already ignores.
        if not isinstance(data, dict) or "action_id" not in data:
            continue
        steps = [e.get("step", "?") for e in data.get("steps", []) if e.get("container")]
        if steps:
            found.append((str(yaml_path), data.get("target_kind", ""), steps))
    return found


def test_workflows_dir_present():
    """Guard against a wrong _REPO_ROOT — the glob would match nothing and make
    the pin below vacuously pass."""
    assert _WORKFLOWS_DIR.is_dir(), f"expected workflows/ at {_WORKFLOWS_DIR}"


def test_at_least_one_container_workflow_exists():
    """If this goes empty the pin below stops protecting anything."""
    assert _container_workflows(), "expected at least one workflow with a container step"


@pytest.mark.parametrize(
    "yaml_path,target_kind,steps",
    _container_workflows(),
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_container_steps_have_a_dispatchable_scope(
    yaml_path: str, target_kind: str, steps: list[str]
):
    """A workflow that runs container steps must carry a target_kind the
    backends will actually dispatch — otherwise every one of those steps fails
    CONTRACT_VIOLATION at submit and the workflow can never complete."""
    assert target_kind in _CONTAINER_SUPPORTED_SCOPES, (
        f"{yaml_path}: target_kind={target_kind!r} is not in the backend's container"
        f" scope allowlist {sorted(_CONTAINER_SUPPORTED_SCOPES)}, but the workflow"
        f" declares container steps {steps}. Every one of them would be rejected at"
        f" submit. Either add the kind to _CONTAINER_SUPPORTED_SCOPES (the dispatch"
        f" path treats scope_target opaquely) or make the steps native."
    )
