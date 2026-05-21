"""Unit tests for the action-YAML loader (no DB)."""

import pytest
import yaml
from pydantic import ValidationError

_REFERENCE_ADD_YAML = """
action_id: reference-add
version: 1.0.0
target_kind: reference
description: Hash, mint features, write membership, load reference data.

scopes:
  - feature:mint
  - reference:write

audience:
  service: false
  human_roles: [wet_lab_admin, system_admin]

steps:
  - step: hash
    step_type: singleton
    container: qiita/reference-hash:1.0.0
    baseline_resources: {cpu: 4, mem_gb: 8, walltime: PT1H}
  - action: mint-features
    inputs: [hash.manifest]
  - action: write-membership

action_ceiling: {cpu: 16, mem_gb: 64, walltime: PT4H, gpu: 0}
"""

_DEBLUR_YAML = """
action_id: deblur
version: 0.1.0
target_kind: study_prep
scopes: [feature:mint]
audience: {service: false, human_roles: [user, wet_lab_admin]}
steps:
  - step: denoise
    step_type: map
    container: qiita/deblur:0.1.0
    baseline_resources: {cpu: 8, mem_gb: 16, walltime: PT2H}
action_ceiling: {cpu: 32, mem_gb: 128, walltime: PT8H}
"""

# A YAML without a top-level `action_id` (workflow scaffolding, container
# build manifest, anything else). The loader must skip these silently so
# they don't block sync.
_NON_ACTION_YAML = """
name: amplicon
version: 2026.3.0
steps:
  - name: quality_filter
    type: map
"""


def _write(dir_path, rel_path: str, content: str) -> None:
    p = dir_path / rel_path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content)


def test_load_actions_returns_each_yaml(tmp_path):
    """A directory with two valid action YAMLs returns both, sorted."""
    from qiita_control_plane.actions import load_actions

    _write(tmp_path, "reference-add/1.0.0.yaml", _REFERENCE_ADD_YAML)
    _write(tmp_path, "deblur/0.1.0.yaml", _DEBLUR_YAML)

    actions = load_actions(tmp_path)
    assert [(a.action_id, a.version) for a in actions] == [
        ("deblur", "0.1.0"),
        ("reference-add", "1.0.0"),
    ]


def test_load_actions_skips_non_action_yaml(tmp_path):
    """A YAML without a top-level `action_id` key (scaffolding, container
    build manifest, anything else) is skipped without error."""
    from qiita_control_plane.actions import load_actions

    _write(tmp_path, "reference-add/1.0.0.yaml", _REFERENCE_ADD_YAML)
    _write(tmp_path, "amplicon/workflow.yaml", _NON_ACTION_YAML)

    actions = load_actions(tmp_path)
    assert [a.action_id for a in actions] == ["reference-add"]


def test_load_actions_propagates_validation_errors(tmp_path):
    """Malformed action YAML (e.g., unknown scope) bubbles ValidationError —
    loader does not silently swallow validation failures."""
    from qiita_control_plane.actions import load_actions

    bad = yaml.safe_load(_REFERENCE_ADD_YAML)
    bad["scopes"] = ["features:mint"]  # plural — convention is singular
    _write(tmp_path, "reference-add/1.0.0.yaml", yaml.safe_dump(bad))

    with pytest.raises(ValidationError) as exc_info:
        load_actions(tmp_path)
    assert "unknown scope" in str(exc_info.value)


def test_load_actions_rejects_duplicate_action_id_version(tmp_path):
    """Two YAMLs declaring the same (action_id, version) is a hard error."""
    from qiita_control_plane.actions import DuplicateActionError, load_actions

    _write(tmp_path, "first/1.0.0.yaml", _REFERENCE_ADD_YAML)
    _write(tmp_path, "second/1.0.0.yaml", _REFERENCE_ADD_YAML)

    with pytest.raises(DuplicateActionError) as exc_info:
        load_actions(tmp_path)
    msg = str(exc_info.value)
    assert "reference-add" in msg
    assert "1.0.0" in msg


def test_load_actions_empty_dir_returns_empty_list(tmp_path):
    """A workflows directory with no YAMLs yields zero actions, no error."""
    from qiita_control_plane.actions import load_actions

    actions = load_actions(tmp_path)
    assert actions == []


def test_load_actions_missing_dir_raises(tmp_path):
    """A non-existent workflows directory is a fast failure — typo'd path
    shouldn't be silently treated as 'no actions to sync'."""
    from qiita_control_plane.actions import load_actions

    with pytest.raises(FileNotFoundError):
        load_actions(tmp_path / "does-not-exist")


def test_load_actions_loads_on_disk_reference_add_yaml():
    """The actual on-disk `workflows/reference-add/1.0.0.yaml` (not a
    synthetic inline copy) loads as a valid ActionDefinition: the legacy
    container `hash` step is gone, replaced by `hash_sequences` (module);
    `load` is a module step pointing at `reference_load`; the
    `context_schema` requires `fasta_upload_idx` rather than `fasta_path`.
    Locks the on-disk YAML so an accidental revert to the legacy container
    shape surfaces here."""
    from pathlib import Path

    from qiita_control_plane.actions import load_actions

    repo_root = Path(__file__).resolve().parents[2]
    actions = load_actions(repo_root / "workflows")
    by_id = {a.action_id: a for a in actions}
    ref_add = by_id["reference-add"]

    step_names = [s.name for s in ref_add.steps]
    assert "hash" not in step_names, "legacy `hash` container step must be gone"
    assert "hash_sequences" in step_names

    hash_step = next(s for s in ref_add.steps if s.name == "hash_sequences")
    assert hash_step.module == "qiita_compute_orchestrator.jobs.hash_sequences"
    assert hash_step.container is None

    load_step = next(s for s in ref_add.steps if s.name == "load")
    assert load_step.module == "qiita_compute_orchestrator.jobs.reference_load"
    assert load_step.container is None

    # context_schema must require `fasta_upload_idx`, not `fasta_path`.
    assert ref_add.context_schema["required"] == ["fasta_upload_idx"]


def test_load_actions_handles_two_versions_of_same_action(tmp_path):
    """Different versions of the same action_id are distinct rows, not
    duplicates."""
    from qiita_control_plane.actions import load_actions

    _write(tmp_path, "reference-add/1.0.0.yaml", _REFERENCE_ADD_YAML)
    v2 = yaml.safe_load(_REFERENCE_ADD_YAML)
    v2["version"] = "1.1.0"
    _write(tmp_path, "reference-add/1.1.0.yaml", yaml.safe_dump(v2))

    actions = load_actions(tmp_path)
    assert [(a.action_id, a.version) for a in actions] == [
        ("reference-add", "1.0.0"),
        ("reference-add", "1.1.0"),
    ]
