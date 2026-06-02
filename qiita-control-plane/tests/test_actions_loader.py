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

    # Pin the CLI's hardcoded action_id/version against the YAML the deploy
    # syncs into qiita.action. `qiita reference load` submits these literals;
    # a YAML id/version bump without the CLI following would 404 at submit.
    # Fail here at build time instead (mirrors the bcl-convert pin below).
    from qiita_control_plane.cli.reference_load import (
        _REFERENCE_ADD_ACTION_ID,
        _REFERENCE_ADD_ACTION_VERSION,
    )

    assert _REFERENCE_ADD_ACTION_ID == ref_add.action_id == "reference-add"
    assert _REFERENCE_ADD_ACTION_VERSION == ref_add.version == "1.0.0"


def test_load_actions_loads_on_disk_host_reference_add_yaml():
    """The actual on-disk `workflows/host-reference-add/1.0.0.yaml` loads as a
    valid ActionDefinition with the host-indexing shape:

      * target_kind reference, success_status active;
      * context_schema REQUIRES both fasta_upload_idx and taxonomy_upload_idx
        (taxonomy is mandatory for a host reference — it's the rype mapping
        authority's source);
      * the trailing steps are build_rype_index (module, target_status
        indexing) → register-files → register-index, in that order;
      * build_rype_index runs BEFORE register-files. register-files MOVES the
        feature-keyed reference_sequence_chunks part files into permanent
        DuckLake storage (data-plane move_file), so the index build must read
        them from staging first — an ordering regression would leave the index
        step with nothing to read. Locking the order here guards that.
    """
    from pathlib import Path

    from qiita_common.models import ScopeTargetKind

    from qiita_control_plane.actions import load_actions

    repo_root = Path(__file__).resolve().parents[2]
    actions = load_actions(repo_root / "workflows")
    by_id = {a.action_id: a for a in actions}
    assert "host-reference-add" in by_id, "workflows/host-reference-add/1.0.0.yaml must load"
    host = by_id["host-reference-add"]

    assert host.target_kind == ScopeTargetKind.REFERENCE
    assert host.success_status == "active"
    assert host.failure_status == "failed"

    # Taxonomy is required (unlike reference-add, where it's optional).
    assert set(host.context_schema["required"]) == {"fasta_upload_idx", "taxonomy_upload_idx"}

    step_names = [s.name for s in host.steps]
    # Shares the reference-add prefix, then adds the host-indexing tail.
    assert step_names == [
        "hash_sequences",
        "mint-features",
        "write-membership",
        "load",
        "build_rype_index",
        "register-files",
        "register-index",
    ]
    # build_rype_index must precede register-files (move-on-register).
    assert step_names.index("build_rype_index") < step_names.index("register-files")

    build = next(s for s in host.steps if s.name == "build_rype_index")
    assert build.module == "qiita_compute_orchestrator.jobs.build_rype_index"
    assert build.container is None
    assert build.target_status == "indexing"
    # Consumes the feature-keyed chunks the load step re-exposes as a binding.
    assert build.inputs == ["reference_sequence_chunks"]
    assert build.outputs == ["rype_index_path", "rype_index_meta"]

    # The load step re-exposes the feature-keyed chunks under its own binding so
    # build_rype_index can consume them; reference-add declares only staging_dir.
    load = next(s for s in host.steps if s.name == "load")
    assert "reference_sequence_chunks" in load.outputs

    # register-index consumes the build step's meta JSON (native-step outputs
    # are path strings, so the params ride a file).
    register_index = next(s for s in host.steps if s.name == "register-index")
    assert register_index.inputs == ["rype_index_meta"]

    # Pin the CLI's hardcoded action_id/version against the YAML — `qiita
    # reference load --host` submits these literals; an id/version drift would
    # 404 at submit. (Version constant is shared with reference-add.)
    from qiita_control_plane.cli.reference_load import (
        _HOST_REFERENCE_ADD_ACTION_ID,
        _REFERENCE_ADD_ACTION_VERSION,
    )

    assert _HOST_REFERENCE_ADD_ACTION_ID == host.action_id == "host-reference-add"
    assert _REFERENCE_ADD_ACTION_VERSION == host.version == "1.0.0"


def test_load_actions_loads_on_disk_bcl_convert_yaml():
    """The actual on-disk `workflows/bcl-convert/1.0.0.yaml` loads as a
    valid ActionDefinition: target_kind is sequenced_pool; the
    bcl_convert_prep step is a module pointing at
    qiita_compute_orchestrator.jobs.bcl_convert_prep; the bcl_convert
    step is a container with the SIF filename Settings.path_derived_images
    resolves against; baseline_resources for bcl_convert uses the lookup
    population (from_step_output + profiles with the three supported
    Illumina families); and action_ceiling matches the largest profile.

    Locks the YAML shape so the runner's A4 resolution branch (the
    lookup vs flat split in qiita_control_plane.runner._dispatch_step)
    is exercised end-to-end the first time sync drops bcl-convert into
    qiita.action.
    """
    from datetime import timedelta
    from pathlib import Path

    from qiita_common.models import ScopeTargetKind

    from qiita_control_plane.actions import load_actions

    repo_root = Path(__file__).resolve().parents[2]
    actions = load_actions(repo_root / "workflows")
    by_id = {a.action_id: a for a in actions}
    assert "bcl-convert" in by_id, "workflows/bcl-convert/1.0.0.yaml must load"
    bcl = by_id["bcl-convert"]

    assert bcl.target_kind == ScopeTargetKind.SEQUENCED_POOL
    assert bcl.audience.service is False

    # Pin the CLI's hardcoded action_id/version against the YAML the
    # operator's deploy syncs into qiita.action. `qiita submit-bcl-convert`
    # submits its work_ticket against these two literals; if the YAML bumps
    # its action_id or version without the CLI following, the bundled flow
    # would 404 against a non-existent action at submit time. Fail here at
    # build time instead.
    from qiita_control_plane.cli.user import (
        _BCL_CONVERT_ACTION_ID,
        _BCL_CONVERT_ACTION_VERSION,
    )

    assert _BCL_CONVERT_ACTION_ID == bcl.action_id == "bcl-convert"
    assert _BCL_CONVERT_ACTION_VERSION == bcl.version == "1.0.0"

    step_names = [s.name for s in bcl.steps]
    assert step_names == ["bcl_convert_prep", "bcl_convert"]

    prep = next(s for s in bcl.steps if s.name == "bcl_convert_prep")
    assert prep.module == "qiita_compute_orchestrator.jobs.bcl_convert_prep"
    assert prep.container is None

    convert = next(s for s in bcl.steps if s.name == "bcl_convert")
    assert convert.container == "bcl-convert-4.5.4.sif"
    assert convert.module is None
    # Lookup-population baseline_resources: from_step_output names the
    # upstream output file that carries the instrument key, and profiles
    # covers exactly the three A4-supported Illumina families.
    br = convert.baseline_resources
    assert br.from_step_output == "instrument_model"
    assert br.profiles is not None
    assert set(br.profiles) == {
        "Illumina NovaSeq 6000",
        "Illumina NovaSeq X",
        "Illumina iSeq",
    }
    # Flat-side fields are unset when the lookup population is used.
    assert br.cpu is None and br.mem_gb is None and br.walltime is None

    # action_ceiling matches the largest profile (NovaSeq X). A future
    # profile bump that exceeds this must update both axes in the same PR
    # because the runner enforces resolved <= ceiling at dispatch.
    novaseqx = br.profiles["Illumina NovaSeq X"]
    assert bcl.action_ceiling.cpu == novaseqx.cpu == 16
    assert bcl.action_ceiling.mem_gb == novaseqx.mem_gb == 480
    assert bcl.action_ceiling.walltime == novaseqx.walltime == timedelta(hours=12)

    # context_schema gates on the operator-supplied BCL folder path; the
    # absolute-path pattern keeps the launcher from resolving against a
    # surprise CWD on the compute node.
    assert bcl.context_schema["required"] == ["bcl_input_dir"]
    assert bcl.context_schema["properties"]["bcl_input_dir"]["pattern"] == "^/"


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
