"""Tests for ActionDefinition and the action-registry Pydantic shape.

`walltime` values are ISO 8601 duration strings (`PT1H` = 1 hour,
`PT1M` = 1 minute, `PT4H` = 4 hours). Pydantic parses these into
`datetime.timedelta` automatically.
"""

from datetime import timedelta

import pytest
from pydantic import ValidationError

from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import ScopeTargetKind, StepType
from qiita_common.testing.containers import REFERENCE_HASH_CONTAINER
from qiita_common.testing.native_steps import FASTQ_TO_PARQUET_MODULE


def _minimal_action_kwargs() -> dict:
    """Smallest valid kwargs for ActionDefinition — used as the base for
    targeted negative tests so each test only varies the field under
    scrutiny."""
    return dict(
        action_id="reference-add",
        version="1.0.0",
        target_kind=ScopeTargetKind.REFERENCE,
        scopes=[Scope.FEATURE_MINT, Scope.REFERENCE_WRITE],
        audience={"service": False, "human_roles": [SystemRole.WET_LAB_ADMIN]},
        steps=[
            {
                "step": "hash",
                "step_type": StepType.SINGLETON,
                "container": REFERENCE_HASH_CONTAINER,
                "baseline_resources": {
                    "cpu": 4,
                    "mem_gb": 8,
                    "walltime": "PT1H",
                },
            }
        ],
        action_ceiling={
            "cpu": 16,
            "mem_gb": 64,
            "walltime": "PT4H",
            "gpu": 0,
        },
    )


def test_minimal_action_definition_loads():
    """The smallest valid YAML-shape dict must validate cleanly."""
    from qiita_common.actions import ActionDefinition, StepType, WorkflowStep

    a = ActionDefinition(**_minimal_action_kwargs())
    assert a.action_id == "reference-add"
    assert a.version == "1.0.0"
    assert a.target_kind == ScopeTargetKind.REFERENCE
    assert a.scopes == [Scope.FEATURE_MINT, Scope.REFERENCE_WRITE]
    assert len(a.steps) == 1
    assert isinstance(a.steps[0], WorkflowStep)
    assert a.steps[0].name == "hash"
    assert a.steps[0].step_type == StepType.SINGLETON
    assert a.steps[0].baseline_resources.walltime == timedelta(hours=1)
    assert a.action_ceiling.walltime == timedelta(hours=4)


def test_step_and_action_shorthand_normalize():
    """Both `step: <name>` and `action: <name>` shorthand must rewrite into
    the discriminator form."""
    from qiita_common.actions import ActionDefinition, WorkflowAction, WorkflowStep

    kwargs = _minimal_action_kwargs()
    kwargs["steps"] = [
        {
            "step": "hash",
            "step_type": StepType.SINGLETON,
            "container": "img:1",
            "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
        },
        {"action": "mint-features", "inputs": ["hash.manifest"]},
    ]
    a = ActionDefinition(**kwargs)
    assert isinstance(a.steps[0], WorkflowStep)
    assert a.steps[0].name == "hash"
    assert isinstance(a.steps[1], WorkflowAction)
    assert a.steps[1].name == "mint-features"
    assert a.steps[1].inputs == ["hash.manifest"]


def test_step_entry_rejects_both_keys():
    """An entry with both `step:` and `action:` keys must be rejected."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["steps"] = [{"step": "x", "action": "y"}]
    with pytest.raises(ValidationError) as exc_info:
        ActionDefinition(**kwargs)
    assert "exactly one of" in str(exc_info.value)


def test_duplicate_step_names_rejected():
    """Two `step:` entries sharing a name must be rejected — SLURM keys
    job_name / job adoption on the entry name, so a collision would silently
    adopt the wrong job."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    step = {
        "step_type": StepType.SINGLETON,
        "container": "img:1",
        "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    }
    kwargs["steps"] = [
        {"step": "build", **step},
        {"step": "build", **step},
    ]
    with pytest.raises(ValidationError) as exc_info:
        ActionDefinition(**kwargs)
    assert "duplicate step name" in str(exc_info.value)


def test_duplicate_action_names_allowed():
    """Two `action:` entries may share a name — they run in-process keyed on the
    step index, not the name (e.g. two `register-index` actions in the
    host-reference workflows)."""
    from qiita_common.actions import ActionDefinition, WorkflowAction

    kwargs = _minimal_action_kwargs()
    kwargs["steps"] = [
        {
            "step": "build",
            "step_type": StepType.SINGLETON,
            "container": "img:1",
            "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
        },
        {"action": "register-index", "inputs": ["rype_index_meta"]},
        {"action": "register-index", "inputs": ["minimap2_index_meta"]},
    ]
    a = ActionDefinition(**kwargs)
    register = [s for s in a.steps if isinstance(s, WorkflowAction)]
    assert [s.name for s in register] == ["register-index", "register-index"]
    assert [s.inputs for s in register] == [["rype_index_meta"], ["minimap2_index_meta"]]


def test_step_entry_rejects_neither_key():
    """An entry with neither `step:` nor `action:` falls through to the
    discriminator and fails with a missing-discriminator error."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["steps"] = [{"name": "orphan"}]
    with pytest.raises(ValidationError):
        ActionDefinition(**kwargs)


def test_unknown_scope_rejected():
    """A scope string outside the Scope enum must be rejected at load time —
    YAML typos become deploy errors instead of runtime auth bypasses."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    # First entry is a real scope; second is the deliberate typo this test
    # exercises — keep the typo as a bare string so it doesn't have to
    # exist in the enum.
    kwargs["scopes"] = [Scope.FEATURE_MINT, "references:write"]
    with pytest.raises(ValidationError) as exc_info:
        ActionDefinition(**kwargs)
    assert "unknown scope" in str(exc_info.value)
    assert "references:write" in str(exc_info.value)


def test_duplicate_scopes_rejected():
    """Duplicate scopes are meaningless and rejected."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["scopes"] = [Scope.FEATURE_MINT, Scope.FEATURE_MINT]
    with pytest.raises(ValidationError) as exc_info:
        ActionDefinition(**kwargs)
    assert "duplicate" in str(exc_info.value)


def test_empty_scopes_allowed():
    """An action with no scopes is unusual but legal — audience and
    resource-ACL gates still apply."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["scopes"] = []
    a = ActionDefinition(**kwargs)
    assert a.scopes == []


def test_baseline_resources_walltime_must_be_positive():
    """walltime=PT0S is rejected by the field validator."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["steps"][0]["baseline_resources"]["walltime"] = "PT0S"
    with pytest.raises(ValidationError) as exc_info:
        ActionDefinition(**kwargs)
    assert "walltime must be positive" in str(exc_info.value)


def test_baseline_resources_rejects_zero_cpu_or_mem():
    """cpu and mem_gb are gt=0."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["steps"][0]["baseline_resources"]["cpu"] = 0
    with pytest.raises(ValidationError):
        ActionDefinition(**kwargs)

    kwargs2 = _minimal_action_kwargs()
    kwargs2["steps"][0]["baseline_resources"]["mem_gb"] = 0
    with pytest.raises(ValidationError):
        ActionDefinition(**kwargs2)


def test_baseline_resources_gpu_defaults_zero_and_rejects_negative():
    """gpu defaults to 0 and rejects negatives."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    # gpu omitted from baseline_resources
    a = ActionDefinition(**kwargs)
    assert a.steps[0].baseline_resources.gpu == 0

    kwargs2 = _minimal_action_kwargs()
    kwargs2["steps"][0]["baseline_resources"]["gpu"] = -1
    with pytest.raises(ValidationError):
        ActionDefinition(**kwargs2)


def test_target_kind_must_be_locked_value():
    """target_kind ∈ {study_prep, reference}."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["target_kind"] = "bogus"
    with pytest.raises(ValidationError):
        ActionDefinition(**kwargs)


def test_steps_required_non_empty():
    """At least one step entry is required."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["steps"] = []
    with pytest.raises(ValidationError):
        ActionDefinition(**kwargs)


def test_audience_human_roles_must_be_known_values():
    """human_roles values must be SystemRole members."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["audience"] = {"service": False, "human_roles": ["super_admin"]}
    with pytest.raises(ValidationError):
        ActionDefinition(**kwargs)


def test_action_ceiling_uses_iso8601_walltime():
    """ActionCeiling shares BaselineResources's walltime parsing."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["action_ceiling"] = {
        "cpu": 8,
        "mem_gb": 32,
        "walltime": "PT2H30M",
        "gpu": 1,
    }
    a = ActionDefinition(**kwargs)
    assert a.action_ceiling.walltime == timedelta(hours=2, minutes=30)
    assert a.action_ceiling.gpu == 1


def test_baseline_resources_flat_shape_validates():
    """The flat population — cpu/mem_gb/walltime/gpu — that every existing
    workflow uses must keep validating cleanly."""
    from qiita_common.actions import BaselineResources

    br = BaselineResources(cpu=4, mem_gb=8, walltime=timedelta(hours=1))
    assert br.cpu == 4
    assert br.mem_gb == 8
    assert br.walltime == timedelta(hours=1)
    assert br.from_step_output is None
    assert br.profiles is None


def test_baseline_resources_lookup_shape_validates():
    """The lookup population — from_step_output + profiles — used by the
    bcl-convert workflow."""
    from qiita_common.actions import BaselineResources, FlatBaselineResources

    br = BaselineResources(
        from_step_output="instrument_model",
        profiles={
            "Illumina NovaSeq 6000": FlatBaselineResources(
                cpu=16, mem_gb=240, walltime=timedelta(hours=3)
            ),
            "Illumina iSeq": FlatBaselineResources(cpu=16, mem_gb=16, walltime=timedelta(hours=3)),
        },
    )
    assert br.from_step_output == "instrument_model"
    assert set(br.profiles) == {"Illumina NovaSeq 6000", "Illumina iSeq"}
    assert br.cpu is None
    assert br.mem_gb is None


def test_baseline_resources_rejects_neither_population():
    """An empty `baseline_resources: {}` block must fail fast."""
    from qiita_common.actions import BaselineResources

    with pytest.raises(ValidationError) as exc:
        BaselineResources()
    assert "must populate either flat fields" in str(exc.value)


def test_baseline_resources_rejects_mixed_populations():
    """A YAML that sets both flat and lookup fields is ambiguous."""
    from qiita_common.actions import BaselineResources, FlatBaselineResources

    with pytest.raises(ValidationError) as exc:
        BaselineResources(
            cpu=4,
            mem_gb=8,
            walltime=timedelta(hours=1),
            from_step_output="instrument_model",
            profiles={"X": FlatBaselineResources(cpu=2, mem_gb=4, walltime=timedelta(minutes=10))},
        )
    assert "cannot mix flat fields" in str(exc.value)


def test_baseline_resources_flat_requires_all_three_required_fields():
    """Setting just one or two flat fields is a partial population and rejects."""
    from qiita_common.actions import BaselineResources

    with pytest.raises(ValidationError) as exc:
        BaselineResources(cpu=4)
    assert "requires cpu, mem_gb, and walltime" in str(exc.value)


def test_baseline_resources_lookup_requires_both_fields():
    """from_step_output without profiles (or vice versa) is an incomplete lookup."""
    from qiita_common.actions import BaselineResources

    with pytest.raises(ValidationError) as exc:
        BaselineResources(from_step_output="instrument_model")
    assert "requires both from_step_output and profiles" in str(exc.value)


def test_baseline_resources_lookup_rejects_empty_profiles():
    """An empty profiles dict is a footgun — no key can possibly match."""
    from qiita_common.actions import BaselineResources

    with pytest.raises(ValidationError) as exc:
        BaselineResources(from_step_output="instrument_model", profiles={})
    assert "profiles must be non-empty" in str(exc.value)


def test_baseline_resources_walltime_zero_rejected_via_direct_construction():
    """Zero walltime is rejected by the field validator at the BaselineResources
    level (the older test goes through ActionDefinition; this one exercises
    the model directly to confirm the field-level guard fires before the
    structural exactly-one-population check)."""
    from qiita_common.actions import BaselineResources

    with pytest.raises(ValidationError) as exc:
        BaselineResources(cpu=4, mem_gb=8, walltime=timedelta(0))
    assert "walltime must be positive" in str(exc.value)


def test_action_ceiling_does_not_accept_lookup_fields():
    """Ceilings are always flat — a single upper bound. Accepting lookup
    fields on ActionCeiling would be semantically meaningless."""
    from qiita_common.actions import ActionCeiling

    with pytest.raises(ValidationError):
        ActionCeiling(  # type: ignore[call-arg]
            from_step_output="instrument_model",
            profiles={},
        )


def test_status_fields_default_to_none():
    """success_status / failure_status / target_status are all optional —
    a workflow that doesn't track a resource lifecycle leaves them unset
    and the runner skips status PATCHes."""
    from qiita_common.actions import ActionDefinition

    a = ActionDefinition(**_minimal_action_kwargs())
    assert a.success_status is None
    assert a.failure_status is None
    assert a.steps[0].target_status is None


def test_workflow_status_round_trip():
    """ActionDefinition round-trips success_status / failure_status and
    per-entry target_status through model_dump / model_validate."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["success_status"] = "active"
    kwargs["failure_status"] = "failed"
    kwargs["steps"][0]["target_status"] = "hashing"
    kwargs["steps"].append(
        {
            "action": "mint-features",
            "target_status": "minting",
            "inputs": ["manifest"],
            "outputs": ["feature_map"],
        }
    )

    a = ActionDefinition(**kwargs)
    assert a.success_status == "active"
    assert a.failure_status == "failed"
    assert a.steps[0].target_status == "hashing"
    assert a.steps[1].target_status == "minting"

    rehydrated = ActionDefinition.model_validate(a.model_dump(mode="json"))
    assert rehydrated.success_status == "active"
    assert rehydrated.failure_status == "failed"
    assert rehydrated.steps[0].target_status == "hashing"
    assert rehydrated.steps[1].target_status == "minting"


def test_when_and_params_default_empty():
    """`when` defaults to None (entry always runs) and `params` to an empty
    dict (no scalar build params) — an entry that declares neither behaves
    exactly as before."""
    from qiita_common.actions import ActionDefinition

    a = ActionDefinition(**_minimal_action_kwargs())
    assert a.steps[0].when is None
    assert a.steps[0].params == {}


def test_when_and_params_round_trip():
    """A WorkflowStep's `when` (conditional gate) and `params` (action_context
    key -> Inputs field) and a WorkflowAction's `when` survive
    model_dump / model_validate — the path each takes into the DB (sync stores
    `model_dump(mode="json")`; runtime reconstructs `ActionDefinition`)."""
    from qiita_common.actions import ActionDefinition, WorkflowAction, WorkflowStep

    kwargs = _minimal_action_kwargs()
    # Turn the lone container step into a native build step carrying when/params.
    kwargs["steps"][0] = {
        "step": "build_rype_index",
        "step_type": StepType.SINGLETON,
        "module": "qiita_compute_orchestrator.jobs.build_rype_index",
        "inputs": ["reference_sequence_chunks"],
        "params": {"rype_w": "w"},
        "when": "build_rype",
        "outputs": ["rype_index_meta"],
        "baseline_resources": {"cpu": 4, "mem_gb": 32, "walltime": "PT2H"},
    }
    kwargs["steps"].append(
        {
            "action": "register-index",
            "inputs": ["rype_index_meta"],
            "when": "build_rype",
            "outputs": [],
        }
    )

    a = ActionDefinition(**kwargs)
    assert isinstance(a.steps[0], WorkflowStep)
    assert a.steps[0].when == "build_rype"
    assert a.steps[0].params == {"rype_w": "w"}
    assert isinstance(a.steps[1], WorkflowAction)
    assert a.steps[1].when == "build_rype"

    rehydrated = ActionDefinition.model_validate(a.model_dump(mode="json"))
    assert rehydrated.steps[0].when == "build_rype"
    assert rehydrated.steps[0].params == {"rype_w": "w"}
    assert rehydrated.steps[1].when == "build_rype"


def test_status_fields_reject_blank_strings():
    """min_length=1 — passing an empty string for any status field is a
    sentinel-versus-empty smell, rejected so YAML authors notice."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["success_status"] = ""
    with pytest.raises(ValidationError):
        ActionDefinition(**kwargs)

    kwargs2 = _minimal_action_kwargs()
    kwargs2["steps"][0]["target_status"] = ""
    with pytest.raises(ValidationError):
        ActionDefinition(**kwargs2)


# --- WorkflowStep runtime-selection validator -----------------------------
# Shape-only: every step must declare exactly one of `container:` or
# `module:`. Prefix validation on `module` is enforced separately (not
# in this validator).


def test_workflow_step_native_module_form_validates():
    """A step with `module` set (and no container) validates cleanly."""
    from qiita_common.actions import ActionDefinition, WorkflowStep

    kwargs = _minimal_action_kwargs()
    kwargs["steps"][0] = {
        "step": "fastq",
        "step_type": StepType.SINGLETON,
        "module": FASTQ_TO_PARQUET_MODULE,
        "baseline_resources": {"cpu": 4, "mem_gb": 8, "walltime": "PT1H"},
    }
    a = ActionDefinition(**kwargs)
    assert isinstance(a.steps[0], WorkflowStep)
    assert a.steps[0].container is None
    assert a.steps[0].module == FASTQ_TO_PARQUET_MODULE


def test_workflow_step_rejects_both_container_and_module():
    """A step with both `container` and `module` is rejected — runtime
    must be unambiguous."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["steps"][0]["module"] = "qiita_compute_orchestrator.jobs.x"
    with pytest.raises(ValidationError) as exc_info:
        ActionDefinition(**kwargs)
    assert "exactly one" in str(exc_info.value)


def test_workflow_step_rejects_neither_container_nor_module():
    """A step with neither runtime field is rejected."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    del kwargs["steps"][0]["container"]
    with pytest.raises(ValidationError) as exc_info:
        ActionDefinition(**kwargs)
    assert "exactly one" in str(exc_info.value)


def test_workflow_step_rejects_entrypoint_without_container():
    """`entrypoint` overrides a container's ENTRYPOINT — it's meaningless
    for native steps, which dispatch via `python -m`."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["steps"][0] = {
        "step": "fastq",
        "step_type": StepType.SINGLETON,
        "module": FASTQ_TO_PARQUET_MODULE,
        "entrypoint": "/usr/local/bin/run",
        "baseline_resources": {"cpu": 4, "mem_gb": 8, "walltime": "PT1H"},
    }
    with pytest.raises(ValidationError) as exc_info:
        ActionDefinition(**kwargs)
    assert "entrypoint" in str(exc_info.value).lower()


def test_workflow_step_entrypoint_with_container_ok():
    """Container steps may set `entrypoint` to override the image's
    default ENTRYPOINT."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["steps"][0]["entrypoint"] = "/usr/local/bin/qiita-hash"
    a = ActionDefinition(**kwargs)
    assert a.steps[0].entrypoint == "/usr/local/bin/qiita-hash"


def test_native_module_prefix_constant_value():
    """NATIVE_MODULE_PREFIX is the single source of truth for the allowed
    module path; CP sync, CO boot scan, and the wire validator all import it."""
    from qiita_common.actions import NATIVE_MODULE_PREFIX

    assert NATIVE_MODULE_PREFIX == "qiita_compute_orchestrator.jobs."


# ---------------------------------------------------------------------------
# derived_inputs (container-only PATH_DERIVED artifacts)
# ---------------------------------------------------------------------------


def test_workflow_step_derived_inputs_on_container_ok():
    """The happy path: a container step names an operator-provisioned artifact
    under PATH_DERIVED, keyed by the env var its entrypoint reads."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["steps"][0]["derived_inputs"] = {"QIITA_CHECKM_DB": "checkm_data"}
    a = ActionDefinition(**kwargs)
    assert a.steps[0].derived_inputs == {"QIITA_CHECKM_DB": "checkm_data"}


def test_workflow_step_rejects_derived_inputs_on_native_step():
    """`derived_inputs` is a bind + env-forward into a container. A native step
    runs in the orchestrator's own env and reads PATH_DERIVED from its settings,
    so declaring it there is a contract error, not a no-op."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["steps"][0] = {
        "step": "fastq",
        "step_type": StepType.SINGLETON,
        "module": FASTQ_TO_PARQUET_MODULE,
        "derived_inputs": {"QIITA_CHECKM_DB": "checkm_data"},
        "baseline_resources": {"cpu": 4, "mem_gb": 8, "walltime": "PT1H"},
    }
    with pytest.raises(ValidationError) as exc_info:
        ActionDefinition(**kwargs)
    assert "derived_inputs" in str(exc_info.value)


def test_workflow_step_rejects_absolute_derived_input():
    """Values are relative to the orchestrator's PATH_DERIVED. An absolute path
    would let a workflow name any host directory for the orchestrator to bind
    into a container."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["steps"][0]["derived_inputs"] = {"QIITA_CHECKM_DB": "/etc"}
    with pytest.raises(ValidationError) as exc_info:
        ActionDefinition(**kwargs)
    assert "relative" in str(exc_info.value)


def test_workflow_step_rejects_traversing_derived_input():
    """Same containment rule, via `..` rather than a leading slash."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["steps"][0]["derived_inputs"] = {"QIITA_CHECKM_DB": "../../etc"}
    with pytest.raises(ValidationError) as exc_info:
        ActionDefinition(**kwargs)
    assert "traverse" in str(exc_info.value)


def test_workflow_step_rejects_non_env_name_derived_input_key():
    """The key is interpolated into an `--env K=V` apptainer argument, so a
    stray `=` or space would corrupt the argument rather than fail."""
    from qiita_common.actions import ActionDefinition

    kwargs = _minimal_action_kwargs()
    kwargs["steps"][0]["derived_inputs"] = {"not a var": "checkm_data"}
    with pytest.raises(ValidationError) as exc_info:
        ActionDefinition(**kwargs)
    assert "env var name" in str(exc_info.value)
