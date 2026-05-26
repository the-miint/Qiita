"""Unit tests for qiita_compute_orchestrator.slurm.

Covers the two pure pieces of the SLURM backend:

- payload.build_job_submit_payload: input shape => slurmrestd dict.
  Asserts the exact JSON shape so a future schema bump in slurmrestd
  forces an explicit test update.
- verify.verify_container_output: walks an $QIITA_OUTPUT_PATH dir and
  reports every container-contract violation. Each gate has its own
  test so a regression in one doesn't mask another.

The HTTP client (slurm/client.py) and SlurmBackend.run_step (the wiring)
are tested separately in test_slurm_client.py and test_slurm_backend.py.
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest
from qiita_common.actions import BaselineResources
from qiita_common.testing.native_steps import FASTQ_TO_PARQUET_MODULE

from qiita_compute_orchestrator.slurm import (
    VerificationFailure,
    build_job_submit_payload,
    verify_container_output,
)

# ============================================================================
# build_job_submit_payload
# ============================================================================


@pytest.fixture
def baseline() -> BaselineResources:
    return BaselineResources(cpu=4, mem_gb=8, walltime=timedelta(hours=2))


@pytest.fixture
def common_kwargs(baseline, tmp_path):
    return {
        "step_name": "hash",
        "work_ticket_idx": 42,
        "container": "/opt/qiita/containers/hash:1.0.0.sif",
        "entrypoint": "/usr/local/bin/hash",
        "baseline_resources": baseline,
        "input_path": tmp_path / "in",
        "output_path": tmp_path / "out",
        "workspace": tmp_path / "ws",
        "log_stdout": tmp_path / "logs" / "stdout",
        "log_stderr": tmp_path / "logs" / "stderr",
        "partition": "qiita",
        "account": "qiita-prod",
    }


def test_payload_top_level_shape(common_kwargs):
    payload = build_job_submit_payload(**common_kwargs)
    assert set(payload.keys()) == {"script", "job"}
    assert set(payload["job"].keys()) == {
        "name",
        "account",
        "partition",
        "current_working_directory",
        "environment",
        "memory_per_node",
        "tasks",
        "cpus_per_task",
        "time_limit",
        "standard_output",
        "standard_error",
    }


def test_payload_resource_fields(common_kwargs, baseline):
    payload = build_job_submit_payload(**common_kwargs)
    assert payload["job"]["cpus_per_task"] == baseline.cpu
    # memory is MB on slurmrestd; gb*1024.
    assert payload["job"]["memory_per_node"] == {
        "number": baseline.mem_gb * 1024,
        "set": True,
        "infinite": False,
    }
    # 2-hour walltime => 120 minutes.
    assert payload["job"]["time_limit"] == {"number": 120, "set": True, "infinite": False}


def test_payload_environment_includes_qiita_paths(common_kwargs):
    payload = build_job_submit_payload(**common_kwargs)
    env = dict(item.split("=", 1) for item in payload["job"]["environment"])
    assert env["QIITA_INPUT_PATH"] == str(common_kwargs["input_path"])
    assert env["QIITA_OUTPUT_PATH"] == str(common_kwargs["output_path"])
    assert env["QIITA_WORK_TICKET_IDX"] == str(common_kwargs["work_ticket_idx"])


def test_payload_environment_sets_home_to_workspace(common_kwargs):
    """HOME must point at the per-job workspace so DuckDB+miint's
    extension cache lands inside the cleaned-up workspace tree
    instead of failing on a no-HOME compute node."""
    payload = build_job_submit_payload(**common_kwargs)
    env = dict(item.split("=", 1) for item in payload["job"]["environment"])
    assert env["HOME"] == str(common_kwargs["workspace"])


def test_payload_environment_extra_env_merged(common_kwargs):
    common_kwargs["extra_env"] = {"FOO": "bar", "BAZ": "qux"}
    payload = build_job_submit_payload(**common_kwargs)
    env = dict(item.split("=", 1) for item in payload["job"]["environment"])
    assert env["FOO"] == "bar"
    assert env["BAZ"] == "qux"
    # qiita paths still present.
    assert "QIITA_INPUT_PATH" in env


def test_payload_environment_is_sorted_for_determinism(common_kwargs):
    common_kwargs["extra_env"] = {"Z_LAST": "z", "A_FIRST": "a"}
    payload = build_job_submit_payload(**common_kwargs)
    keys = [item.split("=", 1)[0] for item in payload["job"]["environment"]]
    assert keys == sorted(keys)


def test_payload_job_name_includes_step_and_ticket(common_kwargs):
    payload = build_job_submit_payload(**common_kwargs)
    assert payload["job"]["name"] == "qiita-hash-wt42"


def test_payload_script_invokes_apptainer_with_bind_mounts(common_kwargs):
    payload = build_job_submit_payload(**common_kwargs)
    script = payload["script"]
    assert script.startswith("#!/bin/bash\n")
    assert "set -euo pipefail" in script
    assert "apptainer exec --containall" in script
    assert common_kwargs["container"] in script
    # bind mounts for input + output paths
    in_path = str(common_kwargs["input_path"])
    out_path = str(common_kwargs["output_path"])
    assert f"--bind {in_path}:{in_path}" in script
    assert f"--bind {out_path}:{out_path}" in script
    # entrypoint appended at the end
    assert script.rstrip().endswith(common_kwargs["entrypoint"])


def test_payload_script_without_entrypoint(common_kwargs):
    common_kwargs["entrypoint"] = None
    payload = build_job_submit_payload(**common_kwargs)
    # No trailing entrypoint — container's own ENTRYPOINT runs.
    assert payload["script"].rstrip().endswith(common_kwargs["container"])


def test_payload_walltime_rounds_up(common_kwargs):
    """A 90-second walltime must request 2 minutes (ceil), not 1 — under-
    allocating walltime causes spurious TIMEOUT failures."""
    common_kwargs["baseline_resources"] = BaselineResources(
        cpu=1, mem_gb=1, walltime=timedelta(seconds=90)
    )
    payload = build_job_submit_payload(**common_kwargs)
    assert payload["job"]["time_limit"]["number"] == 2


def test_payload_walltime_floor_one_minute(common_kwargs):
    """A walltime under 1 minute must round up to 1 — slurmrestd
    rejects 0-minute time_limit."""
    common_kwargs["baseline_resources"] = BaselineResources(
        cpu=1, mem_gb=1, walltime=timedelta(seconds=15)
    )
    payload = build_job_submit_payload(**common_kwargs)
    assert payload["job"]["time_limit"]["number"] == 1


def test_payload_zero_walltime_rejected(common_kwargs):
    """BaselineResources already enforces walltime > 0; this is
    defense-in-depth on the builder side. A test against the value
    0 directly via timedelta to make sure the builder doesn't crash
    silently."""
    with pytest.raises(ValueError, match="walltime must be positive"):
        # Bypass BaselineResources's own validator — payload builder
        # must still refuse a zero on its own.
        build_job_submit_payload(
            **{
                **common_kwargs,
                "baseline_resources": BaselineResources.model_construct(
                    cpu=1, mem_gb=1, walltime=timedelta(0)
                ),
            }
        )


# ----------------------------------------------------------------------------
# Native (`module:`) script branch
# ----------------------------------------------------------------------------


@pytest.fixture
def native_kwargs(baseline, tmp_path):
    """Same shape as common_kwargs but the runtime is `module:` instead
    of `container:`. The non-runtime fields stay identical so the test
    can isolate the runtime difference."""
    return {
        "step_name": "fastq",
        "work_ticket_idx": 42,
        "container": None,
        "module": FASTQ_TO_PARQUET_MODULE,
        "entrypoint": None,
        "baseline_resources": baseline,
        "input_path": tmp_path / "in",
        "output_path": tmp_path / "out",
        "workspace": tmp_path / "ws",
        "log_stdout": tmp_path / "logs" / "stdout",
        "log_stderr": tmp_path / "logs" / "stderr",
        "partition": "qiita",
        "account": "qiita-prod",
    }


def test_native_payload_script_invokes_python_m_launcher(native_kwargs):
    """Native step's SBATCH script must call the shared launcher with
    the short job name (NATIVE_MODULE_PREFIX stripped) and must NOT
    contain `apptainer exec` (no container to bind into).

    Default `native_python` is "python" (bare interpreter on PATH) —
    sites whose compute nodes lack the orchestrator on PATH override
    via SLURM_NATIVE_PYTHON; see
    test_native_payload_script_uses_native_python_override below."""
    payload = build_job_submit_payload(**native_kwargs)
    script = payload["script"]
    assert "srun python -m qiita_compute_orchestrator.jobs --job fastq_to_parquet" in script
    assert "apptainer exec" not in script
    assert script.startswith("#!/bin/bash\nset -euo pipefail\n")


def test_native_payload_script_uses_native_python_override(native_kwargs):
    """When the orchestrator threads a non-default native_python through
    (sites whose compute nodes don't have a python on PATH with
    qiita_compute_orchestrator installed), the SBATCH script must call
    `srun <native_python> -m ...` — not bare `srun python`."""
    native_kwargs["native_python"] = (
        "/home/qiita/qiita-miint/qiita-compute-orchestrator/.venv/bin/python"
    )
    payload = build_job_submit_payload(**native_kwargs)
    script = payload["script"]
    assert (
        "srun /home/qiita/qiita-miint/qiita-compute-orchestrator/.venv/bin/python"
        " -m qiita_compute_orchestrator.jobs --job fastq_to_parquet"
    ) in script
    # And the default interpreter token isn't present.
    assert "srun python -m" not in script


def test_native_payload_has_no_bind_mounts(native_kwargs):
    """Native dispatch runs in the orchestrator's installed Python env
    on the compute node; nothing to bind into. The script must not
    carry --bind flags."""
    payload = build_job_submit_payload(**native_kwargs)
    assert "--bind" not in payload["script"]


def test_native_payload_keeps_qiita_env_vars(native_kwargs):
    """The launcher reads $QIITA_INPUT_PATH / $QIITA_OUTPUT_PATH from
    environment just like the container does — both runtimes share the
    same env contract."""
    payload = build_job_submit_payload(**native_kwargs)
    env = dict(item.split("=", 1) for item in payload["job"]["environment"])
    assert env["QIITA_INPUT_PATH"] == str(native_kwargs["input_path"])
    assert env["QIITA_OUTPUT_PATH"] == str(native_kwargs["output_path"])
    assert env["QIITA_WORK_TICKET_IDX"] == str(native_kwargs["work_ticket_idx"])


def test_native_payload_keeps_job_metadata(native_kwargs, baseline):
    """The slurmrestd `job` block (resources, name, partition, etc.) is
    runtime-independent — switching from container to module must not
    perturb the scheduler-visible metadata."""
    payload = build_job_submit_payload(**native_kwargs)
    job = payload["job"]
    assert job["name"] == "qiita-fastq-wt42"
    assert job["account"] == "qiita-prod"
    assert job["partition"] == "qiita"
    assert job["cpus_per_task"] == baseline.cpu
    assert job["memory_per_node"]["number"] == baseline.mem_gb * 1024


def test_payload_rejects_both_container_and_module(common_kwargs):
    """Exactly-one runtime — both set is rejected by the builder. The
    wire validator catches this upstream; the builder's own check
    protects direct callers (tests, future programmatic submission)."""
    common_kwargs["module"] = FASTQ_TO_PARQUET_MODULE
    with pytest.raises(ValueError, match="exactly one"):
        build_job_submit_payload(**common_kwargs)


def test_payload_rejects_neither_container_nor_module(common_kwargs):
    common_kwargs["container"] = None
    with pytest.raises(ValueError, match="exactly one"):
        build_job_submit_payload(**common_kwargs)


def test_payload_rejects_empty_partition(common_kwargs):
    common_kwargs["partition"] = ""
    with pytest.raises(ValueError, match="partition"):
        build_job_submit_payload(**common_kwargs)


def test_payload_rejects_empty_account(common_kwargs):
    common_kwargs["account"] = ""
    with pytest.raises(ValueError, match="account"):
        build_job_submit_payload(**common_kwargs)


def test_payload_rejects_empty_container(common_kwargs):
    common_kwargs["container"] = ""
    with pytest.raises(ValueError, match="container"):
        build_job_submit_payload(**common_kwargs)


def test_payload_log_paths_round_trip(common_kwargs):
    payload = build_job_submit_payload(**common_kwargs)
    assert payload["job"]["standard_output"] == str(common_kwargs["log_stdout"])
    assert payload["job"]["standard_error"] == str(common_kwargs["log_stderr"])
    assert payload["job"]["current_working_directory"] == str(common_kwargs["workspace"])


# ============================================================================
# verify_container_output
# ============================================================================


def _make_output(tmp_path: Path, files: dict[str, bytes], manifest: dict | None) -> Path:
    """Build an output dir with the given file contents and manifest.
    Sets every file mode to 0o440 by default — tests that want a
    different mode override afterwards. The manifest gets a default
    `outputs: {}` if not provided so well-formed-shape tests don't have
    to repeat it; tests exercising the `outputs` contract pass it
    explicitly."""
    out = tmp_path / "out"
    out.mkdir()
    for relative, content in files.items():
        full = out / relative
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_bytes(content)
        full.chmod(0o440)
    if manifest is not None:
        if "outputs" not in manifest:
            manifest = {**manifest, "outputs": {}}
        manifest_path = out / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))
        manifest_path.chmod(0o440)
    return out


def test_verify_happy_path(tmp_path):
    out = _make_output(
        tmp_path,
        {"output.parquet": b"abc", "logs/run.log": b"hi\n"},
        manifest={
            "files": [
                {"path": "output.parquet", "size_bytes": 3},
                {"path": "logs/run.log", "size_bytes": 3},
            ]
        },
    )
    failures = verify_container_output(out)
    assert failures == []


def test_verify_output_dir_missing(tmp_path):
    failures = verify_container_output(tmp_path / "does-not-exist")
    assert len(failures) == 1
    assert "does not exist" in failures[0].reason


def test_verify_output_path_is_file(tmp_path):
    p = tmp_path / "not-a-dir"
    p.write_text("hi")
    failures = verify_container_output(p)
    assert len(failures) == 1
    assert "not a directory" in failures[0].reason


def test_verify_manifest_missing(tmp_path):
    out = _make_output(tmp_path, {"output.parquet": b"abc"}, manifest=None)
    failures = verify_container_output(out)
    assert len(failures) == 1
    assert "manifest.json missing" in failures[0].reason


def test_verify_manifest_not_json(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    (out / "manifest.json").write_text("not-json{{")
    (out / "manifest.json").chmod(0o440)
    failures = verify_container_output(out)
    assert any("not valid JSON" in f.reason for f in failures)


def test_verify_manifest_missing_files_key(tmp_path):
    out = tmp_path / "out"
    out.mkdir()
    (out / "manifest.json").write_text(json.dumps({"wrong_key": []}))
    (out / "manifest.json").chmod(0o440)
    failures = verify_container_output(out)
    assert any("`files` key" in f.reason for f in failures)


def test_verify_declared_file_missing(tmp_path):
    out = _make_output(
        tmp_path,
        {},
        manifest={"files": [{"path": "missing.parquet", "size_bytes": 100}]},
    )
    failures = verify_container_output(out)
    assert any("declared output file missing" in f.reason for f in failures)


def test_verify_size_mismatch(tmp_path):
    out = _make_output(
        tmp_path,
        {"output.parquet": b"abc"},  # 3 bytes
        manifest={"files": [{"path": "output.parquet", "size_bytes": 999}]},
    )
    failures = verify_container_output(out)
    assert any("mismatches actual file size" in f.reason for f in failures)


def test_verify_extra_file_not_in_manifest(tmp_path):
    out = _make_output(
        tmp_path,
        {
            "output.parquet": b"abc",
            "stray.txt": b"not in manifest",  # not declared
        },
        manifest={"files": [{"path": "output.parquet", "size_bytes": 3}]},
    )
    failures = verify_container_output(out)
    assert any("not listed in manifest" in f.reason for f in failures)


def test_verify_wrong_file_mode(tmp_path):
    out = _make_output(
        tmp_path,
        {"output.parquet": b"abc"},
        manifest={"files": [{"path": "output.parquet", "size_bytes": 3}]},
    )
    # Drop output to 0o644 — explicit contract violation.
    (out / "output.parquet").chmod(0o644)
    failures = verify_container_output(out)
    assert any("wrong mode" in f.reason for f in failures)


def test_verify_path_traversal_rejected(tmp_path):
    """A manifest entry with path="../escape" must be rejected — the
    container shouldn't be able to claim files outside its output dir."""
    out = tmp_path / "out"
    out.mkdir()
    # Place an escape target outside out/ to make sure resolution would
    # actually find a file (otherwise the test would conflate with
    # missing-file).
    (tmp_path / "escape").write_bytes(b"x")
    (out / "manifest.json").write_text(
        json.dumps(
            {
                "files": [{"path": "../escape", "size_bytes": 1}],
                "outputs": {},
            }
        )
    )
    (out / "manifest.json").chmod(0o440)
    failures = verify_container_output(out)
    assert any("escapes" in f.reason for f in failures)


def test_verify_multiple_failures_reported(tmp_path):
    """Three independent issues => three failures, not just the first."""
    out = _make_output(
        tmp_path,
        {
            "wrong_size.parquet": b"abc",  # declared 999, actual 3
            "stray.txt": b"x",  # not in manifest
        },
        manifest={
            "files": [
                {"path": "wrong_size.parquet", "size_bytes": 999},
                {"path": "missing.parquet", "size_bytes": 1},
            ]
        },
    )
    # plus drop wrong_size's mode.
    (out / "wrong_size.parquet").chmod(0o600)
    failures = verify_container_output(out)
    reasons = [f.reason for f in failures]
    # Each independent kind of failure shows up.
    assert any("declared output file missing" in r for r in reasons)
    assert any("mismatches actual file size" in r for r in reasons)
    assert any("not listed in manifest" in r for r in reasons)


def test_verification_failure_is_frozen():
    f = VerificationFailure(reason="x", detail="y")
    with pytest.raises(Exception):  # noqa: BLE001 — FrozenInstanceError
        f.reason = "z"  # type: ignore[misc]


# ============================================================================
# manifest.json `outputs` contract
# ============================================================================


def test_verify_outputs_missing_is_a_failure(tmp_path):
    """No `outputs` key in manifest.json => fail-fast contract violation."""
    out = tmp_path / "out"
    out.mkdir()
    (out / "manifest.json").write_text(json.dumps({"files": []}))
    (out / "manifest.json").chmod(0o440)
    failures = verify_container_output(out)
    assert any("outputs`" in f.reason for f in failures)


def test_verify_outputs_must_be_object(tmp_path):
    out = _make_output(
        tmp_path,
        {},
        manifest={"files": [], "outputs": [1, 2, 3]},  # array, not object
    )
    failures = verify_container_output(out)
    assert any("outputs` must be an object" in f.reason for f in failures)


def test_verify_outputs_value_must_be_string(tmp_path):
    out = _make_output(
        tmp_path,
        {"hash.parquet": b"abc"},
        manifest={
            "files": [{"path": "hash.parquet", "size_bytes": 3}],
            "outputs": {"manifest": 12345},  # int instead of string
        },
    )
    failures = verify_container_output(out)
    assert any("must be a string path" in f.reason for f in failures)


def test_verify_outputs_path_traversal_rejected(tmp_path):
    out = _make_output(
        tmp_path,
        {},
        manifest={"files": [], "outputs": {"manifest": "../escape"}},
    )
    failures = verify_container_output(out)
    assert any("outputs.manifest` escapes" in f.reason for f in failures), [
        f.reason for f in failures
    ]


def test_verify_outputs_must_point_at_existing_path(tmp_path):
    out = _make_output(
        tmp_path,
        {},
        manifest={"files": [], "outputs": {"manifest": "missing.parquet"}},
    )
    failures = verify_container_output(out)
    assert any("points at missing path" in f.reason for f in failures)


def test_verify_outputs_dot_means_output_dir(tmp_path):
    """A step whose output IS the directory uses `.` for the path."""
    out = _make_output(
        tmp_path,
        {},
        manifest={"files": [], "outputs": {"staging_dir": "."}},
    )
    assert verify_container_output(out) == []


# ============================================================================
# parse_outputs_map
# ============================================================================


def test_parse_outputs_map_resolves_paths(tmp_path):
    from qiita_compute_orchestrator.slurm import parse_outputs_map

    out = _make_output(
        tmp_path,
        {"manifest.parquet": b"abc"},
        manifest={
            "files": [{"path": "manifest.parquet", "size_bytes": 3}],
            "outputs": {"manifest": "manifest.parquet"},
        },
    )
    outputs = parse_outputs_map(out)
    assert set(outputs.keys()) == {"manifest"}
    assert outputs["manifest"] == (out / "manifest.parquet").resolve()


def test_parse_outputs_map_dot_resolves_to_output_dir(tmp_path):
    from qiita_compute_orchestrator.slurm import parse_outputs_map

    out = _make_output(
        tmp_path,
        {},
        manifest={"files": [], "outputs": {"staging_dir": "."}},
    )
    outputs = parse_outputs_map(out)
    assert outputs["staging_dir"] == out.resolve()
