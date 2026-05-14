"""Unit tests for the shared SLURM launcher
`qiita_compute_orchestrator.jobs.__main__`.

The launcher is invoked as `python -m qiita_compute_orchestrator.jobs
--job <name>` from the SBATCH script SlurmBackend builds. Tests below
call `main()` directly with the same env-var contract so we exercise
the path without a real SLURM submission.

`_install_stub` injects a fake job module into `sys.modules` so the
launcher's `run_native_job` import call resolves to a controlled
behavior per test.
"""

from __future__ import annotations

import json
import stat
import sys
import types
from pathlib import Path

import pytest
from pydantic import BaseModel

from qiita_compute_orchestrator.jobs import RESERVED_INPUT_KEYS
from qiita_compute_orchestrator.jobs.__main__ import _flatten_params, main


class _Inputs(BaseModel):
    fastq_path: Path
    reference_idx: int
    work_ticket_idx: int


def _install_stub(monkeypatch, *, short_name: str, execute_fn) -> None:
    full = f"qiita_compute_orchestrator.jobs.{short_name}"
    mod = types.ModuleType(full)
    mod.Inputs = _Inputs
    mod.execute = execute_fn
    monkeypatch.setitem(sys.modules, full, mod)


@pytest.fixture
def io_dirs(tmp_path, monkeypatch):
    """Set up the env-var contract the launcher reads: an input dir
    holding params.json and an output dir for outputs + manifest."""
    input_path = tmp_path / "input"
    output_path = tmp_path / "output"
    input_path.mkdir()
    output_path.mkdir()
    monkeypatch.setenv("QIITA_INPUT_PATH", str(input_path))
    monkeypatch.setenv("QIITA_OUTPUT_PATH", str(output_path))
    return input_path, output_path


def _write_params(input_path: Path, *, fastq_path: str, reference_idx: int, work_ticket_idx: int):
    from qiita_compute_orchestrator.slurm.contract import JOB_PARAMS_FILENAME, JobParams

    (input_path / JOB_PARAMS_FILENAME).write_text(
        JobParams(
            step_name="fastq",
            scope_target={"kind": "reference", "reference_idx": reference_idx},
            work_ticket_idx=work_ticket_idx,
            inputs={"fastq_path": fastq_path},
            output_path=str(input_path.parent / "output"),
        ).model_dump_json()
    )


def test_main_writes_manifest_and_chmods_on_success(monkeypatch, io_dirs):
    """Happy path: a stub job produces an output file. The launcher
    chmods every file to 0o440, writes manifest.json with the right
    shape (files + outputs), and returns 0."""
    input_path, output_path = io_dirs

    async def execute(inputs, workspace):
        out = workspace / "manifest.parquet"
        out.write_bytes(b"FAKE-PARQUET-BYTES")
        return {"manifest": out}

    _install_stub(monkeypatch, short_name="happy", execute_fn=execute)
    _write_params(input_path, fastq_path="/tmp/x.fa", reference_idx=7, work_ticket_idx=99)

    rc = main(["--job", "happy"])
    assert rc == 0

    manifest = json.loads((output_path / "manifest.json").read_text())
    # `files` array: one entry per output file, with size_bytes.
    assert manifest["files"] == [
        {"path": "manifest.parquet", "size_bytes": len(b"FAKE-PARQUET-BYTES")}
    ]
    # `outputs` map: YAML step output name → relative path.
    assert manifest["outputs"] == {"manifest": "manifest.parquet"}

    # Output file + manifest both chmod 0o440 (verifier requirement).
    for p in (output_path / "manifest.parquet", output_path / "manifest.json"):
        mode = stat.S_IMODE(p.stat().st_mode)
        assert mode == 0o440, f"{p.name}: expected 0o440, got {mode:#o}"


def test_main_handles_directory_output(monkeypatch, io_dirs):
    """When a job declares a directory as an output (e.g. `staging_dir`),
    the manifest records the output as "." and the `files` listing
    enumerates the directory's contents."""
    input_path, output_path = io_dirs

    async def execute(inputs, workspace):
        # Job writes directly into workspace and returns workspace
        # itself as the output — `staging_dir`-style.
        (workspace / "a.parquet").write_bytes(b"A")
        (workspace / "b.parquet").write_bytes(b"BB")
        return {"staging_dir": workspace}

    _install_stub(monkeypatch, short_name="dir_out", execute_fn=execute)
    _write_params(input_path, fastq_path="/tmp/x.fa", reference_idx=1, work_ticket_idx=1)

    rc = main(["--job", "dir_out"])
    assert rc == 0

    manifest = json.loads((output_path / "manifest.json").read_text())
    assert manifest["outputs"] == {"staging_dir": "."}
    paths = sorted(entry["path"] for entry in manifest["files"])
    assert paths == ["a.parquet", "b.parquet"]


def test_flatten_params_merges_scalars_and_inputs():
    """Happy path: framework scalars and step inputs land in one flat
    dict ready for Inputs.model_validate."""
    from qiita_compute_orchestrator.slurm.contract import JobParams

    params = JobParams(
        step_name="fastq",
        scope_target={"kind": "reference", "reference_idx": 7},
        work_ticket_idx=99,
        inputs={"fastq_path": "/data/in.fa"},
        output_path="/scratch/out",  # ignored
    )
    flat = _flatten_params(params)
    assert flat == {"fastq_path": "/data/in.fa", "reference_idx": 7, "work_ticket_idx": 99}


@pytest.mark.parametrize("reserved_key", sorted(RESERVED_INPUT_KEYS))
def test_flatten_params_rejects_reserved_key_collision(reserved_key):
    """If the workflow YAML's `inputs:` list happens to declare a name
    that matches a framework scalar (any name in RESERVED_INPUT_KEYS),
    the launcher refuses with a typed BackendFailure(CONTRACT_VIOLATION)
    rather than silently shadowing the work-ticket value.

    Parameterized over RESERVED_INPUT_KEYS so adding a fourth scalar
    doesn't need a corresponding hardcoded-string test update.
    Symmetric with LocalBackend's path — both go through the shared
    `flatten_native_inputs` helper in jobs/__init__.py."""
    from qiita_common.backend_failure import BackendFailure, FailureKind

    from qiita_compute_orchestrator.slurm.contract import JobParams

    params = JobParams(
        step_name="fastq",
        scope_target={"kind": "reference", "reference_idx": 7},
        work_ticket_idx=99,
        inputs={reserved_key: "/data/in.fa"},  # accidental collision
        output_path="/scratch/out",
    )
    with pytest.raises(BackendFailure) as ei:
        _flatten_params(params)
    assert ei.value.kind is FailureKind.CONTRACT_VIOLATION
    # The failure carries the YAML step name, not the module path.
    assert ei.value.step_name == "fastq"
    assert reserved_key in ei.value.reason


@pytest.mark.parametrize("reserved_key", sorted(RESERVED_INPUT_KEYS))
def test_main_returns_1_on_reserved_key_collision(reserved_key, monkeypatch, io_dirs, capsys):
    """End-to-end: a params.json whose `inputs:` map collides with a
    framework-reserved scalar makes main() exit 1 with a structured
    CONTRACT_VIOLATION JSON line on stderr. The check fires in
    `_flatten_params` (delegating to `flatten_native_inputs`) before
    run_native_job is reached.

    Parameterized over RESERVED_INPUT_KEYS — the set drives coverage."""
    input_path, _ = io_dirs

    async def execute(inputs, workspace):
        raise RuntimeError("should not reach execute()")

    _install_stub(monkeypatch, short_name="collision", execute_fn=execute)
    from qiita_compute_orchestrator.slurm.contract import JOB_PARAMS_FILENAME, JobParams

    # The inputs map shadows the framework scalar — the helper rejects.
    (input_path / JOB_PARAMS_FILENAME).write_text(
        JobParams(
            step_name="fastq",
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=1,
            inputs={reserved_key: "evil-string"},
            output_path=str(input_path.parent / "output"),
        ).model_dump_json()
    )

    rc = main(["--job", "collision"])
    assert rc == 1

    captured = capsys.readouterr()
    err = json.loads(captured.err)
    assert err["kind"] == "contract_violation"
    # Stderr carries the YAML step name from params.json, not the module path.
    assert err["step_name"] == "fastq"
    assert reserved_key in err["reason"]


def test_main_returns_1_on_post_success_manifest_failure(monkeypatch, io_dirs, capsys):
    """If execute() succeeds but the launcher can't honor the output
    contract (chmod fails, manifest write fails, filesystem race,
    etc.), the launcher must emit the structured CONTRACT_VIOLATION
    stderr line — same shape the verifier uses for container-side
    manifest failures — instead of letting the OSError leak as a
    raw traceback."""
    from qiita_compute_orchestrator.jobs import __main__ as launcher

    input_path, _ = io_dirs

    async def execute(inputs, workspace):
        # Produce a real output so the chmod/manifest path is reached.
        out = workspace / "result.parquet"
        out.write_bytes(b"DATA")
        return {"result": out}

    _install_stub(monkeypatch, short_name="post_fail", execute_fn=execute)
    _write_params(input_path, fastq_path="/tmp/x.fa", reference_idx=1, work_ticket_idx=1)

    # Force the post-success manifest write to fail. The exception
    # type doesn't matter — the launcher's broad except is what's
    # under test.
    def boom(*args, **kwargs):
        raise OSError("simulated disk full")

    monkeypatch.setattr(launcher, "_write_manifest", boom)

    rc = main(["--job", "post_fail"])
    assert rc == 1
    err = json.loads(capsys.readouterr().err)
    assert err["kind"] == "contract_violation"
    # YAML step name from params.json, not the module path.
    assert err["step_name"] == "fastq"
    assert "post-success manifest write failed" in err["reason"]
    assert "OSError" in err["reason"]
    assert "simulated disk full" in err["reason"]


def test_main_returns_1_and_prints_structured_error_on_backend_failure(
    monkeypatch, io_dirs, capsys
):
    """Skeleton path: execute() raises NotImplementedError →
    run_native_job translates it to BackendFailure(UNKNOWN_PERMANENT)
    → launcher prints a structured JSON line to stderr and returns 1.
    The SLURM job ends non-zero; orchestrator-side polling sees that
    and classifies."""
    input_path, _ = io_dirs

    async def execute(inputs, workspace):
        raise NotImplementedError("skeleton")

    _install_stub(monkeypatch, short_name="skeleton", execute_fn=execute)
    _write_params(input_path, fastq_path="/tmp/x.fa", reference_idx=1, work_ticket_idx=1)

    rc = main(["--job", "skeleton"])
    assert rc == 1

    captured = capsys.readouterr()
    err = json.loads(captured.err)
    assert err["kind"] == "unknown_permanent"
    # YAML step name from params.json, not the module path; module path
    # stays in the reason text for operator-side debugging.
    assert err["step_name"] == "fastq"
    assert "qiita_compute_orchestrator.jobs.skeleton" in err["reason"]
    assert "not implemented" in err["reason"]
