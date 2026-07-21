"""Functional (end-to-end) tests for the SlurmBackend.

Wires payload + client + verify together. Driven by httpx.MockTransport
so no live SLURM controller is needed; the test handler shapes
slurmrestd responses to drive each branch of the SLURM-state =>
FailureKind mapping.

The control plane drives the decoupled submit_step / status_step /
result_step trio (it owns the poll loop), so these end-to-end tests compose
the trio via the `_run_step_via_trio` helper — exercising the real
production methods against a realistic slurmrestd mock. Per-method behavior
in isolation is covered by the trio tests further down.
"""

from __future__ import annotations

import base64
import json
import shlex
from pathlib import Path

import httpx
import pytest
from qiita_common.backend_failure import BackendFailure, FailureKind, StepNoData
from qiita_common.models import (
    ComputeTarget,
    StepBaselineResources,
    StepStatus,
    WorkTicketFailureStage,
)
from qiita_common.testing.native_steps import FASTQ_TO_PARQUET_MODULE

from qiita_compute_orchestrator.backend import (
    LocalStepHandle,
    SlurmStepHandle,
    StepHandle,
    StepStatusInfo,
)
from qiita_compute_orchestrator.backends.slurm import SlurmBackend
from qiita_compute_orchestrator.slurm import SlurmrestdClient


def _make_jwt(sun: str) -> str:
    """Minimal JWT-shaped string with the given `sun` claim. See
    test_slurm_client._make_jwt for the same helper; duplicated here
    to keep test_slurm_backend.py self-contained without an awkward
    cross-test-file import."""

    def _b64url(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()

    return f"{_b64url({'alg': 'HS256'})}.{_b64url({'sun': sun})}.sig"


@pytest.fixture
def jwt_path(tmp_path):
    p = tmp_path / "jwt"
    # user_name="qiita-orch" in _make_backend; sun must match or
    # SlurmrestdClient refuses to construct.
    p.write_text(_make_jwt("qiita-orch"))
    return p


@pytest.fixture
def baseline():
    return StepBaselineResources(cpu=1, mem_gb=1, walltime_seconds=60)


def _make_backend(
    handler,
    jwt_path,
    *,
    co_to_cp_token: str = "",
    cp_url: str = "",
    path_scratch: str = "",
    path_derived: str = "",
    data_plane_url: str = "",
) -> SlurmBackend:
    client = SlurmrestdClient(
        base_url="http://slurm-test:6820",
        jwt_path=jwt_path,
        user_name="qiita-orch",
        http_client=httpx.AsyncClient(
            base_url="http://slurm-test:6820",
            transport=handler,
            timeout=5,
        ),
    )
    return SlurmBackend(
        client=client,
        partition="qiita",
        account="qiita-prod",
        co_to_cp_token=co_to_cp_token,
        cp_url=cp_url,
        path_scratch=path_scratch,
        path_derived=path_derived,
        data_plane_url=data_plane_url,
    )


async def _run_step_via_trio(backend: SlurmBackend, *args, **kwargs) -> dict[str, Path]:
    """Compose submit_step → poll status_step to terminal → result_step,
    reproducing the end-to-end step execution the control-plane runner drives.
    The runner owns the real poll loop (with sleeps + the CP-side filesystem
    tiebreaker); this test-side composition is the minimal equivalent so the
    SlurmBackend functional tests exercise the production trio against a
    realistic slurmrestd mock. `*args` / `**kwargs` match `submit_step`."""
    handle = await backend.submit_step(*args, **kwargs)
    assert handle.slurm_job_id is not None
    while True:
        info = await backend.status_step(handle)
        if info.is_terminal:
            break
    return await backend.result_step(handle, info)


def _job_running_then(state: str, *, exit_code: int | None = None, reason: str | None = None):
    """Return a request handler that:
    1st request:  POST /job/submit => returns job_id 1
    2nd request:  GET  /job/1 => RUNNING (drives one polling loop)
    3rd+ request: GET  /job/1 => `state` with optional exit_code / reason
    """
    call_log: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        call_log.append(f"{request.method} {request.url.path}")
        if request.method == "POST" and request.url.path.endswith("/job/submit"):
            return httpx.Response(200, json={"job_id": 1})
        if request.method == "GET" and "/job/1" in request.url.path:
            n_get = sum(1 for c in call_log if c.startswith("GET"))
            if n_get == 1:
                return httpx.Response(
                    200,
                    json={"jobs": [{"job_id": 1, "job_state": ["RUNNING"], "exit_code": {}}]},
                )
            payload = {"job_state": [state], "exit_code": {}}
            if exit_code is not None:
                payload["exit_code"] = {
                    "return_code": {"number": exit_code, "set": True, "infinite": False}
                }
            if reason is not None:
                payload["state_reason"] = reason
            return httpx.Response(200, json={"jobs": [{"job_id": 1, **payload}]})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    return httpx.MockTransport(handler), call_log


def _write_completed_output(workspace: Path, *, manifest_extra: dict | None = None) -> None:
    """Pre-create the workspace tree the backend would otherwise build,
    plus a successful manifest.json + the file it declares. Tests that
    drive a COMPLETED state need this so the post-poll verifier finds
    a valid output dir."""
    out = workspace / "output"
    out.mkdir(parents=True, exist_ok=True)
    parquet = out / "result.parquet"
    parquet.write_bytes(b"abc")
    parquet.chmod(0o440)
    manifest = {
        "files": [{"path": "result.parquet", "size_bytes": 3}],
        "outputs": {"manifest": "result.parquet"},
    }
    if manifest_extra:
        manifest.update(manifest_extra)
    (out / "manifest.json").write_text(json.dumps(manifest))
    (out / "manifest.json").chmod(0o440)


# ============================================================================
# Pre-flight validation
# ============================================================================


@pytest.mark.asyncio
async def test_run_step_requires_container_or_module(jwt_path, baseline, tmp_path):
    """Neither runtime field set → CONTRACT_VIOLATION. The wire validator
    on StepSubmitRequest catches this upstream, but direct callers (and
    this test) bypass the wire, so SlurmBackend re-checks."""
    handler = httpx.MockTransport(lambda req: httpx.Response(500))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await _run_step_via_trio(
            backend,
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container=None,
            module=None,
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert "exactly one" in ei.value.reason


@pytest.mark.asyncio
async def test_run_step_rejects_both_container_and_module(jwt_path, baseline, tmp_path):
    """Both set is a contract violation too — must surface as a typed
    BackendFailure, not as a raw ValueError leaking out of the payload
    builder. Both this case and the "neither set" case above ride the
    same `exactly-one` guard."""
    handler = httpx.MockTransport(lambda req: httpx.Response(500))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await _run_step_via_trio(
            backend,
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/hash:1.0.0",
            module=FASTQ_TO_PARQUET_MODULE,
            entrypoint=None,
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert "exactly one" in ei.value.reason


@pytest.mark.asyncio
async def test_run_step_requires_baseline_resources(jwt_path, tmp_path):
    handler = httpx.MockTransport(lambda req: httpx.Response(500))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await _run_step_via_trio(
            backend,
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/hash:1.0.0",
            entrypoint="/opt/qiita/hash.sh",
            baseline_resources=None,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert "baseline_resources" in ei.value.reason


@pytest.mark.asyncio
async def test_run_step_container_rejects_unsupported_scope(jwt_path, baseline, tmp_path):
    """Container steps are gated on a closed set of scope kinds (reference,
    sequenced_pool, prep_sample). A kind outside that set is a workflow-authoring
    error, and the gate fails it at submit rather than dispatching a step no
    backend is known to handle.

    `block` stands in for "some kind not on the list" — no workflow runs a
    container under a block-scoped ticket today. If one ever does, admit the kind
    in the allowlist (the dispatch path treats scope_target opaquely) and repoint
    this test; the workflow-scope pin test catches the mismatch statically either
    way."""
    handler = httpx.MockTransport(lambda req: httpx.Response(500))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await _run_step_via_trio(
            backend,
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "block", "block_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/hash:1.0.0",
            entrypoint="/opt/qiita/hash.sh",
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert "requires a scope_target with kind in" in ei.value.reason
    assert "block" in ei.value.reason


@pytest.mark.asyncio
async def test_run_step_native_accepts_non_reference_scope(jwt_path, baseline, tmp_path):
    """The S4 container-path gate must NOT fire for native steps —
    they thread scope_target through flatten_native_inputs and can
    target any scope kind. Verify by submitting a module step with a
    prep_sample scope; the run gets past the gate and proceeds to the
    slurmrestd submission (which we let fail downstream — we just
    need to confirm the gate didn't catch it)."""
    transport = httpx.MockTransport(
        lambda req: httpx.Response(500, content=b"slurmrestd unreachable")
    )
    backend = _make_backend(transport, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await _run_step_via_trio(
            backend,
            "fastq",
            {},
            tmp_path,
            scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
            work_ticket_idx=99,
            module="qiita_compute_orchestrator.jobs.fastq_to_parquet",
            entrypoint=None,
            baseline_resources=baseline,
        )
    # Past the container-path gate; failure here is from slurmrestd
    # mock returning 500, NOT from a scope-kind contract violation.
    # The kind would be a SLURMRESTD_UNREACHABLE-style failure, not
    # CONTRACT_VIOLATION with a "reference-scoped" reason.
    assert "requires a reference-scoped ticket" not in (ei.value.reason or "")


# ============================================================================
# Happy path
# ============================================================================


@pytest.mark.asyncio
async def test_run_step_completed_returns_outputs(jwt_path, baseline, tmp_path):
    transport, _ = _job_running_then("COMPLETED", exit_code=0)
    backend = _make_backend(transport, jwt_path)

    # Pre-write the output dir's manifest so the verifier passes.
    _write_completed_output(tmp_path)

    outputs = await _run_step_via_trio(
        backend,
        "hash",
        {"fasta_path": tmp_path / "x.fa"},
        tmp_path,
        scope_target={"kind": "reference", "reference_idx": 42},
        work_ticket_idx=99,
        container="docker://qiita/hash:1.0.0",
        entrypoint="/usr/local/bin/hash",
        baseline_resources=baseline,
    )
    # The manifest declares outputs={"manifest": "result.parquet"}.
    out_dir = tmp_path / "output"
    assert outputs == {"manifest": (out_dir / "result.parquet").resolve()}


@pytest.mark.asyncio
async def test_run_step_propagates_co_to_cp_token_and_cp_url_into_job_env(
    jwt_path, baseline, tmp_path
):
    """When the backend was wired with co_to_cp_token / cp_url, those
    values land in the SLURM submit payload's environment list as
    CO_TO_CP_TOKEN / QIITA_CP_URL, plus QIITA_ALLOW_TOKEN_ENV=true so
    the compute-node launcher's `Settings.from_env(
    require_cp_to_co_token=False)` accepts the env-var token shape.

    CP_TO_CO_TOKEN must NOT be propagated — it's inbound /step/*
    auth which the launcher never serves; the job-side
    `get_settings()` no-install fallback skips it."""
    captured: dict[str, dict] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith("/job/submit"):
            captured["payload"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"job_id": 1})
        return httpx.Response(
            200,
            json={"jobs": [{"job_id": 1, "job_state": ["COMPLETED"], "exit_code": {}}]},
        )

    backend = _make_backend(
        httpx.MockTransport(handler),
        jwt_path,
        co_to_cp_token="co-cp-secret",
        cp_url="https://qiita.example.org",
    )
    _write_completed_output(tmp_path)

    await _run_step_via_trio(
        backend,
        "fastq",
        {},
        tmp_path,
        scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
        work_ticket_idx=99,
        module=FASTQ_TO_PARQUET_MODULE,
        baseline_resources=baseline,
    )
    env = dict(item.split("=", 1) for item in captured["payload"]["job"]["environment"])
    assert env["CO_TO_CP_TOKEN"] == "co-cp-secret"
    assert env["QIITA_ALLOW_TOKEN_ENV"] == "true"
    assert env["QIITA_CP_URL"] == "https://qiita.example.org"
    # HOME is wired on every job, not just token-propagating ones.
    assert env["HOME"] == str(tmp_path)
    # CP_TO_CO_TOKEN is the inbound /step/* shared bearer — the
    # launcher never serves that route, so propagating it would only
    # widen the `scontrol show job` exposure with no consumer.
    assert "CP_TO_CO_TOKEN" not in env


@pytest.mark.asyncio
async def test_run_step_omits_token_env_when_backend_has_no_tokens(jwt_path, baseline, tmp_path):
    """When SlurmBackend was constructed without tokens/cp_url (the
    default — unit tests, dev), the submit payload must NOT carry
    CO_TO_CP_TOKEN / QIITA_ALLOW_TOKEN_ENV / QIITA_CP_URL. Defensive
    against an accidental empty-string token leaking through."""
    captured: dict[str, dict] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith("/job/submit"):
            captured["payload"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"job_id": 1})
        return httpx.Response(
            200,
            json={"jobs": [{"job_id": 1, "job_state": ["COMPLETED"], "exit_code": {}}]},
        )

    backend = _make_backend(httpx.MockTransport(handler), jwt_path)
    _write_completed_output(tmp_path)

    await _run_step_via_trio(
        backend,
        "fastq",
        {},
        tmp_path,
        scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
        work_ticket_idx=99,
        module=FASTQ_TO_PARQUET_MODULE,
        baseline_resources=baseline,
    )
    env = dict(item.split("=", 1) for item in captured["payload"]["job"]["environment"])
    assert "CP_TO_CO_TOKEN" not in env
    assert "CO_TO_CP_TOKEN" not in env
    assert "QIITA_ALLOW_TOKEN_ENV" not in env
    assert "QIITA_CP_URL" not in env
    # Likewise PATH_SCRATCH / PATH_DERIVED / DATA_PLANE_URL — only propagated
    # when wired (below).
    assert "PATH_SCRATCH" not in env
    assert "PATH_DERIVED" not in env
    assert "DATA_PLANE_URL" not in env


@pytest.mark.asyncio
async def test_run_step_propagates_path_scratch_into_job_env(jwt_path, baseline, tmp_path):
    """The compute-node native-step launcher calls `get_settings()`, whose
    `path_scratch` falls back to a `$TMPDIR/qiita` DEFAULT when `PATH_SCRATCH` is
    absent. PATH_SCRATCH is the per-ticket workspace base (persistent index
    artifacts now derive from PATH_DERIVED — see the path_derived test below); a
    job resolving the scratch base would otherwise land on node-local /tmp,
    invisible to the CP. So the backend must propagate the resolved value into
    the SLURM job env (the same way it propagates QIITA_CP_URL — /etc/qiita is
    not visible from nodes)."""
    captured: dict[str, dict] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith("/job/submit"):
            captured["payload"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"job_id": 1})
        return httpx.Response(
            200,
            json={"jobs": [{"job_id": 1, "job_state": ["COMPLETED"], "exit_code": {}}]},
        )

    backend = _make_backend(
        httpx.MockTransport(handler),
        jwt_path,
        path_scratch="/scratch/persistent/qiita",
    )
    _write_completed_output(tmp_path)

    await _run_step_via_trio(
        backend,
        "fastq",
        {},
        tmp_path,
        scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
        work_ticket_idx=99,
        module=FASTQ_TO_PARQUET_MODULE,
        baseline_resources=baseline,
    )
    env = dict(item.split("=", 1) for item in captured["payload"]["job"]["environment"])
    assert env["PATH_SCRATCH"] == "/scratch/persistent/qiita"


@pytest.mark.asyncio
async def test_run_step_propagates_path_derived_into_job_env(jwt_path, baseline, tmp_path):
    """Native index builders derive a persistent path from PATH_DERIVED —
    `build_rype_index` / `build_minimap2_index` write
    `{PATH_DERIVED}/references/{idx}/{rype,minimap2}/...`. Like PATH_SCRATCH,
    /etc/qiita is invisible from compute nodes, so the backend must propagate
    the resolved PATH_DERIVED into the SLURM job env or the launcher's
    get_settings() falls back to the $TMPDIR/qiita/derived DEFAULT and the
    index lands in node-local /tmp, invisible to the CP."""
    captured: dict[str, dict] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith("/job/submit"):
            captured["payload"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"job_id": 1})
        return httpx.Response(
            200,
            json={"jobs": [{"job_id": 1, "job_state": ["COMPLETED"], "exit_code": {}}]},
        )

    backend = _make_backend(
        httpx.MockTransport(handler),
        jwt_path,
        path_derived="/scratch/persistent",
    )
    _write_completed_output(tmp_path)

    await _run_step_via_trio(
        backend,
        "fastq",
        {},
        tmp_path,
        scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
        work_ticket_idx=99,
        module=FASTQ_TO_PARQUET_MODULE,
        baseline_resources=baseline,
    )
    env = dict(item.split("=", 1) for item in captured["payload"]["job"]["environment"])
    assert env["PATH_DERIVED"] == "/scratch/persistent"


@pytest.mark.asyncio
async def test_run_step_propagates_data_plane_url_into_job_env(jwt_path, baseline, tmp_path):
    """A native job that streams reference chunks (Flight DoGet) resolves the
    data-plane origin via the launcher's get_settings() on the compute node.
    Like PATH_DERIVED, /etc/qiita is invisible from compute nodes, so the backend
    must propagate the resolved DATA_PLANE_URL into the SLURM job env or the
    launcher falls back to the grpc://localhost:50051 DEFAULT and DoGets against
    the wrong origin."""
    captured: dict[str, dict] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith("/job/submit"):
            captured["payload"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"job_id": 1})
        return httpx.Response(
            200,
            json={"jobs": [{"job_id": 1, "job_state": ["COMPLETED"], "exit_code": {}}]},
        )

    backend = _make_backend(
        httpx.MockTransport(handler),
        jwt_path,
        data_plane_url="grpc://qiita-miint.ucsd.edu:50051",
    )
    _write_completed_output(tmp_path)

    await _run_step_via_trio(
        backend,
        "fastq",
        {},
        tmp_path,
        scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
        work_ticket_idx=99,
        module=FASTQ_TO_PARQUET_MODULE,
        baseline_resources=baseline,
    )
    env = dict(item.split("=", 1) for item in captured["payload"]["job"]["environment"])
    assert env["DATA_PLANE_URL"] == "grpc://qiita-miint.ucsd.edu:50051"


@pytest.mark.asyncio
async def test_run_step_writes_params_json(jwt_path, baseline, tmp_path):
    """params.json must end up at <workspace>/input/params.json so the
    container can read it via $QIITA_INPUT_PATH."""
    transport, _ = _job_running_then("COMPLETED", exit_code=0)
    backend = _make_backend(transport, jwt_path)
    _write_completed_output(tmp_path)

    await _run_step_via_trio(
        backend,
        "hash",
        {"fasta_path": tmp_path / "input.fa"},
        tmp_path,
        scope_target={"kind": "reference", "reference_idx": 42},
        work_ticket_idx=99,
        container="docker://qiita/hash:1.0.0",
        entrypoint="/opt/qiita/hash.sh",
        baseline_resources=baseline,
    )
    params_text = (tmp_path / "input" / "params.json").read_text()
    # Pretty-printed (2-space indent, trailing newline) so a human
    # debugging a job's input dir can read it — pin the shape so a
    # regression to a dense single-line dump is caught.
    assert params_text.endswith("\n")
    assert "\n  " in params_text
    params = json.loads(params_text)
    assert params["step_name"] == "hash"
    assert params["scope_target"] == {"kind": "reference", "reference_idx": 42}
    assert params["work_ticket_idx"] == 99
    assert params["inputs"]["fasta_path"].endswith("input.fa")


# ============================================================================
# State => FailureKind mapping
# ============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("slurm_state", "expected_kind", "expected_transient"),
    [
        ("FAILED", FailureKind.EXIT_NONZERO, False),
        ("CANCELLED", FailureKind.EXIT_NONZERO, False),
        ("DEADLINE", FailureKind.EXIT_NONZERO, False),
        ("NODE_FAIL", FailureKind.NODE_FAIL, True),
        ("BOOT_FAIL", FailureKind.NODE_FAIL, True),
        ("OUT_OF_MEMORY", FailureKind.OOM_KILLED, True),
        ("PREEMPTED", FailureKind.PREEMPTED, True),
        ("TIMEOUT", FailureKind.TIMEOUT_BEFORE_START, True),
    ],
)
async def test_run_step_terminal_states_map_to_kinds(
    jwt_path, baseline, tmp_path, slurm_state, expected_kind, expected_transient
):
    """Each SLURM terminal failure state maps to the documented
    FailureKind, and the resulting BackendFailure has the right
    transient-classification."""
    transport, _ = _job_running_then(slurm_state, exit_code=1, reason="something")
    backend = _make_backend(transport, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await _run_step_via_trio(
            backend,
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/hash:1.0.0",
            entrypoint="/opt/qiita/hash.sh",
            baseline_resources=baseline,
        )
    assert ei.value.kind == expected_kind
    assert ei.value.transient is expected_transient
    assert ei.value.stage == WorkTicketFailureStage.STEP_RUN
    assert ei.value.step_name == "hash"
    # State name surfaces in reason for ops triage.
    assert slurm_state in ei.value.reason


# ============================================================================
# Launcher-stderr enrichment for native steps
# ============================================================================


@pytest.mark.asyncio
async def test_run_step_native_failure_enriched_from_launcher_stderr(jwt_path, baseline, tmp_path):
    """A native step that fails inside its execute() writes a structured
    JSON line to stderr; SlurmBackend reads that line and uses it to
    populate the BackendFailure so the work_ticket's failure_reason
    carries the application-level message (not just "exit_code=1").

    Sets up SLURM to report FAILED, drops a real stderr file under
    `<workspace>/logs/stderr` matching what jobs/__main__.py would
    emit on a NotImplementedError, and asserts the BackendFailure
    surface."""
    import json

    transport, _ = _job_running_then("FAILED", exit_code=1)
    backend = _make_backend(transport, jwt_path)

    # SlurmBackend creates <workspace>/logs/ itself, but we pre-create
    # the stderr file before the trio runs so the launcher's would-be
    # output is in place when result_step looks for it.
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    launcher_reason = f"native job {FASTQ_TO_PARQUET_MODULE!r} not implemented: skeleton"
    (logs_dir / "stderr").write_text(
        json.dumps(
            {
                "kind": "unknown_permanent",
                "step_name": "fastq",
                "reason": launcher_reason,
            }
        )
        + "\n"
    )

    with pytest.raises(BackendFailure) as ei:
        await _run_step_via_trio(
            backend,
            "fastq",  # YAML step name passed in by the runner
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            module=FASTQ_TO_PARQUET_MODULE,
            baseline_resources=baseline,
        )

    # Launcher's kind/step_name/reason override the state-based defaults.
    assert ei.value.kind is FailureKind.UNKNOWN_PERMANENT  # not EXIT_NONZERO
    assert ei.value.step_name == "fastq"  # YAML name, not the job name passed at `name=`
    assert "fastq_to_parquet" in ei.value.reason
    assert "not implemented" in ei.value.reason
    # SLURM-side context is preserved in the reason (bracketed suffix)
    # so operators still see the job_id / state / exit_code.
    assert "FAILED" in ei.value.reason
    assert "exit_code=1" in ei.value.reason


@pytest.mark.asyncio
async def test_run_step_native_no_data_raises_step_no_data(jwt_path, baseline, tmp_path):
    """A native step that hit a terminal no-data outcome (an empty FASTQ well)
    writes a structured no-data line to stderr and exits non-zero (SLURM marks
    it FAILED). result_step parses that line BEFORE failure classification and
    raises StepNoData — NOT a BackendFailure — so the runner transitions the
    ticket to NO_DATA, never FAILED."""
    transport, _ = _job_running_then("FAILED", exit_code=1)
    backend = _make_backend(transport, jwt_path)

    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    no_data_reason = "FASTQ file contains no records: /data/well_R1.fastq.gz"
    (logs_dir / "stderr").write_text(
        json.dumps({"kind": "no_data", "step_name": "fastq", "reason": no_data_reason}) + "\n"
    )

    with pytest.raises(StepNoData) as ei:
        await _run_step_via_trio(
            backend,
            "fastq",
            {},
            tmp_path,
            scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
            work_ticket_idx=99,
            module=FASTQ_TO_PARQUET_MODULE,
            baseline_resources=baseline,
        )
    assert not isinstance(ei.value, BackendFailure)
    assert ei.value.step_name == "fastq"
    assert "contains no records" in ei.value.reason


@pytest.mark.asyncio
async def test_run_step_falls_back_to_state_based_kind_when_no_launcher_line(
    jwt_path, baseline, tmp_path
):
    """When stderr has no structured line (container step, or an
    infra-killed native job that died before flushing), SlurmBackend
    falls back to the state-based classification — preserves the
    current behavior unchanged."""
    transport, _ = _job_running_then("FAILED", exit_code=1, reason="container exited nonzero")
    backend = _make_backend(transport, jwt_path)

    # Pre-create logs/ with a stderr file that has only container-style
    # text (no JSON line). This is what the container case looks like.
    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    (logs_dir / "stderr").write_text("ERROR: container exited with code 1\n")

    with pytest.raises(BackendFailure) as ei:
        await _run_step_via_trio(
            backend,
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/hash:1.0.0",
            entrypoint="/opt/qiita/hash.sh",
            baseline_resources=baseline,
        )
    # State-based classification stands.
    assert ei.value.kind is FailureKind.EXIT_NONZERO
    assert ei.value.step_name == "hash"  # the `name` arg, not a launcher-supplied value
    assert "FAILED" in ei.value.reason
    assert "exit_code=1" in ei.value.reason


# ============================================================================
# Verifier integration
# ============================================================================


@pytest.mark.asyncio
async def test_run_step_completed_but_missing_manifest_is_contract_violation(
    jwt_path, baseline, tmp_path
):
    """SLURM job exits 0 but the container didn't write manifest.json.
    Verifier flags it; backend surfaces as CONTRACT_VIOLATION
    (permanent) — retry won't fix a workflow that can't honor the
    contract."""
    transport, _ = _job_running_then("COMPLETED", exit_code=0)
    backend = _make_backend(transport, jwt_path)
    # Don't pre-create the output dir / manifest. The backend creates
    # the directory itself, but the container would have populated it
    # with a manifest — we leave it empty.

    with pytest.raises(BackendFailure) as ei:
        await _run_step_via_trio(
            backend,
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/hash:1.0.0",
            entrypoint="/opt/qiita/hash.sh",
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert not ei.value.transient
    assert "manifest" in ei.value.reason.lower()


# ============================================================================
# slurmrestd error classification
# ============================================================================


@pytest.mark.asyncio
async def test_run_step_submit_5xx_is_unreachable(jwt_path, baseline, tmp_path):
    """slurmrestd 5xx on submit => SLURMRESTD_UNREACHABLE (retriable)."""
    handler = httpx.MockTransport(lambda req: httpx.Response(503, text="slurmctld down"))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await _run_step_via_trio(
            backend,
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/hash:1.0.0",
            entrypoint="/opt/qiita/hash.sh",
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.SLURMRESTD_UNREACHABLE
    assert ei.value.transient is True


@pytest.mark.asyncio
async def test_run_step_submit_4xx_is_contract_violation(jwt_path, baseline, tmp_path):
    """slurmrestd 4xx on submit => CONTRACT_VIOLATION (permanent) —
    bad payload won't fix itself on retry."""
    handler = httpx.MockTransport(lambda req: httpx.Response(400, json={"errors": ["malformed"]}))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await _run_step_via_trio(
            backend,
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/hash:1.0.0",
            entrypoint="/opt/qiita/hash.sh",
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert ei.value.transient is False


@pytest.mark.asyncio
async def test_run_step_submit_200_with_error_code_is_contract_violation(
    jwt_path, baseline, tmp_path
):
    """slurmrestd HTTP 200 carrying a non-zero result.error_code (the job
    was rejected by slurmctld, e.g. partition unavailable) => permanent
    CONTRACT_VIOLATION, not a successful submit and not a retriable
    transport failure."""
    handler = httpx.MockTransport(
        lambda req: httpx.Response(
            200,
            json={
                "job_id": 0,
                "result": {
                    "job_id": 0,
                    "error_code": 2015,
                    "error": "Requested partition configuration not available now",
                },
            },
        )
    )
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await _run_step_via_trio(
            backend,
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/hash:1.0.0",
            entrypoint="/opt/qiita/hash.sh",
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert ei.value.transient is False


@pytest.mark.asyncio
async def test_run_step_submit_persistent_401_is_unreachable(jwt_path, baseline, tmp_path):
    """slurmrestd 401 that survives the client's JWT-refresh retry must
    classify as SLURMRESTD_UNREACHABLE (retriable). The user's rotation
    pipeline is broken (token unreadable / wrong principal / not
    refreshed) — operator-fixable, not a workflow contract violation."""
    handler = httpx.MockTransport(lambda req: httpx.Response(401, text="bad token"))
    backend = _make_backend(handler, jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await _run_step_via_trio(
            backend,
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/hash:1.0.0",
            entrypoint="/opt/qiita/hash.sh",
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.SLURMRESTD_UNREACHABLE
    assert ei.value.transient is True


@pytest.mark.asyncio
async def test_run_step_submit_transport_error_is_unreachable(jwt_path, baseline, tmp_path):
    """A connection error on submit => SLURMRESTD_UNREACHABLE."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    backend = _make_backend(httpx.MockTransport(handler), jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await _run_step_via_trio(
            backend,
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/hash:1.0.0",
            entrypoint="/opt/qiita/hash.sh",
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.SLURMRESTD_UNREACHABLE


# ============================================================================
# Decoupled interface: submit_step / status_step / result_step
# ============================================================================


def _slurm_handle(tmp_path, *, job_id: int = 1) -> StepHandle:
    """A SLURM StepHandle pointing at the workspace tree, for driving
    status_step / result_step in isolation."""
    return SlurmStepHandle(
        step_name="hash",
        slurm_job_id=job_id,
        job_name="qiita-wt99-hash-a0",
        output_path=tmp_path / "output",
        logs_path=tmp_path / "logs",
    )


@pytest.mark.asyncio
async def test_submit_step_returns_slurm_handle_without_polling(jwt_path, baseline, tmp_path):
    """submit_step submits the job and returns a StepHandle carrying the
    job id, the deterministic name, and the workspace paths that
    status_step / result_step need — and does NOT poll (the CP drives
    polling now, so a single submit must not block on completion)."""
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith("/job/submit"):
            captured["payload"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"job_id": 4242})
        raise AssertionError(f"submit_step must not poll; saw {request.method} {request.url}")

    backend = _make_backend(httpx.MockTransport(handler), jwt_path)
    handle = await backend.submit_step(
        "hash",
        {"fasta_path": tmp_path / "x.fa"},
        tmp_path,
        scope_target={"kind": "reference", "reference_idx": 42},
        work_ticket_idx=99,
        attempt=3,
        container="docker://qiita/hash:1.0.0",
        entrypoint="/usr/local/bin/hash",
        baseline_resources=baseline,
    )
    assert isinstance(handle, SlurmStepHandle)
    assert handle.compute_target == ComputeTarget.SLURM
    assert handle.slurm_job_id == 4242
    assert handle.job_name == "qiita-wt99-hash-a3"
    assert handle.output_path == tmp_path / "output"
    assert handle.logs_path == tmp_path / "logs"
    # The deterministic name also went onto the submit payload.
    assert captured["payload"]["job"]["name"] == "qiita-wt99-hash-a3"
    # params.json was written so a later status/result (or a re-attach)
    # finds the job's workspace fully laid out.
    assert (tmp_path / "input" / "params.json").exists()


@pytest.mark.asyncio
async def test_submit_step_classifies_submit_error(jwt_path, baseline, tmp_path):
    """submit_step classifies a slurmrestd submit error — a 5xx is retriable
    SLURMRESTD_UNREACHABLE."""
    backend = _make_backend(httpx.MockTransport(lambda r: httpx.Response(503)), jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.submit_step(
            "hash",
            {},
            tmp_path,
            scope_target={"kind": "reference", "reference_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/hash:1.0.0",
            entrypoint="/opt/qiita/hash.sh",
            baseline_resources=baseline,
        )
    assert ei.value.kind == FailureKind.SLURMRESTD_UNREACHABLE
    assert ei.value.transient is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("slurm_state", "exit_code", "expected"),
    [
        ("PENDING", None, StepStatus.PENDING),
        ("RUNNING", None, StepStatus.RUNNING),
        ("COMPLETING", None, StepStatus.RUNNING),
        ("COMPLETED", 0, StepStatus.COMPLETED),
        ("COMPLETED", 1, StepStatus.FAILED),  # exited "COMPLETED" but nonzero rc
        ("FAILED", 1, StepStatus.FAILED),
        ("OUT_OF_MEMORY", None, StepStatus.FAILED),
    ],
)
async def test_status_step_classifies_live_state(
    jwt_path, tmp_path, slurm_state, exit_code, expected
):
    """status_step is a single (non-looping) slurmrestd read that maps the
    live SLURM state to the coarse StepStatus the runner/summary use."""

    def handler(request: httpx.Request) -> httpx.Response:
        job: dict = {"job_id": 7, "job_state": [slurm_state]}
        if exit_code is not None:
            job["exit_code"] = {"return_code": {"number": exit_code, "set": True}}
        return httpx.Response(200, json={"jobs": [job]})

    backend = _make_backend(httpx.MockTransport(handler), jwt_path)
    info = await backend.status_step(_slurm_handle(tmp_path, job_id=7))
    assert info.status == expected
    assert info.raw_state == slurm_state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_code", "raises_transport", "expected_kind", "expected_transient"),
    [
        (404, False, FailureKind.UNKNOWN_PERMANENT, False),  # purged: status unknowable
        (400, False, FailureKind.UNKNOWN_PERMANENT, False),
        (401, False, FailureKind.SLURMRESTD_UNREACHABLE, True),  # broken rotation, retriable
        (503, False, FailureKind.SLURMRESTD_UNREACHABLE, True),
        (None, True, FailureKind.SLURMRESTD_UNREACHABLE, True),  # transport error
    ],
)
async def test_status_step_classifies_slurmrestd_errors(
    jwt_path, tmp_path, status_code, raises_transport, expected_kind, expected_transient
):
    """status_step turns a slurmrestd error into a typed BackendFailure so
    the route serializes it and the runner can classify retry: 4xx-not-401
    is permanent (status unknowable); 401 / 5xx / transport are retriable."""

    def handler(request: httpx.Request) -> httpx.Response:
        if raises_transport:
            raise httpx.ConnectError("connection refused")
        return httpx.Response(status_code, json={"errors": ["x"]})

    backend = _make_backend(httpx.MockTransport(handler), jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.status_step(_slurm_handle(tmp_path, job_id=7))
    assert ei.value.kind == expected_kind
    assert ei.value.transient is expected_transient


@pytest.mark.asyncio
async def test_result_step_completed_returns_outputs(jwt_path, tmp_path):
    """On a COMPLETED status, result_step runs the container-output
    verifier and returns the parsed outputs map — no slurmrestd call."""
    backend = _make_backend(httpx.MockTransport(lambda r: httpx.Response(500)), jwt_path)
    _write_completed_output(tmp_path)
    outputs = await backend.result_step(
        _slurm_handle(tmp_path),
        StepStatusInfo(status=StepStatus.COMPLETED, raw_state="COMPLETED", exit_code=0),
    )
    assert outputs == {"manifest": (tmp_path / "output" / "result.parquet").resolve()}


@pytest.mark.asyncio
async def test_result_step_completed_missing_manifest_is_contract_violation(jwt_path, tmp_path):
    backend = _make_backend(httpx.MockTransport(lambda r: httpx.Response(500)), jwt_path)
    (tmp_path / "output").mkdir(parents=True)
    with pytest.raises(BackendFailure) as ei:
        await backend.result_step(
            _slurm_handle(tmp_path),
            StepStatusInfo(status=StepStatus.COMPLETED, raw_state="COMPLETED", exit_code=0),
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert "manifest" in ei.value.reason.lower()


@pytest.mark.asyncio
async def test_result_step_failed_maps_state_to_kind(jwt_path, tmp_path):
    """On a FAILED status, result_step maps the SLURM state to a
    FailureKind."""
    backend = _make_backend(httpx.MockTransport(lambda r: httpx.Response(500)), jwt_path)
    (tmp_path / "logs").mkdir(parents=True)
    (tmp_path / "logs" / "stderr").write_text("ERROR: oom\n")
    with pytest.raises(BackendFailure) as ei:
        await backend.result_step(
            _slurm_handle(tmp_path),
            StepStatusInfo(
                status=StepStatus.FAILED, raw_state="OUT_OF_MEMORY", exit_code=None, reason="oom"
            ),
        )
    assert ei.value.kind == FailureKind.OOM_KILLED
    assert ei.value.transient is True
    assert "OUT_OF_MEMORY" in ei.value.reason


@pytest.mark.asyncio
async def test_result_step_rejects_non_slurm_handle(jwt_path, tmp_path):
    """A handle missing the SLURM job id / workspace paths (e.g. a local
    handle that wandered into SlurmBackend.result_step) is a caller bug —
    fail loudly with a typed BackendFailure, not an opaque AttributeError
    from dereferencing a None path."""
    backend = _make_backend(httpx.MockTransport(lambda r: httpx.Response(500)), jwt_path)
    local_handle = LocalStepHandle(step_name="hash", terminal_outputs={})
    with pytest.raises(BackendFailure) as ei:
        await backend.result_step(
            local_handle,
            StepStatusInfo(status=StepStatus.COMPLETED, raw_state="COMPLETED", exit_code=0),
        )
    assert ei.value.kind == FailureKind.UNKNOWN_PERMANENT
    assert "SLURM handle" in ei.value.reason


@pytest.mark.asyncio
async def test_result_step_failed_prefers_launcher_stderr(jwt_path, tmp_path):
    """result_step honors a native step's structured stderr line, exactly
    like the old inline post-poll path."""
    backend = _make_backend(httpx.MockTransport(lambda r: httpx.Response(500)), jwt_path)
    (tmp_path / "logs").mkdir(parents=True)
    (tmp_path / "logs" / "stderr").write_text(
        json.dumps({"kind": "unknown_permanent", "step_name": "fastq", "reason": "boom in execute"})
        + "\n"
    )
    with pytest.raises(BackendFailure) as ei:
        await backend.result_step(
            _slurm_handle(tmp_path),
            StepStatusInfo(status=StepStatus.FAILED, raw_state="FAILED", exit_code=1),
        )
    assert ei.value.kind is FailureKind.UNKNOWN_PERMANENT
    assert ei.value.step_name == "fastq"
    assert "boom in execute" in ei.value.reason
    assert "FAILED" in ei.value.reason  # SLURM context preserved in bracket suffix


@pytest.mark.asyncio
async def test_result_step_step_level_oom_upgrades_to_oom_killed(jwt_path, tmp_path):
    """A cgroup step-level oom_kill surfaces only as a coarse
    FAILED/exit_code=1 (no OUT_OF_MEMORY state, no launcher line). result_step
    must read the stderr tail, recognize the OOM signature, and upgrade the
    EXIT_NONZERO classification to the (retriable) OOM_KILLED."""
    backend = _make_backend(httpx.MockTransport(lambda r: httpx.Response(500)), jwt_path)
    (tmp_path / "logs").mkdir(parents=True)
    (tmp_path / "logs" / "stderr").write_text(
        "loading reference...\n"
        "slurmstepd: error: Detected 1 oom_kill event in StepId=141763.0. "
        "Some of the step tasks have been OOM Killed.\n"
    )
    with pytest.raises(BackendFailure) as ei:
        await backend.result_step(
            _slurm_handle(tmp_path),
            StepStatusInfo(
                status=StepStatus.FAILED,
                raw_state="FAILED",
                exit_code=1,
                reason="NonZeroExitCode",
            ),
        )
    assert ei.value.kind is FailureKind.OOM_KILLED
    assert ei.value.transient is True
    # The stderr tail is folded into the reason so `qiita ticket status` shows it.
    assert "oom_kill" in ei.value.reason
    assert "NonZeroExitCode" in ei.value.reason  # SLURM context preserved


@pytest.mark.asyncio
async def test_result_step_non_oom_failure_stays_exit_nonzero_with_tail(jwt_path, tmp_path):
    """A generic FAILED whose stderr carries no OOM signature stays
    EXIT_NONZERO, but the stderr tail is still folded into failure_reason."""
    backend = _make_backend(httpx.MockTransport(lambda r: httpx.Response(500)), jwt_path)
    (tmp_path / "logs").mkdir(parents=True)
    (tmp_path / "logs" / "stderr").write_text("FileNotFoundError: missing input.fastq\n")
    with pytest.raises(BackendFailure) as ei:
        await backend.result_step(
            _slurm_handle(tmp_path),
            StepStatusInfo(status=StepStatus.FAILED, raw_state="FAILED", exit_code=1),
        )
    assert ei.value.kind is FailureKind.EXIT_NONZERO
    assert "missing input.fastq" in ei.value.reason


@pytest.mark.asyncio
async def test_result_step_oom_signature_does_not_downgrade_node_fail(jwt_path, tmp_path):
    """An infra kind already correctly classified from the SLURM state
    (NODE_FAIL) must NOT be reclassified to OOM_KILLED even if a broad
    'Killed' token appears in stderr — we only upgrade generic kinds."""
    backend = _make_backend(httpx.MockTransport(lambda r: httpx.Response(500)), jwt_path)
    (tmp_path / "logs").mkdir(parents=True)
    (tmp_path / "logs" / "stderr").write_text("srun: Killed\n")
    with pytest.raises(BackendFailure) as ei:
        await backend.result_step(
            _slurm_handle(tmp_path),
            StepStatusInfo(status=StepStatus.FAILED, raw_state="NODE_FAIL", exit_code=None),
        )
    assert ei.value.kind is FailureKind.NODE_FAIL


@pytest.mark.asyncio
async def test_find_jobs_by_name_returns_matching_jobs(jwt_path):
    """find_jobs_by_name lists slurmrestd jobs, filters to the deterministic
    name, and maps each to a FoundJob with id + coarse status."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/jobs")
        return httpx.Response(
            200,
            json={
                "jobs": [
                    {"job_id": 11, "name": "qiita-wt99-hash-a0", "job_state": ["RUNNING"]},
                    {"job_id": 22, "name": "some-other-job", "job_state": ["RUNNING"]},
                ]
            },
        )

    backend = _make_backend(httpx.MockTransport(handler), jwt_path)
    found = await backend.find_jobs_by_name("qiita-wt99-hash-a0")
    assert len(found) == 1
    assert found[0].slurm_job_id == 11
    assert found[0].job_name == "qiita-wt99-hash-a0"
    assert found[0].status.status == StepStatus.RUNNING


@pytest.mark.asyncio
async def test_find_jobs_by_name_empty_when_no_match(jwt_path):
    """No job carries the name (purged, or never submitted) => empty list,
    so the runner falls through to a fresh submit."""
    backend = _make_backend(
        httpx.MockTransport(lambda r: httpx.Response(200, json={"jobs": []})), jwt_path
    )
    assert await backend.find_jobs_by_name("qiita-wt1-hash-a0") == []


@pytest.mark.asyncio
async def test_find_jobs_by_name_classifies_slurmrestd_error(jwt_path):
    """A 5xx from the job-list read classifies like status_step — retriable
    SLURMRESTD_UNREACHABLE so the runner's recovery retries the lookup."""
    backend = _make_backend(httpx.MockTransport(lambda r: httpx.Response(503)), jwt_path)
    with pytest.raises(BackendFailure) as ei:
        await backend.find_jobs_by_name("qiita-wt1-hash-a0")
    assert ei.value.kind == FailureKind.SLURMRESTD_UNREACHABLE
    assert ei.value.transient is True


# ============================================================================
# derived_inputs — operator-provisioned PATH_DERIVED artifacts into containers
# ============================================================================


def _capture_submit() -> tuple[httpx.MockTransport, dict]:
    """MockTransport that records the /job/submit payload and then reports the
    job COMPLETED, so a full trio run can assert on what was submitted."""
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith("/job/submit"):
            captured["payload"] = json.loads(request.content.decode())
            return httpx.Response(200, json={"job_id": 1})
        return httpx.Response(
            200,
            json={"jobs": [{"job_id": 1, "job_state": ["COMPLETED"], "exit_code": {}}]},
        )

    return httpx.MockTransport(handler), captured


@pytest.mark.asyncio
async def test_derived_inputs_bind_and_forward_env_into_container(jwt_path, baseline, tmp_path):
    """A container step's `derived_inputs` is joined against PATH_DERIVED, bound
    into the container, and forwarded as `--env NAME=<abs>`. This is what makes
    an operator-staged artifact (CheckM's DB) visible inside the SIF at all:
    apptainer runs `--containall`, so an unforwarded host env var is invisible.
    """
    transport, captured = _capture_submit()
    derived = tmp_path / "derived"
    (derived / "checkm_data").mkdir(parents=True)
    backend = _make_backend(transport, jwt_path, path_derived=str(derived))
    _write_completed_output(tmp_path)

    await _run_step_via_trio(
        backend,
        "checkm",
        {"refined_bins_dir": tmp_path / "bins"},
        tmp_path,
        scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
        work_ticket_idx=99,
        container="docker://qiita/checkm:1.0.0",
        entrypoint="/opt/qiita/checkm.sh",
        baseline_resources=baseline,
        derived_inputs={"QIITA_CHECKM_DB": "checkm_data"},
    )

    script = captured["payload"]["script"]
    db = derived / "checkm_data"
    # Read-only: one shared DB copy, many concurrent jobs.
    assert f"{db}:{db}:ro" in shlex.split(script)
    assert f"QIITA_CHECKM_DB={db}" in shlex.split(script)


@pytest.mark.asyncio
async def test_derived_inputs_without_path_derived_is_contract_violation(
    jwt_path, baseline, tmp_path
):
    """PATH_DERIVED unset + a step that declares derived_inputs = a
    misconfigured orchestrator. Fail loudly at submit rather than binding a
    path rooted at "" and letting apptainer produce a cryptic error."""
    transport, _ = _capture_submit()
    backend = _make_backend(transport, jwt_path, path_derived="")

    with pytest.raises(BackendFailure) as ei:
        await backend.submit_step(
            "checkm",
            {},
            tmp_path,
            scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/checkm:1.0.0",
            baseline_resources=baseline,
            derived_inputs={"QIITA_CHECKM_DB": "checkm_data"},
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert "PATH_DERIVED" in (ei.value.reason or "")


@pytest.mark.asyncio
async def test_derived_inputs_escaping_path_derived_is_contract_violation(
    jwt_path, baseline, tmp_path
):
    """A `..` that slipped past the wire validator must not reach apptainer: the
    backend is the last gate before a host path is bind-mounted into a container,
    so it re-runs the full shared contract itself rather than trusting the
    wire."""
    transport, _ = _capture_submit()
    backend = _make_backend(transport, jwt_path, path_derived=str(tmp_path / "derived"))

    with pytest.raises(BackendFailure) as ei:
        await backend.submit_step(
            "checkm",
            {},
            tmp_path,
            scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/checkm:1.0.0",
            baseline_resources=baseline,
            derived_inputs={"QIITA_CHECKM_DB": "../../etc"},
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert "traverse above PATH_DERIVED" in (ei.value.reason or "")


@pytest.mark.asyncio
async def test_derived_inputs_naming_the_derived_root_is_contract_violation(
    jwt_path, baseline, tmp_path
):
    """A bare "." passes the relative/no-`..` checks but resolves to PATH_DERIVED
    itself — binding the WHOLE derived root (every SIF under images/) into the
    container. Least privilege: a derived input must name something strictly
    under the root."""
    transport, _ = _capture_submit()
    backend = _make_backend(transport, jwt_path, path_derived=str(tmp_path / "derived"))

    with pytest.raises(BackendFailure) as ei:
        await backend.submit_step(
            "checkm",
            {},
            tmp_path,
            scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
            work_ticket_idx=99,
            container="docker://qiita/checkm:1.0.0",
            baseline_resources=baseline,
            derived_inputs={"QIITA_CHECKM_DB": "."},
        )
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert "strictly under PATH_DERIVED" in (ei.value.reason or "")


@pytest.mark.asyncio
async def test_derived_input_value_cannot_inject_shell(jwt_path, baseline, tmp_path):
    """The apptainer args are interpolated into a bash script. A derived_inputs
    VALUE carrying shell metacharacters must not be able to terminate the
    `apptainer exec` and run something else — every arg is shlex-quoted, so the
    `;` survives as literal text inside a quoted argument."""
    transport, captured = _capture_submit()
    derived = tmp_path / "derived"
    (derived / "evil; touch pwned").mkdir(parents=True)
    backend = _make_backend(transport, jwt_path, path_derived=str(derived))
    _write_completed_output(tmp_path)

    await _run_step_via_trio(
        backend,
        "checkm",
        {},
        tmp_path,
        scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
        work_ticket_idx=99,
        container="docker://qiita/checkm:1.0.0",
        entrypoint="/opt/qiita/checkm.sh",
        baseline_resources=baseline,
        derived_inputs={"QIITA_CHECKM_DB": "evil; touch pwned"},
    )

    script = captured["payload"]["script"]
    cmd_line = next(ln for ln in script.splitlines() if ln.startswith("apptainer "))

    # Parse the line the way the shell would. If the `;` were unquoted it would
    # be a command separator and `touch`/`pwned` would surface as their own
    # tokens; quoted, the whole bind stays a single argv entry.
    tokens = shlex.split(cmd_line)
    assert "touch" not in tokens
    db = derived / "evil; touch pwned"
    assert f"{db}:{db}:ro" in tokens
    assert f"QIITA_CHECKM_DB={db}" in tokens


@pytest.mark.asyncio
async def test_container_tmpdir_points_at_the_workspace_not_the_tmpfs(jwt_path, baseline, tmp_path):
    """`apptainer exec --containall` mounts a tmpfs /tmp — 64 MiB on the live
    deploy, per the host's `sessiondir max size` — and scrubs the environment, so
    an entrypoint's bare `mktemp -d` lands on a tiny in-memory disk. A step that
    stages real work through it (an assembly, a decompressed FASTQ) dies partway
    through, and the bytes it does write are charged to the job's cgroup memory.

    Forward TMPDIR to the per-job workspace: real disk, already bound via --home.
    The directory must exist before submit, since nothing inside the container can
    create it under the read-only image root."""
    transport, captured = _capture_submit()
    backend = _make_backend(transport, jwt_path)
    _write_completed_output(tmp_path)

    await _run_step_via_trio(
        backend,
        "assemble",
        {},
        tmp_path,
        scope_target={"kind": "prep_sample", "prep_sample_idx": 1},
        work_ticket_idx=99,
        container="docker://qiita/assemble:1.0.0",
        entrypoint="/opt/qiita/assemble.sh",
        baseline_resources=baseline,
    )

    assert f"TMPDIR={tmp_path}/tmp" in shlex.split(captured["payload"]["script"])
    assert (tmp_path / "tmp").is_dir(), "workspace tmp/ must exist before the job starts"


# ============================================================================
# cancel — scancel every attempt of a ticket by name prefix
# ============================================================================


@pytest.mark.asyncio
async def test_cancel_scancels_all_attempts_by_prefix_and_ignores_other_tickets(jwt_path):
    """cancel(42) lists jobs, keeps only those whose name starts with
    `qiita-wt42-` (all attempts), DELETEs each, and returns their ids — a job for
    a DIFFERENT ticket (wt5, which `qiita-wt42-` must not prefix-match) is left
    untouched."""
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(f"{request.method} {request.url.path}")
        if request.method == "GET" and request.url.path.endswith("/jobs"):
            return httpx.Response(
                200,
                json={
                    "jobs": [
                        {"job_id": 100, "job_state": ["RUNNING"], "name": "qiita-wt42-fastq-a0"},
                        {"job_id": 101, "job_state": ["PENDING"], "name": "qiita-wt42-fastq-a1"},
                        {"job_id": 200, "job_state": ["RUNNING"], "name": "qiita-wt5-fastq-a0"},
                    ]
                },
            )
        if request.method == "DELETE" and "/job/" in request.url.path:
            return httpx.Response(200, json={})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    backend = _make_backend(httpx.MockTransport(handler), jwt_path)
    cancelled = await backend.cancel(42)

    assert cancelled == [100, 101]
    deletes = [c for c in seen if c.startswith("DELETE")]
    assert any(c.endswith("/job/100") for c in deletes)
    assert any(c.endswith("/job/101") for c in deletes)
    assert not any(c.endswith("/job/200") for c in deletes)  # other ticket untouched


@pytest.mark.asyncio
async def test_cancel_no_live_jobs_returns_empty(jwt_path):
    """A ticket with no live jobs (all finished/purged) cancels to []."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path.endswith("/jobs"):
            return httpx.Response(200, json={"jobs": []})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    backend = _make_backend(httpx.MockTransport(handler), jwt_path)
    assert await backend.cancel(42) == []


@pytest.mark.asyncio
async def test_cancel_swallows_404_on_a_job_that_finished_mid_reap(jwt_path):
    """A job listed as live but gone (404) by the time we DELETE it is a no-op,
    not an error — cancel stays idempotent."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path.endswith("/jobs"):
            return httpx.Response(
                200,
                json={
                    "jobs": [{"job_id": 100, "job_state": ["RUNNING"], "name": "qiita-wt42-x-a0"}]
                },
            )
        if request.method == "DELETE" and request.url.path.endswith("/job/100"):
            return httpx.Response(404, json={"errors": [{"error": "unknown job"}]})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    backend = _make_backend(httpx.MockTransport(handler), jwt_path)
    # No exception; the id is still reported (we targeted it).
    assert await backend.cancel(42) == [100]
