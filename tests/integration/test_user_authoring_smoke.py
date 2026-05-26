"""End-to-end smoke test: a regular USER walks the full authoring flow
by invoking the real `qiita` CLI against a live control-plane server.

A uvicorn subprocess serves the control-plane app against the test
Postgres; the test shells out to `qiita <subcommand>` for every step —
study, biosample, sequencing-run, sequenced-pool, sequenced-sample,
fastq-to-parquet ticket submit, ticket status. Driving the actual CLI
(not raw HTTP) also pins the flag names documented in
docs/runbooks/user-cli-quickstart.md against argparse drift.

Each step clears a per-resource auth gate (study owner / run-or-pool
creator / per-study ADMIN) — the regression guard against any gate
silently reverting to a blanket role check.

The control plane dispatches the submitted ticket to a compute
orchestrator that is deliberately unreachable here: this test verifies
the auth + CLI surface, not workflow execution (the reference-add
smoke covers full-pipeline execution). The ticket-status assertion is
therefore permissive on `state`.
"""

import base64
import json
import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import pytest
from qiita_common.models import WorkTicketState

from qiita_control_plane.testing.postgres import resolve_postgres_url

_FASTQ_TO_PARQUET_YAML_PATH = (
    Path(__file__).parent.parent.parent
    / "workflows"
    / "fastq-to-parquet"
    / "1.0.0.yaml"
)

# The ticket-status assertion accepts any WorkTicketState value because
# the background dispatch (against a dead orchestrator) races with the
# read. WorkTicketState is a StrEnum, so its members compare equal to
# the plain strings the CLI prints.
_WORK_TICKET_STATES = frozenset(WorkTicketState)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def cp_server(tmp_path, hmac_secret):
    """Spawn the control-plane app under uvicorn against the test
    Postgres; yield its base URL.

    `COMPUTE_ORCHESTRATOR_URL` points at a dead port so the
    work-ticket POST's compute-backend guard passes (it only checks the
    client is non-None) while the background dispatch simply fails —
    irrelevant to what this test pins. The CP→CO token file must exist
    on disk because `ComputeBackendClient.__init__` reads it eagerly,
    so the fixture writes a dummy one.
    """
    port = _free_port()
    token_file = tmp_path / "cp-to-co.token"
    token_file.write_text("unused-dispatch-token")
    # Settings.from_env() requires WORK_TICKET_WORKSPACE_ROOT — the CP
    # would fail to boot without it. The dir doesn't need to exist for
    # this smoke (the dispatch points at a dead orchestrator port, so
    # the runner never reaches mkdir); the value just needs to be an
    # absolute path so the boot-time validation passes.
    env = {
        **os.environ,
        "DATABASE_URL": resolve_postgres_url(),
        "HMAC_SECRET_KEY": base64.b64encode(hmac_secret).decode(),
        "COMPUTE_ORCHESTRATOR_URL": "http://127.0.0.1:1",
        "CP_TO_CO_TOKEN_PATH": str(token_file),
        "WORK_TICKET_WORKSPACE_ROOT": str(tmp_path / "orch-workspace"),
    }
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "qiita_control_plane.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "warning",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    base_url = f"http://127.0.0.1:{port}"

    def _fail(reason: str) -> None:
        proc.terminate()
        out, err = proc.communicate(timeout=5)
        pytest.fail(
            f"{reason}\nstdout: {out.decode()[:2000]}\nstderr: {err.decode()[:2000]}"
        )

    deadline = time.monotonic() + 20.0
    healthy = False
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            _fail(f"cp server exited during startup (rc={proc.returncode})")
        try:
            resp = httpx.get(f"{base_url}/health", timeout=1.0)
            if resp.status_code == 200 and resp.json().get("status") == "ok":
                healthy = True
                break
        except httpx.HTTPError:
            pass
        time.sleep(0.25)
    if not healthy:
        _fail("cp server did not become healthy within 20s")

    yield base_url

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@pytest.fixture
async def synced_fastq_to_parquet_action(postgres_pool, tmp_path):
    """Load workflows/fastq-to-parquet/1.0.0.yaml into qiita.action under
    a uniquified version so concurrent test runs do not collide.

    Drops every dependent work_ticket before the action row at teardown
    so the FK RESTRICT on (action_id, action_version) does not block."""
    from qiita_control_plane.actions import load_actions, sync_actions

    workflows_dir = tmp_path / "workflows" / "fastq-to-parquet"
    workflows_dir.mkdir(parents=True)
    yaml_text = _FASTQ_TO_PARQUET_YAML_PATH.read_text()
    test_version = f"smoke-{uuid.uuid4()}"
    yaml_text = yaml_text.replace("version: 1.0.0", f"version: {test_version}")
    (workflows_dir / "1.0.0.yaml").write_text(yaml_text)

    actions = load_actions(tmp_path / "workflows")
    assert len(actions) == 1
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, actions)

    yield ("fastq-to-parquet", test_version)

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        "fastq-to-parquet",
        test_version,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        "fastq-to-parquet",
        test_version,
    )


def _invoke_cli(base_url: str, token: str, *args: str) -> subprocess.CompletedProcess:
    """Run `qiita <args>` as a subprocess via `python -m`. The PAT is
    handed over through the QIITA_TOKEN env var the CLI reads."""
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "qiita_control_plane.cli.user",
            "--base-url",
            base_url,
            *args,
        ],
        env={**os.environ, "QIITA_TOKEN": token},
        capture_output=True,
        text=True,
        timeout=30,
    )


def _run_cli(base_url: str, token: str, *args: str) -> dict:
    """Invoke the CLI; assert exit 0; parse the JSON it prints to stdout."""
    result = _invoke_cli(base_url, token, *args)
    assert result.returncode == 0, (
        f"`qiita {' '.join(args)}` failed (rc={result.returncode})\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    return json.loads(result.stdout)


def _run_cli_expect_failure(
    base_url: str, token: str, *args: str
) -> subprocess.CompletedProcess:
    """Invoke the CLI; assert it exited non-zero; return the
    CompletedProcess so the caller can inspect stderr."""
    result = _invoke_cli(base_url, token, *args)
    assert result.returncode != 0, (
        f"`qiita {' '.join(args)}` unexpectedly succeeded\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    return result


async def _fetch_prep_protocol_idx(
    postgres_pool, name: str = "short_read_metagenomics"
) -> int:
    """The standard prep_protocol seeded by migrations is referenced by
    name from the CLI flag; tests resolve by lookup so a renumber on the
    seed doesn't fan out to every fixture."""
    return await postgres_pool.fetchval(
        "SELECT idx FROM qiita.prep_protocol WHERE name = $1", name
    )


async def test_user_authoring_smoke_via_cli(
    postgres_pool,
    cp_server,
    synced_fastq_to_parquet_action,
    regular_user_session,
):
    """As a plain USER (not admin), drive the real `qiita` CLI through
    study → biosample → run → pool → sample → work-ticket submit →
    work-ticket status. Every command must exit 0; each clears the
    per-resource auth gate it composes. Final `ticket status` must
    report the USER as originator.
    """
    action_id, action_version = synced_fastq_to_parquet_action
    user_token = regular_user_session["token"]
    user_idx = regular_user_session["principal_idx"]

    created_ticket_idxs: list[int] = []
    created_prep_sample_idxs: list[int] = []
    created_sequenced_pool_idxs: list[int] = []
    created_sequencing_run_idxs: list[int] = []
    created_biosample_idxs: list[int] = []
    created_study_idxs: list[int] = []

    try:
        # 1. study create — USER owns the new study; satisfies the
        #    owner-bypass on every downstream require_study_access.
        study = _run_cli(
            cp_server,
            user_token,
            "study",
            "create",
            "--title",
            f"user-cli-smoke-{uuid.uuid4()}",
        )
        study_idx = study["study_idx"]
        created_study_idxs.append(study_idx)

        # 2. biosample create — owner-bypass on
        #    require_study_access(min_tier=ADMIN); --owner-idx omitted so
        #    the CLI resolves it via whoami.
        biosample = _run_cli(
            cp_server,
            user_token,
            "biosample",
            "create",
            "--study-idx",
            str(study_idx),
            "--owner-biosample-id-field-name",
            "sample_name",
            "--owner-biosample-id-value",
            "USER-CLI-SMOKE-1",
        )
        biosample_idx = biosample["biosample_idx"]
        created_biosample_idxs.append(biosample_idx)

        # 3. sequencing-run create — no role/tier gate.
        run = _run_cli(
            cp_server,
            user_token,
            "sequencing-run",
            "create",
            "--instrument-run-id",
            f"USER-CLI-SMOKE-{uuid.uuid4()}",
            "--platform",
            "illumina",
        )
        run_idx = run["sequencing_run_idx"]
        created_sequencing_run_idxs.append(run_idx)

        # 4. sequenced-pool create — require_caller_owns_run (USER created
        #    the run in step 3).
        pool = _run_cli(
            cp_server,
            user_token,
            "sequenced-pool",
            "create",
            "--run-idx",
            str(run_idx),
        )
        pool_idx = pool["sequenced_pool_idx"]
        created_sequenced_pool_idxs.append(pool_idx)

        # 5. sequenced-sample create — require_caller_owns_pool +
        #    require_caller_has_admin_on_all_studies (owner-bypass on the
        #    primary study).
        protocol_idx = await _fetch_prep_protocol_idx(postgres_pool)
        # --pool-item-id anchors the filename-prefix rule: it must be the
        # prefix of every fastq the work-ticket in step 6 processes.
        pool_item_id = f"ITEM-{uuid.uuid4()}"
        sample = _run_cli(
            cp_server,
            user_token,
            "sequenced-sample",
            "create",
            "--run-idx",
            str(run_idx),
            "--pool-idx",
            str(pool_idx),
            "--biosample-idx",
            str(biosample_idx),
            "--prep-protocol-idx",
            str(protocol_idx),
            "--pool-item-id",
            pool_item_id,
            "--primary-study-idx",
            str(study_idx),
        )
        prep_sample_idx = sample["prep_sample_idx"]
        created_prep_sample_idxs.append(prep_sample_idx)

        # 6. ticket submit — fastq-to-parquet, prep_sample-scoped. The
        #    audience admits USER; the per-study ADMIN check passes via
        #    owner-bypass. Both fastq basenames start with the
        #    --pool-item-id from step 5, so the work-ticket POST route's
        #    filename-prefix gate admits the submission.
        fastq_fwd = f"/scratch/{pool_item_id}_R1.fastq"
        fastq_rev = f"/scratch/{pool_item_id}_R2.fastq"
        ticket = _run_cli(
            cp_server,
            user_token,
            "ticket",
            "submit",
            "--action-id",
            action_id,
            "--action-version",
            action_version,
            "--prep-sample-idx",
            str(prep_sample_idx),
            "--context-json",
            json.dumps({"fastq_path": fastq_fwd, "reverse_fastq_path": fastq_rev}),
        )
        ticket_idx = ticket["work_ticket_idx"]
        created_ticket_idxs.append(ticket_idx)
        # The POST response always reports PENDING regardless of dispatch.
        assert ticket["state"] == "pending"

        # 7. ticket status — originator-bypass; full WorkTicket record.
        status = _run_cli(
            cp_server,
            user_token,
            "ticket",
            "status",
            str(ticket_idx),
        )
        assert status["work_ticket_idx"] == ticket_idx
        assert status["originator_principal_idx"] == user_idx
        assert status["scope_target"] == {
            "kind": "prep_sample",
            "prep_sample_idx": prep_sample_idx,
        }
        assert status["action_context"] == {
            "fastq_path": fastq_fwd,
            "reverse_fastq_path": fastq_rev,
        }
        # State may have advanced (or FAILED) as the background dispatch
        # raced against the dead orchestrator — assert only that it is a
        # valid WorkTicketState.
        assert status["state"] in _WORK_TICKET_STATES
    finally:
        # FK-reverse cleanup of every row the flow created. The server
        # committed these to the shared test DB; teardown runs them
        # through the test's own pool.
        if created_ticket_idxs:
            await postgres_pool.execute(
                "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = ANY($1::bigint[])",
                created_ticket_idxs,
            )
        if created_prep_sample_idxs:
            await postgres_pool.execute(
                "DELETE FROM qiita.sequenced_sample WHERE prep_sample_idx = ANY($1::bigint[])",
                created_prep_sample_idxs,
            )
            await postgres_pool.execute(
                "DELETE FROM qiita.prep_sample_metadata WHERE prep_sample_idx = ANY($1::bigint[])",
                created_prep_sample_idxs,
            )
            await postgres_pool.execute(
                "DELETE FROM qiita.prep_sample_to_study WHERE prep_sample_idx = ANY($1::bigint[])",
                created_prep_sample_idxs,
            )
            await postgres_pool.execute(
                "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])",
                created_prep_sample_idxs,
            )
        if created_sequenced_pool_idxs:
            await postgres_pool.execute(
                "DELETE FROM qiita.sequenced_pool WHERE idx = ANY($1::bigint[])",
                created_sequenced_pool_idxs,
            )
        if created_sequencing_run_idxs:
            await postgres_pool.execute(
                "DELETE FROM qiita.sequencing_run WHERE idx = ANY($1::bigint[])",
                created_sequencing_run_idxs,
            )
        if created_biosample_idxs:
            await postgres_pool.execute(
                "DELETE FROM qiita.biosample_metadata WHERE biosample_idx = ANY($1::bigint[])",
                created_biosample_idxs,
            )
            await postgres_pool.execute(
                "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = ANY($1::bigint[])",
                created_biosample_idxs,
            )
            await postgres_pool.execute(
                "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])",
                created_biosample_idxs,
            )
        if created_study_idxs:
            await postgres_pool.execute(
                "DELETE FROM qiita.biosample_study_field WHERE study_idx = ANY($1::bigint[])",
                created_study_idxs,
            )
            # POST /study auto-grants the owner an ADMIN study_access row
            # inside the same transaction; drop it before the study.
            await postgres_pool.execute(
                "DELETE FROM qiita.study_access WHERE study_idx = ANY($1::bigint[])",
                created_study_idxs,
            )
            await postgres_pool.execute(
                "DELETE FROM qiita.study WHERE idx = ANY($1::bigint[])",
                created_study_idxs,
            )


async def test_user_cannot_author_on_study_without_admin_access(
    postgres_pool,
    cp_server,
    human_admin_session,
    regular_user_session,
):
    """Negative path: a USER with no ADMIN access to a study cannot
    create a biosample on it. The route's require_study_access gate
    returns 403; the CLI surfaces that as a non-zero exit carrying
    `http error 403`."""
    user_token = regular_user_session["token"]
    admin_idx = human_admin_session["principal_idx"]

    # A study owned by the admin. The USER gets no study_access row, so
    # their effective tier is public-by-absence -- below Tier.ADMIN.
    study_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        admin_idx,
        f"admin-owned-{uuid.uuid4()}",
    )
    try:
        result = _run_cli_expect_failure(
            cp_server,
            user_token,
            "biosample",
            "create",
            "--study-idx",
            str(study_idx),
            "--owner-biosample-id-field-name",
            "sample_name",
            "--owner-biosample-id-value",
            "DENIED-1",
        )
        assert "http error 403" in result.stderr
    finally:
        await postgres_pool.execute("DELETE FROM qiita.study WHERE idx = $1", study_idx)
