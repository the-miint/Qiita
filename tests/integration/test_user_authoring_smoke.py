"""End-to-end smoke test: a regular USER walks the full authoring CLI
flow against an in-process control plane.

Pins the Phase 2.b / 2.c widening: a non-admin user with the standard
USER role ceiling can stand up a study, biosample, sequencing-run,
sequenced-pool, sequenced-sample, then submit a fastq-to-parquet
work-ticket and read it back via GET. Each step exercises the new
per-resource auth gate (owner / caller-creator / per-study ADMIN).

Dispatch is short-circuited at the schedule_dispatch entry point —
this test verifies the AUTH path end-to-end, not the orchestrator
execution. The reference-add smoke covers full-pipeline execution.
"""

import uuid
from pathlib import Path

import httpx
import pytest
from httpx import ASGITransport

from qiita_control_plane.config import Settings as CPSettings
from qiita_control_plane.main import app as cp_app

_FASTQ_TO_PARQUET_YAML_PATH = (
    Path(__file__).parent.parent.parent
    / "workflows"
    / "fastq-to-parquet"
    / "1.0.0.yaml"
)


@pytest.fixture
async def cp_app_with_pool(postgres_pool, hmac_secret):
    """Wire the CP FastAPI app to the integration postgres pool +
    settings so its routes work in-process under ASGITransport. Mirrors
    `test_sequence_range_e2e.cp_app_with_pool`."""
    cp_app.state.pool = postgres_pool
    cp_app.state.settings = CPSettings(
        database_url="unused-in-test",
        hmac_secret_key=hmac_secret,
        data_plane_url="grpc://unused:0",
    )
    # compute_backend_client is None by default; the work-ticket POST
    # gates on it via `_require_compute_backend_client` (503 otherwise).
    # A truthy stand-in is enough — schedule_dispatch is patched below.
    cp_app.state.compute_backend_client = object()
    cp_app.state.running_dispatches = set()
    yield cp_app


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


async def _fetch_prep_protocol_idx(
    postgres_pool, name: str = "short_read_metagenomics"
) -> int:
    """The standard prep_protocol seeded by migrations is referenced by
    name from the route body; tests resolve by lookup so a renumber on
    the seed doesn't fan out to every fixture."""
    return await postgres_pool.fetchval(
        "SELECT idx FROM qiita.prep_protocol WHERE name = $1", name
    )


async def test_user_authoring_smoke_end_to_end(
    monkeypatch,
    postgres_pool,
    cp_app_with_pool,
    synced_fastq_to_parquet_action,
    regular_user_session,
):
    """As a plain USER (not admin), walk study → biosample → run → pool →
    sample → work-ticket submit → work-ticket read-back. Each step must
    return 2xx and the per-resource gate it composes must let the USER
    through under owner / caller-creator / admin-tier semantics. Final
    GET must report the USER as the originator and PENDING as state.
    """
    # Stop the background dispatch from touching an orchestrator — every
    # work-ticket POST schedules one on app.state. We assert ticket state
    # is PENDING right after submit, so the dispatch is a no-op for this
    # test's purposes.
    monkeypatch.setattr(
        "qiita_control_plane.routes.work_ticket.schedule_dispatch",
        lambda _app, _idx: None,
    )

    action_id, action_version = synced_fastq_to_parquet_action
    user_token = regular_user_session["token"]
    user_idx = regular_user_session["principal_idx"]
    headers = {"Authorization": f"Bearer {user_token}"}

    created_ticket_idxs: list[int] = []
    created_prep_sample_idxs: list[int] = []
    created_sequenced_pool_idxs: list[int] = []
    created_sequencing_run_idxs: list[int] = []
    created_biosample_idxs: list[int] = []
    created_study_idxs: list[int] = []

    transport = ASGITransport(app=cp_app_with_pool)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        try:
            # 1. POST /study — USER owns the new study (owner_idx defaults
            #    to caller server-side); satisfies the owner-bypass on every
            #    downstream require_study_access call.
            r = await client.post(
                "/api/v1/study",
                json={"title": f"user-smoke-{uuid.uuid4()}"},
                headers=headers,
            )
            assert r.status_code == 201, r.text
            study_idx = r.json()["study_idx"]
            created_study_idxs.append(study_idx)

            # 2. POST /study/{idx}/biosample — owner-bypass on
            #    require_study_access(min_tier=ADMIN) admits the study owner.
            r = await client.post(
                f"/api/v1/study/{study_idx}/biosample",
                json={
                    "owner_idx": user_idx,
                    "owner_biosample_id_field_name": "sample_name",
                    "owner_biosample_id_value": "USER-SMOKE-1",
                },
                headers=headers,
            )
            assert r.status_code == 201, r.text
            biosample_idx = r.json()["biosample_idx"]
            created_biosample_idxs.append(biosample_idx)

            # 3. POST /sequencing-run — no role/tier gate; any USER with
            #    PREP_SAMPLE_WRITE can stand up a run.
            r = await client.post(
                "/api/v1/sequencing-run",
                json={
                    "instrument_run_id": f"USER-SMOKE-{uuid.uuid4()}",
                    "platform": "illumina",
                },
                headers=headers,
            )
            assert r.status_code == 201, r.text
            run_idx = r.json()["sequencing_run_idx"]
            created_sequencing_run_idxs.append(run_idx)

            # 4. POST /sequencing-run/{r}/sequenced-pool — passes
            #    require_caller_owns_run because the user just created the run.
            r = await client.post(
                f"/api/v1/sequencing-run/{run_idx}/sequenced-pool",
                json={},
                headers=headers,
            )
            assert r.status_code == 201, r.text
            pool_idx = r.json()["sequenced_pool_idx"]
            created_sequenced_pool_idxs.append(pool_idx)

            # 5. POST /sequencing-run/{r}/sequenced-pool/{p}/sequenced-sample
            #    — passes require_caller_owns_pool (user created the pool)
            #    AND require_caller_has_admin_on_all_studies (user owns
            #    primary study by owner-bypass).
            protocol_idx = await _fetch_prep_protocol_idx(postgres_pool)
            r = await client.post(
                f"/api/v1/sequencing-run/{run_idx}/sequenced-pool/{pool_idx}/sequenced-sample",
                json={
                    "biosample_idx": biosample_idx,
                    "prep_protocol_idx": protocol_idx,
                    "owner_idx": user_idx,
                    "sequenced_pool_item_id": f"ITEM-{uuid.uuid4()}",
                    "primary_study_idx": study_idx,
                },
                headers=headers,
            )
            assert r.status_code == 201, r.text
            prep_sample_idx = r.json()["prep_sample_idx"]
            created_prep_sample_idxs.append(prep_sample_idx)

            # 6. POST /work-ticket — fastq-to-parquet, prep_sample-scoped.
            #    audience admits USER; per-study ADMIN check passes via
            #    owner-bypass on the prep_sample's one non-retired study link.
            r = await client.post(
                "/api/v1/work-ticket",
                json={
                    "action_id": action_id,
                    "action_version": action_version,
                    "scope_target": {
                        "kind": "prep_sample",
                        "prep_sample_idx": prep_sample_idx,
                    },
                    "action_context": {"fastq_path": "/scratch/user-smoke.fastq"},
                },
                headers=headers,
            )
            assert r.status_code == 202, r.text
            ticket_idx = r.json()["work_ticket_idx"]
            created_ticket_idxs.append(ticket_idx)

            # 7. GET /work-ticket/{idx} — originator-bypass; full record back.
            r = await client.get(
                f"/api/v1/work-ticket/{ticket_idx}",
                headers=headers,
            )
            assert r.status_code == 200, r.text
            body = r.json()
            assert body["work_ticket_idx"] == ticket_idx
            assert body["originator_principal_idx"] == user_idx
            assert body["state"] == "pending"
            assert body["scope_target"] == {
                "kind": "prep_sample",
                "prep_sample_idx": prep_sample_idx,
            }
            assert body["action_context"] == {"fastq_path": "/scratch/user-smoke.fastq"}
        finally:
            # FK-reverse cleanup of every row the test created. Order:
            # work_ticket → sequenced_sample → prep_sample → links →
            # sequenced_pool → sequencing_run → biosample_to_study →
            # biosample → study_access → study. The
            # biosample_metadata + biosample_study_field for the owner-id
            # field are cleaned up by RESTRICT-walking the biosample.
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
                    "DELETE FROM qiita.prep_sample_metadata"
                    " WHERE prep_sample_idx = ANY($1::bigint[])",
                    created_prep_sample_idxs,
                )
                await postgres_pool.execute(
                    "DELETE FROM qiita.prep_sample_to_study"
                    " WHERE prep_sample_idx = ANY($1::bigint[])",
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
                # POST /study auto-grants the owner an ADMIN study_access
                # row inside the same transaction; drop it before the study.
                await postgres_pool.execute(
                    "DELETE FROM qiita.study_access WHERE study_idx = ANY($1::bigint[])",
                    created_study_idxs,
                )
                await postgres_pool.execute(
                    "DELETE FROM qiita.study WHERE idx = ANY($1::bigint[])",
                    created_study_idxs,
                )
