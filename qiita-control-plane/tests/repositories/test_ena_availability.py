"""Tests for the ENA re-import availability-check repository functions:
`fetch_held_ena_run_accessions_for_study`, `update_sequenced_sample_ena_status`,
and `fetch_non_terminal_work_tickets_for_sequenced_sample`
(`repositories.sequenced_sample`), plus `fetch_study_idx_by_either_ena_accession`
(`repositories.study`).

Each test seeds and asserts inside one rolled-back transaction.
"""

import secrets

import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, SystemRole

from qiita_control_plane.repositories._sample_helpers import insert_entity_to_study
from qiita_control_plane.repositories.biosample import insert_biosample
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC
from qiita_control_plane.repositories.prep_sample import insert_prep_sample
from qiita_control_plane.repositories.sequenced_sample import (
    fetch_held_ena_run_accessions_for_study,
    fetch_non_terminal_work_tickets_for_sequenced_sample,
    insert_sequenced_sample,
    update_sequenced_sample_ena_status,
)
from qiita_control_plane.repositories.sequencing_run import (
    insert_sequenced_pool,
    insert_sequencing_run,
)
from qiita_control_plane.repositories.study import fetch_study_idx_by_either_ena_accession

pytestmark = pytest.mark.db


def _suffix(label: str) -> str:
    return f"{label}-{secrets.token_hex(4)}"


async def _create_user(conn) -> int:
    pidx = await conn.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx",
        _suffix("user"),
        SystemRole.USER,
        SYSTEM_PRINCIPAL_IDX,
    )
    await conn.execute(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
        pidx,
        f"{_suffix('u')}@example.com",
    )
    return pidx


async def _create_study(conn, owner, **cols) -> int:
    columns = {"owner_idx": owner, "title": _suffix("study"), "created_by_idx": owner, **cols}
    keys = list(columns)
    placeholders = ", ".join(f"${i + 1}" for i in range(len(keys)))
    return await conn.fetchval(
        f"INSERT INTO qiita.study ({', '.join(keys)}) VALUES ({placeholders}) RETURNING idx",
        *[columns[k] for k in keys],
    )


async def _seed_held_run(
    conn, owner, *, study_idx: int, ena_run_accession: str
) -> tuple[int, int, int]:
    """Return (prep_sample_idx, sequenced_sample_idx, sequenced_pool_idx)."""
    biosample_idx = await insert_biosample(conn, owner_idx=owner, created_by_idx=owner)
    await insert_entity_to_study(
        conn,
        spec=BIOSAMPLE_METADATA_SPEC,
        entity_idx=biosample_idx,
        study_idx=study_idx,
        created_by_idx=owner,
    )
    protocol_idx = await conn.fetchval(
        "SELECT idx FROM qiita.prep_protocol WHERE name = 'short_read_metagenomics'"
    )
    run_idx, _ = await insert_sequencing_run(
        conn, instrument_run_id=_suffix("RUN"), platform="illumina", created_by_idx=owner
    )
    pool_idx, _ = await insert_sequenced_pool(
        conn, sequencing_run_idx=run_idx, created_by_idx=owner
    )
    ps_idx = await insert_prep_sample(
        conn,
        biosample_idx=biosample_idx,
        owner_idx=owner,
        prep_protocol_idx=protocol_idx,
        processing_kind="sequenced",
        created_by_idx=owner,
    )
    await conn.execute(
        "INSERT INTO qiita.prep_sample_to_study (prep_sample_idx, study_idx, created_by_idx)"
        " VALUES ($1, $2, $3)",
        ps_idx,
        study_idx,
        owner,
    )
    ss_idx = await insert_sequenced_sample(
        conn,
        prep_sample_idx=ps_idx,
        sequenced_pool_idx=pool_idx,
        sequenced_pool_item_id=_suffix("ITEM"),
        created_by_idx=owner,
        ena_run_accession=ena_run_accession,
    )
    return ps_idx, ss_idx, pool_idx


async def test_fetch_held_ena_run_accessions_for_study_returns_regardless_of_status(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            study_idx = await _create_study(conn, owner)
            healthy_acc = _suffix("SRR")
            flagged_acc = _suffix("SRR")
            await _seed_held_run(conn, owner, study_idx=study_idx, ena_run_accession=healthy_acc)
            _, flagged_ss, _ = await _seed_held_run(
                conn, owner, study_idx=study_idx, ena_run_accession=flagged_acc
            )
            await update_sequenced_sample_ena_status(
                conn, ena_run_accession=flagged_acc, ena_status="suppressed"
            )

            held = await fetch_held_ena_run_accessions_for_study(conn, study_idx=study_idx)
            by_accession = {r["ena_run_accession"]: r for r in held}
            assert set(by_accession) == {healthy_acc, flagged_acc}
            assert by_accession[healthy_acc]["ena_status"] is None
            assert by_accession[flagged_acc]["ena_status"] == "suppressed"
            assert by_accession[flagged_acc]["sequenced_sample_idx"] == flagged_ss
        finally:
            await tr.rollback()


async def test_fetch_held_ena_run_accessions_for_study_excludes_retired(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            study_idx = await _create_study(conn, owner)
            acc = _suffix("SRR")
            ps_idx, _, _ = await _seed_held_run(
                conn, owner, study_idx=study_idx, ena_run_accession=acc
            )
            await conn.execute(
                "UPDATE qiita.prep_sample SET retired = true,"
                " retired_by_idx = $2, retired_at = now()"
                " WHERE idx = $1",
                ps_idx,
                owner,
            )

            held = await fetch_held_ena_run_accessions_for_study(conn, study_idx=study_idx)
            assert held == []
        finally:
            await tr.rollback()


async def test_update_sequenced_sample_ena_status_sets_and_clears(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            study_idx = await _create_study(conn, owner)
            acc = _suffix("SRR")
            await _seed_held_run(conn, owner, study_idx=study_idx, ena_run_accession=acc)

            await update_sequenced_sample_ena_status(
                conn, ena_run_accession=acc, ena_status="withdrawn"
            )
            row = await conn.fetchrow(
                "SELECT ena_status, ena_availability_checked_at FROM qiita.sequenced_sample"
                " WHERE ena_run_accession = $1",
                acc,
            )
            assert row["ena_status"] == "withdrawn"
            assert row["ena_availability_checked_at"] is not None

            await update_sequenced_sample_ena_status(conn, ena_run_accession=acc, ena_status=None)
            row = await conn.fetchrow(
                "SELECT ena_status FROM qiita.sequenced_sample WHERE ena_run_accession = $1", acc
            )
            assert row["ena_status"] is None
        finally:
            await tr.rollback()


async def test_fetch_non_terminal_work_tickets_matches_prep_sample_or_pool(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            study_idx = await _create_study(conn, owner)
            acc = _suffix("SRR")
            ps_idx, _, pool_idx = await _seed_held_run(
                conn, owner, study_idx=study_idx, ena_run_accession=acc
            )
            action_id, action_version = _suffix("action"), "1.0.0"
            await conn.execute(
                "INSERT INTO qiita.action"
                " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
                "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
                " VALUES ($1, $2, 'prep_sample', '{}'::text[], $3::jsonb, '{}'::jsonb, '[]'::jsonb,"
                "         1, 1, '1 minute', NULL, NULL)",
                action_id,
                action_version,
                '{"service": false, "human_roles": ["system_admin"]}',
            )

            pool_ticket = await conn.fetchval(
                "INSERT INTO qiita.work_ticket"
                " (action_id, action_version, originator_principal_idx,"
                "  scope_target_kind, sequenced_pool_idx, state)"
                " VALUES ($1, $2, $3, 'sequenced_pool', $4, 'queued') RETURNING work_ticket_idx",
                action_id,
                action_version,
                owner,
                pool_idx,
            )
            sample_ticket = await conn.fetchval(
                "INSERT INTO qiita.work_ticket"
                " (action_id, action_version, originator_principal_idx,"
                "  scope_target_kind, prep_sample_idx, state)"
                " VALUES ($1, $2, $3, 'prep_sample', $4, 'processing') RETURNING work_ticket_idx",
                action_id,
                action_version,
                owner,
                ps_idx,
            )
            terminal_ticket = await conn.fetchval(
                "INSERT INTO qiita.work_ticket"
                " (action_id, action_version, originator_principal_idx,"
                "  scope_target_kind, prep_sample_idx, state)"
                " VALUES ($1, $2, $3, 'prep_sample', $4, 'completed') RETURNING work_ticket_idx",
                action_id,
                action_version,
                owner,
                ps_idx,
            )

            rows = await fetch_non_terminal_work_tickets_for_sequenced_sample(
                conn, prep_sample_idx=ps_idx, sequenced_pool_idx=pool_idx
            )
            idxs = {r["work_ticket_idx"] for r in rows}
            assert idxs == {pool_ticket, sample_ticket}
            assert terminal_ticket not in idxs
        finally:
            await tr.rollback()


async def test_fetch_study_idx_by_either_ena_accession(postgres_pool):
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            owner = await _create_user(conn)
            bioproject = _suffix("PRJNA")
            secondary = _suffix("SRP")
            study_idx = await _create_study(
                conn, owner, bioproject_accession=bioproject, ena_study_accession=secondary
            )

            assert await fetch_study_idx_by_either_ena_accession(conn, bioproject) == study_idx
            assert await fetch_study_idx_by_either_ena_accession(conn, secondary) == study_idx
            assert await fetch_study_idx_by_either_ena_accession(conn, _suffix("PRJNA")) is None
        finally:
            await tr.rollback()
