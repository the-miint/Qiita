"""Schema invariants for qiita.prep_sample_qc_check and the qiita.qc_verdict view."""

import re
import secrets

import asyncpg
import pytest
import pytest_asyncio
from qiita_common.models import (
    MaskDefinitionStatus,
    QcCheck,
    QcCheckState,
    QcSeverity,
    WorkTicketState,
)

from qiita_control_plane.testing.db_seeds import (
    delete_action_if_created,
    seed_action_if_absent,
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import delete_principal, teardown_entity_graph

pytestmark = pytest.mark.db

_ACTION_ID = "qc-check-schema-test"

_INSERT = (
    "INSERT INTO qiita.prep_sample_qc_check"
    ' (prep_sample_idx, "check", severity, reason, mask_idx, alignment_idx,'
    "  work_ticket_idx, state, invalidated_at, invalidated_by_idx, invalidation_reason)"
    " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)"
)

_DEPRECATE_MASK = (
    "UPDATE qiita.mask_definition SET status = $3, deprecated_at = now(),"
    " deprecated_by_idx = $2, deprecation_reason = 'test' WHERE mask_idx = $1"
)
_INVALIDATE_MASK_SAMPLE = (
    "UPDATE qiita.mask_sample SET state = 'invalidated', invalidated_at = now(),"
    " invalidated_by_idx = $3, invalidation_reason = 'test'"
    " WHERE mask_idx = $1 AND prep_sample_idx = $2"
)
_INVALIDATE_CHECK = (
    "UPDATE qiita.prep_sample_qc_check SET state = $3, invalidated_at = now(),"
    " invalidated_by_idx = $2, invalidation_reason = 'test' WHERE idx = $1"
)


async def _seed_prep_sample_and_ticket(pool, principal_idx: int, version: str):
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        pool, owner_idx=principal_idx
    )
    ticket_idx = await pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, prep_sample_idx, action_context, state"
        ") VALUES ($1, $2, $3, 'prep_sample', $4, '{}'::jsonb, $5)"
        " RETURNING work_ticket_idx",
        _ACTION_ID,
        version,
        principal_idx,
        prep_sample_idx,
        WorkTicketState.COMPLETED,
    )
    return biosample_idx, prep_sample_idx, ticket_idx


@pytest_asyncio.fixture
async def seeded(postgres_pool):
    pool = postgres_pool
    principal_idx = await seed_user_principal(pool, prefix="qcchk", suffix=secrets.token_hex(4))
    version = f"v-{secrets.token_hex(4)}"
    action_created = await seed_action_if_absent(
        pool, action_id=_ACTION_ID, version=version, target_kind="prep_sample"
    )
    biosample_idx, prep_sample_idx, ticket_idx = await _seed_prep_sample_and_ticket(
        pool, principal_idx, version
    )
    state = {
        "pool": pool,
        "principal_idx": principal_idx,
        "version": version,
        "prep_sample_idx": prep_sample_idx,
        "ticket_idx": ticket_idx,
        "biosamples": [biosample_idx],
        "prep_samples": [prep_sample_idx],
        "masks": [],
        "alignments": [],
    }
    yield state
    await pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        _ACTION_ID,
        version,
    )
    await pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])", state["masks"]
    )
    await pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = ANY($1::bigint[])",
        state["alignments"],
    )
    await teardown_entity_graph(
        pool,
        study_idxs=[],
        biosample_idxs=state["biosamples"],
        prep_sample_idxs=state["prep_samples"],
    )
    await delete_action_if_created(
        pool, action_id=_ACTION_ID, version=version, created=action_created
    )
    await delete_principal(pool, [principal_idx])


async def _seed_mask(s) -> int:
    mask_idx = await s["pool"].fetchval(
        "INSERT INTO qiita.mask_definition"
        " (params_hash, filter_workflow, filter_version, params, created_by_idx)"
        " VALUES ($1, 'read-mask', '1.0.0', '{}'::jsonb, $2) RETURNING mask_idx",
        secrets.token_bytes(32),
        s["principal_idx"],
    )
    s["masks"].append(mask_idx)
    return mask_idx


async def _seed_alignment(s) -> int:
    alignment_idx = await s["pool"].fetchval(
        "INSERT INTO qiita.alignment_definition (params_hash, params, created_by_idx)"
        " VALUES ($1, '{}'::jsonb, $2) RETURNING alignment_idx",
        secrets.token_bytes(32),
        s["principal_idx"],
    )
    s["alignments"].append(alignment_idx)
    return alignment_idx


async def _seed_mask_sample(
    s, mask_idx: int, state: str = "completed", prep_sample_idx: int | None = None
) -> None:
    await s["pool"].execute(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state) VALUES ($1, $2, $3)",
        mask_idx,
        prep_sample_idx or s["prep_sample_idx"],
        state,
    )


async def _seed_alignment_sample(
    s, alignment_idx: int, state: str = "completed", prep_sample_idx: int | None = None
) -> None:
    await s["pool"].execute(
        "INSERT INTO qiita.alignment_sample (alignment_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, $3)",
        alignment_idx,
        prep_sample_idx or s["prep_sample_idx"],
        state,
    )


async def _add(
    s,
    check: QcCheck,
    severity: QcSeverity,
    *,
    prep_sample_idx: int | None = None,
    reason: str | None = None,
    mask_idx: int | None = None,
    alignment_idx: int | None = None,
    state: QcCheckState = QcCheckState.CURRENT,
    invalidated_at=None,
    invalidated_by_idx: int | None = None,
    invalidation_reason: str | None = None,
) -> int:
    return await s["pool"].fetchval(
        _INSERT + " RETURNING idx",
        prep_sample_idx or s["prep_sample_idx"],
        check,
        severity,
        reason,
        mask_idx,
        alignment_idx,
        s["ticket_idx"],
        state,
        invalidated_at,
        invalidated_by_idx,
        invalidation_reason,
    )


async def _verdict(s, prep_sample_idx: int | None = None):
    row = await s["pool"].fetchrow(
        'SELECT severity, "check" FROM qiita.qc_verdict WHERE prep_sample_idx = $1',
        prep_sample_idx or s["prep_sample_idx"],
    )
    return None if row is None else (row["severity"], row["check"])


@pytest.mark.parametrize(
    ("conname", "enum"),
    [
        ("prep_sample_qc_check_check_values", QcCheck),
        ("prep_sample_qc_check_severity_values", QcSeverity),
        ("prep_sample_qc_check_state_values", QcCheckState),
    ],
)
async def test_strenum_matches_check_list(postgres_pool, conname, enum):
    definition = await postgres_pool.fetchval(
        "SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c"
        "  JOIN pg_class t ON t.oid = c.conrelid"
        "  JOIN pg_namespace n ON n.oid = t.relnamespace"
        " WHERE c.conname = $1 AND n.nspname = 'qiita' AND t.relname = 'prep_sample_qc_check'",
        conname,
    )
    assert set(re.findall(r"'([a-z_]+)'", definition)) == {e.value for e in enum}


@pytest.mark.parametrize(
    ("column", "bad"),
    [
        ("check", "not_a_check"),
        ("severity", "critical"),
        ("state", "pending"),
        ("reason", "Not-Snake"),
        ("reason", "1_leading_digit"),
        ("reason", ""),
    ],
)
async def test_value_outside_check_rejected(seeded, column, bad):
    kwargs = {"check": QcCheck.DATA_TYPE, "severity": QcSeverity.FAIL, column: bad}
    with pytest.raises(asyncpg.CheckViolationError):
        await _add(seeded, **kwargs)


async def test_null_reason_accepted(seeded):
    await _add(seeded, QcCheck.DATA_TYPE, QcSeverity.INFO, reason=None)


async def test_snake_case_reason_accepted(seeded):
    await _add(seeded, QcCheck.DATA_TYPE, QcSeverity.FAIL, reason="data_type_mismatch")


async def test_both_producers_rejected(seeded):
    mask_idx, alignment_idx = await _seed_mask(seeded), await _seed_alignment(seeded)
    with pytest.raises(asyncpg.CheckViolationError):
        await _add(
            seeded,
            QcCheck.DATA_TYPE,
            QcSeverity.FAIL,
            mask_idx=mask_idx,
            alignment_idx=alignment_idx,
        )


@pytest.mark.parametrize("producer", ["none", "mask", "alignment"])
async def test_second_row_under_same_key_refused(seeded, producer):
    kwargs = {}
    if producer == "mask":
        kwargs["mask_idx"] = await _seed_mask(seeded)
    elif producer == "alignment":
        kwargs["alignment_idx"] = await _seed_alignment(seeded)
    await _add(seeded, QcCheck.COMPOSITION, QcSeverity.INFO, **kwargs)
    with pytest.raises(asyncpg.UniqueViolationError):
        await _add(seeded, QcCheck.COMPOSITION, QcSeverity.FAIL, **kwargs)


async def test_rerun_after_invalidation_is_refused_as_a_second_row(seeded):
    idx = await _add(seeded, QcCheck.CLASSIFIER, QcSeverity.WARNING)
    await seeded["pool"].execute(
        _INVALIDATE_CHECK, idx, seeded["principal_idx"], QcCheckState.INVALIDATED
    )
    with pytest.raises(asyncpg.UniqueViolationError):
        await _add(seeded, QcCheck.CLASSIFIER, QcSeverity.INFO)


async def test_same_check_under_different_producers_allowed(seeded):
    mask_idx = await _seed_mask(seeded)
    await _add(seeded, QcCheck.COMPOSITION, QcSeverity.INFO)
    await _add(seeded, QcCheck.COMPOSITION, QcSeverity.INFO, mask_idx=mask_idx)


@pytest.mark.parametrize("n_fields", [0, 1, 2])
async def test_invalidated_row_without_all_fields_rejected(seeded, n_fields):
    fields = {
        "invalidated_at": await seeded["pool"].fetchval("SELECT now()"),
        "invalidated_by_idx": seeded["principal_idx"],
        "invalidation_reason": "why",
    }
    with pytest.raises(asyncpg.CheckViolationError):
        await _add(
            seeded,
            QcCheck.DATA_TYPE,
            QcSeverity.FAIL,
            state=QcCheckState.INVALIDATED,
            **dict(list(fields.items())[:n_fields]),
        )


@pytest.mark.parametrize("n_fields", [1, 2, 3])
async def test_current_row_with_invalidation_fields_set_rejected(seeded, n_fields):
    fields = {
        "invalidated_at": await seeded["pool"].fetchval("SELECT now()"),
        "invalidated_by_idx": seeded["principal_idx"],
        "invalidation_reason": "why",
    }
    with pytest.raises(asyncpg.CheckViolationError):
        await _add(
            seeded,
            QcCheck.DATA_TYPE,
            QcSeverity.FAIL,
            **dict(list(fields.items())[:n_fields]),
        )


async def test_blank_invalidation_reason_rejected(seeded):
    idx = await _add(seeded, QcCheck.DATA_TYPE, QcSeverity.FAIL)
    with pytest.raises(asyncpg.CheckViolationError):
        await seeded["pool"].execute(
            "UPDATE qiita.prep_sample_qc_check SET state = $3,"
            " invalidated_at = now(), invalidated_by_idx = $2, invalidation_reason = '  '"
            " WHERE idx = $1",
            idx,
            seeded["principal_idx"],
            QcCheckState.INVALIDATED,
        )


async def test_verdict_is_worst_severity_and_names_its_check(seeded):
    pool = seeded["pool"]
    await _add(seeded, QcCheck.DATA_TYPE, QcSeverity.INFO)
    await _add(seeded, QcCheck.COMPOSITION, QcSeverity.WARNING)
    await _add(seeded, QcCheck.HOST_FRACTION, QcSeverity.FAIL)
    assert await _verdict(seeded) == (QcSeverity.FAIL, QcCheck.HOST_FRACTION)

    biosample_idx, other_ps, _ = await _seed_prep_sample_and_ticket(
        pool, seeded["principal_idx"], seeded["version"]
    )
    seeded["biosamples"].append(biosample_idx)
    seeded["prep_samples"].append(other_ps)
    await _add(seeded, QcCheck.DATA_TYPE, QcSeverity.INFO, prep_sample_idx=other_ps)
    await _add(seeded, QcCheck.COMPOSITION, QcSeverity.WARNING, prep_sample_idx=other_ps)
    assert await _verdict(seeded, other_ps) == (QcSeverity.WARNING, QcCheck.COMPOSITION)


async def test_verdict_tie_goes_to_first_check_by_name(seeded):
    await _add(seeded, QcCheck.SOURCE_TRACKER, QcSeverity.FAIL)
    await _add(seeded, QcCheck.CLASSIFIER, QcSeverity.FAIL)
    assert await _verdict(seeded) == (QcSeverity.FAIL, QcCheck.CLASSIFIER)


async def test_verdict_absent_without_rows(seeded):
    assert await _verdict(seeded) is None


async def test_verdict_drops_row_whose_mask_is_deprecated(seeded):
    mask_idx = await _seed_mask(seeded)
    await _seed_mask_sample(seeded, mask_idx)
    await _add(seeded, QcCheck.DATA_TYPE, QcSeverity.FAIL, mask_idx=mask_idx)
    await _add(seeded, QcCheck.COMPOSITION, QcSeverity.INFO)
    assert await _verdict(seeded) == (QcSeverity.FAIL, QcCheck.DATA_TYPE)
    await seeded["pool"].execute(
        _DEPRECATE_MASK, mask_idx, seeded["principal_idx"], MaskDefinitionStatus.DEPRECATED
    )
    assert await _verdict(seeded) == (QcSeverity.INFO, QcCheck.COMPOSITION)


async def test_verdict_drops_row_whose_mask_sample_is_invalidated(seeded):
    mask_idx = await _seed_mask(seeded)
    await _seed_mask_sample(seeded, mask_idx)
    await _add(seeded, QcCheck.DATA_TYPE, QcSeverity.FAIL, mask_idx=mask_idx)
    await _add(seeded, QcCheck.COMPOSITION, QcSeverity.INFO)
    await seeded["pool"].execute(
        _INVALIDATE_MASK_SAMPLE,
        mask_idx,
        seeded["prep_sample_idx"],
        seeded["principal_idx"],
    )
    assert await _verdict(seeded) == (QcSeverity.INFO, QcCheck.COMPOSITION)


async def test_verdict_drops_mask_row_without_a_completed_mask_sample(seeded):
    no_gate, pending = await _seed_mask(seeded), await _seed_mask(seeded)
    await _seed_mask_sample(seeded, pending, state="pending")
    await _add(seeded, QcCheck.DATA_TYPE, QcSeverity.FAIL, mask_idx=no_gate)
    await _add(seeded, QcCheck.COMPOSITION, QcSeverity.FAIL, mask_idx=pending)
    await _add(seeded, QcCheck.HOST_FRACTION, QcSeverity.INFO)
    assert await _verdict(seeded) == (QcSeverity.INFO, QcCheck.HOST_FRACTION)


@pytest.mark.parametrize("producer", ["mask", "alignment"])
async def test_verdict_ignores_a_gate_row_of_another_prep_sample(seeded, producer):
    biosample_idx, other_ps, _ = await _seed_prep_sample_and_ticket(
        seeded["pool"], seeded["principal_idx"], seeded["version"]
    )
    seeded["biosamples"].append(biosample_idx)
    seeded["prep_samples"].append(other_ps)
    if producer == "mask":
        mask_idx = await _seed_mask(seeded)
        await _seed_mask_sample(seeded, mask_idx, prep_sample_idx=other_ps)
        await _add(seeded, QcCheck.HOST_FRACTION, QcSeverity.FAIL, mask_idx=mask_idx)
    else:
        alignment_idx = await _seed_alignment(seeded)
        await _seed_alignment_sample(seeded, alignment_idx, prep_sample_idx=other_ps)
        await _add(seeded, QcCheck.CLASSIFIER, QcSeverity.FAIL, alignment_idx=alignment_idx)
    assert await _verdict(seeded) is None


async def test_verdict_drops_alignment_row_without_a_completed_alignment_sample(seeded):
    no_gate, pending = await _seed_alignment(seeded), await _seed_alignment(seeded)
    await _seed_alignment_sample(seeded, pending, state="pending")
    await _add(seeded, QcCheck.DATA_TYPE, QcSeverity.FAIL, alignment_idx=no_gate)
    await _add(seeded, QcCheck.COMPOSITION, QcSeverity.FAIL, alignment_idx=pending)
    await _add(seeded, QcCheck.HOST_FRACTION, QcSeverity.INFO)
    assert await _verdict(seeded) == (QcSeverity.INFO, QcCheck.HOST_FRACTION)


async def test_verdict_keeps_alignment_and_unproduced_rows_when_mask_row_drops(seeded):
    mask_idx, alignment_idx = await _seed_mask(seeded), await _seed_alignment(seeded)
    await _seed_alignment_sample(seeded, alignment_idx)
    await _add(seeded, QcCheck.DATA_TYPE, QcSeverity.FAIL, mask_idx=mask_idx)
    await _add(seeded, QcCheck.COMPOSITION, QcSeverity.WARNING, alignment_idx=alignment_idx)
    await _add(seeded, QcCheck.HOST_FRACTION, QcSeverity.INFO)
    assert await _verdict(seeded) == (QcSeverity.WARNING, QcCheck.COMPOSITION)

    await _add(seeded, QcCheck.READ_RETENTION, QcSeverity.FAIL)
    assert await _verdict(seeded) == (QcSeverity.FAIL, QcCheck.READ_RETENTION)


@pytest.mark.parametrize("producer", ["none", "mask", "alignment"])
async def test_verdict_drops_invalidated_row_of_each_producer_kind(seeded, producer):
    kwargs = {}
    if producer == "mask":
        kwargs["mask_idx"] = await _seed_mask(seeded)
        await _seed_mask_sample(seeded, kwargs["mask_idx"])
    elif producer == "alignment":
        kwargs["alignment_idx"] = await _seed_alignment(seeded)
        await _seed_alignment_sample(seeded, kwargs["alignment_idx"])
    failing = await _add(seeded, QcCheck.DATA_TYPE, QcSeverity.FAIL, **kwargs)
    await _add(seeded, QcCheck.COMPOSITION, QcSeverity.INFO)
    assert await _verdict(seeded) == (QcSeverity.FAIL, QcCheck.DATA_TYPE)
    await seeded["pool"].execute(
        _INVALIDATE_CHECK, failing, seeded["principal_idx"], QcCheckState.INVALIDATED
    )
    assert await _verdict(seeded) == (QcSeverity.INFO, QcCheck.COMPOSITION)


async def test_deleting_mask_alignment_and_ticket_cascades_to_rows(seeded):
    pool = seeded["pool"]
    mask_idx, alignment_idx = await _seed_mask(seeded), await _seed_alignment(seeded)
    await _add(seeded, QcCheck.DATA_TYPE, QcSeverity.FAIL, mask_idx=mask_idx)
    await _add(seeded, QcCheck.COMPOSITION, QcSeverity.FAIL, alignment_idx=alignment_idx)
    await _add(seeded, QcCheck.HOST_FRACTION, QcSeverity.FAIL)
    await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
    await pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
    )
    remaining = await pool.fetch(
        'SELECT "check" FROM qiita.prep_sample_qc_check WHERE prep_sample_idx = $1',
        seeded["prep_sample_idx"],
    )
    assert [r["check"] for r in remaining] == [QcCheck.HOST_FRACTION]
    await pool.execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", seeded["ticket_idx"]
    )
    assert (
        await pool.fetchval(
            "SELECT count(*) FROM qiita.prep_sample_qc_check WHERE prep_sample_idx = $1",
            seeded["prep_sample_idx"],
        )
        == 0
    )
