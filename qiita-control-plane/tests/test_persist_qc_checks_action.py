"""persist-qc-checks: producer stamping, upsert semantics and all-or-nothing validation."""

import json
import secrets

import pytest
import pytest_asyncio
from qiita_common.models import (
    QcCheck,
    QcCheckState,
    QcSeverity,
    ScopeTargetKind,
    WorkTicketState,
)

from qiita_control_plane.actions.library import QcProducer, persist_qc_checks
from qiita_control_plane.repositories.prep_sample_qc_check import (
    fetch_qc_checks_by_prep_sample_idx,
    fetch_qc_verdict_by_prep_sample_idx,
)
from qiita_control_plane.testing.db_seeds import (
    delete_action_if_created,
    seed_action_if_absent,
    seed_biosample_with_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import (
    delete_idxs,
    delete_principal,
    teardown_entity_graph,
)

pytestmark = pytest.mark.db

_ACTION_ID = "persist-qc-checks-test"


@pytest_asyncio.fixture
async def seeded(postgres_pool):
    pool = postgres_pool
    principal_idx = await seed_user_principal(pool, prefix="pqc", suffix=secrets.token_hex(4))
    version = f"v-{secrets.token_hex(4)}"
    action_created = await seed_action_if_absent(
        pool, action_id=_ACTION_ID, version=version, target_kind="prep_sample"
    )
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        pool, owner_idx=principal_idx
    )
    run_idx, pool_idx, _ = await seed_sequenced_sample_subtype(
        pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=principal_idx,
        sequenced_pool_item_id=f"item-{version}",
    )
    state = {
        "pool": pool,
        "principal_idx": principal_idx,
        "version": version,
        "prep_sample_idx": prep_sample_idx,
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
        biosample_idxs=[biosample_idx],
        prep_sample_idxs=[prep_sample_idx],
    )
    await delete_idxs(pool, "sequenced_pool", pool_idx)
    await delete_idxs(pool, "sequencing_run", run_idx)
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


async def _ticket(s, *, mask_idx: int | None = None, alignment_idx: int | None = None) -> int:
    return await s["pool"].fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, prep_sample_idx, action_context, state, mask_idx, alignment_idx"
        ") VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, $6, $7, $8)"
        " RETURNING work_ticket_idx",
        _ACTION_ID,
        s["version"],
        s["principal_idx"],
        ScopeTargetKind.PREP_SAMPLE,
        s["prep_sample_idx"],
        WorkTicketState.COMPLETED,
        mask_idx,
        alignment_idx,
    )


def _entry(s, check=QcCheck.DATA_TYPE, severity=QcSeverity.INFO, **extra) -> dict:
    return {
        "prep_sample_idx": s["prep_sample_idx"],
        "check": str(check),
        "severity": str(severity),
        **extra,
    }


async def _persist(
    s, tmp_path, ticket_idx: int, entries, producer: QcProducer = QcProducer.NONE
) -> int:
    path = tmp_path / "qc_checks.json"
    path.write_text(entries if isinstance(entries, str) else json.dumps(entries))
    return await persist_qc_checks(
        s["pool"],
        work_ticket_idx=ticket_idx,
        prep_sample_idx=s["prep_sample_idx"],
        qc_checks_path=path,
        expected_producer=producer,
    )


async def _rows(s):
    return await fetch_qc_checks_by_prep_sample_idx(s["pool"], s["prep_sample_idx"])


async def _count(s) -> int:
    return len(await _rows(s))


async def test_first_write_without_producer(seeded, tmp_path):
    ticket = await _ticket(seeded)
    entry = _entry(
        seeded,
        severity=QcSeverity.FAIL,
        reason="data_type_mismatch",
        measured_value=0.25,
        threshold=0.5,
    )
    assert await _persist(seeded, tmp_path, ticket, [entry]) == 1
    (row,) = await _rows(seeded)
    assert (row["mask_idx"], row["alignment_idx"]) == (None, None)
    assert row["work_ticket_idx"] == ticket
    assert row["state"] == QcCheckState.CURRENT
    assert (row["severity"], row["reason"]) == (QcSeverity.FAIL, "data_type_mismatch")
    assert (row["measured_value"], row["threshold"]) == (0.25, 0.5)


async def test_first_write_under_a_mask_carries_mask_idx(seeded, tmp_path):
    mask_idx = await _seed_mask(seeded)
    ticket = await _ticket(seeded, mask_idx=mask_idx)
    await _persist(seeded, tmp_path, ticket, [_entry(seeded)], QcProducer.MASK)
    (row,) = await _rows(seeded)
    assert (row["mask_idx"], row["alignment_idx"]) == (mask_idx, None)


async def test_ticket_with_both_producers_stamps_alignment_only(seeded, tmp_path):
    mask_idx, alignment_idx = await _seed_mask(seeded), await _seed_alignment(seeded)
    ticket = await _ticket(seeded, mask_idx=mask_idx, alignment_idx=alignment_idx)
    await _persist(seeded, tmp_path, ticket, [_entry(seeded)], QcProducer.ALIGNMENT)
    (row,) = await _rows(seeded)
    assert (row["mask_idx"], row["alignment_idx"]) == (None, alignment_idx)


@pytest.mark.parametrize("producer", ["none", "mask", "alignment"])
async def test_rerun_overwrites_its_row(seeded, tmp_path, producer):
    kwargs = {}
    if producer == "mask":
        kwargs["mask_idx"] = await _seed_mask(seeded)
    elif producer == "alignment":
        kwargs["alignment_idx"] = await _seed_alignment(seeded)
    kind = QcProducer(producer)
    first, second = await _ticket(seeded, **kwargs), await _ticket(seeded, **kwargs)
    full = _entry(
        seeded,
        severity=QcSeverity.FAIL,
        reason="data_type_mismatch",
        measured_value=0.25,
        threshold=0.5,
    )
    await _persist(seeded, tmp_path, first, [full], kind)
    (before,) = await _rows(seeded)
    idx = await seeded["pool"].fetchval(
        "SELECT idx FROM qiita.prep_sample_qc_check WHERE prep_sample_idx = $1",
        seeded["prep_sample_idx"],
    )
    await _persist(
        seeded,
        tmp_path,
        second,
        [_entry(seeded, severity=QcSeverity.WARNING)],
        kind,
    )
    (after,) = await _rows(seeded)
    assert (
        await seeded["pool"].fetchval(
            "SELECT idx FROM qiita.prep_sample_qc_check WHERE prep_sample_idx = $1",
            seeded["prep_sample_idx"],
        )
        == idx
    )
    assert after["severity"] == QcSeverity.WARNING
    assert (after["reason"], after["measured_value"], after["threshold"]) == (None, None, None)
    assert after["work_ticket_idx"] == second
    assert after["created_at"] > before["created_at"]


async def test_rerun_returns_an_invalidated_row_to_current(seeded, tmp_path):
    ticket = await _ticket(seeded)
    await _persist(seeded, tmp_path, ticket, [_entry(seeded, severity=QcSeverity.FAIL)])
    await seeded["pool"].execute(
        "UPDATE qiita.prep_sample_qc_check SET state = $3, invalidated_at = now(),"
        " invalidated_by_idx = $1, invalidation_reason = 'test' WHERE prep_sample_idx = $2",
        seeded["principal_idx"],
        seeded["prep_sample_idx"],
        QcCheckState.INVALIDATED,
    )
    await _persist(seeded, tmp_path, await _ticket(seeded), [_entry(seeded)])
    (row,) = await _rows(seeded)
    assert row["state"] == QcCheckState.CURRENT
    assert (row["invalidated_at"], row["invalidation_reason"]) == (None, None)
    assert (
        await seeded["pool"].fetchval(
            "SELECT invalidated_by_idx FROM qiita.prep_sample_qc_check WHERE prep_sample_idx = $1",
            seeded["prep_sample_idx"],
        )
        is None
    )


async def test_a_new_mask_adds_a_row(seeded, tmp_path):
    first_mask, second_mask = await _seed_mask(seeded), await _seed_mask(seeded)
    for mask in (first_mask, second_mask):
        ticket = await _ticket(seeded, mask_idx=mask)
        await _persist(seeded, tmp_path, ticket, [_entry(seeded)], QcProducer.MASK)
    rows = await _rows(seeded)
    assert sorted(r["mask_idx"] for r in rows) == sorted([first_mask, second_mask])


async def test_verdict_is_fail_and_stays_fail_when_another_check_passes(seeded, tmp_path):
    await _persist(
        seeded,
        tmp_path,
        await _ticket(seeded),
        [
            _entry(seeded, QcCheck.DATA_TYPE, QcSeverity.FAIL),
            _entry(seeded, QcCheck.COMPOSITION, QcSeverity.WARNING),
        ],
    )
    await _persist(
        seeded,
        tmp_path,
        await _ticket(seeded),
        [_entry(seeded, QcCheck.HOST_FRACTION, QcSeverity.INFO)],
    )
    verdict = await fetch_qc_verdict_by_prep_sample_idx(seeded["pool"], seeded["prep_sample_idx"])
    assert (verdict["severity"], verdict["check"]) == (QcSeverity.FAIL, QcCheck.DATA_TYPE)


async def test_verdict_warning_names_its_check(seeded, tmp_path):
    await _persist(
        seeded,
        tmp_path,
        await _ticket(seeded),
        [
            _entry(seeded, QcCheck.DATA_TYPE, QcSeverity.INFO),
            _entry(seeded, QcCheck.COMPOSITION, QcSeverity.WARNING),
        ],
    )
    verdict = await fetch_qc_verdict_by_prep_sample_idx(seeded["pool"], seeded["prep_sample_idx"])
    assert (verdict["severity"], verdict["check"]) == (QcSeverity.WARNING, QcCheck.COMPOSITION)


async def test_verdict_is_info_when_every_check_is_info(seeded, tmp_path):
    await _persist(
        seeded,
        tmp_path,
        await _ticket(seeded),
        [
            _entry(seeded, QcCheck.DATA_TYPE, QcSeverity.INFO),
            _entry(seeded, QcCheck.COMPOSITION, QcSeverity.INFO),
        ],
    )
    verdict = await fetch_qc_verdict_by_prep_sample_idx(seeded["pool"], seeded["prep_sample_idx"])
    assert verdict["severity"] == QcSeverity.INFO


async def test_empty_array_rejected_and_existing_row_left_alone(seeded, tmp_path):
    await _persist(seeded, tmp_path, await _ticket(seeded), [_entry(seeded)])
    with pytest.raises(ValueError, match="empty"):
        await _persist(seeded, tmp_path, await _ticket(seeded), [])
    (row,) = await _rows(seeded)
    assert row["severity"] == QcSeverity.INFO


async def test_entry_for_another_prep_sample_rejected(seeded, tmp_path):
    entry = _entry(seeded) | {"prep_sample_idx": seeded["prep_sample_idx"] + 10_000_000}
    with pytest.raises(ValueError, match="prep_sample_idx"):
        await _persist(seeded, tmp_path, await _ticket(seeded), [_entry(seeded), entry])
    assert await _count(seeded) == 0


@pytest.mark.parametrize(
    ("entries", "match"),
    [
        ([{"check": "nope", "severity": "info"}], "check"),
        ([{"check": "data_type", "severity": "critical"}], "severity"),
        ([{"check": "data_type", "severity": "fail", "reason": "high_unknown"}], "high_unknown"),
        ([{"check": "data_type", "severity": "info", "mask_idx": 1}], "mask_idx"),
        ([{"check": "data_type", "severity": "info", "measured_value": float("nan")}], "finite"),
        ([{"check": "data_type", "severity": "info", "threshold": float("inf")}], "finite"),
        ([{"check": "data_type", "severity": "info", "prep_sample_idx": True}], "integer"),
        ([{"check": "data_type", "severity": "info", "prep_sample_idx": "7"}], "integer"),
        ([{"check": "data_type", "severity": "info", "measured_value": True}], "number"),
        ([{"check": "data_type", "severity": "info", "threshold": "1.5"}], "number"),
        (
            [
                {"check": "data_type", "severity": "info"},
                {"check": "data_type", "severity": "fail"},
            ],
            "duplicate",
        ),
    ],
)
async def test_bad_entry_rejected_and_nothing_written(seeded, tmp_path, entries, match):
    entries = [{"prep_sample_idx": seeded["prep_sample_idx"], **e} for e in entries]
    with pytest.raises(ValueError, match=match):
        await _persist(seeded, tmp_path, await _ticket(seeded), entries)
    assert await _count(seeded) == 0


async def test_one_bad_entry_voids_the_good_ones(seeded, tmp_path):
    entries = [_entry(seeded), _entry(seeded, QcCheck.COMPOSITION, reason="data_type_mismatch")]
    with pytest.raises(ValueError, match="data_type_mismatch"):
        await _persist(seeded, tmp_path, await _ticket(seeded), entries)
    assert await _count(seeded) == 0


async def test_problem_list_is_capped(seeded, tmp_path):
    entries = [_entry(seeded)] * 25
    with pytest.raises(ValueError) as exc:
        await _persist(seeded, tmp_path, await _ticket(seeded), entries)
    assert exc.value.args[0].count("duplicate") == 20
    assert "4 more" in exc.value.args[0]


async def test_missing_file_raises(seeded, tmp_path):
    with pytest.raises(FileNotFoundError):
        await persist_qc_checks(
            seeded["pool"],
            work_ticket_idx=await _ticket(seeded),
            prep_sample_idx=seeded["prep_sample_idx"],
            qc_checks_path=tmp_path / "absent.json",
            expected_producer=QcProducer.NONE,
        )


async def test_prep_sample_without_sequenced_sample_rejected(seeded, tmp_path):
    ticket = await _ticket(seeded)
    await seeded["pool"].execute(
        "DELETE FROM qiita.sequenced_sample WHERE prep_sample_idx = $1", seeded["prep_sample_idx"]
    )
    with pytest.raises(RuntimeError, match="sequenced_sample"):
        await _persist(seeded, tmp_path, ticket, [_entry(seeded)])
    assert await _count(seeded) == 0


async def test_missing_ticket_row_rejected(seeded, tmp_path):
    with pytest.raises(RuntimeError, match="work_ticket"):
        await _persist(seeded, tmp_path, 2_000_000_000, [_entry(seeded)])
    assert await _count(seeded) == 0


async def test_producerless_run_ignores_the_ticket_producer(seeded, tmp_path):
    ticket = await _ticket(seeded, mask_idx=await _seed_mask(seeded))
    await _persist(seeded, tmp_path, ticket, [_entry(seeded)], QcProducer.NONE)
    (row,) = await _rows(seeded)
    assert (row["mask_idx"], row["alignment_idx"]) == (None, None)


@pytest.mark.parametrize("producer", [QcProducer.MASK, QcProducer.ALIGNMENT])
async def test_detached_producer_rejected_and_nothing_written(seeded, tmp_path, producer):
    if producer is QcProducer.MASK:
        idx = await _seed_mask(seeded)
        ticket = await _ticket(seeded, mask_idx=idx)
        await seeded["pool"].execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", idx)
    else:
        idx = await _seed_alignment(seeded)
        ticket = await _ticket(seeded, alignment_idx=idx)
        await seeded["pool"].execute(
            "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", idx
        )
    with pytest.raises(RuntimeError, match=f"{producer.value}_idx"):
        await _persist(seeded, tmp_path, ticket, [_entry(seeded)], producer)
    assert await _count(seeded) == 0


async def test_missing_expected_producer_column_rejected(seeded, tmp_path):
    with pytest.raises(RuntimeError, match="alignment_idx"):
        await _persist(
            seeded, tmp_path, await _ticket(seeded), [_entry(seeded)], QcProducer.ALIGNMENT
        )
