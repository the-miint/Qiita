"""DB tests for the de novo alignment resolver and its terminal gate flip.

Three things a de novo ticket must have settled before its first step runs, and one
after its last: the assembly subject exists, the identity is minted and idempotent,
both ticket columns and the gate row are written, and the gate is flipped only over
rows that are already registered.

The signal, the params shape and the workflow wiring are string-level and live in
`test_align_denovo_submit.py`; what needs a database is everything below.
"""

import secrets

import pytest
import pytest_asyncio
from qiita_common.backend_failure import StepNoData

from qiita_control_plane.actions.library import finalize_alignment_sample_gate
from qiita_control_plane.repositories.assembly import (
    create_assembly_sample_pending,
    upsert_assembly_sample_completed,
    upsert_assembly_sample_no_data,
)
from qiita_control_plane.repositories.block import (
    create_mask_sample_pending,
    upsert_mask_sample_completed,
)
from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.runner._alignment import (
    ALIGN_MASK_IDX_BINDING,
    ASSEMBLY_PROCESSING_IDX_BINDING,
    MIN_IDENTITY_BINDING,
    MIN_QUERY_COVERAGE_BINDING,
    PRESET_BINDING,
    _create_alignment_gate_pending,
    _require_assembly_subject,
    _require_masked_query,
    _resolve_denovo_alignment_idx,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_legacy_mask_definition,
    seed_user_principal,
)

pytestmark = pytest.mark.db

_DEFAULTS = {
    PRESET_BINDING: "map-hifi",
    MIN_IDENTITY_BINDING: 0.95,
    MIN_QUERY_COVERAGE_BINDING: 0.90,
}


@pytest_asyncio.fixture
async def sample(postgres_pool):
    """A principal, a sequenced prep_sample, and an assembly run identity to key the
    gate on. The gate row itself is left unwritten so each test sets the state it is
    about."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="denovo", suffix=suffix)
    _, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    async with postgres_pool.acquire() as conn:
        run = await mint_processing(
            conn,
            workflow="long-read-assembly",
            version="1.0.0",
            params={"mask_idx": 1, "assembler": "hifiasm_meta", "s": suffix},
        )
    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "prep_sample_idx": prep_sample_idx,
        "processing_idx": run["processing_idx"],
        "suffix": suffix,
    }


def _bound(sample, **overrides):
    return {
        ASSEMBLY_PROCESSING_IDX_BINDING: sample["processing_idx"],
        ALIGN_MASK_IDX_BINDING: 4321,
        **overrides,
    }


def _schema(**defaults):
    return {"properties": {k: {"default": v} for k, v in {**_DEFAULTS, **defaults}.items()}}


async def _resolve(sample, *, work_ticket_idx=None, schema=None, **overrides):
    """Resolve against a ticket whose `alignment_idx` column is NULL unless a caller
    supplies one — the mint path. Pass a ticket that already carries one to exercise
    the resume re-attach."""
    return await _resolve_denovo_alignment_idx(
        sample["pool"],
        action_id="align-denovo",
        action_version="1.0.0",
        context_schema=schema or _schema(),
        bound=_bound(sample, **overrides),
        originator_principal_idx=sample["principal_idx"],
        work_ticket_idx=(
            work_ticket_idx if work_ticket_idx is not None else await _seed_ticket(sample)
        ),
    )


async def test_a_completed_assembly_is_the_only_state_that_admits_the_ticket(sample):
    """The outcomes, in one place because the caller does not branch on them.

    'pending' and a missing row are the same answer — the run is not over — but they
    reach it differently, and only one of them is reachable for a sample that really
    did assemble: a run predating the gate wrote no row at all. So each carries its own
    message, and the assertions pin which is which rather than letting one stand in for
    the other.
    """
    pool, processing_idx = sample["pool"], sample["processing_idx"]
    prep_sample_idx = sample["prep_sample_idx"]

    with pytest.raises(Exception, match="no assembly_sample gate row exists"):
        await _require_assembly_subject(
            pool, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
        )

    async with pool.acquire() as conn, conn.transaction():
        await create_assembly_sample_pending(
            conn, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
        )
    with pytest.raises(Exception, match="is not complete"):
        await _require_assembly_subject(
            pool, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
        )

    async with pool.acquire() as conn, conn.transaction():
        await upsert_assembly_sample_completed(
            conn, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
        )
    await _require_assembly_subject(
        pool, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
    )


async def test_an_assembly_that_produced_nothing_ends_the_ticket_rather_than_failing_it(
    sample,
):
    """A run that assembled no contig of any kind leaves this sample with no subject.
    That is the same outcome the assembly ticket recorded, so it is terminal and NOT a
    failure — `StepNoData` is what the runner turns into NO_DATA with NULL failure_*
    columns."""
    pool, processing_idx = sample["pool"], sample["processing_idx"]
    prep_sample_idx = sample["prep_sample_idx"]
    async with pool.acquire() as conn, conn.transaction():
        await create_assembly_sample_pending(
            conn, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
        )
        await upsert_assembly_sample_no_data(
            conn, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
        )
    with pytest.raises(StepNoData):
        await _require_assembly_subject(
            pool, processing_idx=processing_idx, prep_sample_idx=prep_sample_idx
        )


async def test_only_a_completed_mask_admits_the_ticket(sample):
    """The query side of the same question `_require_assembly_subject` asks of the
    subject. The mask CONFIG existing says nothing about whether it has RUN on this
    sample: a 'pending' row means a covering block is still masking, so the pass-set is
    a fraction of the sample's reads, and aligning it would record that fraction as the
    whole. Absence is not "exempt" either (contract: `fetch_mask_sample_state`).

    Without this the refusal still happens — `routes/read_masked.py` will not sign the
    DoGet ticket — but only once the job asks, from a node already holding its
    allocation and after the mint has left an alignment_sample row at 'pending'.
    """
    pool, prep_sample_idx = sample["pool"], sample["prep_sample_idx"]
    mask_idx = await seed_legacy_mask_definition(
        pool,
        params={
            "filter_workflow": "read-mask",
            "filter_version": "1.0.0",
            "s": secrets.token_hex(4),
        },
        created_by_idx=sample["principal_idx"],
    )

    with pytest.raises(Exception, match="is not masked-complete"):
        await _require_masked_query(pool, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx)

    async with pool.acquire() as conn, conn.transaction():
        await create_mask_sample_pending(
            conn, mask_idx=mask_idx, prep_sample_idxs=[prep_sample_idx]
        )
    with pytest.raises(Exception, match="is not masked-complete"):
        await _require_masked_query(pool, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx)

    async with pool.acquire() as conn, conn.transaction():
        await upsert_mask_sample_completed(conn, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx)
    await _require_masked_query(pool, mask_idx=mask_idx, prep_sample_idx=prep_sample_idx)


async def test_the_same_params_resolve_to_the_same_alignment_idx(sample):
    """What makes a resume safe: the mint runs again on every re-drive, and the
    params-hash upsert must return the id whose rows are already in the lake rather
    than a second one beside them."""
    first = await _resolve(sample)
    second = await _resolve(sample)
    assert first == second
    # And the resolved knobs come back bound, so the job applies the values the hash
    # covered rather than defaults of its own.
    assert first[PRESET_BINDING] == "map-hifi"
    assert first[MIN_IDENTITY_BINDING] == 0.95


async def test_a_moved_threshold_mints_a_distinct_alignment(sample):
    """The control for the test above: the mint is idempotent because the PARAMS are
    the same, not because it is keyed on the sample. A run at a different floor keeps
    a different set of rows, so it must not reuse the first run's identity."""
    base = await _resolve(sample)
    relaxed = await _resolve(sample, **{MIN_QUERY_COVERAGE_BINDING: 0.5})
    assert relaxed["alignment_idx"] != base["alignment_idx"]


async def test_a_knob_with_no_default_and_no_value_is_refused(sample):
    """An action whose context_schema forgot a default would otherwise hash None and
    hand the job a value it cannot validate — a failure at step run, on the cluster,
    attributed to the job."""
    with pytest.raises(Exception, match="incomplete"):
        await _resolve(sample, schema={"properties": {}})


async def test_the_ticket_column_and_the_gate_row_are_both_written(sample):
    """`delete-alignment-sample` and `finalize-alignment-sample` both read the COLUMN
    and refuse on NULL, and the gate row is what the terminal action flips — so a
    resolver that bound the identity without writing these two would produce a ticket
    that dies at its second entry."""
    pool = sample["pool"]
    work_ticket_idx = await _seed_ticket(sample)
    alignment_idx = (await _resolve(sample, work_ticket_idx=work_ticket_idx))["alignment_idx"]

    await _create_alignment_gate_pending(
        pool, alignment_idx=alignment_idx, prep_sample_idx=sample["prep_sample_idx"]
    )

    assert (
        await pool.fetchval(
            "SELECT alignment_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
        == alignment_idx
    )
    assert await _gate_state(sample, alignment_idx) == "pending"


async def test_the_terminal_action_flips_the_gate_and_re_running_it_is_a_no_op(sample):
    """A workflow retried from the start re-runs its terminal entry, so the flip has to
    re-affirm 'completed' rather than error or reopen."""
    alignment_idx = (await _resolve(sample))["alignment_idx"]
    await _create_alignment_gate_pending(
        sample["pool"], alignment_idx=alignment_idx, prep_sample_idx=sample["prep_sample_idx"]
    )
    for _ in range(2):
        await finalize_alignment_sample_gate(
            sample["pool"],
            alignment_idx=alignment_idx,
            prep_sample_idx=sample["prep_sample_idx"],
        )
        assert await _gate_state(sample, alignment_idx) == "completed"


async def test_flipping_a_gate_row_that_was_never_materialized_is_refused(sample):
    """The row is the resolver's to create. Upserting one here instead would report a
    sample as aligned under an identity whose submit path never ran."""
    alignment_idx = (await _resolve(sample))["alignment_idx"]
    with pytest.raises(RuntimeError, match="gate row missing"):
        await finalize_alignment_sample_gate(
            sample["pool"],
            alignment_idx=alignment_idx,
            prep_sample_idx=sample["prep_sample_idx"],
        )


async def test_a_resume_re_attaches_to_the_ticket_column_instead_of_re_deriving(sample):
    """The identity is the delete key, the register key and the gate key at once, and
    the runner re-enters the whole pre-loop block on every resume. Re-deriving would key
    those three off whatever the action declares NOW: `qiita-admin actions sync` upserts
    `context_schema` in place, so a deploy that edits a knob default without a version
    bump moves what a re-derivation produces. A ticket already carrying the column must
    come back with the SAME idx and the STORED knobs, not the new default's."""
    ticket = await _seed_ticket(sample)
    first = await _resolve(sample, work_ticket_idx=ticket)

    resumed = await _resolve(
        sample,
        work_ticket_idx=ticket,
        schema=_schema(**{MIN_QUERY_COVERAGE_BINDING: 0.5}),
    )
    assert resumed == first
    assert resumed[MIN_QUERY_COVERAGE_BINDING] == 0.90

    # The control: the same moved default on a ticket with a NULL column DOES mint a
    # new identity, so the assertion above is about re-attachment, not about the
    # default being ignored.
    fresh = await _resolve(sample, schema=_schema(**{MIN_QUERY_COVERAGE_BINDING: 0.5}))
    assert fresh["alignment_idx"] != first["alignment_idx"]
    assert fresh[MIN_QUERY_COVERAGE_BINDING] == 0.5


async def test_an_integer_and_a_float_threshold_are_one_identity(sample):
    """The action's `type: number` admits both `1` and `1.0`, and the hash is over
    canonical JSON, which renders them as different strings. Without the coercion the
    same configuration mints two alignment_idx — and `assert_params_survive_storage`
    cannot catch it, because an integer round-trips jsonb unchanged."""
    as_int = await _resolve(sample, **{MIN_IDENTITY_BINDING: 1})
    as_float = await _resolve(sample, **{MIN_IDENTITY_BINDING: 1.0})
    assert as_int["alignment_idx"] == as_float["alignment_idx"]
    assert isinstance(as_int[MIN_IDENTITY_BINDING], float)


async def _gate_state(sample, alignment_idx: int) -> str | None:
    return await sample["pool"].fetchval(
        "SELECT state FROM qiita.alignment_sample"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
        alignment_idx,
        sample["prep_sample_idx"],
    )


async def _seed_ticket(sample) -> int:
    """A prep_sample-scoped ticket with a NULL `alignment_idx`, under an action of its
    own.

    Its own action because `work_ticket_one_in_flight_per_prep_sample` admits one
    non-terminal ticket per `(scope target, action_id, action_version)`, and several
    tests here need two live tickets against one sample. That index is also why a
    second `align-denovo` submission for a sample is refused while the first is in
    flight, whatever its thresholds — the guard carries no params term.
    """
    suffix = secrets.token_hex(4)
    action_id = f"denovo-act-{suffix}"
    await sample["pool"].execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, '1.0.0', 'prep_sample', '{}'::text[], $2::jsonb, '{}'::jsonb,"
        "         '[]'::jsonb, 1, 1, '1 minute', NULL, NULL)",
        action_id,
        '{"service": false, "human_roles": ["system_admin"]}',
    )
    return await sample["pool"].fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx, scope_target_kind,"
        "  prep_sample_idx)"
        " VALUES ($1, '1.0.0', $2, 'prep_sample', $3) RETURNING work_ticket_idx",
        action_id,
        sample["principal_idx"],
        sample["prep_sample_idx"],
    )


async def test_an_invalidated_run_is_not_re_run_under_the_same_identity(sample):
    """The workflow's first tail step deletes the sample's alignment rows, which would
    destroy the output a withdrawal judged, and its terminal step could not complete the
    row anyway. So the gate step refuses before any step runs."""
    pool = sample["pool"]
    alignment_idx = (await _resolve(sample))["alignment_idx"]
    await _create_alignment_gate_pending(
        pool, alignment_idx=alignment_idx, prep_sample_idx=sample["prep_sample_idx"]
    )
    await pool.execute(
        "UPDATE qiita.alignment_sample SET state = 'invalidated', invalidated_at = now(),"
        "       invalidated_by_idx = $3, invalidation_reason = 'bad run'"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
        alignment_idx,
        sample["prep_sample_idx"],
        sample["principal_idx"],
    )
    with pytest.raises(Exception, match="invalidated"):
        await _create_alignment_gate_pending(
            pool, alignment_idx=alignment_idx, prep_sample_idx=sample["prep_sample_idx"]
        )
    assert await _gate_state(sample, alignment_idx) == "invalidated"


async def test_a_deprecated_alignment_is_refused_on_mint_and_on_resume(sample):
    ticket = await _seed_ticket(sample)
    first = await _resolve(sample, work_ticket_idx=ticket)
    await sample["pool"].execute(
        "UPDATE qiita.alignment_definition SET status = 'deprecated', deprecated_at = now(),"
        "       deprecated_by_idx = $2, deprecation_reason = 'bad preset'"
        " WHERE alignment_idx = $1",
        first["alignment_idx"],
        sample["principal_idx"],
    )
    with pytest.raises(Exception, match="bad preset"):
        await _resolve(sample, work_ticket_idx=ticket)
    with pytest.raises(Exception, match="bad preset"):
        await _resolve(sample)


async def test_the_mint_records_the_alignment_on_the_ticket(sample):
    """The mint records the alignment on the ticket in its own transaction
    (`runner._record`)."""
    ticket = await _seed_ticket(sample)
    minted = await _resolve(sample, work_ticket_idx=ticket)
    assert (
        await sample["pool"].fetchval(
            "SELECT alignment_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket
        )
        == minted["alignment_idx"]
    )


async def test_a_re_attach_after_an_admin_edit_of_the_recorded_alignment_fails(
    sample, human_admin_session
):
    """A resume re-attaches to the recorded alignment while its settings are unchanged;
    once an admin edited them after the ticket was created, the stored params are not
    the ones the ticket resolved, and the ticket fails as bad input."""
    from qiita_common.backend_failure import BackendFailure
    from qiita_common.models import DefinitionKind, DefinitionSettingsEditKind

    pool = sample["pool"]
    ticket = await _seed_ticket(sample)
    first = await _resolve(sample, work_ticket_idx=ticket)
    assert await _resolve(sample, work_ticket_idx=ticket) == first
    # Append-only, with no FK to the definition, so the row is left as the other
    # settings-edit tests leave theirs.
    await pool.execute(
        "INSERT INTO qiita.definition_settings_edit (definition_kind, definition_ref,"
        "  edit_kind, actor_principal_idx, old_settings, new_settings, reason)"
        " VALUES ($1, $2, $3, $4, '{\"params\": 1}', '{\"params\": 2}', 'corrected in a test')",
        DefinitionKind.ALIGNMENT.value,
        first["alignment_idx"],
        DefinitionSettingsEditKind.ADMIN_EDIT.value,
        human_admin_session["principal_idx"],
    )
    with pytest.raises(BackendFailure, match="submit a new ticket"):
        await _resolve(sample, work_ticket_idx=ticket)


async def test_an_earlier_edit_of_the_tickets_mask_does_not_hide_an_alignment_edit(
    sample, human_admin_session
):
    """The ticket also records a mask. An edit of the mask alone does not stop the
    re-attach; once the alignment is edited too, after the mask, the re-attach finds
    the alignment's edit and fails, rather than reading only the ticket's earliest
    edit."""
    from qiita_common.backend_failure import BackendFailure
    from qiita_common.models import DefinitionKind, DefinitionSettingsEditKind

    from qiita_control_plane.repositories.mask_definition import mint_mask_definition

    pool = sample["pool"]
    ticket = await _seed_ticket(sample)
    first = await _resolve(sample, work_ticket_idx=ticket)
    async with pool.acquire() as conn:
        mask_idx = (
            await mint_mask_definition(
                conn,
                filter_workflow="read-mask",
                filter_version="1.0.0",
                params={"filter_workflow": "read-mask", "probe": secrets.token_hex(4)},
                principal_idx=human_admin_session["principal_idx"],
            )
        )["mask_idx"]
    try:
        await pool.execute(
            "UPDATE qiita.work_ticket SET mask_idx = $2 WHERE work_ticket_idx = $1",
            ticket,
            mask_idx,
        )

        # Append-only, with no FK to the definition, so the rows are left as the
        # other settings-edit tests leave theirs. The mask's edit is the earlier.
        async def edit(kind, ref, minutes):
            await pool.execute(
                "INSERT INTO qiita.definition_settings_edit (definition_kind, definition_ref,"
                "  edit_kind, actor_principal_idx, old_settings, new_settings, reason,"
                "  occurred_at)"
                " VALUES ($1, $2, $3, $4, '{\"params\": 1}', '{\"params\": 2}',"
                "  'corrected in a test', now() + make_interval(mins => $5))",
                kind.value,
                ref,
                DefinitionSettingsEditKind.ADMIN_EDIT.value,
                human_admin_session["principal_idx"],
                minutes,
            )

        await edit(DefinitionKind.MASK, mask_idx, 1)
        assert await _resolve(sample, work_ticket_idx=ticket) == first
        await edit(DefinitionKind.ALIGNMENT, first["alignment_idx"], 2)
        with pytest.raises(BackendFailure, match="submit a new ticket"):
            await _resolve(sample, work_ticket_idx=ticket)
    finally:
        await pool.execute(
            "UPDATE qiita.work_ticket SET mask_idx = NULL WHERE work_ticket_idx = $1", ticket
        )
        await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)


async def test_an_edit_committed_inside_the_de_novo_mint_re_mints(
    sample, human_admin_session, monkeypatch
):
    """The real mint path: the alignment the ticket's params resolve to already exists,
    and an admin edit of it commits during the mint, before the hold. The mint
    re-mints, so the ticket records an alignment with the params it hashed, and the
    edited row keeps the edit."""
    import json

    from qiita_common.models import DefinitionKind

    from qiita_control_plane.definition_settings import edit_definition_settings
    from qiita_control_plane.runner import _alignment

    pool = sample["pool"]
    first_ticket = await _seed_ticket(sample)
    existing = (await _resolve(sample, work_ticket_idx=first_ticket))["alignment_idx"]
    # Finished, so the edit's live-work check does not count it.
    await pool.execute(
        "UPDATE qiita.work_ticket SET state = 'cancelled', notified_at = now()"
        " WHERE work_ticket_idx = $1",
        first_ticket,
    )
    stored = json.loads(
        await pool.fetchval(
            "SELECT params FROM qiita.alignment_definition WHERE alignment_idx = $1", existing
        )
    )
    real_mint = _alignment.mint_alignment_definition
    edits: list[int] = []

    async def mint_then_edit(conn, **kwargs):
        row = await real_mint(conn, **kwargs)
        if not edits:
            async with pool.acquire() as other, other.transaction():
                await edit_definition_settings(
                    other,
                    kind=DefinitionKind.ALIGNMENT,
                    idx=row["alignment_idx"],
                    params={**stored, "edited": True},
                    workflow=None,
                    version=None,
                    reason="corrected in a test",
                    principal_idx=human_admin_session["principal_idx"],
                )
            edits.append(row["alignment_idx"])
        return row

    monkeypatch.setattr(_alignment, "mint_alignment_definition", mint_then_edit)
    ticket = await _seed_ticket(sample)
    recorded = (await _resolve(sample, work_ticket_idx=ticket))["alignment_idx"]
    assert edits == [existing]
    assert recorded != existing
    assert (
        await pool.fetchval(
            "SELECT alignment_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket
        )
        == recorded
    )
    params_of = "SELECT params FROM qiita.alignment_definition WHERE alignment_idx = $1"
    assert json.loads(await pool.fetchval(params_of, recorded)) == stored
    assert json.loads(await pool.fetchval(params_of, existing))["edited"] is True


async def test_a_mint_whose_row_moves_before_every_hold_fails_as_bad_input(sample, monkeypatch):
    """When the held row's hash differs on every attempt, the ticket fails as bad
    submission input (`runner._record.fail_exhausted_hold`), not the runner's
    catch-all. The held hash is stubbed: a real edit cannot reach a row the mint's
    own transaction inserted."""
    from qiita_common.backend_failure import BackendFailure, FailureKind

    from qiita_control_plane.runner import _alignment

    async def moved_hash(conn, alignment_idx):
        return {"params_hash": b"\x00" * 32}

    monkeypatch.setattr(_alignment, "lock_active_alignment", moved_hash)
    with pytest.raises(BackendFailure, match="edited between the mint and its hold") as ei:
        await _resolve(sample)
    assert ei.value.kind == FailureKind.BAD_INPUT
