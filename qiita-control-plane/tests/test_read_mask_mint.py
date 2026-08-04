"""Tests for the runner's read-mask identity (mask_idx) minting.

`_mint_read_mask` resolves a prep_sample's filtering config (filter workflow +
version + the host references the host_filter step actually applies, passed
through from action_context + the resolved QC config) and mints a deduplicated
mask_idx via mint_mask_definition. The same effective config resolves to the same
mask_idx; a different config (a different host reference, instrument, or adapter
set) mints a new one.

`_workflow_needs_mask` is the pure-unit gate that decides whether the runner
mints a mask before the step loop (keys off a step threading `mask_idx` via
`params:`).
"""

import json
import secrets

import pytest
import pytest_asyncio
from qiita_common.actions import WorkflowAction, WorkflowStep
from qiita_common.hashing import canonical_params_hash

from qiita_control_plane import runner
from qiita_control_plane.repositories.mask_definition import (
    ADAPTER_HASH_SCHEME_SEQUENCE_HASH,
    fetch_mask_definition_by_idx,
)
from qiita_control_plane.repositories.reference_membership import (
    reference_sequence_hashes,
    reference_sequence_set_hash,
)
from qiita_control_plane.testing.db_seeds import (
    delete_reference_with_sequences,
    seed_biosample_with_sequenced_prep_sample,
    seed_legacy_mask_definition,
    seed_reference_with_sequences,
    seed_sequenced_sample_subtype,
    seed_user_principal,
)


def _step(name: str, params: dict | None = None) -> WorkflowStep:
    return WorkflowStep.model_validate(
        {
            "kind": "step",
            "name": name,
            "step_type": "singleton",
            "module": f"qiita_compute_orchestrator.jobs.{name}",
            "inputs": [],
            "params": params or {},
            "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
        }
    )


def test_workflow_needs_mask_true_when_param_threads_mask_idx():
    steps = [_step("fastq"), _step("host_filter", params={"mask_idx": "mask_idx"})]
    assert runner._workflow_needs_mask(steps) is True


def test_workflow_needs_mask_false_without_mask_param():
    steps = [
        _step("fastq"),
        _step("qc", params={"instrument_model": "instrument_model"}),
        WorkflowAction.model_validate(
            {"kind": "action", "name": "register-files", "inputs": ["x"]}
        ),
    ]
    assert runner._workflow_needs_mask(steps) is False


# --------------------------------------------------------------------------- DB


@pytest_asyncio.fixture
async def seeded(postgres_pool):
    """Seed principal + biosample + sequenced prep_sample + sequenced_sample
    subtype; yield the ids plus `adapter_reference(sequences)`, and clean up
    FK-reverse. Adapter references go through the tracker so the principal
    cleanup below is not blocked by reference.created_by_idx (ON DELETE
    RESTRICT)."""
    principal_idx = await seed_user_principal(postgres_pool, prefix="mask-mint", suffix="owner")
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
        postgres_pool,
        prep_sample_idx=prep_sample_idx,
        owner_idx=principal_idx,
        sequenced_pool_item_id=f"item-{secrets.token_hex(4)}",
    )
    references: list[int] = []

    async def adapter_reference(sequences: list[str]) -> int:
        reference_idx = await seed_reference_with_sequences(
            postgres_pool,
            name=f"adapters-{secrets.token_hex(6)}",
            created_by_idx=principal_idx,
            sequences=sequences,
        )
        references.append(reference_idx)
        return reference_idx

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "prep_sample_idx": prep_sample_idx,
        "adapter_reference": adapter_reference,
    }
    for reference_idx in references:
        await delete_reference_with_sequences(postgres_pool, reference_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequenced_sample WHERE idx = $1", ss_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await postgres_pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
    await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


@pytest.mark.db
async def test_mint_read_mask_binds_and_dedups(seeded):
    """The same config mints once and resolves to the same mask_idx; a different
    instrument model mints a distinct mask_idx (config drives identity)."""
    pool = seeded["pool"]
    common = dict(
        action_id="fastq-to-parquet",
        action_version="1.3.0",
        prep_sample_idx=seeded["prep_sample_idx"],
        originator_principal_idx=seeded["principal_idx"],
        adapter_parquet=None,
        default_adapter_reference_idx=None,
        host_rype_reference_idx=None,
        host_minimap2_reference_idx=None,
        resolved_lima=None,
        resolved_syndna=None,
    )
    a = await runner._mint_read_mask(pool, instrument_model="NextSeq 550", **common)
    b = await runner._mint_read_mask(pool, instrument_model="NextSeq 550", **common)
    c = await runner._mint_read_mask(pool, instrument_model="Illumina MiSeq", **common)

    assert runner.MASK_IDX_BINDING in a
    assert a["mask_idx"] == b["mask_idx"]  # same config -> same mask
    assert c["mask_idx"] != a["mask_idx"]  # different instrument -> different mask
    # cleanup the minted rows so the shared DB stays clean
    await pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])",
        [a["mask_idx"], c["mask_idx"]],
    )


@pytest.mark.db
async def test_mint_read_mask_host_ref_drives_identity(seeded):
    """The host refs that go into the mask identity come from the APPLIED filter
    config (passed straight through from action_context). Two mints differing only
    in `host_rype_reference_idx` produce DIFFERENT mask_idx; identical config (incl.
    host refs) collapses to the SAME mask_idx."""
    pool = seeded["pool"]
    common = dict(
        action_id="fastq-to-parquet",
        action_version="1.3.0",
        prep_sample_idx=seeded["prep_sample_idx"],
        originator_principal_idx=seeded["principal_idx"],
        instrument_model="NextSeq 550",
        adapter_parquet=None,
        default_adapter_reference_idx=None,
        host_minimap2_reference_idx=None,
        resolved_lima=None,
        resolved_syndna=None,
    )
    a = await runner._mint_read_mask(pool, host_rype_reference_idx=7, **common)
    a_again = await runner._mint_read_mask(pool, host_rype_reference_idx=7, **common)
    b = await runner._mint_read_mask(pool, host_rype_reference_idx=9, **common)

    assert a["mask_idx"] == a_again["mask_idx"]  # identical config -> same mask
    assert b["mask_idx"] != a["mask_idx"]  # different applied host ref -> diff mask
    await pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])",
        [a["mask_idx"], b["mask_idx"]],
    )


@pytest.mark.db
async def test_adapter_set_hash_is_keyed_on_sequences_not_storage(seeded):
    """`reference_sequence_set_hash` is a function of the reference's SEQUENCES. Two
    references holding the same sequences hash identically even though they are
    different reference_idx values loaded separately; changing one sequence
    changes the digest; member order does not."""
    pool = seeded["pool"]
    same_a = await seeded["adapter_reference"](["ACGTACGT", "TTTTGGGG"])
    same_b = await seeded["adapter_reference"](["TTTTGGGG", "ACGTACGT"])
    different = await seeded["adapter_reference"](["ACGTACGT", "GATTACAG"])

    hash_a = await reference_sequence_set_hash(pool, same_a)
    assert hash_a == await reference_sequence_set_hash(pool, same_b)
    assert hash_a != await reference_sequence_set_hash(pool, different)


@pytest.mark.db
async def test_adapter_set_hash_is_strand_canonical(seeded):
    """An adapter and its reverse complement are ONE feature — `qiita.feature`
    dedups on `canonical_sequence_hash_expr`, which keeps the lex-smaller of the
    two strands' md5. So a reference holding a sequence and one holding its
    reverse complement resolve to the same adapter identity, and a reference
    holding both has one member, not two.

    Pinned because the identity's whole premise is "a function of the sequences",
    and this says which function: strand-collapsed, not literal."""
    pool = seeded["pool"]
    forward = await seeded["adapter_reference"](["TTTTGGGG"])
    revcomp = await seeded["adapter_reference"](["CCCCAAAA"])
    both = await seeded["adapter_reference"](["TTTTGGGG", "CCCCAAAA"])

    forward_hash = await reference_sequence_set_hash(pool, forward)
    assert forward_hash == await reference_sequence_set_hash(pool, revcomp)
    assert forward_hash == await reference_sequence_set_hash(pool, both)
    assert len(await reference_sequence_hashes(pool, both)) == 1


@pytest.mark.db
async def test_adapter_set_hash_is_none_for_a_memberless_reference(seeded):
    """A reference naming no sequences yields None, not the digest of an empty
    input — every memberless reference would otherwise share one identity."""
    pool = seeded["pool"]
    empty = await seeded["adapter_reference"]([])
    assert await reference_sequence_set_hash(pool, empty) is None


@pytest.mark.db
async def test_mint_read_mask_adapter_set_drives_identity(seeded, tmp_path):
    """Different adapter SEQUENCES -> different mask_idx (the adapter_set_hash is
    folded into the config); the same sequences collapse to one mask.

    The adapter Parquet's bytes differ between the two mints of the same adapter
    set — the thing the original digest keyed on. Identity follows the sequences,
    so those two mints share a mask.
    """
    pool = seeded["pool"]
    ref_a = await seeded["adapter_reference"](["ACGTACGT", "TTTTGGGG"])
    ref_b = await seeded["adapter_reference"](["ACGTACGT", "GATTACAG"])
    parquet_1 = tmp_path / "adapters_1.parquet"
    parquet_2 = tmp_path / "adapters_2.parquet"
    parquet_1.write_bytes(b"serialized-by-one-writer")
    parquet_2.write_bytes(b"serialized-by-another-writer")

    common = dict(
        action_id="fastq-to-parquet",
        action_version="1.3.0",
        prep_sample_idx=seeded["prep_sample_idx"],
        originator_principal_idx=seeded["principal_idx"],
        instrument_model="NextSeq 550",
        host_rype_reference_idx=None,
        host_minimap2_reference_idx=None,
        resolved_lima=None,
        resolved_syndna=None,
    )
    a = await runner._mint_read_mask(
        pool, adapter_parquet=parquet_1, default_adapter_reference_idx=ref_a, **common
    )
    a_again = await runner._mint_read_mask(
        pool, adapter_parquet=parquet_2, default_adapter_reference_idx=ref_a, **common
    )
    b = await runner._mint_read_mask(
        pool, adapter_parquet=parquet_1, default_adapter_reference_idx=ref_b, **common
    )

    assert a["mask_idx"] == a_again["mask_idx"]
    assert b["mask_idx"] != a["mask_idx"]
    await pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])",
        [a["mask_idx"], b["mask_idx"]],
    )


@pytest.mark.db
async def test_mint_read_mask_rekeys_a_mask_minted_on_the_byte_hash(seeded, tmp_path):
    """A mask minted under the legacy byte digest is found and re-keyed in place:
    same mask_idx, current params_hash, scheme stamped.

    The fallback matches on the byte digest, so it finds the row only while the
    writer bytes are unchanged — this fixture holds them fixed. Once the row is
    re-keyed its identity no longer involves those bytes, which is the point: a
    later pyarrow bump cannot move it."""
    pool = seeded["pool"]
    ref_idx = await seeded["adapter_reference"](["ACGTACGT", "TTTTGGGG"])
    adapter_parquet = tmp_path / "adapters.parquet"
    adapter_parquet.write_bytes(b"bytes-an-older-pyarrow-wrote")

    # The mint reads prep_protocol_idx off the seeded sample, so the legacy row
    # must be built with the same value or the two configs are not one config.
    prep_protocol_idx = await pool.fetchval(
        "SELECT prep_protocol_idx FROM qiita.prep_sample WHERE idx = $1",
        seeded["prep_sample_idx"],
    )
    params_common = dict(
        action_id="fastq-to-parquet",
        action_version="1.3.0",
        prep_protocol_idx=prep_protocol_idx,
        instrument_model="NextSeq 550",
        host_rype_reference_idx=None,
        host_minimap2_reference_idx=None,
        resolved_lima=None,
        resolved_syndna=None,
    )
    legacy_mask_idx = await seed_legacy_mask_definition(
        pool,
        params=runner._build_mask_params(
            adapter_set_hash=runner._adapter_set_hash_legacy(adapter_parquet), **params_common
        ),
        created_by_idx=seeded["principal_idx"],
    )
    assert (await fetch_mask_definition_by_idx(pool, legacy_mask_idx))[
        "adapter_hash_scheme"
    ] is None

    minted = await runner._mint_read_mask(
        pool,
        action_id="fastq-to-parquet",
        action_version="1.3.0",
        prep_sample_idx=seeded["prep_sample_idx"],
        originator_principal_idx=seeded["principal_idx"],
        instrument_model="NextSeq 550",
        adapter_parquet=adapter_parquet,
        default_adapter_reference_idx=ref_idx,
        host_rype_reference_idx=None,
        host_minimap2_reference_idx=None,
        resolved_lima=None,
        resolved_syndna=None,
    )

    assert minted["mask_idx"] == legacy_mask_idx
    row = await fetch_mask_definition_by_idx(pool, legacy_mask_idx)
    assert row["adapter_hash_scheme"] == ADAPTER_HASH_SCHEME_SEQUENCE_HASH
    current_hash = await reference_sequence_set_hash(pool, ref_idx)
    assert json.loads(row["params"])["resolved_qc"]["adapter_set_hash"] == current_hash
    assert bytes(row["params_hash"]) == canonical_params_hash(
        runner._build_mask_params(adapter_set_hash=current_hash, **params_common)
    )
    await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", legacy_mask_idx)


@pytest.mark.db
async def test_persist_mask_idx_writes_minted_mask_onto_ticket(seeded):
    """The runner persists the minted mask_idx onto the ticket row. Mint a real
    mask via `_mint_read_mask`, then persist it via the same `_persist_mask_idx`
    the pre-loop block calls; the prep_sample-scoped work_ticket's `mask_idx`
    column equals the minted value. Re-running is idempotent (a resume re-mints
    to the same mask_idx and re-writes the same value)."""
    pool = seeded["pool"]
    principal_idx = seeded["principal_idx"]
    prep_sample_idx = seeded["prep_sample_idx"]

    action_id = "read-mask"
    version = f"mask-persist-test-{secrets.token_hex(4)}"
    await pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience, "
        "  context_schema, steps, "
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, "
        "  success_status, failure_status"
        ") VALUES ($1, $2, 'prep_sample', $3::text[], $4::jsonb,"
        "  $5::jsonb, $6::jsonb, 1, 1, '1 minute', $7, $8)",
        action_id,
        version,
        ["feature:mint"],
        json.dumps({"service": False, "human_roles": ["wet_lab_admin"]}),
        json.dumps({}),
        json.dumps([]),
        "active",
        "failed",
    )
    work_ticket_idx = await pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, prep_sample_idx, action_context"
        ") VALUES ($1, $2, $3, 'prep_sample', $4, '{}'::jsonb)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        principal_idx,
        prep_sample_idx,
    )
    try:
        minted = await runner._mint_read_mask(
            pool,
            action_id=action_id,
            action_version=version,
            prep_sample_idx=prep_sample_idx,
            originator_principal_idx=principal_idx,
            instrument_model="NextSeq 550",
            adapter_parquet=None,
            default_adapter_reference_idx=None,
            host_rype_reference_idx=None,
            host_minimap2_reference_idx=None,
            resolved_lima=None,
            resolved_syndna=None,
        )
        mask_idx = minted[runner.MASK_IDX_BINDING]

        # Before persist: column is NULL.
        assert (
            await pool.fetchval(
                "SELECT mask_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            is None
        )

        await runner._persist_mask_idx(pool, work_ticket_idx, mask_idx)
        assert (
            await pool.fetchval(
                "SELECT mask_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            == mask_idx
        )

        # Idempotent: a re-mint (same config -> same mask_idx) re-writes the same
        # value, no error.
        await runner._persist_mask_idx(pool, work_ticket_idx, mask_idx)
        assert (
            await pool.fetchval(
                "SELECT mask_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
                work_ticket_idx,
            )
            == mask_idx
        )
    finally:
        # work_ticket.mask_idx FK is ON DELETE SET NULL, but the work_ticket row
        # still references the mask_definition; drop the ticket first.
        await pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", work_ticket_idx
        )
        await pool.execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
            action_id,
            version,
        )
        await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)


@pytest.mark.db
async def test_mint_read_mask_requires_sequenced_sample(seeded):
    """A prep_sample with no sequenced_sample row is a SUBMISSION BAD_INPUT
    (the sample must be pooled before a mask can be minted)."""
    from qiita_common.backend_failure import BackendFailure

    with pytest.raises(BackendFailure, match="no sequenced_sample row"):
        await runner._mint_read_mask(
            seeded["pool"],
            action_id="fastq-to-parquet",
            action_version="1.3.0",
            prep_sample_idx=2_000_000_001,  # nonexistent
            originator_principal_idx=seeded["principal_idx"],
            instrument_model=None,
            adapter_parquet=None,
            default_adapter_reference_idx=None,
            host_rype_reference_idx=None,
            host_minimap2_reference_idx=None,
            resolved_lima=None,
            resolved_syndna=None,
        )


# --------------------------------------------------------------------------- lima / syndna
#
# `resolved_lima` + `resolved_syndna` are what distinguish the five PacBio
# protocols in the mask identity. `prep_protocol_idx` cannot: it is the operator's
# `--prep-protocol-idx` flag, uniform across them.


def _params(**overrides):
    base = dict(
        action_id="read-mask",
        action_version="1.0.0",
        prep_protocol_idx=2,
        instrument_model="Revio",
        adapter_set_hash="a3f2",
        host_rype_reference_idx=None,
        host_minimap2_reference_idx=None,
        resolved_lima=None,
        resolved_syndna=None,
    )
    base.update(overrides)
    return runner._mask._build_mask_params(**base)


def test_lima_and_syndna_discriminate_pacbio_protocols():
    """Case 5 (lima + syndna) and case 1 (neither) differ ONLY in these two keys:
    same operator flags, same run, same everything else. Without them the two
    hash identically and share one mask_idx whose params describe only case 1."""
    case1 = _params()
    case5 = _params(
        resolved_lima=runner._mask._resolved_lima(
            {"lima_enabled": True, "lima_preset": "ASYMMETRIC"}
        ),
        resolved_syndna=runner._mask._resolved_syndna(
            {"syndna_enabled": True, "syndna_reference_idx": 57}
        ),
    )
    assert case1 != case5
    assert case1["resolved_lima"] is None and case1["resolved_syndna"] is None
    assert case5["resolved_syndna"]["reference_idx"] == 57
    assert case5["resolved_lima"]["preset"] == "ASYMMETRIC"


def test_resolved_lima_is_none_when_disabled_even_with_a_stale_preset():
    """A stale `lima_preset` left by a disabled run must not shift the hash."""
    assert runner._mask._resolved_lima({"lima_preset": "ASYMMETRIC"}) is None
    assert runner._mask._resolved_lima({"lima_enabled": False, "lima_preset": "ASYMMETRIC"}) is None


def test_resolved_lima_carries_cp_resolved_args_and_adapter_md5():
    """The client picks the preset; the CP resolves the args + adapter identity.
    `--neighbors` (which makes the adapter FASTA's record order load-bearing) is
    NOT implied by the preset, so it must appear in the resolved args."""
    r = runner._mask._resolved_lima({"lima_enabled": True, "lima_preset": "ASYMMETRIC"})
    assert r["args"] == "--hifi-preset ASYMMETRIC --neighbors --peek-guess"
    assert r["adapter_set_md5"] == runner._mask._LIMA_ADAPTER_SET_MD5
    # lima decides where the clip lands, so its version is part of the filter.
    assert r["version"] == runner._mask._LIMA_VERSION
    sym = runner._mask._resolved_lima({"lima_enabled": True, "lima_preset": "SYMMETRIC"})
    assert "--neighbors" not in sym["args"]


@pytest.mark.parametrize("preset", [None, "", "asymmetric", "--rm -rf", 5, True])
def test_resolved_lima_rejects_an_unknown_preset(preset):
    """The preset is the ONLY client-facing lima knob; anything outside the table
    fails loud at SUBMISSION rather than reaching a container as a flag."""
    with pytest.raises(Exception, match="lima_preset"):
        runner._mask._resolved_lima({"lima_enabled": True, "lima_preset": preset})


def test_resolved_syndna_is_gated_on_enabled():
    """A stale `syndna_reference_idx` left by a disabled run must not shift the hash."""
    assert runner._mask._resolved_syndna({"syndna_reference_idx": 57}) is None
    assert (
        runner._mask._resolved_syndna({"syndna_enabled": False, "syndna_reference_idx": 57}) is None
    )
    assert (
        runner._mask._resolved_syndna({"syndna_enabled": True, "syndna_reference_idx": 57})[
            "reference_idx"
        ]
        == 57
    )


def test_resolved_syndna_carries_the_effective_alignment_config():
    """The reference alone does not describe the filter: a read is a spike-in when it has
    a PRIMARY alignment at >= min_identity under a preset. Every one of those belongs in
    the identity, so that changing any of them RE-MINTS rather than silently reusing a
    mask built under the old rule.

    Asserted as a WHOLE-DICT equality, deliberately: a new knob added to the filter must
    fail here until it is folded into the hash. `primary_only` was added exactly because
    this equality did NOT fail when the predicate changed — the numbers were pinned but
    the semantics were not."""
    r = runner._mask._resolved_syndna({"syndna_enabled": True, "syndna_reference_idx": 57})
    assert r == {
        "reference_idx": 57,
        "aligner": "minimap2",
        "preset": "map-hifi",
        "identity_method": "blast",
        "min_identity": 0.95,
        # The aligned-fraction gate. Correct for masking only because the reference is
        # PLASMID-level: a junction-spanning read is fully aligned against the plasmid.
        # Against an insert-only index the same read looks ~60% aligned, which is why the
        # old reference could carry no such filter at all.
        "min_aligned_fraction": 0.90,
        "primary_only": True,
    }


def test_syndna_threshold_bump_remints_only_syndna_masks(monkeypatch):
    """Moving the identity threshold changes the effective spike-in filter, so a
    syndna mask must re-hash. Because `resolved_syndna` is None when syndna is off,
    it leaves every non-syndna mask hashing exactly as before."""
    ctx = {"syndna_enabled": True, "syndna_reference_idx": 57}
    before_syndna = _params(resolved_syndna=runner._mask._resolved_syndna(ctx))
    before_plain = _params()

    monkeypatch.setattr(runner._mask, "_SYNDNA_MIN_IDENTITY", 0.99)
    after_syndna = _params(resolved_syndna=runner._mask._resolved_syndna(ctx))
    after_plain = _params()

    assert before_syndna != after_syndna, "a threshold bump must re-mint a syndna mask"
    assert before_plain == after_plain, "it must NOT disturb a non-syndna mask"


def test_mask_params_are_canonical_json_serializable():
    """mint_mask_definition hashes canonical JSON of this dict; a non-serializable
    value would fail at mint time, not here."""
    json.dumps(
        _params(
            resolved_lima=runner._mask._resolved_lima(
                {"lima_enabled": True, "lima_preset": "ASYMMETRIC"}
            ),
            resolved_syndna=runner._mask._resolved_syndna(
                {"syndna_enabled": True, "syndna_reference_idx": 57}
            ),
        ),
        sort_keys=True,
    )


def test_lima_version_bump_remints_only_lima_masks():
    """A lima upgrade changes where the clip lands, so it must re-mint. Because
    `resolved_lima` is None when lima is off, the bump leaves every Illumina (and
    non-lima PacBio) mask hashing exactly as before."""
    on = {"lima_enabled": True, "lima_preset": "ASYMMETRIC"}
    before = _params(resolved_lima=runner._mask._resolved_lima(on))
    bumped = dict(runner._mask._resolved_lima(on), version="2.14.0")
    assert _params(resolved_lima=bumped) != before
    # ...while a mask that never ran lima is unaffected by the same bump.
    assert _params() == _params()
