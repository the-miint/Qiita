"""End-to-end de novo alignment against live components: real Flight wire, real
Rust streaming DoGet on both inputs, real miint alignment, and the real action tail
(`delete-alignment-sample` → `register-files` → `finalize-alignment-sample`).

What this reaches that the job's unit test cannot. There, both streams are
monkeypatched readers over in-memory Arrow; here the contigs come off the live data
plane's `assembled_sequence_chunks` resolution (which is `assembly_membership`, not a
column — neither sequence table carries `prep_sample_idx`) and the reads off the live
`read_masked` macro, so the redaction of every non-'pass' read is part of the path.
And the two Parquets are REGISTERED: `ducklake_add_data_files` schema-matches on the
full column list, so a projection that named the right columns in the wrong order —
or typed one differently — fails here rather than on the cluster after the alignment
has been paid for.

The fixture is one 20 kb contig held linearised, the shape an assembler emits a
circular contig in, plus a decoy contig belonging to a DIFFERENT assembly run of the
same sample. The decoy is what makes the run scoping falsifiable: a read drawn from
it must produce no alignment, because the signed pair names only the run under test.

The two ticket mints are signed directly against the fixture data plane's secret
rather than through the CP routes — those have their own tests
(`routes/test_read_masked.py`, `routes/test_assembly_doget.py`), and what this file is for
is everything downstream of a correctly-signed ticket.
"""

from __future__ import annotations

import random
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.models import ReadMaskReason

from conftest import ducklake_connect

_CONTIG_LENGTH = 20_000
_READ_LENGTH = 6_000

# feature_idx of the run's contig and of the decoy the OTHER run assembled.
_CONTIG = 710_001
_DECOY_CONTIG = 710_002

_CHUNK_BP = 4_096


def _u8(values: list[int]) -> str:
    return "[" + ",".join(str(v) for v in values) + "]::UTINYINT[]"


def _rand_seq(rng: random.Random, n: int) -> str:
    return "".join(rng.choice("ACGT") for _ in range(n))


def _chunks(feature_idx: int, sequence: str) -> str:
    return ", ".join(
        f"({feature_idx}, {i}, '{sequence[i * _CHUNK_BP : (i + 1) * _CHUNK_BP]}')"
        for i in range((len(sequence) + _CHUNK_BP - 1) // _CHUNK_BP)
    )


@pytest.fixture(scope="module")
def genome():
    rng = random.Random(20260823)
    contig = _rand_seq(rng, _CONTIG_LENGTH)
    decoy = _rand_seq(rng, _CONTIG_LENGTH)
    return {
        "contig": contig,
        "decoy": decoy,
        "reads": {
            # Crosses the origin: two records, half the read each.
            1: contig[_CONTIG_LENGTH - 3_000 :] + contig[:3_000],
            # Interior control.
            2: contig[5_000 : 5_000 + _READ_LENGTH],
            # Masked out as host — must never cross the Flight boundary at all.
            3: contig[9_000 : 9_000 + _READ_LENGTH],
            # Belongs to the OTHER run's contig: the run scoping is what drops it.
            4: decoy[4_000 : 4_000 + _READ_LENGTH],
        },
    }


def _sign(table, filter_dict, secret_bytes):
    from qiita_control_plane.auth.tickets import sign_ticket

    return sign_ticket(table=table, filter=filter_dict, secret=secret_bytes)


@pytest.fixture(scope="module")
async def seeded(postgres_pool, data_plane, genome):
    """Seed both stores ONCE for the module.

    Module-scoped because `data_plane` is: it resets the DuckLake catalog at its own
    setup, so a function-scoped seeder would insert the same contig chunks again for
    every test and `string_agg` would reassemble a doubled contig — on which an
    origin-crossing read is an ordinary interior read, and the case under test
    silently stops existing.

    Postgres carries the sample and the assembly runs; DuckLake carries the data the
    two streams read. Each test mints its OWN alignment identity (see `denovo`).
    """
    from qiita_control_plane.repositories.processing import mint_processing
    from qiita_control_plane.repositories.sequence_range import mint_sequence_range
    from qiita_control_plane.testing.db_seeds import (
        seed_biosample_with_sequenced_prep_sample,
        seed_legacy_mask_definition,
        seed_user_principal,
    )

    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(
        postgres_pool, prefix="denovo-e2e", suffix=suffix
    )
    _, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    # A real mask_definition row: `work_ticket.mask_idx` has an FK onto it.
    mask_idx = await seed_legacy_mask_definition(
        postgres_pool,
        params={"filter_workflow": "read-mask", "filter_version": "1.0.0", "s": suffix},
        created_by_idx=principal_idx,
    )
    async with postgres_pool.acquire() as conn:
        await mint_sequence_range(
            conn,
            prep_sample_idx=prep_sample_idx,
            count=len(genome["reads"]),
            principal_idx=principal_idx,
            work_ticket_idx=None,
        )
        run = await mint_processing(
            conn,
            workflow="long-read-assembly",
            version="1.0.0",
            params={"mask_idx": mask_idx, "assembler": "hifiasm_meta", "s": suffix},
        )
        other_run = await mint_processing(
            conn,
            workflow="long-read-assembly",
            version="1.0.0",
            params={"mask_idx": mask_idx, "assembler": "myloasm", "s": suffix},
        )
    processing_idx = run["processing_idx"]

    conn = ducklake_connect(data_plane["data_path"])
    try:
        conn.execute(
            "INSERT INTO qiita_lake.assembled_sequence VALUES "
            f"({_CONTIG}, 'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380d01'::UUID, {_CONTIG_LENGTH}), "
            f"({_DECOY_CONTIG}, 'c0eebc99-9c0b-4ef8-bb6d-6bb9bd380d02'::UUID, "
            f"{_CONTIG_LENGTH})"
        )
        conn.execute(
            "INSERT INTO qiita_lake.assembled_sequence_chunks VALUES "
            + _chunks(_CONTIG, genome["contig"])
            + ", "
            + _chunks(_DECOY_CONTIG, genome["decoy"])
        )
        conn.execute(
            "INSERT INTO qiita_lake.assembly_membership VALUES "
            f"({prep_sample_idx}, {processing_idx}, 'LCG', 'contig_1', {_CONTIG}), "
            f"({prep_sample_idx}, {other_run['processing_idx']}, 'LCG', 'contig_1', "
            f"{_DECOY_CONTIG})"
        )
        conn.execute(
            "INSERT INTO qiita_lake.read"
            " (prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)"
            " VALUES "
            + ", ".join(
                f"({prep_sample_idx}, {idx}, 'm84001/{idx}/ccs', '{seq}',"
                f" {_u8([40] * len(seq))}, NULL, NULL)"
                for idx, seq in genome["reads"].items()
            )
        )
        conn.execute(
            "INSERT INTO qiita_lake.read_mask"
            " (mask_idx, prep_sample_idx, sequence_idx, reason,"
            "  left_trim1, right_trim1, left_trim2, right_trim2) VALUES "
            + ", ".join(
                f"({mask_idx}, {prep_sample_idx}, {idx},"
                f" '{(ReadMaskReason.HOST_RYPE if idx == 3 else ReadMaskReason.PASS).value}',"
                " 0, 0, 0, 0)"
                for idx in genome["reads"]
            )
        )
    finally:
        conn.close()

    yield {
        "pool": postgres_pool,
        "data_plane": data_plane,
        "principal_idx": principal_idx,
        "prep_sample_idx": prep_sample_idx,
        "processing_idx": processing_idx,
        "mask_idx": mask_idx,
        "suffix": suffix,
    }


@pytest.fixture
async def denovo(seeded, tmp_path, request):
    """One test's own alignment identity, PENDING gate row and prep_sample ticket.

    Per test rather than per module so each starts from a gate nothing has flipped
    and rows nothing has registered — the identity is what those are keyed on, so a
    distinct one is a clean slate without touching the shared lake seeding.
    """
    from qiita_control_plane.repositories.alignment_definition import (
        mint_alignment_definition,
    )
    from qiita_control_plane.repositories.block import create_alignment_sample_pending

    pool = seeded["pool"]
    suffix = f"{seeded['suffix']}-{request.node.name}"
    async with pool.acquire() as conn:
        align = await mint_alignment_definition(
            conn,
            params={
                "subject": "assembly",
                "processing_idx": seeded["processing_idx"],
                "mask_idx": seeded["mask_idx"],
                "aligner": "minimap2",
                "s": suffix,
            },
            principal_idx=seeded["principal_idx"],
        )
    alignment_idx = align["alignment_idx"]
    async with pool.acquire() as conn, conn.transaction():
        await create_alignment_sample_pending(
            conn,
            alignment_idx=alignment_idx,
            prep_sample_idxs=[seeded["prep_sample_idx"]],
        )

    action_id = f"denovo-e2e-{suffix}"[:64]
    await pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, '1.0.0', 'prep_sample', '{}'::text[], $2::jsonb, '{}'::jsonb,"
        "         '[]'::jsonb, 1, 1, '1 minute', NULL, NULL)",
        action_id,
        '{"service": false, "human_roles": ["system_admin"]}',
    )
    work_ticket_idx = await pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx, scope_target_kind,"
        "  prep_sample_idx, mask_idx, alignment_idx)"
        " VALUES ($1, '1.0.0', $2, 'prep_sample', $3, $4, $5) RETURNING work_ticket_idx",
        action_id,
        seeded["principal_idx"],
        seeded["prep_sample_idx"],
        seeded["mask_idx"],
        alignment_idx,
    )

    yield {
        **seeded,
        "alignment_idx": alignment_idx,
        "work_ticket_idx": work_ticket_idx,
        "workspace": tmp_path / "ws",
    }

    await pool.execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", work_ticket_idx
    )
    await pool.execute("DELETE FROM qiita.action WHERE action_id = $1", action_id)


def _install_real_streams(monkeypatch, denovo):
    """Point both seams at the LIVE data plane, bypassing only the CP mint hop.

    Each replacement signs the same table + filter the CP route signs and then calls
    the real `open_doget_stream`, so everything the job sees — the Flight wire, the
    Rust query, the `read_masked` redaction, the `assembly_membership` resolution — is
    the production path.
    """
    from qiita_compute_orchestrator import data_plane_client
    from qiita_compute_orchestrator.jobs import align_denovo

    url = f"grpc://{LOOPBACK_HOST}:{denovo['data_plane']['port']}"

    @asynccontextmanager
    async def _contigs(
        conn, *, prep_sample_idx, processing_idx, relation="assembly_chunks"
    ):
        ticket = _sign(
            "assembled_sequence_chunks",
            {"prep_sample_idx": [prep_sample_idx], "processing_idx": [processing_idx]},
            denovo["data_plane"]["secret"],
        )
        with data_plane_client.open_doget_stream(
            conn, data_plane_url=url, ticket_bytes=ticket, relation=relation
        ) as rel:
            yield rel

    @asynccontextmanager
    async def _reads(conn, *, prep_sample_idx, mask_idx, relation="masked_reads"):
        ticket = _sign(
            "read_masked",
            {"prep_sample_idx": [prep_sample_idx], "mask_idx": [mask_idx]},
            denovo["data_plane"]["secret"],
        )
        with data_plane_client.open_doget_stream(
            conn, data_plane_url=url, ticket_bytes=ticket, relation=relation
        ) as rel:
            yield rel

    monkeypatch.setattr(align_denovo, "open_assembly_chunk_stream", _contigs)
    monkeypatch.setattr(align_denovo, "open_read_masked_stream", _reads)


async def _run_job(denovo, monkeypatch) -> dict[str, Path]:
    from qiita_compute_orchestrator.jobs.align_denovo import Inputs, execute

    _install_real_streams(monkeypatch, denovo)
    return await execute(
        Inputs(
            prep_sample_idx=denovo["prep_sample_idx"],
            work_ticket_idx=denovo["work_ticket_idx"],
            assembly_processing_idx=denovo["processing_idx"],
            align_mask_idx=denovo["mask_idx"],
            alignment_idx=denovo["alignment_idx"],
            preset="map-hifi",
            min_identity=0.95,
            min_query_coverage=0.90,
        ),
        denovo["workspace"],
    )


async def _run_tail(denovo, workspace) -> None:
    """The workflow's three action entries, through the REAL runner adapter."""
    from qiita_common.actions import WorkflowAction
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_control_plane.runner import _run_action_primitive

    scope_target = {"kind": "prep_sample", "prep_sample_idx": denovo["prep_sample_idx"]}
    url = f"grpc://{LOOPBACK_HOST}:{denovo['data_plane']['port']}"
    for name, inputs in (
        (LibraryPrimitive.DELETE_ALIGNMENT_SAMPLE, []),
        (LibraryPrimitive.REGISTER_FILES, ["alignment_staging_dir"]),
        (LibraryPrimitive.FINALIZE_ALIGNMENT_SAMPLE, []),
    ):
        await _run_action_primitive(
            denovo["pool"],
            WorkflowAction(kind="action", name=str(name), inputs=inputs, outputs=[]),
            {"alignment_staging_dir": str(workspace)},
            workspace,
            scope_target,
            work_ticket_idx=denovo["work_ticket_idx"],
            signing_key=denovo["data_plane"]["secret"],
            data_plane_url=url,
        )


def _lake(denovo, sql: str):
    conn = ducklake_connect(denovo["data_plane"]["data_path"])
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_de_novo_alignment_streams_both_inputs_and_registers_both_tables(
    denovo, monkeypatch
):
    """The whole path. Four assertions that each fail for a different reason:

    * the origin-spanning read reaches `alignment` with BOTH its records — the
      circular gate is what admits them, since each covers half the read;
    * the interior control reaches it with one — so the split is a property of the
      origin, not of the read length;
    * the host-masked read reaches nothing — the `read_masked` macro never streamed
      it, so this is the redaction, not the gate;
    * the decoy-contig read reaches nothing — the signed pair names one assembly
      run, and the data plane resolves its contigs from `assembly_membership`.
    """
    assert await _gate(denovo) == "pending"
    outputs = await _run_job(denovo, monkeypatch)
    await _run_tail(denovo, outputs["alignment_staging_dir"])
    # Flipped by the terminal entry, which reads `work_ticket.alignment_idx` rather
    # than action_context and runs after register-files — so a consumer that sees
    # 'completed' can rely on the rows below being in the lake.
    assert await _gate(denovo) == "completed"

    rows = _lake(
        denovo,
        "SELECT sequence_idx, feature_idx, count(*) FROM qiita_lake.alignment"
        f" WHERE alignment_idx = {denovo['alignment_idx']}"
        " GROUP BY 1, 2 ORDER BY 1",
    )
    assert rows == [(1, _CONTIG, 2), (2, _CONTIG, 1)]

    spanning = _lake(
        denovo,
        "SELECT sequence_idx, feature_idx, query_start, query_stop, feature_start,"
        " feature_stop, is_reverse, pooled_identity, pooled_coverage, fragment_count"
        " FROM qiita_lake.alignment_origin_spanning"
        f" WHERE alignment_idx = {denovo['alignment_idx']} ORDER BY sequence_idx",
    )
    assert spanning == [
        (
            1,
            _CONTIG,
            0,
            _READ_LENGTH,
            _CONTIG_LENGTH - 3_000 + 1,
            3_001,
            False,
            1.0,
            1.0,
            2,
        )
    ]


@pytest.mark.asyncio
async def test_a_re_run_replaces_its_own_rows_rather_than_appending(
    denovo, monkeypatch
):
    """`delete-alignment-sample` before `register-files` is what makes a retry exact.
    Without it the second registration would double every row — which is why the
    counts are asserted rather than the row set."""
    first = await _run_job(denovo, monkeypatch)
    await _run_tail(denovo, first["alignment_staging_dir"])
    before = _lake(
        denovo,
        "SELECT count(*) FROM qiita_lake.alignment"
        f" WHERE alignment_idx = {denovo['alignment_idx']}",
    )

    denovo["workspace"] = denovo["workspace"].parent / "ws2"
    second = await _run_job(denovo, monkeypatch)
    await _run_tail(denovo, second["alignment_staging_dir"])
    after = _lake(
        denovo,
        "SELECT count(*) FROM qiita_lake.alignment"
        f" WHERE alignment_idx = {denovo['alignment_idx']}",
    )
    assert before == after == [(3,)]
    assert _lake(
        denovo,
        "SELECT count(*) FROM qiita_lake.alignment_origin_spanning"
        f" WHERE alignment_idx = {denovo['alignment_idx']}",
    ) == [(1,)]


async def _gate(denovo) -> str | None:
    return await denovo["pool"].fetchval(
        "SELECT state FROM qiita.alignment_sample"
        " WHERE alignment_idx = $1 AND prep_sample_idx = $2",
        denovo["alignment_idx"],
        denovo["prep_sample_idx"],
    )
