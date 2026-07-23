"""End-to-end integration test: drive workflows/reference-add via the CLI's
`qiita reference load` programmatic entry point against real CP +
real data-plane Flight + DuckLake, then DoGet the registered Parquet
rows back.

Exercises every layer of the production path:
  * CLI (`cli.reference_load.do_reference_load`) — Arrow conversion,
    POST /upload + Flight DoPut, POST /upload/{idx}/done, POST /work-ticket.
  * Control plane (route layer + runner upload resolution +
    `_consume_upload_handles` + LIBRARY primitives + status transitions).
  * Compute orchestrator (LocalBackend in-process via
    LocalComputeBackendClient).
  * Data plane (Flight DoPut for upload, DoAction for register, DoGet
    for verification).
  * DuckLake (Parquet registration via the data plane).

Differs from the legacy direct-INSERT version: the work_ticket flows
through POST /work-ticket → schedule_dispatch → background asyncio task
running `run_workflow`. The test awaits completion via the CLI's
work_ticket-poll loop (short poll interval).

Shared fixtures: `data_plane`, `signing_key`, `postgres_pool`,
`human_admin_session` live in conftest.py.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import duckdb
import pyarrow.flight as flight
import pytest
from httpx import ASGITransport, AsyncClient

from _runner_helpers import LocalComputeBackendClient

_REFERENCE_ADD_YAML_PATH = (
    Path(__file__).parent.parent.parent / "workflows" / "reference-add" / "1.0.0.yaml"
)


@pytest.fixture
def flight_client(data_plane):
    from qiita_common.api_paths import LOOPBACK_HOST

    client = flight.FlightClient(f"grpc://{LOOPBACK_HOST}:{data_plane['port']}")
    yield client
    client.close()


@pytest.fixture
async def synced_reference_add_action(postgres_pool, tmp_path):
    """Materialize workflows/reference-add/1.0.0.yaml under tmp_path/workflows/
    so the loader's directory walk picks it up, sync it into qiita.action,
    and clean the row up after.

    Tests don't share the on-disk version_pin (1.0.0) because parallel
    sessions would step on each other's action row; each run synthesizes a
    unique version suffix and the CLI submits against that. But the CLI
    hard-codes `action_version="1.0.0"` — so we keep that pin and accept
    the parallel-session collision risk (the integration suite serializes
    in pytest by default)."""
    from qiita_control_plane.actions import load_actions, sync_actions

    workflows_dir = tmp_path / "workflows" / "reference-add"
    workflows_dir.mkdir(parents=True)
    (workflows_dir / "1.0.0.yaml").write_text(_REFERENCE_ADD_YAML_PATH.read_text())

    actions = load_actions(tmp_path / "workflows")
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, actions)

    yield ("reference-add", "1.0.0")

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        "reference-add",
        "1.0.0",
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        "reference-add",
        "1.0.0",
    )


@pytest.fixture
async def fresh_reference(postgres_pool, human_admin_session):
    """Create a reference at status='pending'. The CLI's `--reference-idx`
    binds to this row instead of creating its own — keeps the test's
    cleanup scoped to a single row."""
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2)"
        " RETURNING reference_idx",
        f"e2e-{uuid.uuid4()}",
        human_admin_session["principal_idx"],
    )
    yield idx
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", idx
    )


_TEST_SEQUENCES = {
    "seq1": "ATCGATCGATCG",
    "seq2": "GCTAGCTAGCTA",
    "seq3": "AAATTTTCCCGGG",
}


@pytest.fixture
def fasta_e2e(tmp_path):
    path = tmp_path / "test.fasta"
    with open(path, "w") as f:
        for name, seq in _TEST_SEQUENCES.items():
            f.write(f">{name}\n{seq}\n")
    return path


@pytest.fixture
def taxonomy_e2e(tmp_path):
    """Parquet with (feature_id, taxonomy) — feature_id matches FASTA read_ids."""
    path = tmp_path / "taxonomy.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE t (feature_id VARCHAR, taxonomy VARCHAR)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?)",
            [
                ("seq1", "d__Bacteria; p__Bacillota; c__Bacilli; o__; f__; g__; s__"),
                ("seq2", "d__Bacteria; p__Pseudomonadota; c__; o__; f__; g__; s__"),
                ("seq3", "d__Archaea; p__Euryarchaeota; c__; o__; f__; g__; s__"),
            ],
        )
        conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    return path


@pytest.fixture
def tree_e2e(tmp_path):
    """Newick tree whose tip names match the FASTA read_ids — load step
    populates feature_idx on tip nodes via the read_id → feature_idx join."""
    path = tmp_path / "tree.nwk"
    path.write_text("((seq1:0.1,seq2:0.2):0.3,seq3:0.4);")
    return path


@pytest.fixture
def genome_map_e2e(tmp_path):
    """Parquet mapping each FASTA read_id to a genome (source, source_id), so the
    load writes qiita.genome + qiita.feature_genome. That is the provenance the
    exclusion query endpoint surfaces (source/source_id) and the junction a
    genome-level block resolves through (genome_idx → feature_idx). source_ids
    carry a per-run suffix so a re-run within one session can't collide on the
    genome UNIQUE(source, source_id)."""
    suffix = uuid.uuid4().hex[:8]
    path = tmp_path / "genome_map.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TABLE g (read_id VARCHAR, genome_source VARCHAR, genome_source_id VARCHAR)"
        )
        conn.executemany(
            "INSERT INTO g VALUES (?, 'refseq', ?)",
            [
                ("seq1", f"GCF_{suffix}_1"),
                ("seq2", f"GCF_{suffix}_2"),
                ("seq3", f"GCF_{suffix}_3"),
            ],
        )
        conn.execute(f"COPY g TO '{path}' (FORMAT PARQUET)")
    return path


# A shared plasmid: two genomes each carry a distinct chromosome plus an
# IDENTICAL plasmid sequence. Identical bytes → one content-hash-global
# feature_idx under BOTH genomes (the many-to-many the feature_genome fix
# enables). The plasmid's membership accession is the lex-smallest of its two
# read_ids ("plasmidA" < "plasmidB").
_SHARED_PLASMID_SEQS = {
    "chromA": "ATCGATCGATCGAA",
    "chromB": "GCTAGCTAGCTAGG",
    "plasmidA": "AAATTTCCCGGGTTT",
    "plasmidB": "AAATTTCCCGGGTTT",  # identical bytes to plasmidA -> same feature_idx
}


@pytest.fixture
def fasta_shared_plasmid(tmp_path):
    path = tmp_path / "shared_plasmid.fasta"
    with open(path, "w") as f:
        for name, seq in _SHARED_PLASMID_SEQS.items():
            f.write(f">{name}\n{seq}\n")
    return path


@pytest.fixture
def genome_map_shared_plasmid(tmp_path):
    """chromA + plasmidA -> genome A; chromB + plasmidB -> genome B. Since
    plasmidA/plasmidB are identical bytes, the single plasmid feature_idx is
    associated with BOTH genomes."""
    suffix = uuid.uuid4().hex[:8]
    path = tmp_path / "genome_map_shared.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TABLE g (read_id VARCHAR, genome_source VARCHAR, genome_source_id VARCHAR)"
        )
        conn.executemany(
            "INSERT INTO g VALUES (?, 'refseq', ?)",
            [
                ("chromA", f"GCF_{suffix}_A"),
                ("plasmidA", f"GCF_{suffix}_A"),
                ("chromB", f"GCF_{suffix}_B"),
                ("plasmidB", f"GCF_{suffix}_B"),
            ],
        )
        conn.execute(f"COPY g TO '{path}' (FORMAT PARQUET)")
    return path


@pytest.fixture
async def cli_cp_client(postgres_pool, signing_key, human_admin_session, data_plane):
    """Configure cp_app.state for dispatch — pool + settings (with the
    data plane's actual gRPC URL and the spawned PATH_SCRATCH/staging) +
    LocalComputeBackendClient + dispatch task tracking. Yield an
    httpx.AsyncClient over ASGITransport with the admin PAT header."""
    from qiita_common.api_paths import LOOPBACK_HOST
    from qiita_control_plane.config import Settings as CPSettings
    from qiita_control_plane.main import app as cp_app

    cp_app.state.pool = postgres_pool
    cp_app.state.settings = CPSettings(
        database_url="unused-in-test",
        flight_signing_key=signing_key,
        data_plane_url=f"grpc://{LOOPBACK_HOST}:{data_plane['port']}",
        path_scratch_staging=Path(data_plane["upload_staging_root"]),
        path_scratch_ticket=Path(data_plane["workspace_root"]),
    )
    cp_app.state.compute_backend_client = LocalComputeBackendClient()
    cp_app.state.running_dispatches = set()

    async with AsyncClient(
        transport=ASGITransport(app=cp_app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as client:
        yield client

    # Drain any in-flight dispatch tasks so a flaky workflow can't leak
    # across tests.
    import asyncio

    pending = list(cp_app.state.running_dispatches)
    if pending:
        _, leftover = await asyncio.wait(pending, timeout=5)
        for task in leftover:
            task.cancel()


async def test_e2e_create_to_doget(
    postgres_pool,
    data_plane,
    signing_key,
    flight_client,
    synced_reference_add_action,
    fresh_reference,
    fasta_e2e,
    taxonomy_e2e,
    tree_e2e,
    human_admin_session,
    cli_cp_client,
    tmp_path,
):
    """Drive the full production path via `do_reference_load`:
    POST /reference (skipped — we bind to fresh_reference) →
    POST /upload + Flight DoPut + POST /done for each input file →
    POST /work-ticket → schedule_dispatch fires runner in background →
    runner resolves upload handles, walks workflow, registers files →
    --watch polls /work-ticket/{idx} until completed →
    DoGet round-trips sequences / chunks / taxonomy / phylogeny.
    """
    from qiita_control_plane.auth.tickets import sign_ticket
    from qiita_control_plane.cli.reference_load import do_reference_load

    result = await do_reference_load(
        http=cli_cp_client,
        token=human_admin_session["token"],
        flight_client=flight_client,
        fasta_path=fasta_e2e,
        taxonomy_path=taxonomy_e2e,
        tree_path=tree_e2e,
        reference_idx=fresh_reference,
        watch=True,
        poll_interval_seconds=0.1,
        timeout_seconds=60,
    )
    assert result["work_ticket"]["state"] == "completed", result["work_ticket"]

    # Both terminal checks — same as the legacy assertions, but reached
    # via the production HTTP path instead of direct DB INSERT.
    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        result["work_ticket_idx"],
    )
    assert state == "completed"
    ref_status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1",
        fresh_reference,
    )
    assert ref_status == "active"

    # The three uploads transitioned ready → consumed inside the runner's
    # finalize transaction.
    consumed = await postgres_pool.fetch(
        "SELECT upload_idx, status FROM qiita.upload WHERE upload_idx = ANY($1::bigint[])",
        list(result["upload_idxs"].values()),
    )
    assert all(row["status"] == "consumed" for row in consumed), consumed

    # DoGet round-trip via the data plane — confirms register-files ran
    # and the DuckLake catalog carries the new reference's rows.
    def _doget(table_name: str):
        ticket_bytes = sign_ticket(
            table=table_name,
            filter={"reference_idx": [fresh_reference]},
            secret=signing_key,
        )
        return flight_client.do_get(flight.Ticket(ticket_bytes)).read_all()

    table = _doget("reference_sequences")
    assert table.num_rows == 3
    assert {"feature_idx", "sequence_hash", "sequence_length_bp"}.issubset(
        set(table.column_names)
    )

    chunks = _doget("reference_sequence_chunks")
    assert chunks.num_rows == 3
    # Sequences come back in canonical form (LEAST of strand + revcomp)
    # — for the three short fixture sequences each is already canonical.
    canon_seqs = set()
    for seq in _TEST_SEQUENCES.values():
        rc = seq.translate(str.maketrans("ACGTRYSWKMBDHVN", "TGCAYRSWMKVHDBN"))[
            ::-1
        ].upper()
        canon_seqs.add(min(seq.upper(), rc))
    assert set(chunks.column("chunk_data").to_pylist()) == canon_seqs

    tax = _doget("reference_taxonomy_visible")
    assert tax.num_rows == 3
    assert set(tax.column("domain").to_pylist()) == {"Bacteria", "Archaea"}

    phylo = _doget("reference_phylogeny")
    tip_rows = [r for r in phylo.to_pylist() if r["is_tip"]]
    assert len(tip_rows) == 3
    assert all(r["feature_idx"] is not None for r in tip_rows)


async def test_e2e_exclusion_masks_taxonomy_and_reports_provenance(
    postgres_pool,
    data_plane,
    signing_key,
    flight_client,
    synced_reference_add_action,
    fresh_reference,
    fasta_e2e,
    taxonomy_e2e,
    genome_map_e2e,
    human_admin_session,
    cli_cp_client,
):
    """Full curated-exclusion path against a live data plane:

    load (FASTA + taxonomy + genome_map) → accession persisted on
    reference_membership → block a GENOME via POST /reference/exclusion (which
    re-materializes the DuckLake mirror synchronously) → reference_taxonomy_visible
    DoGet omits the blocked genome's feature → GET /reference/{idx}/exclusion
    reports it with source/source_id (genome) + accession (membership) and
    via_genome=True → DELETE re-enables it (taxonomy view whole again).

    The alignment_visible anti-join shares the identical mirror + view mechanism;
    its wire-level omission is pinned by the Rust integration test
    `alignment_visible_doget_omits_blocked_feature` (this workflow produces no
    alignment rows — those come from the separate `align` path over samples)."""
    from qiita_common.api_paths import (
        URL_REFERENCE_EXCLUSION,
        URL_REFERENCE_EXCLUSION_BY_IDX,
    )
    from qiita_control_plane.auth.tickets import sign_ticket
    from qiita_control_plane.cli.reference_load import do_reference_load

    result = await do_reference_load(
        http=cli_cp_client,
        token=human_admin_session["token"],
        flight_client=flight_client,
        fasta_path=fasta_e2e,
        taxonomy_path=taxonomy_e2e,
        genome_map_path=genome_map_e2e,
        reference_idx=fresh_reference,
        watch=True,
        poll_interval_seconds=0.1,
        timeout_seconds=60,
    )
    assert result["work_ticket"]["state"] == "completed", result["work_ticket"]

    # The FASTA-header read_id was persisted as the membership accession.
    accessions = await postgres_pool.fetch(
        "SELECT accession FROM qiita.reference_membership WHERE reference_idx = $1",
        fresh_reference,
    )
    assert {r["accession"] for r in accessions} == {"seq1", "seq2", "seq3"}

    # Resolve seq1's (feature_idx, genome_idx, source_id) via the load-written
    # junction — decoupled from the fixture's suffixed source_id value. Capture
    # ALL of this reference's genome_idxs up front so cleanup drops exactly the
    # rows this test's load created (not other tests' genomes).
    row = await postgres_pool.fetchrow(
        "SELECT m.feature_idx, g.genome_idx, g.source_id"
        " FROM qiita.reference_membership m"
        " JOIN qiita.feature_genome fg ON fg.feature_idx = m.feature_idx"
        " JOIN qiita.genome g ON g.genome_idx = fg.genome_idx"
        " WHERE m.reference_idx = $1 AND m.accession = 'seq1'",
        fresh_reference,
    )
    blocked_feature, blocked_genome, blocked_source_id = (
        row["feature_idx"],
        row["genome_idx"],
        row["source_id"],
    )
    all_genome_idxs = [
        r["genome_idx"]
        for r in await postgres_pool.fetch(
            "SELECT DISTINCT fg.genome_idx FROM qiita.feature_genome fg"
            " JOIN qiita.reference_membership m ON m.feature_idx = fg.feature_idx"
            " WHERE m.reference_idx = $1",
            fresh_reference,
        )
    ]

    def _visible_features() -> set[int]:
        ticket_bytes = sign_ticket(
            table="reference_taxonomy_visible",
            filter={"reference_idx": [fresh_reference]},
            secret=signing_key,
        )
        table = flight_client.do_get(flight.Ticket(ticket_bytes)).read_all()
        return set(table.column("feature_idx").to_pylist())

    try:
        # Baseline: all three features' taxonomy is visible.
        assert _visible_features() == {
            r["feature_idx"]
            for r in await postgres_pool.fetch(
                "SELECT feature_idx FROM qiita.reference_membership WHERE reference_idx = $1",
                fresh_reference,
            )
        }
        assert blocked_feature in _visible_features()

        # Block the GENOME. The route resolves genome → feature(s), writes the
        # Postgres blocklist row, and synchronously REPLACES the lake mirror.
        add = await cli_cp_client.post(
            URL_REFERENCE_EXCLUSION,
            json={"genome_idx": blocked_genome, "reason": "e2e contaminant"},
        )
        assert add.status_code == 201, add.text
        assert add.json()["synced_feature_count"] >= 1

        # The anti-join view now omits the blocked genome's feature; the base is
        # untouched (reference_sequences still has all three).
        assert blocked_feature not in _visible_features()
        assert len(_visible_features()) == 2

        # Query endpoint reports the block with full provenance.
        listing = await cli_cp_client.get(
            URL_REFERENCE_EXCLUSION_BY_IDX.format(reference_idx=fresh_reference)
        )
        assert listing.status_code == 200, listing.text
        items = listing.json()
        assert len(items) == 1
        item = items[0]
        assert item["feature_idx"] == blocked_feature
        assert item["genome_idx"] == blocked_genome
        assert item["source"] == "refseq"
        assert item["source_id"] == blocked_source_id
        assert item["accession"] == "seq1"
        assert item["via_genome"] is True
        assert item["direct_block"] is False

        # Unblock → the mirror clears the feature → the taxonomy view is whole.
        remove = await cli_cp_client.delete(
            URL_REFERENCE_EXCLUSION, params={"genome_idx": blocked_genome}
        )
        assert remove.status_code == 200, remove.text
        assert blocked_feature in _visible_features()
        assert len(_visible_features()) == 3
    finally:
        # Idempotent unblock + UNCONDITIONAL re-sync (the DELETE route always
        # re-materializes) clears seq1's feature from the GLOBAL lake mirror even
        # when an assertion above failed before the happy-path DELETE — critical
        # because seq1's content-hash feature_idx is shared with
        # test_e2e_create_to_doget (same fixture bytes), so a leaked block would
        # wrongly drop a row there. Do this BEFORE dropping the genome (the FK
        # CASCADE would remove the Postgres row without refreshing the mirror).
        await cli_cp_client.delete(
            URL_REFERENCE_EXCLUSION, params={"genome_idx": blocked_genome}
        )
        # Drop exactly this load's genome/junction rows (fresh_reference cleans
        # membership + reference; feature rows accumulate as they do for the
        # sibling e2e — a pre-existing property of content-hash features).
        await postgres_pool.execute(
            "DELETE FROM qiita.feature_genome WHERE genome_idx = ANY($1::bigint[])",
            all_genome_idxs,
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])",
            all_genome_idxs,
        )


async def test_e2e_export_genome_with_shared_plasmid(
    postgres_pool,
    data_plane,
    signing_key,
    flight_client,
    synced_reference_add_action,
    fresh_reference,
    fasta_shared_plasmid,
    genome_map_shared_plasmid,
    human_admin_session,
    cli_cp_client,
    tmp_path,
):
    """End-to-end proof of the feature_genome many-to-many fix + the genome-export
    CLI writers: load a reference whose two genomes share an IDENTICAL plasmid,
    then export EACH genome (FASTA.gz + Parquet) through the CLI's writer path
    (member route → DoGet ticket route → Flight DoGet → miint FASTA / pyarrow
    Parquet). Both genomes' exports must contain the shared plasmid — the payoff
    of dropping the standalone UNIQUE(feature_idx)."""
    import pyarrow.parquet as pq
    from qiita_common.api_paths import URL_REFERENCE_GENOME_MEMBER

    from qiita_control_plane.auth.tickets import sign_ticket
    from qiita_control_plane.cli.reference_load import do_reference_load
    from qiita_control_plane.cli.user import reference as ref_cli
    from qiita_control_plane.miint import connect_with_miint

    result = await do_reference_load(
        http=cli_cp_client,
        token=human_admin_session["token"],
        flight_client=flight_client,
        fasta_path=fasta_shared_plasmid,
        genome_map_path=genome_map_shared_plasmid,
        reference_idx=fresh_reference,
        watch=True,
        poll_interval_seconds=0.1,
        timeout_seconds=60,
    )
    assert result["work_ticket"]["state"] == "completed", result["work_ticket"]

    # The plasmid's two read_ids collapsed to ONE membership row (lex-min accession).
    accessions = {
        r["accession"]
        for r in await postgres_pool.fetch(
            "SELECT accession FROM qiita.reference_membership WHERE reference_idx = $1",
            fresh_reference,
        )
    }
    assert accessions == {"chromA", "chromB", "plasmidA"}

    # The single plasmid feature_idx is associated with BOTH genomes (many-to-many).
    plasmid_feature = await postgres_pool.fetchval(
        "SELECT feature_idx FROM qiita.reference_membership"
        " WHERE reference_idx = $1 AND accession = 'plasmidA'",
        fresh_reference,
    )
    genome_rows = await postgres_pool.fetch(
        "SELECT genome_idx FROM qiita.feature_genome WHERE feature_idx = $1 ORDER BY genome_idx",
        plasmid_feature,
    )
    assert len(genome_rows) == 2, "the shared plasmid must belong to both genomes"

    # Resolve each genome_idx via its OWN chromosome's accession (unambiguous —
    # a chromosome belongs to exactly one genome).
    async def _genome_of(accession: str) -> int:
        return await postgres_pool.fetchval(
            "SELECT fg.genome_idx FROM qiita.reference_membership m"
            " JOIN qiita.feature_genome fg ON fg.feature_idx = m.feature_idx"
            " WHERE m.reference_idx = $1 AND m.accession = $2",
            fresh_reference,
            accession,
        )

    genome_a = await _genome_of("chromA")
    genome_b = await _genome_of("chromB")
    all_genome_idxs = [r["genome_idx"] for r in genome_rows]

    output_dir = tmp_path / "export"
    output_dir.mkdir()
    con = connect_with_miint()
    try:
        results: dict[int, dict] = {}
        for label, genome_idx, own_chrom in (
            ("A", genome_a, "chromA"),
            ("B", genome_b, "chromB"),
        ):
            # 1. Member route: feature_idx + accession for this genome.
            members_resp = await cli_cp_client.get(
                URL_REFERENCE_GENOME_MEMBER.format(
                    reference_idx=fresh_reference, genome_idx=genome_idx
                )
            )
            assert members_resp.status_code == 200, members_resp.text
            members = members_resp.json()
            accession_map = {m["feature_idx"]: m["accession"] for m in members}
            # Each genome's member set = its own chromosome + the shared plasmid.
            assert plasmid_feature in accession_map
            assert {own_chrom, "plasmidA"} == set(accession_map.values())
            feature_idxs = [m["feature_idx"] for m in members]

            # 2. Chunk-bytes DoGet ticket. Signed directly (as the sibling e2e
            # tests do) rather than via the ticket route — this test's focus is the
            # member route + the writers + the many-to-many data flow, not the
            # ticket route's auth (the human session holds reference:read, which the
            # route now accepts, and the CLI's own ticket-route call is covered by
            # the pure-unit test + test_auth_boundary).
            ticket = sign_ticket(
                table="reference_sequence_chunks",
                filter={
                    "reference_idx": [fresh_reference],
                    "feature_idx": feature_idxs,
                },
                secret=signing_key,
            )

            # 3. FASTA export via the CLI writer (real miint FORMAT FASTA).
            fasta_out = output_dir / f"{fresh_reference}.{genome_idx}.fasta.gz"
            reader = flight_client.do_get(flight.Ticket(ticket)).to_reader()
            ref_cli._write_genome_fasta(reader, accession_map, fasta_out, con)

            # 4. Parquet export via the CLI writer (raw chunk rows).
            parquet_out = output_dir / f"{fresh_reference}.{genome_idx}.parquet"
            reader2 = flight_client.do_get(flight.Ticket(ticket)).to_reader()
            ref_cli._write_genome_parquet(reader2, parquet_out)

            fasta_records = _read_fasta_gz(con, fasta_out)
            pq_features = set(
                pq.read_table(parquet_out).column("feature_idx").to_pylist()
            )
            results[genome_idx] = {"fasta": fasta_records, "pq_features": pq_features}

        # Genome A's FASTA: its chromosome + the shared plasmid, headed by accession.
        # Stored chunk_data is the ORIGINAL strand (never strand-normalized — the
        # canonical hash is used only for feature_idx dedup), so exported bytes ==
        # the submitted bytes for these byte-identical fixture records.
        a = results[genome_a]["fasta"]
        assert set(a) == {"chromA", "plasmidA"}
        assert a["chromA"] == _SHARED_PLASMID_SEQS["chromA"]
        assert a["plasmidA"] == _SHARED_PLASMID_SEQS["plasmidA"]

        # Genome B's FASTA carries the SAME plasmid record (shared accession + bytes)
        # PLUS its own chromosome — the many-to-many payoff.
        b = results[genome_b]["fasta"]
        assert set(b) == {"chromB", "plasmidA"}
        assert b["chromB"] == _SHARED_PLASMID_SEQS["chromB"]
        assert b["plasmidA"] == a["plasmidA"]  # identical shared-plasmid bytes

        # The plasmid feature appears in both genomes' parquet chunk exports.
        assert plasmid_feature in results[genome_a]["pq_features"]
        assert plasmid_feature in results[genome_b]["pq_features"]
    finally:
        con.close()
        await postgres_pool.execute(
            "DELETE FROM qiita.feature_genome WHERE genome_idx = ANY($1::bigint[])",
            all_genome_idxs,
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])",
            all_genome_idxs,
        )


def _read_fasta_gz(con, path: Path) -> dict[str, str]:
    """Parse a gzipped FASTA into {read_id: sequence} using miint's read_fastx —
    the reader side of the write→read round-trip, never a hand-rolled parser (we
    do not reimplement fundamental parsers). read_fastx yields one row per record
    with `read_id` (the FASTA header) and `sequence1` (the sequence)."""
    rows = con.execute(
        "SELECT read_id, sequence1 FROM read_fastx(?)", [str(path)]
    ).fetchall()
    return {read_id: sequence for read_id, sequence in rows}


async def test_ticket_endpoint_rejects_non_active_reference(
    postgres_pool, signing_key, fresh_reference, compute_worker_service_account
):
    """Ticket route guard still works — reference at status='pending' refuses."""
    from qiita_common.api_paths import LOOPBACK_HOST, URL_REFERENCE_DOGET
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused-in-test",
        flight_signing_key=signing_key,
        data_plane_url=f"grpc://{LOOPBACK_HOST}:0",
    )

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {compute_worker_service_account['token']}"},
    ) as ac:
        resp = await ac.post(
            URL_REFERENCE_DOGET.format(reference_idx=fresh_reference),
            json={"table": "reference_sequences"},
        )
    assert resp.status_code == 409
