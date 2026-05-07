"""System test: GG2 2024.09 backbone — full reference-add pipeline on real data.

Drives `workflows/reference-add/1.0.0.yaml` end-to-end at production
scale through the control-plane runner, with real LocalBackend +
real Flight register/DoGet:

  - 331K sequences hashed → manifest.parquet
  - mint-features with genome_map_path (~72K genome associations on top
    of the 331K feature mints)
  - write-membership over the full feature set
  - load step writes reference_sequences, reference_sequence_chunks,
    reference_membership, reference_taxonomy, reference_phylogeny Parquet
  - register-files via real Arrow Flight DoAction (data plane subprocess)
  - DoGet round-trip verifying expected row counts

Run via: ``make test-system`` (intended to be invoked manually before
cutting a release candidate). Triple-gated:

  - ``pytest.mark.system`` keeps it out of ``make test-integration``
  - ``skipif(not FASTA.exists())`` skips cleanly on machines without the
    GG2 data (CI, fresh checkouts) so the target reports "1 skipped"
    rather than failing
  - the ``localdocs/`` directory itself is gitignored so the multi-GB
    datasets never enter version control

Pinned-version note: the row-count constants below
(_EXPECTED_FEATURES / _EXPECTED_TAXONOMY / _EXPECTED_PHYLOGENY) are
specific to the GG2 2024.09 backbone snapshot. A future release-cycle
update that re-pins to a newer snapshot must re-derive these.

Expected runtime: ~10 minutes (FASTA hashing dominates).
"""

import json
import uuid
from pathlib import Path

import duckdb
import pyarrow.flight as flight
import pytest

from _runner_helpers import LocalComputeBackendClient

DATA_DIR = Path(__file__).parent.parent.parent / "localdocs" / "scratch"
FASTA = DATA_DIR / "2024.09.backbone.sequence.fna.gz"
TREE = DATA_DIR / "2024.09.backbone.nwk.gz"
TAXONOMY = DATA_DIR / "2024.09.backbone.taxonomy.parquet"
GG2_GENOME_MAP = DATA_DIR / "2024.09.backbone.feature-to-genome.parquet"

# Locked-in counts for the GG2 2024.09 backbone — drift in any of these
# is either real data corruption upstream or a regression in our
# pipeline; either way it should fail loudly.
_EXPECTED_FEATURES = 331269
_EXPECTED_TAXONOMY = 331240
_EXPECTED_PHYLOGENY = 662537

_REFERENCE_ADD_YAML_PATH = (
    Path(__file__).parent.parent.parent / "workflows" / "reference-add" / "1.0.0.yaml"
)

pytestmark = [
    pytest.mark.system,
    pytest.mark.skipif(not FASTA.exists(), reason="GG2 data not in localdocs/scratch/"),
]


@pytest.fixture
def gg2_genome_map(tmp_path):
    """Convert GG2's `(feature_id, genome_id)` Parquet to the path-based
    schema `(read_id, genome_source, genome_source_id)` that
    `mint_features` JOINs against the manifest's read_id.

    GG2 doesn't carry a genome_source column (every entry is implicitly
    sourced from GG2 itself), so we hardcode it here. Other corpora that
    write a self-describing genome_source column won't need this fixture.
    """
    out = tmp_path / "gg2_genome_map.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY ("
            "  SELECT feature_id AS read_id,"
            "         'gg2' AS genome_source,"
            "         genome_id AS genome_source_id"
            "  FROM read_parquet(?) WHERE genome_id IS NOT NULL"
            f") TO '{out}' (FORMAT PARQUET)",
            [str(GG2_GENOME_MAP)],
        )
    return out


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
    and clean the row up after."""
    from qiita_control_plane.actions import load_actions, sync_actions

    workflows_dir = tmp_path / "workflows" / "reference-add"
    workflows_dir.mkdir(parents=True)
    yaml_text = _REFERENCE_ADD_YAML_PATH.read_text()
    test_version = f"gg2-system-{uuid.uuid4()}"
    yaml_text = yaml_text.replace("version: 1.0.0", f"version: {test_version}")
    (workflows_dir / "1.0.0.yaml").write_text(yaml_text)

    actions = load_actions(tmp_path / "workflows")
    async with postgres_pool.acquire() as conn:
        await sync_actions(conn, actions)

    yield ("reference-add", test_version)

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        "reference-add",
        test_version,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        "reference-add",
        test_version,
    )


@pytest.fixture
async def gg2_reference(postgres_pool, human_admin_session):
    """Fresh reference for the system run; cleans up everything pointing
    at it before dropping the row."""
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '2024.09', 'sequence_reference', 'pending', $2)"
        " RETURNING reference_idx",
        f"gg2-backbone-{uuid.uuid4()}",
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


async def test_gg2_backbone_full_pipeline(
    postgres_pool,
    data_plane,
    hmac_secret,
    flight_client,
    synced_reference_add_action,
    gg2_reference,
    gg2_genome_map,
    human_admin_session,
    tmp_path,
):
    """Full reference-add pipeline on GG2 2024.09: hash → mint (with
    genome associations) → write-membership → load (with taxonomy +
    tree) → register via Flight → DoGet. Asserts row counts at every
    verifiable stage and confirms genome_map_path produced the expected
    feature_genome rows.
    """
    from qiita_common.api_paths import LOOPBACK_HOST
    from qiita_control_plane.auth.tickets import sign_ticket
    from qiita_control_plane.runner import run_workflow

    action_id, action_version = synced_reference_add_action
    action_context = json.dumps(
        {
            "fasta_path": str(FASTA),
            "taxonomy_path": str(TAXONOMY),
            "tree_path": str(TREE),
            "genome_map_path": str(gg2_genome_map),
        }
    )
    work_ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket ("
        "  action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context"
        ") VALUES ($1, $2, $3, 'reference', $4, $5::jsonb)"
        " RETURNING work_ticket_idx",
        action_id,
        action_version,
        human_admin_session["principal_idx"],
        gg2_reference,
        action_context,
    )

    await run_workflow(
        work_ticket_idx,
        postgres_pool,
        LocalComputeBackendClient(),  # type: ignore[arg-type]
        hmac_secret=hmac_secret,
        data_plane_url=f"grpc://{LOOPBACK_HOST}:{data_plane['port']}",
        workspace_root=tmp_path / "workspace",
    )

    # --- Pipeline reached the success terminals ---
    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert state == "completed"
    ref_status = await postgres_pool.fetchval(
        "SELECT status FROM qiita.reference WHERE reference_idx = $1", gg2_reference
    )
    assert ref_status == "active"

    # --- Mint count: every feature on this reference's membership ---
    membership_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.reference_membership WHERE reference_idx = $1",
        gg2_reference,
    )
    assert membership_count == _EXPECTED_FEATURES

    # --- Genome associations: every row in the converted genome map
    # should have produced exactly one feature_genome row scoped to this
    # reference's feature set.
    expected_genome_count = (
        duckdb.connect(":memory:")
        .execute("SELECT count(*) FROM read_parquet(?)", [str(gg2_genome_map)])
        .fetchone()[0]
    )
    actual_genome_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.feature_genome fg"
        " JOIN qiita.genome g USING (genome_idx)"
        " JOIN qiita.reference_membership m ON m.feature_idx = fg.feature_idx"
        " WHERE m.reference_idx = $1 AND g.source = 'gg2'",
        gg2_reference,
    )
    assert actual_genome_count == expected_genome_count

    # --- DoGet round-trips via real Flight ---
    def _doget(table: str):
        ticket_bytes = sign_ticket(
            table=table,
            filter={"reference_idx": [gg2_reference]},
            secret=hmac_secret,
        )
        return flight_client.do_get(flight.Ticket(ticket_bytes)).read_all()

    # reference_sequences — sequence metadata.
    seq_table = _doget("reference_sequences")
    assert seq_table.num_rows == _EXPECTED_FEATURES
    assert {"feature_idx", "sequence_hash", "sequence_length_bp"}.issubset(
        set(seq_table.column_names)
    )

    # reference_taxonomy — locked count for the 2024.09 snapshot.
    tax_table = _doget("reference_taxonomy")
    assert tax_table.num_rows == _EXPECTED_TAXONOMY
    assert "domain" in tax_table.column_names

    # reference_phylogeny — tips + internal nodes for the backbone tree.
    phylo_table = _doget("reference_phylogeny")
    assert phylo_table.num_rows == _EXPECTED_PHYLOGENY
    assert "feature_idx" in phylo_table.column_names
