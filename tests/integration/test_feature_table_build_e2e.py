"""End-to-end: a plain USER runs `qiita feature-table build` and gets a publishable
feature table.

The whole client-side recipe, with nothing faked below the CLI's own entry point:
the real `qiita` argparse `main()`, a live control plane in a subprocess, real
Ed25519-signed Flight tickets it mints, a live data plane that verifies them with
the matching public key, and real miint doing the coverage and the woltka roll-up.
The only thing the test supplies is the seed and the two URLs.

That combination is what no narrower test can reach. The unit suite fakes HTTP and
Flight; the orchestrator's `test_estimate_feature_table.py` runs the same analytic
server-side but signs its own tickets and never relabels. **Here the identifiers a
user would publish come out of the real mint routes**, so the assertion at the
bottom is on the file itself: `QM…` handles, the genomes' `source_id`s, and not one
column of ours.

The fixture makes every load-bearing rule of the analytic visible in the OUTPUT:

  * genome A (10 kb) is covered 0.6% in each of two samples, in EXTENDING regions
    — 1.2% pooled, so it clears a 1% threshold only when the cohort is pooled.
    It is the pooled/per-sample discriminator, end to end. It is also the
    UNCLASSIFIED genome: it must reach the taxonomy sidecar with empty ranks
    rather than be left out of it.
  * genome B (1 kb) is covered 0.5% and must never appear, in either scope — and
    it is classified, so it also shows that the sidecar is exactly as wide as the
    table rather than as wide as the reference.
  * genome C (two contigs, 1.1 kb) carries TWO reads in one sample, so its value
    is 2.0 — the table is counting, not merely reporting presence. Its LOWER
    contig is blocked by a curated exclusion and carries a different lineage, so
    what C publishes says which contig spoke for it.

The tree is the third artifact and the only one whose shape is not row-wise: it
carries one tip per published genome, and shearing it to the survivors leaves an
ancestor with a single child, whose branch length must be added to the surviving
one rather than dropped.

Requires real miint (native BIGINT ids through `woltka_ogu`) and the built
data-plane debug binary, like every other test in this directory.
"""

import json
import uuid

import duckdb
import pytest
from conftest import ducklake_connect
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.models.reference import Tier
from qiita_common.taxonomy import RANK_COLUMNS

from qiita_control_plane.repositories.alignment_definition import mint_alignment_definition
from qiita_control_plane.repositories.block import (
    create_alignment_sample_pending,
    finalize_alignment_sample,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_prep_sample_to_study_link,
    seed_sequenced_sample_subtype,
)

_THRESHOLD = "0.01"

# Each genome's contigs, in creation order. `feature_idx` ascends with insertion and a
# genome speaks through its lowest CLASSIFIED contig, so C's blocked contig is created
# FIRST: it is the one a reduction that merely took the lowest would pick, which would
# publish C as unclassified.
_CONTIGS = {"A": ("A",), "B": ("B",), "C": ("C.blocked", "C")}

# Every contig's length in bp. Lengths are the coverage DENOMINATOR and it is the full
# GENOME length — which is what makes B's 5 covered bases 0.5% rather than something
# that survives, and what puts C's blocked contig in C's denominator: an exclusion is a
# read-time mask over the alignment and the taxonomy, not a claim that the sequence is
# not there.
_LENGTHS = {"A": 10000, "B": 1000, "C.blocked": 100, "C": 1000}

# Lineages as ingest stores them — prefixes stripped, an absent rank NULL. The sidecar
# restores the prefixes, so `Bacteria` here must come back as `d__Bacteria`.
_LINEAGE_B = ("Bacteria", "Bacteroidota", "Bacteroidia", "Bacteroidales", "Bacteroidaceae")
_LINEAGE_C = (
    "Bacteria",
    "Pseudomonadota",
    "Gammaproteobacteria",
    "Enterobacterales",
    "Enterobacteriaceae",
    "Escherichia",
    "coli",
)
# The blocked contig's own lineage, deliberately unlike its sibling's: if the exclusion
# stopped being applied to the taxonomy stream, C would publish THIS instead, and a test
# that gave both contigs one lineage could not tell.
_LINEAGE_C_BLOCKED = (
    "Bacteria",
    "Bacillota",
    "Bacilli",
    "Lactobacillales",
    "Streptococcaceae",
    "Streptococcus",
    "pyogenes",
)

# The reference's own tree, as ingest would have written it: tips named by FASTA header,
# and a jplace edge id per branch. No tip for C's blocked contig — a genome-level tree
# has one tip per genome, and a second tip mapping to C would be refused as ambiguous.
#
#   ((contig-A:0.25, contig-B:0.5)inner:0.125, contig-C:0.75);
#
# Shearing to {A, C} drops B's tip, leaving `inner` with one child. Both branch lengths
# are exact binary fractions, so their sum is exact too.
_BRANCH_A, _BRANCH_B, _BRANCH_C, _BRANCH_INNER = 0.25, 0.5, 0.75, 0.125
_EDGE_A, _EDGE_C = 11, 14


def _rank_literals(lineage: tuple[str, ...]) -> str:
    """A lineage as the eight SQL literals its row carries, NULL past its end.

    One column per rank with the prefix stripped and an unreported rank left NULL is
    what ingest writes, so a short lineage here is a genome classified only that far —
    and an empty one is a genome nothing classified at all.
    """
    padded = (*lineage, *(None,) * (len(RANK_COLUMNS) - len(lineage)))
    return ", ".join("NULL" if rank is None else f"'{rank}'" for rank in padded)


@pytest.fixture
async def publishable_cohort(postgres_pool, human_admin_session, regular_user_session, data_plane):
    """Seed the whole recipe's inputs across both stores with coordinated ids, in one
    study the calling user holds `Tier.VIEWER` on.

    Postgres carries what only it can (the reference's feature→genome map, the
    alignment definition and its per-sample completion gates, the study links the
    cohort is authorized against); DuckLake carries everything the build streams —
    per-feature lengths, taxonomy, phylogeny, and the alignment slice — plus the
    resolved blocklist the exclusion-aware views anti-join. The genomes get
    recognizable `source_id`s because they are what the published table is expected
    to be named by.
    """
    db = postgres_pool
    owner = human_admin_session["principal_idx"]
    reader = regular_user_session["principal_idx"]
    tag = uuid.uuid4().hex[:8]

    study_idx = await db.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner,
        f"ft-build-e2e-{tag}",
    )
    await db.execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier, granted_by_idx)"
        " VALUES ($1, $2, $3::qiita.tier, $4)",
        study_idx,
        reader,
        Tier.VIEWER,
        owner,
    )

    # `active`, not the default `pending`: the reference DoGet ticket route refuses to
    # sign against a reference that is not ready, and the lengths stream goes through
    # that route. A test that signed its own tickets would not have noticed.
    reference_idx = await db.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, is_host, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', false, 'active', $2) RETURNING reference_idx",
        f"ft-build-e2e-{tag}",
        owner,
    )
    features: dict[str, int] = {}
    genomes: dict[str, int] = {}
    source_ids = {name: f"GCF_{tag}_{name}" for name in ("A", "B", "C")}
    for name, source_id in source_ids.items():
        genomes[name] = await db.fetchval(
            "INSERT INTO qiita.genome (source, source_id) VALUES ('refseq', $1)"
            " RETURNING genome_idx",
            source_id,
        )
        for contig in _CONTIGS[name]:
            features[contig] = await db.fetchval(
                "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid())"
                " RETURNING feature_idx"
            )
            await db.execute(
                "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
                features[contig],
                genomes[name],
            )
            await db.execute(
                "INSERT INTO qiita.reference_membership (reference_idx, feature_idx)"
                " VALUES ($1, $2)",
                reference_idx,
                features[contig],
            )

    samples: list[tuple[int, int, int]] = []
    run_idx = pool_idx = None
    for i in range(2):
        biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
            db, owner_idx=owner
        )
        run_idx, pool_idx, ss_idx = await seed_sequenced_sample_subtype(
            db,
            prep_sample_idx=prep_sample_idx,
            owner_idx=owner,
            sequenced_pool_item_id=f"ft-build-{tag}-{i}",
            sequencing_run_idx=run_idx,
            sequenced_pool_idx=pool_idx,
        )
        samples.append((biosample_idx, prep_sample_idx, ss_idx))
        await seed_biosample_to_study_link(
            db, biosample_idx=biosample_idx, study_idx=study_idx, created_by_idx=owner
        )
        await seed_prep_sample_to_study_link(
            db, prep_sample_idx=prep_sample_idx, study_idx=study_idx, created_by_idx=owner
        )
    prep_sample_idxs = [ps for _, ps, _ in samples]
    ps0, ps1 = prep_sample_idxs

    async with db.acquire() as conn:
        alignment_idx = (
            await mint_alignment_definition(
                conn,
                params={
                    "reference_idx": reference_idx,
                    "aligner": "minimap2",
                    "mask_idx": 1,
                    "shard_ids": [0],
                },
                principal_idx=owner,
            )
        )["alignment_idx"]
    # `completed` is first-class: alignment rows are not 1:1 with reads, so their
    # presence never means done — both the cohort route and the ticket mint read it.
    async with db.acquire() as conn, conn.transaction():
        await create_alignment_sample_pending(
            conn, alignment_idx=alignment_idx, prep_sample_idxs=prep_sample_idxs
        )
        for prep_sample_idx in prep_sample_idxs:
            await finalize_alignment_sample(
                conn, alignment_idx=alignment_idx, prep_sample_idx=prep_sample_idx
            )

    lake = ducklake_connect(data_plane["data_path"])
    try:
        lake.execute(
            "INSERT INTO qiita_lake.reference_sequences"
            " (feature_idx, sequence_hash, sequence_length_bp) VALUES "
            + ", ".join(
                f"({features[contig]}, gen_random_uuid(), {bp})"
                for contig, bp in _LENGTHS.items()
            )
        )
        lake.execute(
            "INSERT INTO qiita_lake.reference_membership VALUES "
            + ", ".join(f"({reference_idx}, {features[contig]})" for contig in _LENGTHS)
        )
        # Ingest writes taxonomy 1-1 with the reference's features, so an unclassified
        # feature is an all-NULL row rather than a missing one — that is what makes
        # "absent from the sidecar" mean "not in the table" and nothing else.
        lake.execute(
            "INSERT INTO qiita_lake.reference_taxonomy"
            " (reference_idx, feature_idx, domain, phylum, class, \"order\", family,"
            " genus, species, strain) VALUES "
            + ", ".join(
                f"({reference_idx}, {features[contig]}, {_rank_literals(lineage)})"
                for contig, lineage in (
                    ("A", ()),
                    ("B", _LINEAGE_B),
                    ("C.blocked", _LINEAGE_C_BLOCKED),
                    ("C", _LINEAGE_C),
                )
            )
        )
        # The resolved blocklist, as `sync_reference_exclusion` ships it from
        # `qiita.reference_exclusion`. It is what the `_visible` views anti-join, so
        # writing it here is what a curator's block amounts to on the read path.
        lake.execute(
            "INSERT INTO qiita_lake.reference_exclusion (feature_idx) VALUES"
            f" ({features['C.blocked']})"
        )
        lake.execute(
            "INSERT INTO qiita_lake.reference_phylogeny (reference_idx, node_index, name,"
            " branch_length, edge_id, parent_index, is_tip, feature_idx) VALUES"
            f" ({reference_idx}, 0, 'contig-A', {_BRANCH_A}, {_EDGE_A}, 2, true,"
            f"  {features['A']}),"
            f" ({reference_idx}, 1, 'contig-B', {_BRANCH_B}, 12, 2, true, {features['B']}),"
            f" ({reference_idx}, 2, 'inner', {_BRANCH_INNER}, 13, 4, false, NULL),"
            f" ({reference_idx}, 3, 'contig-C', {_BRANCH_C}, {_EDGE_C}, 4, true,"
            f"  {features['C']}),"
            f" ({reference_idx}, 4, '', NULL, 15, NULL, false, NULL)"
        )
        lake.execute(
            "INSERT INTO qiita_lake.alignment"
            " (alignment_idx, prep_sample_idx, sequence_idx, feature_idx, flags,"
            " position, stop_position) VALUES"
            # A: 0.6% in each sample, extending -> 1.2% pooled only.
            f" ({alignment_idx}, {ps0}, 1, {features['A']}, 0, 0, 60),"
            f" ({alignment_idx}, {ps1}, 2, {features['A']}, 0, 60, 120),"
            # B: 5 bp of 1000 -> 0.5%, under the threshold in either scope.
            f" ({alignment_idx}, {ps0}, 3, {features['B']}, 0, 0, 5),"
            # C: two reads in one sample -> value 2.0, and 70% covered.
            f" ({alignment_idx}, {ps0}, 4, {features['C']}, 0, 0, 500),"
            f" ({alignment_idx}, {ps0}, 5, {features['C']}, 0, 200, 700)"
        )
    finally:
        lake.close()

    yield {
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "alignment_idx": alignment_idx,
        "prep_sample_idxs": prep_sample_idxs,
        "source_ids": source_ids,
    }

    # Postgres teardown in FK-reverse order. The DuckLake rows stay: the catalog is
    # module-scoped and reset on the next module, and every id here is freshly minted,
    # so the signed ticket's filter can only ever match this test's own rows.
    bio_idxs = [bs for bs, _, _ in samples]
    ss_idxs = [ss for _, _, ss in samples]
    await db.execute(
        "DELETE FROM qiita.exported_identifier WHERE alignment_idx = $1", alignment_idx
    )
    await db.execute("DELETE FROM qiita.alignment_sample WHERE alignment_idx = $1", alignment_idx)
    await db.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
    )
    await db.execute(
        "DELETE FROM qiita.prep_sample_to_study WHERE prep_sample_idx = ANY($1::bigint[])",
        prep_sample_idxs,
    )
    await db.execute(
        "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = ANY($1::bigint[])", bio_idxs
    )
    await db.execute("DELETE FROM qiita.sequenced_sample WHERE idx = ANY($1::bigint[])", ss_idxs)
    await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await db.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", prep_sample_idxs
    )
    await db.execute("DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", bio_idxs)
    await db.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", reference_idx
    )
    await db.execute(
        "DELETE FROM qiita.feature_genome WHERE feature_idx = ANY($1::bigint[])",
        list(features.values()),
    )
    await db.execute(
        "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])", list(features.values())
    )
    await db.execute(
        "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])", list(genomes.values())
    )
    await db.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)
    await db.execute(
        "DELETE FROM qiita.study_access WHERE study_idx = $1 AND principal_idx = $2",
        study_idx,
        reader,
    )
    await db.execute("DELETE FROM qiita.study WHERE idx = $1", study_idx)


def _read_artifact(path, *, fmt: str = "parquet") -> list[tuple]:
    """Read a written bundle member back with its own reader, values and all."""
    from qiita_control_plane.miint import connect_with_miint

    reader = "read_parquet" if fmt == "parquet" else "read_biom"
    with connect_with_miint() as conn:
        return sorted(conn.execute(f"SELECT * FROM {reader}('{path}')").fetchall())


async def test_a_user_builds_a_publishable_feature_table(
    cp_server, data_plane, regular_user_session, publishable_cohort, tmp_path, monkeypatch, capsys
):
    """The whole recipe from the client's side: three verbs, two stores, one file.

    Nothing below the CLI is stubbed and nothing names a `prep_sample_idx` after the
    seed — the cohort is discovered, the reference is read out of the alignment's own
    params, and the sample handles are minted. So a narrowing bug anywhere in that
    chain shows up as a wrong table rather than as a passing test.
    """
    from qiita_control_plane.cli import user as cli

    seed = publishable_cohort
    dp_url = f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"
    monkeypatch.setenv("QIITA_TOKEN", regular_user_session["token"])
    # A directory of the test's own: `cp_server` keeps its dummy token file in tmp_path,
    # and one assertion below is that the two bundles are ALL that a build leaves behind.
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    # The flags every invocation below shares, so each verb reads as its own delta.
    pool = [
        "--sequencing-run-idx",
        str(seed["run_idx"]),
        "--sequenced-pool-idx",
        str(seed["pool_idx"]),
    ]
    alignment = ["--alignment-idx", str(seed["alignment_idx"])]

    def _run(*argv: str) -> int:
        return cli.main(["--base-url", cp_server, *argv])

    def _build(*extra: str, output) -> int:
        return _run(
            "feature-table",
            "build",
            *pool,
            *alignment,
            "--coverage-threshold",
            _THRESHOLD,
            "--output",
            str(output),
            "--data-plane-url",
            dp_url,
            *extra,
        )

    # --- Discovery: the two verbs a user reaches for first. ---
    assert _run("alignment", "list", *pool) == 0
    listed = json.loads(capsys.readouterr().out)
    summary = next(a for a in listed["alignments"] if a["alignment_idx"] == seed["alignment_idx"])
    assert (summary["samples_completed"], summary["samples_total"]) == (2, 2)

    assert _run("alignment", "cohort", *pool, *alignment) == 0
    assert json.loads(capsys.readouterr().out)["prep_sample_idx"] == seed["prep_sample_idxs"]

    # --- Pooled, Parquet: the whole bundle, over the discovered cohort. ---
    pooled = out_dir / "pooled.parquet"
    assert _build("--taxonomy", "--tree", output=pooled) == 0
    printed = capsys.readouterr().out

    map_path = out_dir / "pooled.exported-identifier.json"
    identifiers = json.loads(map_path.read_text())["identifiers"]
    handles = {entry["prep_sample_idx"]: entry["export_id"] for entry in identifiers}
    assert sorted(handles) == seed["prep_sample_idxs"]
    assert all(handle.startswith("QM") for handle in handles.values())

    ps0, ps1 = seed["prep_sample_idxs"]
    src = seed["source_ids"]
    # A survives only by pooling; B never survives; C counts two reads in one sample.
    assert _read_artifact(pooled) == sorted(
        [
            (handles[ps0], src["A"], 1.0),
            (handles[ps1], src["A"], 1.0),
            (handles[ps0], src["C"], 2.0),
        ]
    )

    # Nothing of ours is in the file a user would publish, and the map that does
    # carry the join key says so in the file rather than only in the terminal.
    described = duckdb.connect(":memory:").execute(
        f"DESCRIBE SELECT * FROM read_parquet('{pooled}')"
    )
    assert [row[0] for row in described.fetchall()] == ["sample_id", "feature_id", "value"]
    assert "prep_sample_idx" in json.loads(map_path.read_text())["note"]
    # Every member is named on stdout, so a user knows what the build left behind.
    taxonomy_path = out_dir / "pooled.taxonomy.parquet"
    tree_path = out_dir / "pooled.tree.parquet"
    assert all(str(path) in printed for path in (map_path, taxonomy_path, tree_path))

    # --- The taxonomy sidecar: one row per published row, named the same way. ---
    ranks = {row[0]: row[1:] for row in _read_artifact(taxonomy_path)}
    # B is classified in the reference and absent here: the sidecar is as wide as the
    # table, not as wide as the reference.
    assert set(ranks) == {src["A"], src["C"]}
    # A is unclassified and says so by being PRESENT with empty ranks — a genome missing
    # from a taxonomy file is indistinguishable from one missing from the table.
    assert ranks[src["A"]] == (None,) * len(RANK_COLUMNS)
    # C speaks through its lowest CLASSIFIED contig. The blocked one is lower and carries
    # a Streptococcus lineage, so this is equally the assertion that the exclusion reached
    # the taxonomy stream. Prefixes restored; the unreported strain stays NULL, not `t__`.
    assert ranks[src["C"]] == (
        "d__Bacteria",
        "p__Pseudomonadota",
        "c__Gammaproteobacteria",
        "o__Enterobacterales",
        "f__Enterobacteriaceae",
        "g__Escherichia",
        "s__coli",
        None,
    )

    # --- The sheared tree: (node_index, name, branch_length, edge_id, parent, is_tip). ---
    tree = _read_artifact(tree_path)
    tips = {row[1]: row for row in tree if row[5]}
    assert set(tips) == {src["A"], src["C"]}
    # `inner` lost B's tip and so has a single child; collapse removes it and adds its
    # branch to A's, so a distance read off the published tree is the distance the whole
    # reference tree carried. Both are exact binary fractions, hence an exact ==.
    assert tips[src["A"]][2] == _BRANCH_A + _BRANCH_INNER
    assert tips[src["C"]][2] == _BRANCH_C
    # A surviving edge keeps its OWN jplace id — the handle back to the reference's
    # placements, and why the tree ships as a node table rather than as Newick.
    assert (tips[src["A"]][3], tips[src["C"]][3]) == (_EDGE_A, _EDGE_C)
    # A root and its two tips, and not one reference-internal tip name in the file: every
    # tip is named from the same mint the table's rows are.
    assert len(tree) == 3
    assert not any("contig" in (row[1] or "") for row in tree)

    # --- Per-sample, BIOM: the stricter scope and the other writer, same recipe. ---
    per_sample = out_dir / "per-sample.biom"
    assert _build("--coverage-scope", "per-sample", "--format", "biom", output=per_sample) == 0
    # A's 0.6% per sample is under the threshold that its 1.2% pooled cleared, so the
    # only survivor is the genome one sample really covers.
    assert _read_artifact(per_sample, fmt="biom") == [(handles[ps0], src["C"], 2.0)]

    # Both builds' bundles coexist, which is what naming the map after the table buys —
    # and this build asked for neither companion, so a bundle is three files or five.
    assert sorted(p.name for p in out_dir.iterdir()) == [
        "per-sample.biom",
        "per-sample.exported-identifier.json",
        "per-sample.manifest.json",
        "pooled.exported-identifier.json",
        "pooled.manifest.json",
        "pooled.parquet",
        "pooled.taxonomy.parquet",
        "pooled.tree.parquet",
    ]
    # The mint is idempotent, which only a second real build can show: the two bundles
    # name the same samples the same way, so the tables above are comparable to each
    # other and a re-run does not rename anybody's columns.
    second_map = json.loads((out_dir / "per-sample.exported-identifier.json").read_text())
    assert {e["prep_sample_idx"]: e["export_id"] for e in second_map["identifiers"]} == handles

    # --- The manifest, against the real mint and the real reference row. ---
    manifests = [
        json.loads((out_dir / f"{stem}.manifest.json").read_text())
        for stem in ("pooled", "per-sample")
    ]
    for manifest in manifests:
        # Public handle, resolved reference, and the digest the client verified before
        # reading anything off `params` — none of it an identifier of ours.
        assert manifest["processing"]["export_processing_id"].startswith("QP")
        assert manifest["processing"]["reference"]["name"]
        assert len(manifest["processing"]["params_hash"]) == 64
        assert sorted(manifest["table"]["cohort"]) == sorted(handles.values())
        assert "idx" not in json.dumps(manifest)
    # The processing handle is a property of the PROCESSING, not of the cohort or the
    # scope — so two bundles built from one alignment cite the same one, which is how a
    # reader sees they share it. The scopes differ, and must.
    assert (
        manifests[0]["processing"]["export_processing_id"]
        == manifests[1]["processing"]["export_processing_id"]
    )
    assert [m["table"]["coverage_scope"] for m in manifests] == ["pooled", "per-sample"]
    # Each manifest lists the bundle it belongs to, itself included — the members a
    # reader needs to have received, in the order they were written.
    assert manifests[0]["files"] == [
        "pooled.parquet",
        "pooled.exported-identifier.json",
        "pooled.manifest.json",
        "pooled.taxonomy.parquet",
        "pooled.tree.parquet",
    ]
    assert manifests[1]["files"] == [
        "per-sample.biom",
        "per-sample.exported-identifier.json",
        "per-sample.manifest.json",
    ]
