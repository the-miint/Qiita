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

The fixture makes every rule of the analytic visible in the OUTPUT:

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

The tree is the third artifact and the only one whose shape is not row-wise. It is
contig-level here, so C has a tip per contig — and only the exclusion decides which
of the two speaks for it, because the phylogeny is the one stream with no
exclusion-aware view. Shearing to the survivors leaves each ancestor with a single
child, whose branch length must be added to the surviving one rather than dropped.

Requires real miint (native BIGINT ids through `woltka_ogu`) and the built
data-plane debug binary, like every other test in this directory.
"""

import json
import uuid

import duckdb
import pytest
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.models.reference import Tier
from qiita_common.taxonomy import RANK_COLUMNS
from qiita_control_plane.repositories.alignment_definition import mint_alignment_definition
from qiita_control_plane.repositories.block import (
    MASK_SAMPLE_COMPLETED,
    create_alignment_sample_pending,
    finalize_alignment_sample,
)
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_prep_sample_to_study_link,
    seed_sequenced_sample_subtype,
)
from qiita_control_plane.testing.db_teardown import teardown_entity_graph

from conftest import ducklake_connect

_THRESHOLD = "0.01"

# Each genome's contigs, in creation order. `feature_idx` ascends with insertion and a
# genome speaks through its lowest CLASSIFIED contig, so C's blocked contig is created
# FIRST: it is the one a reduction that merely took the lowest would pick, which would
# publish C as unclassified.
_CONTIGS = {"A": ("A",), "B": ("B",), "C": ("C.blocked", "C")}

# Every contig's length in bp. The denominator is the full GENOME length — see
# `genome_lengths_table_sql` — which is what makes B's 5 covered bases 0.5% rather than
# something that survives, and what puts C's blocked contig in C's 1100.
_LENGTHS = {"A": 10000, "B": 1000, "C.blocked": 100, "C": 1000}

# Lineages as ingest stores them — prefixes stripped, an absent rank NULL. The sidecar
# restores the prefixes, so `Bacteria` here must come back as `d__Bacteria`.
_LINEAGE_B = (
    "Bacteria",
    "Bacteroidota",
    "Bacteroidia",
    "Bacteroidales",
    "Bacteroidaceae",
)
_LINEAGE_C = (
    "Bacteria",
    "Pseudomonadota",
    "Gammaproteobacteria",
    "Enterobacterales",
    "Enterobacteriaceae",
    "Escherichia",
    "coli",
)
# The blocked contig's own lineage, unlike its sibling's: if the exclusion
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
# and a jplace edge id per branch. A contig-level tree, so genome C has a tip per contig
# — including the blocked one, which is the shape that makes the blocklist do work here.
#
#   ((contig-A:0.25, contig-B:0.5)inner:0.125,
#    (contig-C.blocked:0.0625, contig-C:0.75)cnode:0.03125);
#
# Shearing to {A, C} drops B's tip (unpublished) and C.blocked's (blocked), leaving each
# internal node with one child. Every branch length is an exact binary fraction, so both
# collapsed sums are exact too.
_BRANCH_A, _BRANCH_B, _BRANCH_C = 0.25, 0.5, 0.75
_BRANCH_C_BLOCKED, _BRANCH_INNER, _BRANCH_CNODE = 0.0625, 0.125, 0.03125
_EDGE_A, _EDGE_C = 11, 15


async def _masked(db, *, owner: int, tag: str, prep_sample_idxs: list[int]) -> int:
    """Mint a mask and record each sample `completed` under it. An alignment's samples
    resolve to it only when their mask resolves to the mask its params name
    (`definition_resolver`), and the build refuses any that do not."""
    async with db.acquire() as conn:
        mask_idx = (
            await mint_mask_definition(
                conn,
                filter_workflow="read-mask",
                filter_version="1.0.0",
                params={"filter_workflow": "read-mask", "t": tag},
                principal_idx=owner,
            )
        )["mask_idx"]
    await db.executemany(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state) VALUES ($1, $2, $3)",
        [
            (mask_idx, prep_sample_idx, MASK_SAMPLE_COMPLETED)
            for prep_sample_idx in prep_sample_idxs
        ],
    )
    return mask_idx


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

    # The curator's block, in BOTH stores, because the recipe reads both: Postgres is
    # authoritative and is what the tree's own exclusion read answers from, and the lake
    # mirror is what the exclusion-aware views anti-join. `sync_reference_exclusion` is
    # what keeps them equal in production; seeding one without the other would model a
    # state that cannot occur.
    await db.execute(
        "INSERT INTO qiita.reference_exclusion (feature_idx, reason, excluded_by_idx)"
        " VALUES ($1, 'contaminant contig, seeded by the feature-table build test', $2)",
        features["C.blocked"],
        owner,
    )

    samples: list[tuple[int, int, int]] = []
    run_idx = pool_idx = None
    for i in range(2):
        (
            biosample_idx,
            prep_sample_idx,
        ) = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
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
            db,
            prep_sample_idx=prep_sample_idx,
            study_idx=study_idx,
            created_by_idx=owner,
        )
    prep_sample_idxs = [ps for _, ps, _ in samples]
    ps0, ps1 = prep_sample_idxs
    mask_idx = await _masked(db, owner=owner, tag=tag, prep_sample_idxs=prep_sample_idxs)

    async with db.acquire() as conn:
        alignment_idx = (
            await mint_alignment_definition(
                conn,
                params={
                    "reference_idx": reference_idx,
                    "aligner": "minimap2",
                    "mask_idx": mask_idx,
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
                f"({features[contig]}, gen_random_uuid(), {bp})" for contig, bp in _LENGTHS.items()
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
            ' (reference_idx, feature_idx, domain, phylum, class, "order", family,'
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
            f" ({reference_idx}, 2, 'inner', {_BRANCH_INNER}, 13, 6, false, NULL),"
            f" ({reference_idx}, 3, 'contig-C.blocked', {_BRANCH_C_BLOCKED}, 14, 5, true,"
            f"  {features['C.blocked']}),"
            f" ({reference_idx}, 4, 'contig-C', {_BRANCH_C}, {_EDGE_C}, 5, true,"
            f"  {features['C']}),"
            f" ({reference_idx}, 5, 'cnode', {_BRANCH_CNODE}, 16, 6, false, NULL),"
            f" ({reference_idx}, 6, '', NULL, 17, NULL, false, NULL)"
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

    # The DuckLake rows stay: the catalog is module-scoped and reset on the
    # next module, and every id here is freshly minted, so the signed ticket's
    # filter can only ever match this test's own rows.
    bio_idxs = [bs for bs, _, _ in samples]
    await teardown_entity_graph(
        db,
        study_idxs=[study_idx],
        biosample_idxs=bio_idxs,
        prep_sample_idxs=prep_sample_idxs,
    )
    await db.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
    )
    await db.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)
    await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    # These genomes are refseq rather than qiita-derived, so they carry no
    # prep_sample_idx and nothing about them was in the sweep's range.
    await db.execute(
        "DELETE FROM qiita.reference_exclusion WHERE feature_idx = ANY($1::bigint[])",
        list(features.values()),
    )
    await db.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", reference_idx
    )
    await db.execute(
        "DELETE FROM qiita.feature_genome WHERE feature_idx = ANY($1::bigint[])",
        list(features.values()),
    )
    await db.execute(
        "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])",
        list(features.values()),
    )
    await db.execute(
        "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])",
        list(genomes.values()),
    )
    await db.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)


def _read_artifact(path, *, fmt: str = "parquet") -> list[tuple]:
    """Read a written bundle member back with its own reader, values and all."""
    from qiita_control_plane.miint import connect_with_miint

    reader = "read_parquet" if fmt == "parquet" else "read_biom"
    with connect_with_miint() as conn:
        return sorted(conn.execute(f"SELECT * FROM {reader}('{path}')").fetchall())


async def test_a_user_builds_a_publishable_feature_table(
    cp_server,
    data_plane,
    regular_user_session,
    publishable_cohort,
    tmp_path,
    monkeypatch,
    capsys,
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
    assert [row[0] for row in described.fetchall()] == [
        "sample_id",
        "feature_id",
        "value",
    ]
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
    # Each internal node lost one of its two tips and so collapses, its branch adding to
    # the survivor's — so a distance read off the published tree is the distance the whole
    # reference tree carried. All exact binary fractions, hence an exact ==.
    #
    # The two tips went for DIFFERENT reasons, which is the point: B's genome is not in
    # the table, while C.blocked's is — C publishes, on the strength of a sibling contig.
    # Its tip is gone because a curator blocked that contig, and the blocklist reaches
    # this file through a REST read of its own; the phylogeny stream cannot carry it.
    assert tips[src["A"]][2] == _BRANCH_A + _BRANCH_INNER
    assert tips[src["C"]][2] == _BRANCH_C + _BRANCH_CNODE
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
        # reading anything off `params` — none of it an identifier of ours. One part:
        # every sample resolved to the alignment named.
        (part,) = manifest["processing"]["parts"]
        assert part["export_processing_id"].startswith("QP")
        assert manifest["processing"]["reference"]["name"]
        assert len(part["params_hash"]) == 64
        assert manifest["processing"]["sample_part"] == {
            handle: part["export_processing_id"] for handle in handles.values()
        }
        assert sorted(manifest["table"]["cohort"]) == sorted(handles.values())
        assert "idx" not in json.dumps(manifest)
    # The processing handle is a property of the PROCESSING, not of the cohort or the
    # scope — so two bundles built from one alignment cite the same one, which is how a
    # reader sees they share it. The scopes differ, and must.
    assert manifests[0]["processing"]["parts"] == manifests[1]["processing"]["parts"]
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


# ---------------------------------------------------------------------------
# A cohort across two pools and a similarity group.
#
# Masks M1 and M2 are decided similar, so alignments A (on M1) and B (on M2), whose
# params differ only in the mask, are one alignment group. Sample 0 is on pool 1,
# masked under M1 and aligned by A; sample 1 is on pool 2, masked under M2 and aligned
# by B. Genome G (10 kb) is covered 0.6% by each sample in extending regions, so it
# clears a 1% threshold only with coverage pooled over BOTH parts. Sample 2 is on pool
# 1, masked under M1 and aligned by nothing: not processed in the group.
# ---------------------------------------------------------------------------


@pytest.fixture
async def cross_pool_cohort(postgres_pool, human_admin_session, regular_user_session, data_plane):
    from qiita_common.models import DefinitionKind, DefinitionPairVerdict
    from qiita_control_plane.definition_similarity import decide_pair, open_pair

    db = postgres_pool
    owner = human_admin_session["principal_idx"]
    reader = regular_user_session["principal_idx"]
    tag = uuid.uuid4().hex[:8]

    study_idx = await db.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner,
        f"ft-cross-pool-{tag}",
    )
    await db.execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier, granted_by_idx)"
        " VALUES ($1, $2, $3::qiita.tier, $4)",
        study_idx,
        reader,
        Tier.VIEWER,
        owner,
    )
    reference_idx = await db.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, is_host, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', false, 'active', $2) RETURNING reference_idx",
        f"ft-cross-pool-{tag}",
        owner,
    )
    source_id = f"GCF_{tag}_G"
    genome_idx = await db.fetchval(
        "INSERT INTO qiita.genome (source, source_id) VALUES ('refseq', $1) RETURNING genome_idx",
        source_id,
    )
    feature_idx = await db.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid()) RETURNING feature_idx"
    )
    await db.execute(
        "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
        feature_idx,
        genome_idx,
    )
    await db.execute(
        "INSERT INTO qiita.reference_membership (reference_idx, feature_idx) VALUES ($1, $2)",
        reference_idx,
        feature_idx,
    )

    # Samples 0 and 2 on pool 1, sample 1 on pool 2.
    samples: list[tuple[int, int]] = []
    pools: list[tuple[int, int]] = []
    for i, pool_of in enumerate((None, None, 0)):
        biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
            db, owner_idx=owner
        )
        existing = (
            {}
            if pool_of is None
            else dict(
                zip(("sequencing_run_idx", "sequenced_pool_idx"), pools[pool_of], strict=True)
            )
        )
        run_idx, pool_idx, _ = await seed_sequenced_sample_subtype(
            db,
            prep_sample_idx=prep_sample_idx,
            owner_idx=owner,
            sequenced_pool_item_id=f"ft-cross-{tag}-{i}",
            **existing,
        )
        if pool_of is None:
            pools.append((run_idx, pool_idx))
        samples.append((biosample_idx, prep_sample_idx))
        await seed_biosample_to_study_link(
            db, biosample_idx=biosample_idx, study_idx=study_idx, created_by_idx=owner
        )
        await seed_prep_sample_to_study_link(
            db, prep_sample_idx=prep_sample_idx, study_idx=study_idx, created_by_idx=owner
        )
    ps0, ps1, ps2 = (ps for _, ps in samples)

    mask_1 = await _masked(db, owner=owner, tag=f"{tag}-1", prep_sample_idxs=[ps0, ps2])
    mask_2 = await _masked(db, owner=owner, tag=f"{tag}-2", prep_sample_idxs=[ps1])
    async with db.acquire() as conn, conn.transaction():
        pair, _ = await open_pair(
            conn, kind=DefinitionKind.MASK, definition_idxs=[mask_1, mask_2], principal_idx=owner
        )
        await decide_pair(
            conn,
            definition_pair_idx=pair["definition_pair_idx"],
            verdict=DefinitionPairVerdict.SIMILAR,
            reason="same filter, seeded by the cross-pool feature-table test",
            principal_idx=owner,
        )

    alignments: dict[str, int] = {}
    for name, mask_idx, prep_sample_idx in (("A", mask_1, ps0), ("B", mask_2, ps1)):
        async with db.acquire() as conn:
            alignments[name] = (
                await mint_alignment_definition(
                    conn,
                    params={
                        "reference_idx": reference_idx,
                        "aligner": "minimap2",
                        "mask_idx": mask_idx,
                        "shard_ids": [0],
                    },
                    principal_idx=owner,
                )
            )["alignment_idx"]
        async with db.acquire() as conn, conn.transaction():
            await create_alignment_sample_pending(
                conn, alignment_idx=alignments[name], prep_sample_idxs=[prep_sample_idx]
            )
            await finalize_alignment_sample(
                conn, alignment_idx=alignments[name], prep_sample_idx=prep_sample_idx
            )

    lake = ducklake_connect(data_plane["data_path"])
    try:
        lake.execute(
            "INSERT INTO qiita_lake.reference_sequences"
            f" (feature_idx, sequence_hash, sequence_length_bp) VALUES"
            f" ({feature_idx}, gen_random_uuid(), 10000)"
        )
        lake.execute(
            f"INSERT INTO qiita_lake.reference_membership VALUES ({reference_idx}, {feature_idx})"
        )
        lake.execute(
            "INSERT INTO qiita_lake.alignment"
            " (alignment_idx, prep_sample_idx, sequence_idx, feature_idx, flags,"
            " position, stop_position) VALUES"
            f" ({alignments['A']}, {ps0}, 101, {feature_idx}, 0, 0, 60),"
            f" ({alignments['B']}, {ps1}, 102, {feature_idx}, 0, 60, 120)"
        )
    finally:
        lake.close()

    yield {
        "pools": pools,
        "alignments": alignments,
        "samples": (ps0, ps1, ps2),
        "source_id": source_id,
    }

    await teardown_entity_graph(
        db,
        study_idxs=[study_idx],
        biosample_idxs=[bs for bs, _ in samples],
        prep_sample_idxs=[ps for _, ps in samples],
    )
    await db.execute(
        "DELETE FROM qiita.definition_pair WHERE definition_pair_idx = $1",
        pair["definition_pair_idx"],
    )
    await db.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = ANY($1::bigint[])",
        list(alignments.values()),
    )
    await db.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])", [mask_1, mask_2]
    )
    for run_idx, pool_idx in pools:
        await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
        await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await db.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", reference_idx
    )
    await db.execute("DELETE FROM qiita.feature_genome WHERE feature_idx = $1", feature_idx)
    await db.execute("DELETE FROM qiita.feature WHERE feature_idx = $1", feature_idx)
    await db.execute("DELETE FROM qiita.genome WHERE genome_idx = $1", genome_idx)
    await db.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx)


async def test_a_cohort_across_pools_builds_one_table_in_parts(
    cp_server, data_plane, regular_user_session, cross_pool_cohort, tmp_path, monkeypatch, capsys
):
    """Two pools, two alignments of one group, one table: each sample read from the
    alignment it resolves to (the cohort DoGet refuses any other), coverage pooled over
    both, and a manifest naming each part by its own handle."""
    from qiita_control_plane.cli import user as cli

    seed = cross_pool_cohort
    ps0, ps1, ps2 = seed["samples"]
    dp_url = f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"
    monkeypatch.setenv("QIITA_TOKEN", regular_user_session["token"])
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    def pool_flags(*pools) -> list[str]:
        return [
            flag
            for run_idx, pool_idx in pools
            for flag in (
                "--sequencing-run-idx",
                str(run_idx),
                "--sequenced-pool-idx",
                str(pool_idx),
            )
        ]

    def build(*extra: str, pools, output) -> int:
        return cli.main(
            [
                "--base-url",
                cp_server,
                "feature-table",
                "build",
                *pool_flags(*pools),
                "--alignment-idx",
                str(seed["alignments"]["A"]),
                "--coverage-threshold",
                "0.01",
                "--output",
                str(output),
                "--data-plane-url",
                dp_url,
                *extra,
            ]
        )

    # --- Default cohort: A's samples on pool 1, then B's on pool 2 (B is in A's group). ---
    table = out_dir / "cross.parquet"
    assert build(pools=seed["pools"], output=table) == 0, capsys.readouterr().err
    identifiers = json.loads((out_dir / "cross.exported-identifier.json").read_text())
    handles = {e["prep_sample_idx"]: e["export_id"] for e in identifiers["identifiers"]}
    assert sorted(handles) == [ps0, ps1]
    # G survives only because its 0.6% + 0.6% are pooled across the two parts.
    assert _read_artifact(table) == sorted(
        [(handles[ps0], seed["source_id"], 1.0), (handles[ps1], seed["source_id"], 1.0)]
    )
    manifest = json.loads((out_dir / "cross.manifest.json").read_text())
    parts = manifest["processing"]["parts"]
    assert len(parts) == 2
    part_a, part_b = (part["export_processing_id"] for part in parts)
    assert part_a != part_b and all(h.startswith("QP") for h in (part_a, part_b))
    assert manifest["processing"]["sample_part"] == {handles[ps0]: part_a, handles[ps1]: part_b}
    assert "idx" not in json.dumps(manifest)

    # --- A cohort with a sample not processed in the group is refused, not narrowed. ---
    refused = out_dir / "refused.parquet"
    explicit = [f for ps in (ps0, ps1, ps2) for f in ("--prep-sample-idx", str(ps))]
    capsys.readouterr()
    assert build(*explicit, pools=seed["pools"], output=refused) == 1
    err = capsys.readouterr().err
    assert f"prep_sample {ps2}: not_processed (no_run)" in err
    assert not refused.exists()

    # --- A part over no named pool is refused with the flags to add. ---
    assert build(*explicit[:4], pools=seed["pools"][:1], output=refused) == 1
    assert "not over any pool named" in capsys.readouterr().err
    assert not refused.exists()
