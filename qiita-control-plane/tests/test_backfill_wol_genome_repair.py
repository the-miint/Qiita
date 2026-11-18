"""DB tests for `qiita-admin backfill wol-genome-repair`.

A Web of Life reference loaded with a genome map that wrote `GCA_` in front of each WoL
ID left every one of its sequences linked to two genome rows, `G<n>` and `GCA_G<n>`.
The repair folds each `GCA_G<n>` into `G<n>` -- the name the reference's own FASTA
header gave the sequence -- removes links from the same sequence to any other WoL ID,
and records each genome's NCBI parent assembly from the WoL release's metadata.txt.

The repair acts on every `GCA_G` genome in the database, so each test starts from
none and its fixture removes everything it seeded, and everything the repair minted.
"""

import csv
import secrets
import uuid
from pathlib import Path

import pytest
import pytest_asyncio

from qiita_control_plane.backfill.wol_genome_repair import (
    DERIVATION_METHOD,
    RepairPostconditionFailed,
    RepairRefused,
    apply_repair,
    plan_repair,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import delete_principal, teardown_entity_graph

pytestmark = pytest.mark.db

_GCA_G_ROWS = (
    "SELECT count(*) FROM qiita.genome WHERE source = 'genbank' AND source_id ~ '^GCA_G[0-9]{9}$'"
)
_METADATA_COLUMNS = ("genome_id", "assembly_accession", "source", "in_wol2")
_MANIFEST_COLUMNS = [
    "action",
    "table",
    "genome_idx",
    "source",
    "source_id",
    "feature_idx",
    "exported_feature_idx",
    "export_id",
]


class _World:
    """One WoL-style reference and the genome rows around it."""

    def __init__(self, pool, tmp_path: Path, owner_idx: int, reference_idx: int):
        self.pool = pool
        self.tmp_path = tmp_path
        self.owner_idx = owner_idx
        self.reference_idx = reference_idx
        self.features: list[int] = []
        self.source_ids: set[str] = set()
        self.labels: list[int] = []
        self.exclusions: list[int] = []
        self.samples: list[tuple[int, int]] = []
        self.processing: list[int] = []
        self.parents: dict[str, str] = {}  # WoL ID -> versioned NCBI accession

    def wol_id(self) -> str:
        while True:
            source_id = f"G{secrets.randbelow(10**9):09d}"
            if source_id not in self.source_ids:
                self.source_ids.update({source_id, f"GCA_{source_id}"})
                return source_id

    async def feature(self) -> int:
        feature_idx = await self.pool.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES ($1) RETURNING feature_idx",
            uuid.uuid4(),
        )
        self.features.append(feature_idx)
        return feature_idx

    async def member(self, feature_idx: int, accession: str, reference_idx: int | None = None):
        await self.pool.execute(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx, accession)"
            " VALUES ($1, $2, $3)",
            reference_idx or self.reference_idx,
            feature_idx,
            accession,
        )

    async def genome(self, source_id: str, source: str = "genbank") -> int:
        self.source_ids.add(source_id)
        return await self.pool.fetchval(
            "INSERT INTO qiita.genome (source, source_id) VALUES ($1, $2) RETURNING genome_idx",
            source,
            source_id,
        )

    async def link(self, feature_idx: int, genome_idx: int) -> None:
        await self.pool.execute(
            "INSERT INTO qiita.feature_genome (feature_idx, genome_idx) VALUES ($1, $2)",
            feature_idx,
            genome_idx,
        )

    async def label(self, genome_idx: int, accession: str) -> int:
        idx = await self.pool.fetchval(
            "INSERT INTO qiita.exported_feature"
            " (entity_kind, genome_idx, accession, accession_published, created_by_idx)"
            " VALUES ('genome', $1, $2, true, $3) RETURNING idx",
            genome_idx,
            accession,
            self.owner_idx,
        )
        self.labels.append(idx)
        return idx

    async def exclude(self, genome_idx: int) -> int:
        idx = await self.pool.fetchval(
            "INSERT INTO qiita.reference_exclusion (genome_idx, reason, excluded_by_idx)"
            " VALUES ($1, 'test', $2) RETURNING reference_exclusion_idx",
            genome_idx,
            self.owner_idx,
        )
        self.exclusions.append(idx)
        return idx

    async def assembly_claim(self, feature_idx: int, genome_idx: int) -> None:
        biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
            self.pool, owner_idx=self.owner_idx
        )
        self.samples.append((biosample_idx, prep_sample_idx))
        processing_idx = await self.pool.fetchval(
            "INSERT INTO qiita.processing (params_hash, workflow, version, params)"
            " VALUES ($1, 'long-read-assembly', 'test', '{}'::jsonb) RETURNING processing_idx",
            uuid.uuid4().bytes + uuid.uuid4().bytes,
        )
        self.processing.append(processing_idx)
        await self.pool.execute(
            "INSERT INTO qiita.assembly_membership"
            " (prep_sample_idx, processing_idx, kind, bin_id, feature_idx, genome_idx)"
            " VALUES ($1, $2, 'MAG', 'bin.1', $3, $4)",
            prep_sample_idx,
            processing_idx,
            feature_idx,
            genome_idx,
        )

    async def rc4_genome(
        self, cls: str, *, n: str | None = None, parent: str | None = None
    ) -> dict:
        """One sequence of the reference, in one of the fold's classes.

        A: the sequence links to `G<n>` and `GCA_G<n>`.
        B: `G<n>` exists but does not hold the sequence; a stale WoL ID `G<m>` does,
           as reference 16's load left it.
        C: no `G<n>` row; only `GCA_G<n>` holds the sequence.

        `n` and `parent` go together: a WoL ID is `G` and its parent accession's nine
        digits, so a test that names the parent names `n` to match.
        """
        if n is None:
            n = self.wol_id()
        else:
            assert n not in self.source_ids
            self.source_ids.update({n, f"GCA_{n}"})
        feature_idx = await self.feature()
        await self.member(feature_idx, n)
        out = {"n": n, "feature_idx": feature_idx, "g": None, "stale": None, "stale_id": None}
        if cls == "A":
            out["g"] = await self.genome(n)
            await self.link(feature_idx, out["g"])
        elif cls == "B":
            out["g"] = await self.genome(n)
            out["stale_id"] = self.wol_id()
            out["stale"] = await self.genome(out["stale_id"])
            await self.link(feature_idx, out["stale"])
        elif cls != "C":
            raise ValueError(cls)
        out["x"] = await self.genome(f"GCA_{n}")
        await self.link(feature_idx, out["x"])
        self.parents[n] = parent or f"GCF_{n[1:]}.1"
        return out

    def metadata(self, rows: list[tuple[str, str]] | None = None, name="metadata.txt") -> Path:
        """A WoL-shaped metadata.txt (tab-separated, more columns than the repair reads)."""
        if rows is None:
            rows = sorted(self.parents.items())
        path = self.tmp_path / name
        with path.open("w", newline="") as fh:
            writer = csv.writer(fh, delimiter="\t", lineterminator="\n")
            writer.writerow(_METADATA_COLUMNS)
            for genome_id, accession in rows:
                source = "refseq" if accession.startswith("GCF_") else "genbank"
                writer.writerow((genome_id, accession, source, "0"))
        return path

    async def genomes_of(self, feature_idx: int) -> set[str]:
        rows = await self.pool.fetch(
            "SELECT g.source_id FROM qiita.feature_genome fg"
            "  JOIN qiita.genome g USING (genome_idx) WHERE fg.feature_idx = $1",
            feature_idx,
        )
        return {r["source_id"] for r in rows}

    async def genome_idx(self, source_id: str, source: str = "genbank") -> int | None:
        return await self.pool.fetchval(
            "SELECT genome_idx FROM qiita.genome WHERE source = $1 AND source_id = $2",
            source,
            source_id,
        )

    async def snapshot(self) -> dict:
        """Every row the repair could touch, for asserting that nothing changed."""
        ids = sorted(self.source_ids)
        return {
            "genome": await self.pool.fetch(
                "SELECT genome_idx, source, source_id FROM qiita.genome"
                " WHERE source_id = ANY($1::text[]) ORDER BY genome_idx",
                ids,
            ),
            "feature_genome": await self.pool.fetch(
                "SELECT feature_idx, genome_idx FROM qiita.feature_genome"
                " WHERE feature_idx = ANY($1::bigint[]) ORDER BY 1, 2",
                self.features,
            ),
            "genome_derivation": await self.pool.fetch(
                "SELECT d.* FROM qiita.genome_derivation d JOIN qiita.genome g USING (genome_idx)"
                " WHERE g.source_id = ANY($1::text[]) ORDER BY genome_idx",
                ids,
            ),
            "exported_feature": await self.pool.fetch(
                "SELECT idx, genome_idx, retired FROM qiita.exported_feature"
                " WHERE idx = ANY($1::bigint[]) ORDER BY idx",
                self.labels,
            ),
        }


@pytest_asyncio.fixture
async def world(postgres_pool, tmp_path):
    pool = postgres_pool
    assert await pool.fetchval(_GCA_G_ROWS) == 0, "a GCA_G genome leaked from another test"
    suffix = secrets.token_hex(4)
    owner_idx = await seed_user_principal(pool, prefix="wol-repair", suffix=suffix)
    reference_idx = await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '3-rc4', 'sequence_reference', 'active', $2) RETURNING reference_idx",
        f"wol-repair-{suffix}",
        owner_idx,
    )
    w = _World(pool, tmp_path, owner_idx, reference_idx)
    yield w

    ids = sorted(w.source_ids)
    await pool.execute(
        "DELETE FROM qiita.genome_derivation d USING qiita.genome g"
        " WHERE g.genome_idx = d.genome_idx AND g.source_id = ANY($1::text[])",
        ids,
    )
    await pool.execute(
        "DELETE FROM qiita.genome_derivation WHERE asserted_by_reference_idx = $1", reference_idx
    )
    await pool.execute("DELETE FROM qiita.exported_feature WHERE idx = ANY($1::bigint[])", w.labels)
    await pool.execute(
        "DELETE FROM qiita.reference_exclusion WHERE reference_exclusion_idx = ANY($1::bigint[])",
        w.exclusions,
    )
    await teardown_entity_graph(
        pool,
        study_idxs=[],
        biosample_idxs=[bs for bs, _ps in w.samples],
        prep_sample_idxs=[ps for _bs, ps in w.samples],
    )
    await pool.execute(
        "DELETE FROM qiita.processing WHERE processing_idx = ANY($1::bigint[])", w.processing
    )
    await pool.execute(
        "DELETE FROM qiita.feature_genome WHERE feature_idx = ANY($1::bigint[])", w.features
    )
    await pool.execute(
        "DELETE FROM qiita.reference_membership WHERE feature_idx = ANY($1::bigint[])", w.features
    )
    await pool.execute("DELETE FROM qiita.genome WHERE source_id = ANY($1::text[])", ids)
    await pool.execute(
        "DELETE FROM qiita.feature WHERE feature_idx = ANY($1::bigint[])", w.features
    )
    await pool.execute("DELETE FROM qiita.reference WHERE created_by_idx = $1", owner_idx)
    await delete_principal(pool, owner_idx)


async def _plan(w: _World, metadata: Path | None = None):
    return await plan_repair(
        w.pool, reference_idx=w.reference_idx, metadata_path=metadata or w.metadata()
    )


async def _execute(w: _World, metadata: Path | None = None, manifest: str = "manifest.csv"):
    plan = await _plan(w, metadata)
    return await apply_repair(w.pool, plan, manifest_path=w.tmp_path / manifest)


def _manifest(path: Path) -> list[dict]:
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh)
        assert reader.fieldnames == _MANIFEST_COLUMNS
        return list(reader)


# --- the fold ------------------------------------------------------------------------


async def test_execute_folds_every_class_into_g_n(world):
    w = world
    a = await w.rc4_genome("A")
    b = await w.rc4_genome("B")
    c = await w.rc4_genome("C")

    plan = await _plan(w)
    assert (plan.counts.already_linked, plan.counts.to_link, plan.counts.to_create) == (1, 1, 1)
    assert plan.counts.stale_links == 1

    done = await apply_repair(w.pool, plan, manifest_path=w.tmp_path / "manifest.csv")
    assert (done.already_linked, done.to_link, done.to_create, done.stale_links) == (1, 1, 1, 1)

    for g in (a, b, c):
        assert await w.genomes_of(g["feature_idx"]) == {g["n"]}
        assert await w.genome_idx(f"GCA_{g['n']}") is None
    assert await w.pool.fetchval(_GCA_G_ROWS) == 0
    # A and B keep the G<n> row they had; C's is minted as an external genbank genome.
    assert await w.genome_idx(a["n"]) == a["g"]
    assert await w.genome_idx(b["n"]) == b["g"]
    minted = await w.pool.fetchrow(
        "SELECT source, prep_sample_idx FROM qiita.genome WHERE source_id = $1", c["n"]
    )
    assert dict(minted) == {"source": "genbank", "prep_sample_idx": None}


async def test_stale_wol_link_is_removed_and_its_genome_kept(world):
    w = world
    b = await w.rc4_genome("B")
    await _execute(w)
    assert await w.genomes_of(b["feature_idx"]) == {b["n"]}
    assert await w.genome_idx(b["stale_id"]) == b["stale"]


async def test_a_non_wol_genome_on_another_feature_is_untouched(world):
    """Only GCA_G genomes and WoL-ID links from their features are the repair's."""
    w = world
    await w.rc4_genome("A")
    other_feature = await w.feature()
    gg2_id = f"GCA_{secrets.randbelow(10**9):09d}.1"
    await w.link(other_feature, await w.genome(gg2_id))
    await _execute(w)
    assert await w.genomes_of(other_feature) == {gg2_id}


# --- published labels ----------------------------------------------------------------


async def test_gca_g_labels_are_retired_and_g_labels_kept(world):
    w = world
    a = await w.rc4_genome("A")
    g_label = await w.label(a["g"], a["n"])
    x_label = await w.label(a["x"], f"GCA_{a['n']}")

    plan = await _plan(w)
    assert plan.counts.labels_to_retire == 1
    await apply_repair(w.pool, plan, manifest_path=w.tmp_path / "manifest.csv")

    rows = {
        r["idx"]: r
        for r in await w.pool.fetch(
            "SELECT idx, genome_idx, retired, retire_reason, retired_by_idx"
            "  FROM qiita.exported_feature WHERE idx = ANY($1::bigint[])",
            [g_label, x_label],
        )
    }
    assert rows[g_label]["retired"] is False and rows[g_label]["genome_idx"] == a["g"]
    assert rows[x_label]["retired"] is True
    assert rows[x_label]["genome_idx"] is None
    assert rows[x_label]["retired_by_idx"] is None
    assert rows[x_label]["retire_reason"] == (
        f"genome {a['x']} was deleted; the entity this identifier named no longer exists"
    )


async def test_a_live_label_on_a_stale_genome_stays_live(world):
    """The stale G<m> row is kept, so the label naming it still names something."""
    w = world
    b = await w.rc4_genome("B")
    stale_label = await w.label(b["stale"], b["stale_id"])
    plan = await _plan(w)
    assert plan.counts.labels_to_retire == 0
    await apply_repair(w.pool, plan, manifest_path=w.tmp_path / "manifest.csv")
    row = await w.pool.fetchrow(
        "SELECT genome_idx, retired FROM qiita.exported_feature WHERE idx = $1", stale_label
    )
    assert dict(row) == {"genome_idx": b["stale"], "retired": False}


# --- derivation ----------------------------------------------------------------------


async def test_execute_records_each_genomes_parent_assembly(world):
    w = world
    a = await w.rc4_genome("A", n="G000005825", parent="GCF_000005825.2")
    c = await w.rc4_genome("C", n="G000007325", parent="GCA_000007325.1")
    outsider = await w.genome(w.wol_id())  # a WoL genome of no feature in this reference

    plan = await _plan(w)
    assert (plan.counts.derivations_to_add, plan.counts.derivations_present) == (2, 0)
    await apply_repair(w.pool, plan, manifest_path=w.tmp_path / "manifest.csv")

    rows = {
        r["source_id"]: dict(r)
        for r in await w.pool.fetch(
            "SELECT g.source_id, d.parent_source, d.parent_source_id, d.method,"
            "       d.asserted_by_reference_idx"
            "  FROM qiita.genome_derivation d JOIN qiita.genome g USING (genome_idx)"
            " WHERE g.source_id = ANY($1::text[])",
            sorted(w.source_ids),
        )
    }
    assert rows == {
        a["n"]: {
            "source_id": a["n"],
            "parent_source": "refseq",
            "parent_source_id": "GCF_000005825.2",
            "method": DERIVATION_METHOD,
            "asserted_by_reference_idx": w.reference_idx,
        },
        c["n"]: {
            "source_id": c["n"],
            "parent_source": "genbank",
            "parent_source_id": "GCA_000007325.1",
            "method": DERIVATION_METHOD,
            "asserted_by_reference_idx": w.reference_idx,
        },
    }
    assert outsider not in {
        r["genome_idx"]
        for r in await w.pool.fetch("SELECT genome_idx FROM qiita.genome_derivation")
    }


# --- dry run, idempotence, manifest --------------------------------------------------


async def test_plan_writes_nothing(world):
    w = world
    a = await w.rc4_genome("A")
    await w.rc4_genome("B")
    await w.rc4_genome("C")
    await w.label(a["x"], f"GCA_{a['n']}")
    before = await w.snapshot()
    plan = await _plan(w)
    assert plan.counts.labels_to_retire == 1
    assert plan.counts.derivations_to_add == 3
    assert not plan.nothing_to_do
    assert await w.snapshot() == before
    assert list(w.tmp_path.glob("*.csv")) == []


async def test_a_second_execute_changes_nothing(world):
    w = world
    await w.rc4_genome("A")
    await w.rc4_genome("B")
    await w.rc4_genome("C")
    await _execute(w, manifest="first.csv")
    after_first = await w.snapshot()

    plan = await _plan(w)
    assert plan.nothing_to_do
    assert plan.counts.derivations_present == 3
    done = await apply_repair(w.pool, plan, manifest_path=w.tmp_path / "second.csv")
    assert (done.to_link, done.to_create, done.stale_links, done.derivations_to_add) == (0, 0, 0, 0)
    assert await w.snapshot() == after_first
    assert _manifest(w.tmp_path / "second.csv") == []


async def test_manifest_lists_every_deleted_row_and_retired_label(world):
    w = world
    a = await w.rc4_genome("A")
    b = await w.rc4_genome("B")
    c = await w.rc4_genome("C")
    x_label = await w.label(a["x"], f"GCA_{a['n']}")
    export_id = await w.pool.fetchval(
        "SELECT export_feature_id FROM qiita.exported_feature WHERE idx = $1", x_label
    )
    await _execute(w)

    got = {
        (r["action"], r["table"], r["source_id"], r["feature_idx"], r["exported_feature_idx"])
        for r in _manifest(w.tmp_path / "manifest.csv")
    }
    expected = set()
    for g in (a, b, c):
        x_id, f = f"GCA_{g['n']}", str(g["feature_idx"])
        expected.add(("delete", "genome", x_id, "", ""))
        expected.add(("delete", "feature_genome", x_id, f, ""))
    expected.add(("delete", "feature_genome", b["stale_id"], str(b["feature_idx"]), ""))
    expected.add(("retire", "exported_feature", f"GCA_{a['n']}", "", str(x_label)))
    assert got == expected
    retire = next(r for r in _manifest(w.tmp_path / "manifest.csv") if r["action"] == "retire")
    assert retire["export_id"] == export_id
    assert retire["genome_idx"] == str(a["x"])


async def test_an_existing_manifest_is_not_overwritten(world):
    w = world
    await w.rc4_genome("A")
    (w.tmp_path / "manifest.csv").write_text("an earlier run\n")
    before = await w.snapshot()
    with pytest.raises(RepairRefused, match="manifest.csv"):
        await _execute(w)
    assert await w.snapshot() == before
    assert (w.tmp_path / "manifest.csv").read_text() == "an earlier run\n"


async def test_a_manifest_in_a_missing_directory_refuses_before_writing(world):
    w = world
    await w.rc4_genome("A")
    before = await w.snapshot()
    with pytest.raises(RepairRefused, match="no-such-dir"):
        await _execute(w, manifest="no-such-dir/manifest.csv")
    assert await w.snapshot() == before


# --- preconditions: refuse before writing anything ----------------------------------


async def _assert_refused(w: _World, match: str, metadata: Path | None = None) -> None:
    before = await w.snapshot()
    with pytest.raises(RepairRefused, match=match):
        await _plan(w, metadata)
    assert await w.snapshot() == before


async def test_refuses_a_missing_metadata_file(world):
    w = world
    await w.rc4_genome("A")
    await _assert_refused(w, "absent.txt", w.tmp_path / "absent.txt")


async def test_refuses_a_gca_g_genome_with_no_feature(world):
    w = world
    await w.rc4_genome("A")
    n = w.wol_id()
    await w.genome(f"GCA_{n}")
    await _assert_refused(w, f"GCA_{n}")


async def test_refuses_a_gca_g_genome_with_two_features(world):
    w = world
    a = await w.rc4_genome("A")
    second = await w.feature()
    await w.link(second, a["x"])
    await _assert_refused(w, f"GCA_{a['n']}")


async def test_refuses_a_feature_outside_the_reference(world):
    w = world
    await w.rc4_genome("A")
    n = w.wol_id()
    feature_idx = await w.feature()
    await w.link(feature_idx, await w.genome(f"GCA_{n}"))
    await _assert_refused(w, f"GCA_{n}")


async def test_refuses_when_the_reference_header_is_not_g_n(world):
    w = world
    a = await w.rc4_genome("A")
    header = w.wol_id()
    await w.pool.execute(
        "UPDATE qiita.reference_membership SET accession = $3"
        " WHERE reference_idx = $1 AND feature_idx = $2",
        w.reference_idx,
        a["feature_idx"],
        header,
    )
    # The metadata agrees with the reference, so only the header check can fire.
    metadata = w.metadata([(header, f"GCF_{header[1:]}.1")], name="header.txt")
    await _assert_refused(w, f"GCA_{a['n']}", metadata)


async def test_refuses_a_feature_with_two_gca_g_genomes(world):
    w = world
    a = await w.rc4_genome("A")
    n2 = w.wol_id()
    await w.link(a["feature_idx"], await w.genome(f"GCA_{n2}"))
    await _assert_refused(w, rf"feature {a['feature_idx']}\b")


async def test_refuses_an_exclusion_naming_a_gca_g_genome(world):
    w = world
    a = await w.rc4_genome("A")
    await w.exclude(a["x"])
    await _assert_refused(w, f"GCA_{a['n']}")


async def test_refuses_an_exclusion_naming_a_stale_genome(world):
    w = world
    b = await w.rc4_genome("B")
    await w.exclude(b["stale"])
    await _assert_refused(w, b["stale_id"])


async def test_refuses_an_assembly_claim_on_a_gca_g_genome(world):
    w = world
    a = await w.rc4_genome("A")
    await w.assembly_claim(await w.feature(), a["x"])
    await _assert_refused(w, f"GCA_{a['n']}")


def _off_by_one(genome_id: str) -> str:
    return f"GCF_{(int(genome_id[1:]) + 1) % 10**9:09d}.1"


@pytest.mark.parametrize(
    ("mutate", "offender"),
    [
        # A duplicate accession needs two genome IDs on one accession number, which the
        # genome-ID rule already refuses, so it has no case of its own.
        pytest.param(
            lambda rows: [(rows[0][0], "GCF_00000582.1"), *rows[1:]],
            lambda rows: "GCF_00000582.1",
            id="malformed-accession",
        ),
        pytest.param(
            lambda rows: [(rows[0][0], "GCA_000005825"), *rows[1:]],
            lambda rows: "GCA_000005825",
            id="unversioned-accession",
        ),
        pytest.param(
            lambda rows: [(rows[0][0], _off_by_one(rows[0][0])), *rows[1:]],
            lambda rows: rows[0][0],
            id="genome-id-not-the-accession-number",
        ),
        pytest.param(
            lambda rows: [*rows, rows[0]],
            lambda rows: rows[0][0],
            id="duplicate-genome-id",
        ),
    ],
)
async def test_refuses_malformed_metadata(world, mutate, offender):
    w = world
    await w.rc4_genome("A")
    await w.rc4_genome("C")
    rows = sorted(w.parents.items())
    await _assert_refused(w, offender(rows), w.metadata(mutate(rows), name="bad.txt"))


async def test_refuses_metadata_without_the_columns_it_reads(world):
    w = world
    await w.rc4_genome("A")
    path = w.tmp_path / "no-accession.txt"
    path.write_text("genome_id\tsource\n" + "".join(f"{g}\trefseq\n" for g in w.parents))
    await _assert_refused(w, "assembly_accession", path)


async def test_refuses_metadata_missing_a_reference_genome(world):
    w = world
    await w.rc4_genome("A")
    c = await w.rc4_genome("C")
    rows = [(g, acc) for g, acc in sorted(w.parents.items()) if g != c["n"]]
    await _assert_refused(w, c["n"], w.metadata(rows, name="short.txt"))


async def test_refuses_metadata_naming_a_genome_the_reference_lacks(world):
    w = world
    await w.rc4_genome("A")
    extra = f"G{secrets.randbelow(10**9):09d}"
    rows = sorted(w.parents.items()) + [(extra, f"GCA_{extra[1:]}.1")]
    await _assert_refused(w, extra, w.metadata(rows, name="long.txt"))


async def test_refuses_an_existing_derivation_that_disagrees(world):
    w = world
    a = await w.rc4_genome("A", n="G000005825", parent="GCF_000005825.2")
    await w.pool.execute(
        "INSERT INTO qiita.genome_derivation"
        " (genome_idx, parent_source, parent_source_id, method, asserted_by_reference_idx)"
        " VALUES ($1, 'refseq', 'GCF_000005825.1', $2, $3)",
        a["g"],
        DERIVATION_METHOD,
        w.reference_idx,
    )
    await _assert_refused(w, "GCF_000005825.1")


async def test_apply_rechecks_the_preconditions(world):
    """A precondition broken between the plan and the apply refuses the apply."""
    w = world
    a = await w.rc4_genome("A")
    plan = await _plan(w)
    await w.exclude(a["x"])
    before = await w.snapshot()
    with pytest.raises(RepairRefused, match=f"GCA_{a['n']}"):
        await apply_repair(w.pool, plan, manifest_path=w.tmp_path / "manifest.csv")
    assert await w.snapshot() == before
    assert not (w.tmp_path / "manifest.csv").exists()


# --- postconditions: assert in the transaction, roll back ----------------------------


async def test_a_failed_postcondition_rolls_everything_back(world):
    """A reference sequence that also belongs to some other genome would still have two
    genomes after the fold. Nothing in the preconditions forbids that, so the plan
    passes; the apply must see it, name the sequence and leave no trace."""
    w = world
    a = await w.rc4_genome("A")
    await w.rc4_genome("C")
    other = await w.genome(f"GCF_{secrets.randbelow(10**9):09d}.1", source="refseq")
    await w.link(a["feature_idx"], other)
    plan = await _plan(w)
    before = await w.snapshot()
    with pytest.raises(RepairPostconditionFailed, match=rf"feature {a['feature_idx']}\b"):
        await apply_repair(w.pool, plan, manifest_path=w.tmp_path / "manifest.csv")
    assert await w.snapshot() == before
    assert list(w.tmp_path.glob("*manifest*")) == []
