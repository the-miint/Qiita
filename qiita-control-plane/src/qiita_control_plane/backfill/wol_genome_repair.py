"""Fold a Web of Life reference's `GCA_G<n>` genomes into `G<n>`, and record the NCBI
assembly each of its genomes was derived from.

**What it repairs.** A WoL reference loaded with a genome map that put `GCA_` in front
of each WoL ID minted a second genome row, `genbank:GCA_G<n>`, for each of its
sequences. `feature_idx` is global, so a sequence that an earlier WoL load had already
linked to `G<n>` then links to both rows, and a feature table built against the
reference splits that genome's reads between the two names. A few sequences were
linked instead to a different WoL ID, which an earlier release gave the same bytes, or
to no `G<n>` at all. `GCA_G<n>` is not an accession: GenBank's are `GCA_` and nine
digits with a version.

**What it does, in one transaction:**

1. For each `GCA_G<n>` genome `x` and its one feature `f`: create `genbank:G<n>` if it
   is missing, link `(f, G<n>)`, delete every other WoL-ID link from `f` (an earlier
   WoL load's name for the same bytes), then delete the link `(f, x)` and the row `x`.
   Deleting `x` retires its published labels (`qiita.retire_detached_exported_feature`).
   A stale WoL genome loses the link but keeps its row, and so its labels.
2. Insert one `qiita.genome_derivation` row per genome of the WoL release's
   `metadata.txt`: its `assembly_accession` is the parent, and the accession's prefix
   says which archive (`GCA_` GenBank, `GCF_` RefSeq).
3. Assert the result (`_postcondition_problems`) and roll everything back if it does
   not hold.

**Where `G<n>` comes from.** It must be the name the reference's own FASTA header gave
the sequence (`reference_membership.accession`); a `GCA_G<n>` whose feature has a
different name there is refused, not guessed at.

Contract per this package: dry-run by default (`plan_repair` writes nothing), and
idempotent (a second run finds no `GCA_G` genome and every derivation present). There
is no residue: anything the rules do not settle refuses the whole run, naming the
rows, because a partial fold would leave tables that still split reads.

**Retirement.** Delete this module, its CLI and their tests once every deployment has
run it, which its dry run shows by finding no `GCA_G` genome and every derivation
present. `DERIVATION_METHOD`, and the tests outside this module's own that import it,
move to whatever then writes derivations.
"""

from __future__ import annotations

import csv
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import asyncpg
import duckdb
from qiita_common.models.reference import GenomeSource

from ..actions.library import upsert_genomes
from ..delimited_text import read_headed_tsv
from ..repositories import RowLock, row_lock_clause

DERIVATION_METHOD = "wol_concatenated"

# WoL genomes are stored under `genbank`. Each pattern is matched whole: by
# `re.fullmatch` in Python, and through `_anchored` in SQL.
_WOL_SOURCE = GenomeSource.GENBANK
_WOL_ID = "G[0-9]{9}"
_GCA_G_ID = "GCA_G[0-9]{9}"
# The accession shapes and the prefix -> archive pairing that the migration's
# genome_derivation_parent_check also holds; a test fails if the two disagree.
_ACCESSION = r"GC[AF]_[0-9]{9}\.[0-9]+"
_PARENT_SOURCE = {"GCA": GenomeSource.GENBANK, "GCF": GenomeSource.REFSEQ}
_METADATA_COLUMNS = ("genome_id", "assembly_accession")
_MANIFEST_COLUMNS = (
    "action",
    "table",
    "genome_idx",
    "source",
    "source_id",
    "feature_idx",
    "exported_feature_idx",
    "export_id",
)
# Offenders listed per problem; the rest are counted. A whole-release failure would
# otherwise print 196,062 lines.
_LISTED = 20
# A reference load or a label mint holding one of the locked tables makes the repair
# wait this long, then fail, rather than hang.
_LOCK_TIMEOUT = "30s"
_LOCKED_TABLES = (
    "qiita.genome",
    "qiita.feature_genome",
    "qiita.genome_derivation",
    "qiita.reference_membership",
    "qiita.reference_exclusion",
    "qiita.assembly_membership",
    "qiita.exported_feature",
)


def _anchored(pattern: str) -> str:
    return f"^{pattern}$"


class RepairRefused(Exception):
    """A precondition does not hold; nothing was written."""


class RepairPostconditionFailed(Exception):
    """The repaired state did not hold; the transaction was rolled back."""


@dataclass(frozen=True)
class RepairCounts:
    already_linked: int
    to_link: int
    to_create: int
    stale_links: int
    labels_to_retire: int
    derivations_to_add: int
    derivations_present: int


@dataclass(frozen=True)
class Parent:
    genome_id: str
    parent_source: str
    parent_source_id: str


@dataclass(frozen=True)
class RepairPlan:
    reference_idx: int
    parents: tuple[Parent, ...]
    counts: RepairCounts

    @property
    def nothing_to_do(self) -> bool:
        c = self.counts
        return not (
            c.already_linked
            or c.to_link
            or c.to_create
            or c.stale_links
            or c.labels_to_retire
            or c.derivations_to_add
        )


@dataclass(frozen=True)
class _Fold:
    x_idx: int
    x_id: str
    n_id: str
    feature_idx: int
    w_idx: int | None
    w_linked: bool


@dataclass(frozen=True)
class _Stale:
    feature_idx: int
    genome_idx: int


@dataclass
class _Survey:
    folds: list[_Fold]
    stale: list[_Stale]
    labels: list[asyncpg.Record]
    derivations_present: int
    problems: list[str]

    def counts(self, parents: tuple[Parent, ...]) -> RepairCounts:
        return RepairCounts(
            already_linked=sum(f.w_linked for f in self.folds),
            to_link=sum(f.w_idx is not None and not f.w_linked for f in self.folds),
            to_create=sum(f.w_idx is None for f in self.folds),
            stale_links=len(self.stale),
            labels_to_retire=len(self.labels),
            derivations_to_add=len(parents) - self.derivations_present,
            derivations_present=self.derivations_present,
        )


def _listed(items: list[str]) -> str:
    shown = ", ".join(items[:_LISTED])
    return shown if len(items) <= _LISTED else f"{shown} … and {len(items) - _LISTED} more"


def _bulleted(heading: str, problems: list[str]) -> str:
    return f"{heading}:\n" + "\n".join(f"  - {p}" for p in problems)


def _refusal(reference_idx: int, problems: list[str]) -> str:
    return _bulleted(f"reference {reference_idx} cannot be repaired", problems)


def _is_wol_genome(source: str, source_id: str, pattern: str) -> bool:
    return source == _WOL_SOURCE and re.fullmatch(pattern, source_id) is not None


# --- metadata --------------------------------------------------------------------------


def read_parents(metadata_path: Path) -> tuple[Parent, ...]:
    """Each genome's parent assembly from a WoL `metadata.txt` (tab-separated, header).

    Only `genome_id` and `assembly_accession` are read. The archive comes from the
    accession's prefix, not the file's `source` column, which WoL leaves empty for
    genomes carried over from WoL2.
    """
    try:
        table = read_headed_tsv(metadata_path, source_name="metadata file")
    except (ValueError, duckdb.Error) as exc:
        raise RepairRefused(f"metadata file {metadata_path} cannot be read: {exc}") from exc
    # A file with a header and no rows yields no parents, which the comparison with the
    # reference's genomes then refuses.
    if table and (missing := [c for c in _METADATA_COLUMNS if c not in table[0]]):
        raise RepairRefused(f"metadata file {metadata_path} lacks column(s): {', '.join(missing)}")
    rows = [
        (genome_id, acc, re.fullmatch(_ACCESSION, acc) is not None)
        for genome_id, acc in ((r["genome_id"], r["assembly_accession"]) for r in table)
    ]

    problems: list[str] = []
    malformed = [str(acc) for _g, acc, ok in rows if not ok]
    if malformed:
        problems.append(f"{len(malformed)} malformed assembly_accession(s): {_listed(malformed)}")
    mismatched = [f"{g} ({acc})" for g, acc, ok in rows if ok and g != f"G{acc[4:13]}"]
    if mismatched:
        problems.append(
            f"{len(mismatched)} genome_id(s) not G followed by the accession's nine digits:"
            f" {_listed(mismatched)}"
        )
    # A repeated accession repeats its genome_id too, or breaks the rule above, so only
    # the genome_id is checked.
    repeated = sorted(g for g, n in Counter(g for g, _a, _ok in rows).items() if n > 1)
    if repeated:
        problems.append(f"{len(repeated)} genome_id(s) repeat: {_listed(repeated)}")
    if problems:
        raise RepairRefused(_bulleted(f"metadata file {metadata_path}", problems))
    return tuple(
        Parent(genome_id=g, parent_source=_PARENT_SOURCE[acc[:3]], parent_source_id=acc)
        for g, acc, _ok in rows
    )


# --- the survey: what the fold touches, and every precondition -------------------------


async def _require_reference(
    conn: asyncpg.Connection, reference_idx: int, lock: RowLock | None
) -> None:
    found = await conn.fetchval(
        f"SELECT 1 FROM qiita.reference WHERE reference_idx = $1{row_lock_clause(conn, lock)}",
        reference_idx,
    )
    if found is None:
        raise RepairRefused(f"reference {reference_idx} does not exist")


async def _gca_g_folds(
    conn: asyncpg.Connection, reference_idx: int, problems: list[str]
) -> tuple[list[int], list[asyncpg.Record], dict[int, str | None]]:
    """Every GCA_G genome, and the reference's header for each feature.

    Returns every GCA_G genome_idx; the GCA_G rows that have exactly one feature; and
    the reference's feature -> accession map, which the metadata is compared against.
    A GCA_G row without exactly one feature is a problem, and so is one whose feature
    the reference does not hold under `G<n>`; the latter are still returned.
    """
    gca_g = await conn.fetch(
        "SELECT x.genome_idx AS x_idx, x.source_id AS x_id, substr(x.source_id, 5) AS n_id,"
        "       w.genome_idx AS w_idx,"
        "       array_remove(array_agg(fg.feature_idx), NULL)"
        "         AS feature_idxs"
        "  FROM qiita.genome x"
        "  LEFT JOIN qiita.feature_genome fg ON fg.genome_idx = x.genome_idx"
        "  LEFT JOIN qiita.genome w ON w.source = x.source AND w.source_id = substr(x.source_id, 5)"
        " WHERE x.source = $1 AND x.source_id ~ $2"
        " GROUP BY x.genome_idx, x.source_id, w.genome_idx"
        " ORDER BY x.source_id",
        _WOL_SOURCE,
        _anchored(_GCA_G_ID),
    )
    headers: dict[int, str | None] = {
        r["feature_idx"]: r["accession"]
        for r in await conn.fetch(
            "SELECT feature_idx, accession FROM qiita.reference_membership"
            " WHERE reference_idx = $1",
            reference_idx,
        )
    }
    not_one, outside, misnamed, single = [], [], [], []
    for r in gca_g:
        features = r["feature_idxs"]
        if len(features) != 1:
            not_one.append(f"{r['x_id']} has {len(features)}")
            continue
        f = features[0]
        single.append(r)
        if f not in headers:
            outside.append(f"{r['x_id']} (feature {f})")
        elif headers[f] != r["n_id"]:
            misnamed.append(f"{r['x_id']} (feature {f} is {headers[f]!r})")
    if not_one:
        problems.append(f"GCA_G genome(s) without exactly one feature: {_listed(not_one)}")
    if outside:
        problems.append(
            f"GCA_G genome(s) whose feature is not a member of reference {reference_idx}:"
            f" {_listed(outside)}"
        )
    if misnamed:
        problems.append(
            f"GCA_G genome(s) whose feature has another name in reference {reference_idx}:"
            f" {_listed(misnamed)}"
        )
    return [r["x_idx"] for r in gca_g], single, headers


async def _links(
    conn: asyncpg.Connection, single: list[asyncpg.Record], problems: list[str]
) -> tuple[list[_Fold], list[_Stale]]:
    """Each fold, whether its `G<n>` already holds the feature, and the stale WoL links
    the fold removes (from the same feature to a WoL ID other than `G<n>`)."""
    links: dict[int, list[asyncpg.Record]] = defaultdict(list)
    for r in await conn.fetch(
        "SELECT fg.feature_idx, g.genome_idx, g.source, g.source_id"
        "  FROM qiita.feature_genome fg JOIN qiita.genome g USING (genome_idx)"
        " WHERE fg.feature_idx = ANY($1::bigint[])",
        [r["feature_idxs"][0] for r in single],
    ):
        links[r["feature_idx"]].append(r)
    doubled = []
    for f, genomes in sorted(links.items()):
        gca_g = sorted(
            g["source_id"]
            for g in genomes
            if _is_wol_genome(g["source"], g["source_id"], _GCA_G_ID)
        )
        if len(gca_g) > 1:
            doubled.append(f"feature {f} ({', '.join(gca_g)})")
    if doubled:
        problems.append(f"feature(s) linked to more than one GCA_G genome: {_listed(doubled)}")

    folds, stale = [], []
    for r in single:
        f = r["feature_idxs"][0]
        folds.append(
            _Fold(
                x_idx=r["x_idx"],
                x_id=r["x_id"],
                n_id=r["n_id"],
                feature_idx=f,
                w_idx=r["w_idx"],
                w_linked=any(g["genome_idx"] == r["w_idx"] for g in links[f]),
            )
        )
        stale.extend(
            _Stale(feature_idx=f, genome_idx=g["genome_idx"])
            for g in links[f]
            if _is_wol_genome(g["source"], g["source_id"], _WOL_ID) and g["source_id"] != r["n_id"]
        )
    return folds, stale


async def _guards(
    conn: asyncpg.Connection, x_idxs: list[int], stale: list[_Stale], problems: list[str]
) -> None:
    """Rows outside the fold that name a genome it deletes or unlinks.

    An exclusion on a genome hides that genome's features, found through feature_genome,
    so once the fold moves a feature off the genome the exclusion silently stops hiding
    it; its genome_idx has no FK for Postgres to refuse on. An assembly claim's FK would
    refuse the genome's delete; checking it here names the claims before any write."""
    excluded = [
        f"exclusion {r['reference_exclusion_idx']} names {r['source_id']}"
        for r in await conn.fetch(
            "SELECT e.reference_exclusion_idx, g.source_id"
            "  FROM qiita.reference_exclusion e JOIN qiita.genome g USING (genome_idx)"
            " WHERE e.genome_idx = ANY($1::bigint[])"
            " ORDER BY e.reference_exclusion_idx",
            x_idxs + [s.genome_idx for s in stale],
        )
    ]
    if excluded:
        problems.append(
            f"exclusion(s) naming a genome the repair deletes or unlinks: {_listed(excluded)}"
        )
    claimed = [
        f"{r['source_id']} (prep_sample {r['prep_sample_idx']})"
        for r in await conn.fetch(
            "SELECT DISTINCT g.source_id, am.prep_sample_idx"
            "  FROM qiita.assembly_membership am JOIN qiita.genome g USING (genome_idx)"
            " WHERE am.genome_idx = ANY($1::bigint[])"
            " ORDER BY 1, 2",
            x_idxs,
        )
    ]
    if claimed:
        problems.append(f"assembly claim(s) on a GCA_G genome: {_listed(claimed)}")


def _metadata_matches_reference(
    reference_idx: int,
    headers: dict[int, str | None],
    parents: tuple[Parent, ...],
    problems: list[str],
) -> None:
    unnamed = sorted(str(f) for f, acc in headers.items() if acc is None)
    if unnamed:
        problems.append(
            f"reference {reference_idx} has {len(unnamed)} feature(s) with no accession:"
            f" {_listed(unnamed)}"
        )
    named = {acc for acc in headers.values() if acc is not None}
    in_metadata = {p.genome_id for p in parents}
    if not_in_metadata := sorted(named - in_metadata):
        problems.append(
            f"genome(s) of reference {reference_idx} missing from the metadata:"
            f" {_listed(not_in_metadata)}"
        )
    if not_in_reference := sorted(in_metadata - named):
        problems.append(
            f"metadata genome(s) that are not members of reference {reference_idx}:"
            f" {_listed(not_in_reference)}"
        )


async def _derivations_present(
    conn: asyncpg.Connection,
    reference_idx: int,
    parents: tuple[Parent, ...],
    problems: list[str],
) -> int:
    """How many parents are already recorded exactly; one recorded otherwise is a
    problem, since the repair never rewrites a derivation."""
    existing = {
        r["source_id"]: r
        for r in await conn.fetch(
            "SELECT g.source_id, d.parent_source, d.parent_source_id, d.method,"
            "       d.asserted_by_reference_idx"
            "  FROM qiita.genome_derivation d JOIN qiita.genome g USING (genome_idx)"
            " WHERE g.source = $1 AND g.source_id = ANY($2::text[])",
            _WOL_SOURCE,
            [p.genome_id for p in parents],
        )
    }
    present, disagree = 0, []
    for p in parents:
        row = existing.get(p.genome_id)
        if row is None:
            continue
        have = (
            row["parent_source"],
            row["parent_source_id"],
            row["method"],
            row["asserted_by_reference_idx"],
        )
        if have == (p.parent_source, p.parent_source_id, DERIVATION_METHOD, reference_idx):
            present += 1
        else:
            disagree.append(
                f"{p.genome_id} derives from {row['parent_source_id']} by {row['method']},"
                f" asserted by reference {row['asserted_by_reference_idx']}"
                f" (metadata: {p.parent_source_id})"
            )
    if disagree:
        problems.append(f"existing derivation(s) that disagree: {_listed(disagree)}")
    return present


async def _survey(
    conn: asyncpg.Connection, reference_idx: int, parents: tuple[Parent, ...]
) -> _Survey:
    """Read everything the fold touches and every precondition it relies on."""
    problems: list[str] = []
    x_idxs, single, headers = await _gca_g_folds(conn, reference_idx, problems)
    folds, stale = await _links(conn, single, problems)
    await _guards(conn, x_idxs, stale, problems)
    _metadata_matches_reference(reference_idx, headers, parents, problems)
    present = await _derivations_present(conn, reference_idx, parents, problems)
    labels = await conn.fetch(
        "SELECT ef.idx, ef.genome_idx, ef.export_feature_id, g.source, g.source_id"
        "  FROM qiita.exported_feature ef JOIN qiita.genome g USING (genome_idx)"
        " WHERE ef.genome_idx = ANY($1::bigint[]) AND NOT ef.retired"
        " ORDER BY ef.idx",
        x_idxs,
    )
    return _Survey(
        folds=folds, stale=stale, labels=labels, derivations_present=present, problems=problems
    )


async def plan_repair(pool: asyncpg.Pool, *, reference_idx: int, metadata_path: Path) -> RepairPlan:
    """Check every precondition and count what an apply would do. Writes nothing."""
    parents = read_parents(metadata_path)
    async with pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
        await _require_reference(conn, reference_idx, None)
        survey = await _survey(conn, reference_idx, parents)
    if survey.problems:
        raise RepairRefused(_refusal(reference_idx, survey.problems))
    return RepairPlan(reference_idx=reference_idx, parents=parents, counts=survey.counts(parents))


# --- the apply -------------------------------------------------------------------------


async def _fold(conn: asyncpg.Connection, survey: _Survey) -> list[tuple]:
    """Link each feature to its `G<n>`, drop the stale and `GCA_G` links, delete the
    `GCA_G` rows. Returns the manifest rows: what was deleted, and what was retired."""
    folds = survey.folds
    create = [f for f in folds if f.w_idx is None]
    minted = await upsert_genomes(
        conn, [_WOL_SOURCE] * len(create), [f.n_id for f in create], [None] * len(create)
    )
    w_of = {f.x_idx: f.w_idx for f in folds if f.w_idx is not None}
    w_of.update({f.x_idx: idx for f, idx in zip(create, minted, strict=True)})
    # Linked by genome_idx rather than through the reference load's
    # `_write_genome_associations`, which upserts every genome it links: here that would
    # rewrite all of the reference's `G<n>` rows to mint the few that are missing.
    await conn.execute(
        "INSERT INTO qiita.feature_genome (feature_idx, genome_idx)"
        " SELECT * FROM unnest($1::bigint[], $2::bigint[])"
        " ON CONFLICT DO NOTHING",
        [f.feature_idx for f in folds],
        [w_of[f.x_idx] for f in folds],
    )

    manifest: list[tuple] = []
    unlinked = await conn.fetch(
        "DELETE FROM qiita.feature_genome fg"
        " USING unnest($1::bigint[], $2::bigint[]) AS s(feature_idx, genome_idx),"
        "       qiita.genome g"
        " WHERE fg.feature_idx = s.feature_idx AND fg.genome_idx = s.genome_idx"
        "   AND g.genome_idx = fg.genome_idx"
        " RETURNING fg.feature_idx, g.genome_idx, g.source, g.source_id",
        [s.feature_idx for s in survey.stale],
        [s.genome_idx for s in survey.stale],
    )
    manifest.extend(_link_row(r) for r in unlinked)
    # Captured before the delete below nulls their genome_idx; the tables are locked,
    # so these are exactly the labels that the trigger retires.
    manifest.extend(
        (
            "retire",
            "exported_feature",
            r["genome_idx"],
            r["source"],
            r["source_id"],
            None,
            r["idx"],
            r["export_feature_id"],
        )
        for r in survey.labels
    )
    x_idxs = [f.x_idx for f in folds]
    unlinked = await conn.fetch(
        "DELETE FROM qiita.feature_genome fg USING qiita.genome g"
        " WHERE fg.genome_idx = ANY($1::bigint[]) AND g.genome_idx = fg.genome_idx"
        " RETURNING fg.feature_idx, g.genome_idx, g.source, g.source_id",
        x_idxs,
    )
    manifest.extend(_link_row(r) for r in unlinked)
    deleted = await conn.fetch(
        "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])"
        " RETURNING genome_idx, source, source_id",
        x_idxs,
    )
    manifest.extend(
        ("delete", "genome", r["genome_idx"], r["source"], r["source_id"], None, None, None)
        for r in deleted
    )
    return manifest


async def _record_derivations(
    conn: asyncpg.Connection, reference_idx: int, parents: tuple[Parent, ...]
) -> None:
    await conn.execute(
        "INSERT INTO qiita.genome_derivation"
        " (genome_idx, parent_source, parent_source_id, method, asserted_by_reference_idx)"
        " SELECT g.genome_idx, p.parent_source, p.parent_source_id, $5, $6"
        "   FROM unnest($2::text[], $3::text[], $4::text[])"
        "          AS p(genome_id, parent_source, parent_source_id)"
        "   JOIN qiita.genome g ON g.source = $1 AND g.source_id = p.genome_id"
        " ON CONFLICT (genome_idx) DO NOTHING",
        _WOL_SOURCE,
        [p.genome_id for p in parents],
        [p.parent_source for p in parents],
        [p.parent_source_id for p in parents],
        DERIVATION_METHOD,
        reference_idx,
    )


async def _postcondition_problems(
    conn: asyncpg.Connection, reference_idx: int, parents: tuple[Parent, ...]
) -> list[str]:
    problems: list[str] = []
    left = await conn.fetchval(
        "SELECT count(*) FROM qiita.genome WHERE source = $1 AND source_id ~ $2",
        _WOL_SOURCE,
        _anchored(_GCA_G_ID),
    )
    if left:
        problems.append(f"{left} GCA_G genome(s) remain")
    wrong = [
        f"feature {r['feature_idx']} links to {r['genomes'] or 'no genome'}, not {r['accession']}"
        for r in await conn.fetch(
            "SELECT rm.feature_idx, rm.accession, string_agg("
            "         g.source || ':' || g.source_id, ', ' ORDER BY g.source_id) AS genomes"
            "  FROM qiita.reference_membership rm"
            "  LEFT JOIN qiita.feature_genome fg ON fg.feature_idx = rm.feature_idx"
            "  LEFT JOIN qiita.genome g ON g.genome_idx = fg.genome_idx"
            " WHERE rm.reference_idx = $1"
            " GROUP BY rm.feature_idx, rm.accession"
            " HAVING count(g.genome_idx) <> 1"
            "     OR bool_or(g.source <> $2 OR g.source_id <> rm.accession)"
            " ORDER BY rm.feature_idx",
            reference_idx,
            _WOL_SOURCE,
        )
    ]
    if wrong:
        problems.append(
            f"reference {reference_idx} feature(s) not linked to exactly the genome its"
            f" header names: {_listed(wrong)}"
        )
    missing = [
        r["genome_id"]
        for r in await conn.fetch(
            "SELECT p.genome_id"
            "  FROM unnest($2::text[], $3::text[], $4::text[])"
            "         AS p(genome_id, parent_source, parent_source_id)"
            "  LEFT JOIN qiita.genome g ON g.source = $1 AND g.source_id = p.genome_id"
            "  LEFT JOIN qiita.genome_derivation d ON d.genome_idx = g.genome_idx"
            "   AND d.parent_source = p.parent_source"
            "   AND d.parent_source_id = p.parent_source_id"
            "   AND d.method = $5 AND d.asserted_by_reference_idx = $6"
            " WHERE d.genome_idx IS NULL"
            " ORDER BY p.genome_id",
            _WOL_SOURCE,
            [p.genome_id for p in parents],
            [p.parent_source for p in parents],
            [p.parent_source_id for p in parents],
            DERIVATION_METHOD,
            reference_idx,
        )
    ]
    if missing:
        problems.append(f"genome(s) without their derivation: {_listed(missing)}")
    return problems


def _check_manifest_path(manifest_path: Path) -> Path:
    """Refuse a manifest path that would overwrite one, before anything is written.

    The partial file is checked too. One is left behind only by a run whose commit may
    have succeeded but whose manifest was not moved into place, and it may be the only
    record of what that run deleted."""
    partial = manifest_path.with_name(f".{manifest_path.name}.partial")
    for path in (manifest_path, partial):
        if path.exists():
            raise RepairRefused(f"manifest {path} already exists; it is never overwritten")
    if not manifest_path.parent.is_dir():
        raise RepairRefused(f"manifest directory {manifest_path.parent} does not exist")
    return partial


async def _lock(conn: asyncpg.Connection, reference_idx: int) -> None:
    """Hold the reference row, then the tables the repair reads and writes, waiting at
    most `_LOCK_TIMEOUT` for either.

    The row comes first because the reference delete takes it first
    (`assert_reference_deletable`) and then the tables, so whichever of the two comes
    second waits for the first rather than deadlocking with it. A key-share lock is
    enough to make the delete wait, and it leaves the row's other updates (a status
    change) free."""
    await conn.execute(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'")
    try:
        await _require_reference(conn, reference_idx, "key_share")
        await conn.execute(f"LOCK TABLE {', '.join(_LOCKED_TABLES)} IN SHARE ROW EXCLUSIVE MODE")
    except (asyncpg.LockNotAvailableError, asyncpg.DeadlockDetectedError) as exc:
        raise RepairRefused(
            f"reference {reference_idx} or a table that the repair locks is held by another"
            f" session ({exc}; the lock timeout is {_LOCK_TIMEOUT}). Nothing was written;"
            " run it again once that session has finished"
        ) from exc


async def apply_repair(
    pool: asyncpg.Pool, plan: RepairPlan, *, manifest_path: Path
) -> RepairCounts:
    """Re-check, fold, record the derivations, assert the result, then commit.

    Every row deleted and every label retired goes to `manifest_path` (CSV), which is
    written beside it under a temporary name before the commit and moved into place
    after. A precondition that no longer holds raises `RepairRefused`; a result that
    does not hold raises `RepairPostconditionFailed`; either way nothing is written.
    """
    partial = _check_manifest_path(manifest_path)
    reference_idx, parents = plan.reference_idx, plan.parents
    async with pool.acquire() as conn:
        transaction = conn.transaction()
        await transaction.start()
        try:
            await _lock(conn, reference_idx)
            survey = await _survey(conn, reference_idx, parents)
            if survey.problems:
                raise RepairRefused(_refusal(reference_idx, survey.problems))
            counts = survey.counts(parents)
            manifest = await _fold(conn, survey)
            await _record_derivations(conn, reference_idx, parents)
            problems = await _postcondition_problems(conn, reference_idx, parents)
            if problems:
                raise RepairPostconditionFailed(_refusal(reference_idx, problems))
            _write_manifest(partial, manifest)
        except BaseException:
            await transaction.rollback()
            raise
        try:
            await transaction.commit()
        except BaseException as exc:
            # A commit that raises may still have committed (a connection lost during
            # COMMIT cannot tell), and the partial is then the only record of what the
            # run deleted, so it is kept.
            raise RuntimeError(
                f"the commit did not complete ({type(exc).__name__}: {exc}), so whether the"
                f" repair is in the database is not known. {partial} is kept: if a dry run"
                f" still finds GCA_G genomes, nothing was written and it can be deleted;"
                f" otherwise it is this run's manifest"
            ) from exc
    _publish_manifest(partial, manifest_path)
    return counts


def _publish_manifest(partial: Path, manifest_path: Path) -> None:
    """Move the committed run's manifest into place. A hard link, unlike a rename,
    refuses a manifest another run wrote at the same path while this one waited."""
    try:
        os.link(partial, manifest_path)
        partial.unlink()
    except OSError as exc:
        raise RuntimeError(
            f"the repair is committed, but its manifest could not be moved from {partial}"
            f" to {manifest_path}: {exc}"
        ) from exc


def _link_row(r: asyncpg.Record) -> tuple:
    return (
        "delete",
        "feature_genome",
        r["genome_idx"],
        r["source"],
        r["source_id"],
        r["feature_idx"],
        None,
        None,
    )


def _write_manifest(path: Path, rows: list[tuple]) -> None:
    """Write the manifest to a new file at `path`, and remove it if the write fails:
    the transaction it describes is then rolled back."""
    fh = path.open("x", newline="")
    try:
        with fh:
            writer = csv.writer(fh)
            writer.writerow(_MANIFEST_COLUMNS)
            writer.writerows(tuple("" if v is None else v for v in row) for row in rows)
    except BaseException:
        path.unlink(missing_ok=True)
        raise
