"""Fold a Web of Life reference's `GCA_G<n>` genomes into `G<n>`, and record the NCBI
assembly each of its genomes was derived from.

**What it repairs.** A WoL reference loaded with a genome map that put `GCA_` in front
of each WoL ID minted a second genome row, `genbank:GCA_G<n>`, beside the `G<n>` an
earlier WoL load had already minted for the same bytes. `feature_idx` is global, so
every sequence of the reference then links to both rows and a feature table built
against it splits each genome's reads between the two names. `GCA_G<n>` is not an
accession: GenBank's are `GCA_` and nine digits with a version.

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
the sequence (`reference_membership.accession`); a `GCA_G<n>` whose feature the
reference names otherwise is refused, not guessed at.

Contract per this package: dry-run by default (`plan_repair` writes nothing), and
idempotent (a second run finds no `GCA_G` genome and every derivation present). There
is no residue: anything the rules do not settle refuses the whole run, naming the
rows, because a partial fold would leave tables that still split reads.
"""

from __future__ import annotations

import csv
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import asyncpg
import duckdb

from ..actions.library import upsert_genomes

DERIVATION_METHOD = "wol_concatenated"

_WOL_SOURCE = "genbank"
_WOL_ID = r"^G[0-9]{9}$"
_GCA_G_ID = r"^GCA_G[0-9]{9}$"
_PARENT_SOURCE = {"GCA": "genbank", "GCF": "refseq"}
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


@dataclass
class _Survey:
    folds: list[_Fold]
    stale: list[tuple[int, int, str]]  # (feature_idx, genome_idx, source_id)
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


def _refusal(reference_idx: int, problems: list[str]) -> str:
    return f"reference {reference_idx} cannot be repaired:\n" + "\n".join(
        f"  - {p}" for p in problems
    )


def read_parents(metadata_path: Path) -> tuple[Parent, ...]:
    """Each genome's parent assembly from a WoL `metadata.txt` (tab-separated, header).

    Only `genome_id` and `assembly_accession` are read. The archive comes from the
    accession's prefix, not the file's `source` column, which WoL leaves empty for
    genomes carried over from WoL2.
    """
    if not metadata_path.is_file():
        raise RepairRefused(f"metadata file {metadata_path} does not exist")
    read = "read_csv(?, delim='\t', header=true, all_varchar=true, quote='')"
    path = str(metadata_path)
    with duckdb.connect() as con:
        columns = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {read}", [path]).fetchall()}
        missing = [c for c in _METADATA_COLUMNS if c not in columns]
        if missing:
            raise RepairRefused(
                f"metadata file {metadata_path} lacks column(s): {', '.join(missing)}"
            )
        rows = con.execute(
            "SELECT genome_id, assembly_accession,"
            "       regexp_full_match(assembly_accession, 'GC[AF]_[0-9]{9}\\.[0-9]+')"
            f"  FROM {read}",
            [path],
        ).fetchall()

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
    for column, values in (
        ("genome_id", [g for g, _a, _ok in rows]),
        ("assembly_accession", [a for _g, a, _ok in rows]),
    ):
        repeated = sorted(str(v) for v, n in Counter(values).items() if n > 1)
        if repeated:
            problems.append(f"{len(repeated)} {column}(s) repeat: {_listed(repeated)}")
    if problems:
        raise RepairRefused(
            f"metadata file {metadata_path}:\n" + "\n".join(f"  - {p}" for p in problems)
        )
    return tuple(
        Parent(genome_id=g, parent_source=_PARENT_SOURCE[acc[:3]], parent_source_id=acc)
        for g, acc, _ok in rows
    )


async def _survey(
    conn: asyncpg.Connection, reference_idx: int, parents: tuple[Parent, ...]
) -> _Survey:
    """Read everything the fold touches and every precondition it relies on."""
    problems: list[str] = []

    gca_g = await conn.fetch(
        "SELECT x.genome_idx AS x_idx, x.source_id AS x_id, substr(x.source_id, 5) AS n_id,"
        "       w.genome_idx AS w_idx,"
        "       array_remove(array_agg(fg.feature_idx ORDER BY fg.feature_idx), NULL)"
        "         AS feature_idxs"
        "  FROM qiita.genome x"
        "  LEFT JOIN qiita.feature_genome fg ON fg.genome_idx = x.genome_idx"
        "  LEFT JOIN qiita.genome w ON w.source = x.source AND w.source_id = substr(x.source_id, 5)"
        f" WHERE x.source = '{_WOL_SOURCE}' AND x.source_id ~ '{_GCA_G_ID}'"
        " GROUP BY x.genome_idx, x.source_id, w.genome_idx"
        " ORDER BY x.source_id"
    )
    not_one = [
        f"{r['x_id']} has {len(r['feature_idxs'])}" for r in gca_g if len(r["feature_idxs"]) != 1
    ]
    if not_one:
        problems.append(f"GCA_G genome(s) without exactly one feature: {_listed(not_one)}")
    single = [r for r in gca_g if len(r["feature_idxs"]) == 1]
    feature_idxs = [r["feature_idxs"][0] for r in single]

    headers = {
        r["feature_idx"]: r["accession"]
        for r in await conn.fetch(
            "SELECT feature_idx, accession FROM qiita.reference_membership"
            " WHERE reference_idx = $1",
            reference_idx,
        )
    }
    outside = [
        f"{r['x_id']} (feature {r['feature_idxs'][0]})"
        for r in single
        if r["feature_idxs"][0] not in headers
    ]
    if outside:
        problems.append(
            f"GCA_G genome(s) whose feature is not a member of reference {reference_idx}:"
            f" {_listed(outside)}"
        )
    misnamed = [
        f"{r['x_id']} (feature {r['feature_idxs'][0]} is {headers[r['feature_idxs'][0]]!r})"
        for r in single
        if r["feature_idxs"][0] in headers and headers[r["feature_idxs"][0]] != r["n_id"]
    ]
    if misnamed:
        problems.append(
            f"GCA_G genome(s) whose feature reference {reference_idx} names otherwise:"
            f" {_listed(misnamed)}"
        )

    links: dict[int, list[asyncpg.Record]] = defaultdict(list)
    for r in await conn.fetch(
        "SELECT fg.feature_idx, g.genome_idx, g.source, g.source_id"
        "  FROM qiita.feature_genome fg JOIN qiita.genome g USING (genome_idx)"
        " WHERE fg.feature_idx = ANY($1::bigint[])",
        feature_idxs,
    ):
        links[r["feature_idx"]].append(r)
    doubled = sorted(
        f"feature {f} ({', '.join(sorted(g['source_id'] for g in gs if _is_gca_g(g)))})"
        for f, gs in links.items()
        if sum(_is_gca_g(g) for g in gs) > 1
    )
    if doubled:
        problems.append(f"feature(s) linked to more than one GCA_G genome: {_listed(doubled)}")

    folds: list[_Fold] = []
    stale: list[tuple[int, int, str]] = []
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
            (f, g["genome_idx"], g["source_id"])
            for g in links[f]
            if _is_wol_id(g) and g["source_id"] != r["n_id"]
        )

    x_idxs = [r["x_idx"] for r in gca_g]
    excluded = [
        f"exclusion {r['reference_exclusion_idx']} names {r['source_id']}"
        for r in await conn.fetch(
            "SELECT e.reference_exclusion_idx, g.source_id"
            "  FROM qiita.reference_exclusion e JOIN qiita.genome g USING (genome_idx)"
            " WHERE e.genome_idx = ANY($1::bigint[])"
            " ORDER BY e.reference_exclusion_idx",
            x_idxs + [g for _f, g, _s in stale],
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

    named = set(headers.values())
    unnamed = sorted(str(f) for f, acc in headers.items() if acc is None)
    if unnamed:
        problems.append(
            f"reference {reference_idx} has {len(unnamed)} feature(s) with no accession:"
            f" {_listed(unnamed)}"
        )
    in_metadata = {p.genome_id for p in parents}
    not_in_metadata = sorted(a for a in named - in_metadata if a is not None)
    if not_in_metadata:
        problems.append(
            f"genome(s) of reference {reference_idx} missing from the metadata:"
            f" {_listed(not_in_metadata)}"
        )
    not_in_reference = sorted(in_metadata - named)
    if not_in_reference:
        problems.append(
            f"metadata genome(s) that are not members of reference {reference_idx}:"
            f" {_listed(not_in_reference)}"
        )

    existing = {
        r["source_id"]: r
        for r in await conn.fetch(
            "SELECT g.source_id, d.parent_source, d.parent_source_id, d.method,"
            "       d.asserted_by_reference_idx"
            "  FROM qiita.genome_derivation d JOIN qiita.genome g USING (genome_idx)"
            f" WHERE g.source = '{_WOL_SOURCE}' AND g.source_id = ANY($1::text[])",
            [p.genome_id for p in parents],
        )
    }
    disagree = []
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
        want = (p.parent_source, p.parent_source_id, DERIVATION_METHOD, reference_idx)
        if have != want:
            disagree.append(
                f"{p.genome_id} derives from {row['parent_source_id']} by {row['method']},"
                f" asserted by reference {row['asserted_by_reference_idx']}"
                f" (metadata: {p.parent_source_id})"
            )
    if disagree:
        problems.append(f"existing derivation(s) that disagree: {_listed(disagree)}")

    labels = await conn.fetch(
        "SELECT ef.idx, ef.genome_idx, ef.export_feature_id, g.source, g.source_id"
        "  FROM qiita.exported_feature ef JOIN qiita.genome g USING (genome_idx)"
        " WHERE ef.genome_idx = ANY($1::bigint[]) AND NOT ef.retired"
        " ORDER BY ef.idx",
        x_idxs,
    )
    return _Survey(
        folds=folds,
        stale=stale,
        labels=labels,
        derivations_present=len(existing) - len(disagree),
        problems=problems,
    )


def _is_gca_g(g: asyncpg.Record) -> bool:
    s = g["source_id"]
    return g["source"] == _WOL_SOURCE and len(s) == 14 and s.startswith("GCA_G") and s[5:].isdigit()


def _is_wol_id(g: asyncpg.Record) -> bool:
    s = g["source_id"]
    return g["source"] == _WOL_SOURCE and len(s) == 10 and s[0] == "G" and s[1:].isdigit()


async def plan_repair(pool: asyncpg.Pool, *, reference_idx: int, metadata_path: Path) -> RepairPlan:
    """Check every precondition and count what an apply would do. Writes nothing."""
    parents = read_parents(metadata_path)
    async with pool.acquire() as conn, conn.transaction(isolation="repeatable_read", readonly=True):
        survey = await _survey(conn, reference_idx, parents)
    if survey.problems:
        raise RepairRefused(_refusal(reference_idx, survey.problems))
    return RepairPlan(reference_idx=reference_idx, parents=parents, counts=survey.counts(parents))


async def _postcondition_problems(
    conn: asyncpg.Connection, reference_idx: int, parents: tuple[Parent, ...]
) -> list[str]:
    problems: list[str] = []
    left = await conn.fetchval(
        "SELECT count(*) FROM qiita.genome"
        f" WHERE source = '{_WOL_SOURCE}' AND source_id ~ '{_GCA_G_ID}'"
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
            f" HAVING count(g.genome_idx) <> 1"
            f"     OR bool_or(g.source <> '{_WOL_SOURCE}' OR g.source_id <> rm.accession)"
            " ORDER BY rm.feature_idx",
            reference_idx,
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
            "  FROM unnest($1::text[], $2::text[], $3::text[]) AS p(genome_id, parent_source,"
            "                                                    parent_source_id)"
            "  LEFT JOIN qiita.genome g"
            f"    ON g.source = '{_WOL_SOURCE}' AND g.source_id = p.genome_id"
            "  LEFT JOIN qiita.genome_derivation d ON d.genome_idx = g.genome_idx"
            "   AND d.parent_source = p.parent_source"
            "   AND d.parent_source_id = p.parent_source_id"
            "   AND d.method = $4 AND d.asserted_by_reference_idx = $5"
            " WHERE d.genome_idx IS NULL"
            " ORDER BY p.genome_id",
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

    The partial file is checked too: one left behind is the manifest of a committed
    run whose rename failed, and the only record of what that run deleted."""
    partial = manifest_path.with_name(f".{manifest_path.name}.partial")
    for path in (manifest_path, partial):
        if path.exists():
            raise RepairRefused(f"manifest {path} already exists; it is never overwritten")
    if not manifest_path.parent.is_dir():
        raise RepairRefused(f"manifest directory {manifest_path.parent} does not exist")
    return partial


async def apply_repair(
    pool: asyncpg.Pool, plan: RepairPlan, *, manifest_path: Path
) -> RepairCounts:
    """Re-check, fold, record the derivations, assert the result, then commit.

    Every row deleted and every label retired goes to `manifest_path` (CSV), which is
    written beside it under a temporary name before the commit and renamed into place
    after. A precondition that no longer holds raises `RepairRefused`; a result that
    does not hold raises `RepairPostconditionFailed`; either way nothing is written.
    """
    partial = _check_manifest_path(manifest_path)
    reference_idx, parents = plan.reference_idx, plan.parents
    manifest: list[tuple] = []
    wrote_partial = False
    try:
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute(f"SET LOCAL lock_timeout = '{_LOCK_TIMEOUT}'")
            await conn.execute(
                f"LOCK TABLE {', '.join(_LOCKED_TABLES)} IN SHARE ROW EXCLUSIVE MODE"
            )
            survey = await _survey(conn, reference_idx, parents)
            if survey.problems:
                raise RepairRefused(_refusal(reference_idx, survey.problems))
            counts = survey.counts(parents)
            folds = survey.folds

            create = [f for f in folds if f.w_idx is None]
            minted = await upsert_genomes(
                conn,
                [_WOL_SOURCE] * len(create),
                [f.n_id for f in create],
                [None] * len(create),
            )
            w_of = {f.x_idx: f.w_idx for f in folds if f.w_idx is not None}
            w_of.update({f.x_idx: idx for f, idx in zip(create, minted, strict=True)})

            await conn.execute(
                "INSERT INTO qiita.feature_genome (feature_idx, genome_idx)"
                " SELECT * FROM unnest($1::bigint[], $2::bigint[])"
                " ON CONFLICT DO NOTHING",
                [f.feature_idx for f in folds],
                [w_of[f.x_idx] for f in folds],
            )
            for r in await conn.fetch(
                "DELETE FROM qiita.feature_genome fg"
                " USING unnest($1::bigint[], $2::bigint[]) AS s(feature_idx, genome_idx),"
                "       qiita.genome g"
                " WHERE fg.feature_idx = s.feature_idx AND fg.genome_idx = s.genome_idx"
                "   AND g.genome_idx = fg.genome_idx"
                " RETURNING fg.feature_idx, g.genome_idx, g.source, g.source_id",
                [f for f, _g, _s in survey.stale],
                [g for _f, g, _s in survey.stale],
            ):
                manifest.append(_link_row(r))
            for r in survey.labels:
                manifest.append(
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
                )
            x_idxs = [f.x_idx for f in folds]
            for r in await conn.fetch(
                "DELETE FROM qiita.feature_genome fg USING qiita.genome g"
                " WHERE fg.genome_idx = ANY($1::bigint[]) AND g.genome_idx = fg.genome_idx"
                " RETURNING fg.feature_idx, g.genome_idx, g.source, g.source_id",
                x_idxs,
            ):
                manifest.append(_link_row(r))
            for r in await conn.fetch(
                "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])"
                " RETURNING genome_idx, source, source_id",
                x_idxs,
            ):
                manifest.append(
                    (
                        "delete",
                        "genome",
                        r["genome_idx"],
                        r["source"],
                        r["source_id"],
                        None,
                        None,
                        None,
                    )
                )

            await conn.execute(
                "INSERT INTO qiita.genome_derivation"
                " (genome_idx, parent_source, parent_source_id, method,"
                "  asserted_by_reference_idx)"
                " SELECT g.genome_idx, p.parent_source, p.parent_source_id, $4, $5"
                "   FROM unnest($1::text[], $2::text[], $3::text[])"
                "          AS p(genome_id, parent_source, parent_source_id)"
                "   JOIN qiita.genome g"
                f"    ON g.source = '{_WOL_SOURCE}' AND g.source_id = p.genome_id"
                " ON CONFLICT (genome_idx) DO NOTHING",
                [p.genome_id for p in parents],
                [p.parent_source for p in parents],
                [p.parent_source_id for p in parents],
                DERIVATION_METHOD,
                reference_idx,
            )

            problems = await _postcondition_problems(conn, reference_idx, parents)
            if problems:
                raise RepairPostconditionFailed(_refusal(reference_idx, problems))
            _write_manifest(partial, manifest)
            wrote_partial = True
    except BaseException:
        if wrote_partial:  # the commit failed after the manifest was written
            partial.unlink(missing_ok=True)
        raise
    try:
        os.replace(partial, manifest_path)
    except OSError as exc:
        raise RuntimeError(
            f"the repair is COMMITTED, but its manifest could not be moved from {partial}"
            f" to {manifest_path}: {exc}"
        ) from exc
    return counts


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
    with path.open("x", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(_MANIFEST_COLUMNS)
        writer.writerows(tuple("" if v is None else v for v in row) for row in rows)
