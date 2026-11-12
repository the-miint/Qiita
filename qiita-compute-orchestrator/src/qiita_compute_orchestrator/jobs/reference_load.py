"""Native job: re-key hash_sequences' outputs to feature_idx, write the
DuckLake-shape staging Parquets the data plane registers.

Reads the upstream Parquets (manifest from hash_sequences, feature_map
from mint-features) and emits the files `register-files` then hands to
the data plane's DoAction. The staging outputs are:

  - `reference_sequences.parquet`        (feature_idx, sequence_hash, sequence_length_bp)
  - `reference_sequence_chunks/part_*.parquet` (feature_idx, chunk_index, chunk_data)
  - `reference_membership.parquet`       (reference_idx, feature_idx)
  - `reference_taxonomy.parquet`         (if taxonomy_path is set)
  - `reference_phylogeny.parquet`        (if tree_path is set)
  - `reference_placements.parquet`       (if jplace_path is set)
  - `reference_annotation.parquet`       (if annotation_manifest is set)

`reference_sequence_chunks` is a DIRECTORY of `part_*.parquet` files
rather than a single file — the chunks output is bin-pack-batched by
chunk count (same pattern as hash_sequences) so the per-batch sort
stays well under the DuckDB cap on GG2-scale inputs. The runner's
register-files convention treats a top-level subdir as a multi-file
DuckLake table whose name matches the directory.

Single-file output names match the DuckLake table names verbatim;
multi-file outputs use the directory name as the table name. Renames
on either side are cross-component contract breaks.

**Architectural call.** `hash_sequences` writes its intermediates keyed
on `sequence_hash` (it has no feature_idx yet). DuckLake's
`reference_sequences` and `reference_sequence_chunks` carry
`feature_idx`. This module performs the hash→feature_idx re-key by
joining hash_sequences' outputs with mint-features' `feature_map.parquet`
(sequence_hash → feature_idx). The alternative — pinning DuckLake to
hash-keyed sequence tables — would force every query-time consumer to
JOIN through `reference_membership` for `feature_idx`, which is the
lake-wide identifier.

**Optional inputs.** Taxonomy / tree / jplace each fan out into its own
write only when the corresponding `*_path` flows through `bound` from
the work_ticket's `action_context`. The runner injects them under
`taxonomy_path` / `tree_path` / `jplace_path` after upload-handle
resolution; absent uploads → absent paths → absent outputs.

`annotation_manifest` / `annotation_map` are the odd pair: they are not
uploads but STEP OUTPUTS (hash_sequences + mint-annotation-features), so
they arrive already bound, and every reference-add workflow binds BOTH
unconditionally — a reference with no GFF3 simply carries zero annotation
rows through them. They are typed optional here only so a caller
constructing `Inputs` directly (a test, a future workflow) can omit them;
supplying one without the other is a mis-wired workflow whose only symptom
would be a silently EMPTY `reference_annotation` table, so `execute` refuses
it up front.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.parquet import validate_parquet_path
from qiita_common.taxonomy import RANK_COLUMNS, RANK_PREFIXES, quote_rank

from ..miint import (
    PARQUET_OPTS_AT_REST,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ._blob_input import resolve_blob_input
from ._feature_load import (
    build_feature_id_map,
    write_feature_sequence_chunks,
    write_feature_sequences,
)
from ._jplace import load_jplace

_LOG = logging.getLogger(__name__)

YAML_STEP_NAME = "load"

# DuckDB resource caps for this step. `_DUCKDB_MEMORY_GB` is the OFF-SLURM
# fallback (local backend / tests), sized to the YAML baseline
# (workflows/reference-add/1.0.0.yaml: mem_gb=32 minus ~1 GB Python/miint/OS
# headroom). Under SLURM the limit instead tracks the real cgroup via
# `resolve_duckdb_memory_gb()` (SLURM_MEM_PER_NODE), so a `--mem-gb` override
# reaches DuckDB. DuckDB owns the whole box in this step (no in-process
# co-consumer), so it gets the allocation minus headroom.
_DUCKDB_MEMORY_GB = 31
_DUCKDB_THREADS = 8


class Inputs(BaseModel):
    """Typed input contract for reference_load.

    The first three fields are required outputs of the upstream pipeline
    (hash_sequences → mint-features) and carry bare names matching what
    those steps emit and what the YAML's `inputs:` list declares.

    `taxonomy_path` / `tree_path` / `jplace_path` carry the `_path`
    suffix because the runner injects them under that form when the
    work_ticket's `action_context` carries the matching `*_upload_idx`.

    `reference_idx` is framework-injected (REFERENCE-scoped ticket) and
    load-bearing: it's the `reference_membership` row's left-hand idx
    and the per-reference scoping column on every other staging file.
    `work_ticket_idx` flows through for parity; this step doesn't read
    it.
    """

    manifest: Path
    feature_map: Path
    reference_sequence_chunks: Path
    taxonomy_path: Path | None = None
    tree_path: Path | None = None
    jplace_path: Path | None = None
    annotation_manifest: Path | None = None
    annotation_map: Path | None = None
    reference_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    for label, path in [
        ("manifest", inputs.manifest),
        ("feature_map", inputs.feature_map),
        ("reference_sequence_chunks", inputs.reference_sequence_chunks),
    ]:
        if not path.exists():
            raise FileNotFoundError(f"{label} not found: {path}")
    for label, opt in [
        ("taxonomy", inputs.taxonomy_path),
        ("tree", inputs.tree_path),
        ("jplace", inputs.jplace_path),
        ("annotation_manifest", inputs.annotation_manifest),
        ("annotation_map", inputs.annotation_map),
    ]:
        if opt is not None and not opt.exists():
            raise FileNotFoundError(f"{label} not found: {opt}")

    # hash_sequences produces the manifest and mint-annotation-features the
    # feature-map; every workflow binds both. One without the other means the
    # workflow is mis-wired, and the failure it would otherwise produce is a
    # silently EMPTY reference_annotation table — so refuse up front rather than
    # emit a well-formed, wrong result.
    if (inputs.annotation_manifest is None) != (inputs.annotation_map is None):
        raise ValueError(
            "annotation_manifest and annotation_map must be supplied together; got "
            f"annotation_manifest={inputs.annotation_manifest!r}, "
            f"annotation_map={inputs.annotation_map!r}"
        )

    workspace.mkdir(parents=True, exist_ok=True)
    sequences_path = workspace / "reference_sequences.parquet"
    # `reference_sequence_chunks` is a DIRECTORY of `part_*.parquet`
    # files. The runner's register-files convention treats top-level
    # subdirs as multi-file DuckLake tables (table name = subdir name).
    chunks_dir = workspace / "reference_sequence_chunks"
    membership_path = workspace / "reference_membership.parquet"
    taxonomy_out_path = workspace / "reference_taxonomy.parquet"
    phylogeny_out_path = workspace / "reference_phylogeny.parquet"
    placements_out_path = workspace / "reference_placements.parquet"
    annotation_out_path = workspace / "reference_annotation.parquet"

    sequences_out = validate_parquet_path(sequences_path)
    membership_out = validate_parquet_path(membership_path)
    taxonomy_out = validate_parquet_path(taxonomy_out_path)
    phylogeny_out = validate_parquet_path(phylogeny_out_path)
    placements_out = validate_parquet_path(placements_out_path)
    annotation_out = validate_parquet_path(annotation_out_path)

    # miint is needed for `read_newick` and `read_jplace` when the
    # optional tree / jplace inputs are present. LOAD unconditionally even
    # when those inputs are absent — the extension is pre-staged, LOAD is
    # cheap, and it keeps the connection setup uniform.
    written: list[Path] = []
    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )

            # Pull feature_map into a TEMP TABLE once — every downstream
            # write JOINs against it (sequences, chunks, membership) and
            # build_feature_id_map needs it too. Without this each helper would
            # re-scan the file.
            conn.execute(
                "CREATE TEMP TABLE feature_map AS SELECT * FROM read_parquet(?)",
                [str(inputs.feature_map)],
            )

            # id_map: read_id → feature_idx (via sequence_hash). The
            # taxonomy / phylogeny / placements writes all key off
            # read_id, so this single JOIN is the bridge to feature_idx.
            # Counts also drive the unmapped-hash check below.
            build_feature_id_map(conn, inputs.manifest)

            write_feature_sequences(conn, sequences_out)
            written.append(sequences_path)

            write_feature_sequence_chunks(
                conn,
                inputs.reference_sequence_chunks,
                chunks_dir,
            )
            written.append(chunks_dir)

            _write_reference_membership(conn, inputs.reference_idx, membership_out)
            written.append(membership_path)

            if inputs.taxonomy_path is not None:
                _write_taxonomy(conn, inputs.taxonomy_path, inputs.reference_idx, taxonomy_out)
                written.append(taxonomy_out_path)

            if inputs.tree_path is not None:
                # On the REMOTE path the CLI DoPuts Newick / jplace as a chunked
                # `(chunk_index, chunk_data BLOB)` Parquet, so the data plane
                # stays schema-agnostic and large blobs stream under bounded
                # memory; miint's `read_newick` / `read_jplace` parse on-disk
                # text/JSON, so the chunks are stitched back into a temp file.
                # On the LOCAL path no bytes cross the wire and this is already
                # the raw file. `resolve_blob_input` sniffs which one it got —
                # unconditionally unwrapping would `read_parquet()` a raw `.nwk`
                # and raise, which is what a local reference-add carrying a tree
                # used to do.
                newick_path = resolve_blob_input(
                    conn,
                    path=inputs.tree_path,
                    out_path=duckdb_tmp / "tree.nwk",
                )
                _write_phylogeny(conn, newick_path, inputs.reference_idx, phylogeny_out)
                written.append(phylogeny_out_path)

            if inputs.jplace_path is not None:
                jplace_path = resolve_blob_input(
                    conn,
                    path=inputs.jplace_path,
                    out_path=duckdb_tmp / "placement.jplace",
                )
                _write_placements(
                    conn,
                    jplace_path,
                    inputs.reference_idx,
                    placements_out,
                    work_dir=duckdb_tmp / "jplace_parts",
                    phylogeny=phylogeny_out if inputs.tree_path is not None else None,
                )
                written.append(placements_out_path)

            if inputs.annotation_manifest is not None and inputs.annotation_map is not None:
                # Returns False (and writes nothing) for a reference with no
                # annotations — the common case; see _write_annotation.
                if _write_annotation(
                    conn,
                    annotation_manifest=inputs.annotation_manifest,
                    annotation_map=inputs.annotation_map,
                    reference_idx=inputs.reference_idx,
                    out=annotation_out,
                ):
                    written.append(annotation_out_path)

            conn.execute("DROP TABLE id_map")
            conn.execute("DROP TABLE feature_map")
        success = True
    finally:
        if not success:
            for partial in (
                sequences_path,
                membership_path,
                taxonomy_out_path,
                phylogeny_out_path,
                placements_out_path,
                annotation_out_path,
            ):
                partial.unlink(missing_ok=True)
            shutil.rmtree(chunks_dir, ignore_errors=True)

    # `staging_dir` is the binding register-files consumes; the YAML's
    # `outputs: [staging_dir]` declaration matches it. The runner's
    # register-files convention picks up flat `*.parquet` files and
    # top-level subdirs of `part_*.parquet` (multi-file tables).
    #
    # `reference_sequence_chunks` re-exposes the feature-keyed chunks dir
    # (written by `write_feature_sequence_chunks` above) as its own binding.
    # The hash_sequences step's same-named binding is sequence_hash-keyed
    # (pre-minting); this is the feature_idx-keyed re-key, which is what rype
    # needs. host-reference-add's build_rype_index step consumes it — and must
    # run BEFORE register-files, which MOVES these part files into permanent
    # DuckLake storage (data-plane `move_file`). reference-add declares only
    # `staging_dir` as an output, so this extra key is inert there (the runner
    # binds only a step's declared outputs).
    return {"staging_dir": workspace, "reference_sequence_chunks": chunks_dir}


def _write_reference_membership(
    conn: duckdb.DuckDBPyConnection,
    reference_idx: int,
    out: str,
) -> None:
    """One row per (reference_idx, feature_idx) — the DuckLake-side
    membership table. Postgres has its own `qiita.reference_membership`
    populated by the `write-membership` LIBRARY primitive; both tables
    hold the same rows, the Postgres one for transactional queries and
    this one for the lake."""
    conn.execute(
        "COPY ("
        f"  SELECT CAST({reference_idx} AS BIGINT) AS reference_idx, feature_idx"
        "  FROM feature_map"
        "  ORDER BY feature_idx"
        f") TO '{out}' ({PARQUET_OPTS_AT_REST})"
    )


def _write_annotation(
    conn: duckdb.DuckDBPyConnection,
    *,
    annotation_manifest: Path,
    annotation_map: Path,
    reference_idx: int,
    out: str,
) -> bool:
    """Emit DuckLake's `reference_annotation` shape — one row per annotated interval.

    Every identifier on the row is MINTED by the control plane and arrives via
    `annotation_map` (the output of `mint-annotation-features`); this job resolves none
    of them itself:

      * `annotation_idx`     — the OCCURRENCE's identity, and the join back to the
                               Postgres claim and to the annotation's semantic terms.
      * `feature_idx`        — the interval itself (the SynDNA insert, the gene).
      * `parent_feature_idx` — the sequence the interval sits on and that reads actually
                               align to (the plasmid, the chromosome).

    The map is joined on the annotation's NATURAL key — parent + window + type + strand —
    reached from the manifest through `id_map` (GFF `seqid` → FASTA `read_id` →
    feature_idx), the same bridge `_write_taxonomy` and `_write_phylogeny` use. Not on the
    GFF3 `ID`: the spec lets a discontinuous feature repeat one ID across N lines, so it
    is carried as provenance and joined on by nothing.

    Coordinates pass through VERBATIM. They were converted from GFF3's closed
    `[start, end]` to half-open `[position, stop_position)` exactly once, at ingest, in
    `hash_sequences._write_annotation_manifest`. Re-deriving or "correcting" them here
    would be a second conversion.

    Unlike taxonomy's coverage checks (which warn), an unresolved row here is fatal: it
    would emit a lake row whose `annotation_idx` or `feature_idx` is NULL, and a feature
    table keyed on a NULL feature is not a degraded result, it is a wrong one.
    """
    # The zero-annotation case is the COMMON one — almost no reference carries a
    # GFF3, but every reference-add reaches here (the manifest is bound
    # unconditionally so the step's output binding always resolves). Emitting the
    # file anyway would have `register-files` move a zero-row Parquet into permanent
    # lake storage and add a DuckLake snapshot for it, on every single reference-add,
    # forever. So write NOTHING and tell the caller: the table already exists
    # (ensure_reference_tables creates it at data-plane boot), so a reference with no
    # annotations is correctly represented by having no rows, not by an empty file.
    if (
        conn.execute("SELECT count(*) FROM read_parquet(?)", [str(annotation_manifest)]).fetchone()[
            0
        ]
        == 0
    ):
        return False

    conn.execute(
        "CREATE TEMP TABLE annotation_map AS SELECT * FROM read_parquet(?)",
        [str(annotation_map)],
    )
    conn.execute(
        "CREATE TEMP TABLE annotation_manifest AS SELECT * FROM read_parquet(?)",
        [str(annotation_manifest)],
    )

    # The natural-key join, single-sourced so the check below and the COPY cannot drift
    # apart — an unresolved row must be REJECTED here, not silently dropped by the COPY's
    # inner join.
    natural_key_join = (
        "JOIN id_map im ON im.read_id = am.parent_read_id "
        "{kind} JOIN annotation_map map "
        "  ON map.parent_feature_idx = im.feature_idx "
        " AND map.position = am.position "
        " AND map.stop_position = am.stop_position "
        " AND map.annotation_type = am.annotation_type "
        " AND map.strand = am.strand "
    )

    unresolved = conn.execute(
        "SELECT am.parent_read_id, am.annotation_type, am.position, am.stop_position "
        "FROM annotation_manifest am "
        "LEFT " + natural_key_join.format(kind="LEFT") + "WHERE map.annotation_idx IS NULL "
        "ORDER BY am.parent_read_id, am.position LIMIT 5"
    ).fetchall()
    if unresolved:
        raise ValueError(
            "annotation rows with no minted annotation_idx (the annotation map is "
            "incomplete — mint-annotation-features and this load disagree about the "
            "natural key): "
            + ", ".join(f"{t} on {p!r} [{pos}, {stop})" for p, t, pos, stop in unresolved)
        )

    conn.execute(
        "COPY ("
        "  SELECT map.annotation_idx, "
        f"         CAST({reference_idx} AS BIGINT) AS reference_idx, "
        "         map.feature_idx, "
        "         map.parent_feature_idx, "
        "         am.annotation_id, "
        "         am.source, "
        "         am.annotation_type, "
        "         am.position, "
        "         am.stop_position, "
        "         am.strand, "
        "         am.score, "
        "         am.phase, "
        "         am.attributes "
        "  FROM annotation_manifest am "
        + natural_key_join.format(kind="")
        + "  ORDER BY map.parent_feature_idx, am.position, am.stop_position"
        f") TO '{out}' ({PARQUET_OPTS_AT_REST})"
    )
    conn.execute("DROP TABLE annotation_manifest")
    conn.execute("DROP TABLE annotation_map")
    return True


def _anomaly(
    conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None
) -> tuple[int, list]:
    """How many rows `sql` returns, and its first five (each row's first column), for a
    refusal or warning that says both how many and which."""
    params = params or []
    (n,) = conn.execute(f"SELECT count(*) FROM ({sql})", params).fetchone()
    if not n:
        return 0, []
    return n, [r[0] for r in conn.execute(f"{sql} LIMIT 5", params).fetchall()]


def _warn_anomaly(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    describe: str,
    params: list | None = None,
) -> None:
    """Emit a loud WARNING (never a raise) with the count and first five rows of
    `sql`, when it returns any.

    For anomalies that real corpora carry and that the load must survive. An
    unconfigured WARNING reaches stderr via Python's last-resort handler and
    lands in the SLURM job log, the loudest channel this architecture offers
    today."""
    n, first = _anomaly(conn, sql, params)
    if n:
        _LOG.warning("%d %s (e.g. %s)", n, describe, first)


def _write_taxonomy(
    conn: duckdb.DuckDBPyConnection,
    taxonomy_path: Path,
    reference_idx: int,
    out: str,
) -> None:
    """Parse semicolon-delimited rank string from the input Parquet's
    `(feature_id, taxonomy)` rows, JOIN against id_map on read_id, and
    emit exactly one DuckLake row per reference feature — including
    features with no supplied taxonomy, which are recorded at rest as
    all-NULL-rank ("unclassified") rows. So `reference_taxonomy` is 1-1
    with the reference's features.

    Format checks on *supplied* content stay hard ValueErrors (≤8 ranks,
    no blank fields, prefix order). Coverage anomalies (missing / stray /
    duplicate) are warned loudly, not raised: real data isn't strictly
    1-1 and a hard check would reject GG2."""
    conn.execute(
        "CREATE TEMP TABLE parsed_taxonomy AS "
        "SELECT "
        "  m.feature_idx,"
        "  m.read_id,"
        "  string_split_regex(t.taxonomy, ';\\s*') AS ranks,"
        "  len(string_split_regex(t.taxonomy, ';\\s*')) AS nranks "
        "FROM read_parquet(?) t "
        "INNER JOIN id_map m ON t.feature_id = m.read_id",
        [str(taxonomy_path)],
    )

    bad = conn.execute(
        "SELECT feature_idx, nranks FROM parsed_taxonomy WHERE nranks > 8 LIMIT 5"
    ).fetchall()
    if bad:
        raise ValueError(f"Taxonomy has >8 semicolon-delimited fields: {bad}")

    bad = conn.execute(
        "SELECT feature_idx FROM parsed_taxonomy WHERE list_contains(ranks, '') LIMIT 5"
    ).fetchall()
    if bad:
        ids = [r[0] for r in bad]
        raise ValueError(f"Taxonomy contains blank fields for feature_idx: {ids}")

    # Built from RANK_PREFIXES rather than spelled out, because the published
    # taxonomy sidecar RESTORES these prefixes by position from that same tuple —
    # so a drift between the two would silently relabel ranks in a file people
    # join. See qiita_common.taxonomy.
    prefix_order = " OR ".join(
        f"(nranks >= {i} AND NOT starts_with(ranks[{i}], '{prefix}'))"
        for i, prefix in enumerate(RANK_PREFIXES, start=1)
    )
    bad = conn.execute(
        f"SELECT feature_idx FROM parsed_taxonomy WHERE {prefix_order} LIMIT 5"
    ).fetchall()
    if bad:
        ids = [r[0] for r in bad]
        raise ValueError(f"Taxonomy has wrong rank prefix order for feature_idx: {ids}")

    # Coverage anomalies warn rather than fail the load, because real corpora have
    # them: GG2's 2024.09 backbone has ~29 features with no taxonomy row. The count
    # in each warning is what shows a taxonomy keyed by identifiers other than the
    # FASTA's, which leaves nearly every feature unclassified. Each check counts
    # features, not read_ids, so a feature known by several read_ids counts once.
    _warn_anomaly(
        conn,
        "SELECT min(x.read_id) FROM id_map x "
        "ANTI JOIN (SELECT feature_idx FROM parsed_taxonomy WHERE ranks IS NOT NULL) p"
        " ON x.feature_idx = p.feature_idx "
        "GROUP BY x.feature_idx ORDER BY 1",
        "feature(s) have no supplied taxonomy; recording as unclassified (NULL ranks)",
    )
    _warn_anomaly(
        conn,
        "SELECT t.feature_id FROM read_parquet(?) t "
        "ANTI JOIN id_map m ON t.feature_id = m.read_id ORDER BY 1",
        "supplied taxonomy row(s) reference a feature_id that is not a sequence read_id "
        "(stray / unmatched — ID-namespace mismatch); dropping them",
        [str(taxonomy_path)],
    )
    _warn_anomaly(
        conn,
        "SELECT feature_id FROM read_parquet(?) GROUP BY feature_id HAVING count(*) > 1 ORDER BY 1",
        "feature_id(s) have duplicate supplied taxonomy rows; collapsing to one per feature",
        [str(taxonomy_path)],
    )

    # Read_ids whose sequences canonicalize to one feature can carry different
    # lineages, and so can duplicate rows for one read_id. Of the feature's
    # read_ids that carry a lineage, the lexicographically smallest one's wins,
    # the lineage itself breaking a tie between duplicate rows. The feature's
    # accession is its smallest read_id overall, so when that read_id carries a
    # lineage, the lineage kept is the accession's.
    _warn_anomaly(
        conn,
        "SELECT list(DISTINCT read_id ORDER BY read_id) FROM parsed_taxonomy"
        " WHERE ranks IS NOT NULL"
        " GROUP BY feature_idx HAVING count(DISTINCT ranks) > 1 ORDER BY min(read_id)",
        "feature(s) carry more than one distinct lineage; keeping the lineage of the "
        "lexicographically smallest read_id that carries one",
    )

    # LEFT JOIN off the distinct reference feature set (same features
    # reference_sequences / reference_membership emit) so every feature
    # gets exactly one row; features with no supplied taxonomy get NULL
    # ranks (the existing NULLIF(substr(ranks[i], 4), '') yields NULL for
    # a NULL `ranks`). Supplied taxonomy is reduced to one row per
    # feature_idx first, by the rule above, so it can't multiply the join.
    conn.execute(
        "COPY ("
        "  SELECT "
        f"    CAST({reference_idx} AS BIGINT) AS reference_idx,"
        "    m.feature_idx,"
        # Strips the prefix the check above REQUIRED, per rank, so the column a rank
        # lands in and the prefix it was required to carry cannot come apart. The cut
        # length comes from the prefix itself rather than a literal 3: every prefix is
        # three characters today, so a four-character one would otherwise leave a
        # stray character in a published sidecar and nothing here would notice.
        + "".join(
            f"    NULLIF(substr(p.ranks[{i}], {len(prefix) + 1}), '') AS {quote_rank(column)},"
            for i, (column, prefix) in enumerate(
                zip(RANK_COLUMNS, RANK_PREFIXES, strict=True), start=1
            )
        )
        + "    NULL::BIGINT AS ncbi_taxon_id"
        "  FROM (SELECT DISTINCT feature_idx FROM id_map) m"
        "  LEFT JOIN ("
        "    SELECT feature_idx, ranks FROM parsed_taxonomy"
        "    QUALIFY row_number() OVER ("
        "      PARTITION BY feature_idx ORDER BY ranks IS NULL, read_id, ranks) = 1"
        "  ) p ON m.feature_idx = p.feature_idx"
        "  ORDER BY m.feature_idx"
        f") TO '{out}' ({PARQUET_OPTS_AT_REST})"
    )
    conn.execute("DROP TABLE parsed_taxonomy")


def _write_phylogeny(
    conn: duckdb.DuckDBPyConnection,
    tree_path: Path,
    reference_idx: int,
    out: str,
) -> None:
    """Parse a Newick tree and emit one DuckLake row per node, with
    feature_idx populated on tips that match a known read_id.

    A tree that decorates no node at all is given `edge_id = node_index`; one that
    decorates any node keeps exactly what it carried, so a partially decorated tree
    stays partial rather than ending up with two numberings in one column. Why the
    column has to be minted at all, and what an all-NULL one costs: see "Edge
    numbering (`edge_id`)" in `docs/architecture/reference-data.md`, and
    duckdb-miint#272."""
    conn.execute(
        "CREATE TEMP TABLE tree_nodes AS SELECT * FROM read_newick(?)",
        [str(tree_path)],
    )

    conn.execute(
        "COPY ("
        f"  SELECT CAST({reference_idx} AS BIGINT) AS reference_idx,"
        "    t.node_index, t.name, t.branch_length,"
        "    CASE WHEN count(t.edge_id) OVER () = 0 THEN t.node_index ELSE t.edge_id END"
        "      AS edge_id,"
        "    t.parent_index, t.is_tip, m.feature_idx"
        "  FROM tree_nodes t"
        "  LEFT JOIN id_map m ON t.is_tip AND t.name = m.read_id"
        "  ORDER BY t.node_index"
        f") TO '{out}' ({PARQUET_OPTS_AT_REST})",
    )
    conn.execute("DROP TABLE tree_nodes")


def _write_placements(
    conn: duckdb.DuckDBPyConnection,
    jplace_path: Path,
    reference_idx: int,
    out: str,
    *,
    work_dir: Path,
    phylogeny: str | None,
) -> None:
    """Parse a jplace file and emit one DuckLake row per placed feature.

    Every placed sequence lands, or the load fails. Each of these is refused with a
    count and examples, because dropping or keeping it would publish a placement set
    other than the one supplied, and nothing downstream could tell:

      * a fragment placed twice;
      * a fragment that matches no sequence identifier in this load;
      * an edge_num the jplace's own tree does not number;
      * when this load also carries a tree (`phylogeny` is its staged Parquet), a tree
        other than the jplace's, as `_refuse_a_tree_other_than_the_jplaces` defines
        it.

    Fragments that are one feature (identical bytes, or reverse complements, under
    different names) are placements of the same sequence. The feature keeps the
    placement of its lexicographically smallest placed fragment, the rule
    `_write_taxonomy` applies to lineages, and the load warns how many features had
    more than one. That is a choice among placements of one sequence rather than the
    loss of a sequence's placement, which is why it warns where the checks above
    raise."""
    load_jplace(conn, jplace_path, table="jplace_rows", tree_table="jplace_tree", work_dir=work_dir)
    conn.execute(
        "CREATE TEMP TABLE placed AS "
        "SELECT j.*, m.feature_idx FROM jplace_rows j LEFT JOIN id_map m ON j.fragment = m.read_id"
    )
    conn.execute("DROP TABLE jplace_rows")

    n, first = _anomaly(
        conn, "SELECT fragment FROM placed GROUP BY fragment HAVING count(*) > 1 ORDER BY fragment"
    )
    if n:
        raise ValueError(f"{n} jplace fragment(s) are placed more than once (e.g. {first})")
    n, first = _anomaly(
        conn, "SELECT fragment FROM placed WHERE feature_idx IS NULL ORDER BY fragment"
    )
    if n:
        raise ValueError(
            f"{n} jplace fragment(s) match no sequence identifier in this load (e.g. {first})"
        )
    n, first = _anomaly(
        conn,
        "SELECT e.edge_num || ' (' || e.placements || ' placements)'"
        " FROM (SELECT edge_num, count(*) AS placements FROM placed GROUP BY edge_num) e"
        " ANTI JOIN jplace_tree t ON e.edge_num = t.edge_id"
        " ORDER BY e.edge_num",
    )
    if n:
        raise ValueError(
            f"{n} jplace edge_num(s) are not an edge of the jplace's own tree (e.g. {first})"
        )
    if phylogeny is not None:
        _refuse_a_tree_other_than_the_jplaces(conn, phylogeny)
    conn.execute("DROP TABLE jplace_tree")

    _warn_anomaly(
        conn,
        "SELECT list(fragment ORDER BY fragment) FROM placed"
        " GROUP BY feature_idx HAVING count(*) > 1 ORDER BY min(fragment)",
        "feature(s) have placements from more than one fragment; keeping the "
        "lexicographically smallest fragment's",
    )

    conn.execute(
        "COPY ("
        f"  SELECT CAST({reference_idx} AS BIGINT) AS reference_idx,"
        "    feature_idx, edge_num,"
        "    likelihood, like_weight_ratio,"
        "    distal_length, pendant_length"
        "  FROM placed"
        "  QUALIFY row_number() OVER (PARTITION BY feature_idx ORDER BY fragment) = 1"
        "  ORDER BY feature_idx"
        f") TO '{out}' ({PARQUET_OPTS_AT_REST})",
    )
    conn.execute("DROP TABLE placed")


def _refuse_a_tree_other_than_the_jplaces(conn: duckdb.DuckDBPyConnection, phylogeny: str) -> None:
    """Raise unless the tree staged at `phylogeny` is the TEMP TABLE `jplace_tree`, as
    far as a placement can tell: every node's edge number, its parent's edge number,
    its branch length, and, at a tip, its name. Edge numbers count because a jplace's
    edge_nums name edges of its own tree, so the same topology numbered another way
    joins placements to the wrong edges. Branch lengths count because a
    placement's `distal_length` is a position along its edge, and its likelihoods were
    computed on those lengths. Internal labels and the order of a node's children are
    not compared; a placement's meaning does not depend on them."""
    conn.execute(
        "CREATE TEMP TABLE load_tree AS"
        " SELECT node_index, parent_index, edge_id, branch_length, is_tip, name"
        " FROM read_parquet(?)",
        [phylogeny],
    )
    shape = (
        "SELECT c.edge_id, p.edge_id AS parent_edge_id, c.branch_length,"
        " CASE WHEN c.is_tip THEN c.name END AS tip"
        " FROM {t} c LEFT JOIN {t} p ON c.parent_index = p.node_index"
    )
    jplace_nodes, load_nodes = conn.execute(
        "SELECT (SELECT count(*) FROM jplace_tree), (SELECT count(*) FROM load_tree)"
    ).fetchone()
    n, first = _anomaly(
        conn,
        "SELECT 'edge ' || coalesce(CAST(edge_id AS VARCHAR), 'none')"
        " || ' under edge ' || coalesce(CAST(parent_edge_id AS VARCHAR), 'none')"
        " || ', length ' || coalesce(CAST(branch_length AS VARCHAR), 'none')"
        " || coalesce(', tip ' || tip, '') FROM ("
        f"{shape.format(t='jplace_tree')} EXCEPT ALL {shape.format(t='load_tree')}"
        ") ORDER BY edge_id NULLS FIRST, parent_edge_id NULLS FIRST, tip NULLS FIRST",
    )
    conn.execute("DROP TABLE load_tree")
    if n or jplace_nodes != load_nodes:
        raise ValueError(
            f"the tree in this load is not the jplace's tree: it has {load_nodes} nodes to the "
            f"jplace tree's {jplace_nodes}, and {n} of the jplace tree's nodes have no node in "
            f"it with the same edge number, parent edge number, branch length and tip name "
            f"(e.g. {first}). "
            "A jplace's edge_nums name edges of its own tree, so load that tree, `{N}` "
            "decorations included"
        )
