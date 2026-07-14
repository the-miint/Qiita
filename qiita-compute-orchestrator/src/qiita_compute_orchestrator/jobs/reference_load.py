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

`annotation_manifest` / `annotation_feature_map` are the odd pair: they
are not uploads but STEP OUTPUTS (hash_sequences + mint-annotation-
features), so they arrive already bound, and every reference-add workflow
binds BOTH unconditionally — a reference with no GFF3 simply carries zero
annotation rows through them. They are typed optional here only so a caller
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

from ..miint import (
    PARQUET_OPTS,
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
    annotation_feature_map: Path | None = None
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
        ("annotation_feature_map", inputs.annotation_feature_map),
    ]:
        if opt is not None and not opt.exists():
            raise FileNotFoundError(f"{label} not found: {opt}")

    # hash_sequences produces the manifest and mint-annotation-features the
    # feature-map; every workflow binds both. One without the other means the
    # workflow is mis-wired, and the failure it would otherwise produce is a
    # silently EMPTY reference_annotation table — so refuse up front rather than
    # emit a well-formed, wrong result.
    if (inputs.annotation_manifest is None) != (inputs.annotation_feature_map is None):
        raise ValueError(
            "annotation_manifest and annotation_feature_map must be supplied together; got "
            f"annotation_manifest={inputs.annotation_manifest!r}, "
            f"annotation_feature_map={inputs.annotation_feature_map!r}"
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
                _write_placements(conn, jplace_path, inputs.reference_idx, placements_out)
                written.append(placements_out_path)

            if inputs.annotation_manifest is not None and inputs.annotation_feature_map is not None:
                # Returns False (and writes nothing) for a reference with no
                # annotations — the common case; see _write_annotation.
                if _write_annotation(
                    conn,
                    annotation_manifest=inputs.annotation_manifest,
                    annotation_feature_map=inputs.annotation_feature_map,
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
        f") TO '{out}' ({PARQUET_OPTS})"
    )


def _write_annotation(
    conn: duckdb.DuckDBPyConnection,
    *,
    annotation_manifest: Path,
    annotation_feature_map: Path,
    reference_idx: int,
    out: str,
) -> bool:
    """Emit DuckLake's `reference_annotation` shape — one row per annotated
    interval, carrying BOTH feature_idx values the table is about:

      * `feature_idx`        — the interval itself (the SynDNA insert), resolved
                               from `annotation_feature_map` on the sub-sequence's
                               canonical `sequence_hash`.
      * `parent_feature_idx` — the sequence the interval sits on and that reads
                               actually align to (the plasmid), resolved from
                               `id_map` on the GFF `seqid` → FASTA `read_id`.

    The `id_map` join is the same bridge `_write_taxonomy` and `_write_phylogeny`
    use (both key off `read_id`), so a GFF `seqid` naming an unknown sequence has
    already been rejected upstream in `hash_sequences`.

    Coordinates pass through VERBATIM. They were converted from GFF3's closed
    `[start, end]` to half-open `[position, stop_position)` exactly once, at
    ingest, in `hash_sequences._write_annotation_manifest`. Re-deriving or
    "correcting" them here would be a second conversion.

    Unlike taxonomy's coverage checks (which warn), an unresolved hash here is
    fatal: it would emit a row whose `feature_idx` is NULL, and a feature table
    keyed on a NULL feature is not a degraded result, it is a wrong one.
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
        "CREATE TEMP TABLE annotation_feature_map AS SELECT * FROM read_parquet(?)",
        [str(annotation_feature_map)],
    )
    conn.execute(
        "CREATE TEMP TABLE annotation_manifest AS SELECT * FROM read_parquet(?)",
        [str(annotation_manifest)],
    )

    unresolved = conn.execute(
        "SELECT am.annotation_id, am.parent_read_id "
        "FROM annotation_manifest am "
        "LEFT JOIN annotation_feature_map afm ON afm.sequence_hash = am.sequence_hash "
        "LEFT JOIN id_map im ON im.read_id = am.parent_read_id "
        "WHERE afm.feature_idx IS NULL OR im.feature_idx IS NULL "
        "ORDER BY am.annotation_id LIMIT 5"
    ).fetchall()
    if unresolved:
        raise ValueError(
            "annotation rows with an unresolvable feature_idx (the annotation "
            "feature-map or the sequence feature-map is incomplete): "
            + ", ".join(f"{a!r} on {p!r}" for a, p in unresolved)
        )

    conn.execute(
        "COPY ("
        f"  SELECT CAST({reference_idx} AS BIGINT) AS reference_idx, "
        "         afm.feature_idx, "
        "         im.feature_idx AS parent_feature_idx, "
        "         am.annotation_id, "
        "         am.annotation_type, "
        "         am.position, "
        "         am.stop_position, "
        "         am.strand, "
        "         am.attributes "
        "  FROM annotation_manifest am "
        "  JOIN annotation_feature_map afm ON afm.sequence_hash = am.sequence_hash "
        "  JOIN id_map im ON im.read_id = am.parent_read_id "
        "  ORDER BY parent_feature_idx, position, annotation_id"
        f") TO '{out}' ({PARQUET_OPTS})"
    )
    conn.execute("DROP TABLE annotation_manifest")
    conn.execute("DROP TABLE annotation_feature_map")
    return True


def _warn_taxonomy_anomaly(
    conn: duckdb.DuckDBPyConnection,
    *,
    count_sql: str,
    sample_sql: str,
    params: list,
    describe: str,
) -> None:
    """Emit a loud WARNING (never a raise) with a LIMIT-5 sample when a
    taxonomy-coverage anomaly is present.

    Coverage gaps and namespace mismatches are expected on real corpora —
    GG2's 2024.09 backbone has ~29 features with no taxonomy row, and a
    supplied taxonomy keyed in a different ID namespace than the FASTA is
    the exact class that already bit the genome map — so they must NOT
    fail the ingest. They must be visible, though: an unconfigured WARNING
    reaches stderr via Python's last-resort handler and lands in the SLURM
    job log, the loudest channel this architecture offers today."""
    n = conn.execute(count_sql, params).fetchone()[0]
    if n:
        sample = [r[0] for r in conn.execute(sample_sql, params).fetchall()]
        _LOG.warning("%d %s (sample: %s)", n, describe, sample)


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

    bad = conn.execute(
        "SELECT feature_idx FROM parsed_taxonomy WHERE "
        "(nranks >= 1 AND NOT starts_with(ranks[1], 'd__')) OR "
        "(nranks >= 2 AND NOT starts_with(ranks[2], 'p__')) OR "
        "(nranks >= 3 AND NOT starts_with(ranks[3], 'c__')) OR "
        "(nranks >= 4 AND NOT starts_with(ranks[4], 'o__')) OR "
        "(nranks >= 5 AND NOT starts_with(ranks[5], 'f__')) OR "
        "(nranks >= 6 AND NOT starts_with(ranks[6], 'g__')) OR "
        "(nranks >= 7 AND NOT starts_with(ranks[7], 's__')) OR "
        "(nranks >= 8 AND NOT starts_with(ranks[8], 't__')) "
        "LIMIT 5"
    ).fetchall()
    if bad:
        ids = [r[0] for r in bad]
        raise ValueError(f"Taxonomy has wrong rank prefix order for feature_idx: {ids}")

    # Warn (don't raise) on coverage anomalies. Distinct-feature counts so
    # a canonical-hash collision (two read_ids → one feature) isn't
    # double-counted.
    _warn_taxonomy_anomaly(
        conn,
        count_sql=(
            "SELECT count(*) FROM (SELECT DISTINCT feature_idx FROM id_map) x "
            "ANTI JOIN parsed_taxonomy p ON x.feature_idx = p.feature_idx"
        ),
        sample_sql=(
            "SELECT DISTINCT x.read_id FROM id_map x "
            "ANTI JOIN parsed_taxonomy p ON x.feature_idx = p.feature_idx LIMIT 5"
        ),
        params=[],
        describe="feature(s) have no supplied taxonomy; recording as unclassified (NULL ranks)",
    )
    _warn_taxonomy_anomaly(
        conn,
        count_sql=(
            "SELECT count(*) FROM read_parquet(?) t ANTI JOIN id_map m ON t.feature_id = m.read_id"
        ),
        sample_sql=(
            "SELECT t.feature_id FROM read_parquet(?) t "
            "ANTI JOIN id_map m ON t.feature_id = m.read_id LIMIT 5"
        ),
        params=[str(taxonomy_path)],
        describe=(
            "supplied taxonomy row(s) reference a feature_id that is not a sequence read_id "
            "(stray / unmatched — ID-namespace mismatch); dropping them"
        ),
    )
    _warn_taxonomy_anomaly(
        conn,
        count_sql=(
            "SELECT count(*) FROM "
            "(SELECT feature_id FROM read_parquet(?) GROUP BY feature_id HAVING count(*) > 1)"
        ),
        sample_sql=(
            "SELECT feature_id FROM read_parquet(?) GROUP BY feature_id HAVING count(*) > 1 LIMIT 5"
        ),
        params=[str(taxonomy_path)],
        describe=(
            "feature_id(s) have duplicate supplied taxonomy rows; collapsing to one per feature"
        ),
    )

    # LEFT JOIN off the distinct reference feature set (same features
    # reference_sequences / reference_membership emit) so every feature
    # gets exactly one row; features with no supplied taxonomy get NULL
    # ranks (the existing NULLIF(substr(ranks[i], 4), '') yields NULL for
    # a NULL `ranks`). Dedupe supplied taxonomy to one row per feature_idx
    # first so a duplicate read_id can't multiply the LEFT JOIN.
    conn.execute(
        "COPY ("
        "  SELECT "
        f"    CAST({reference_idx} AS BIGINT) AS reference_idx,"
        "    m.feature_idx,"
        "    NULLIF(substr(p.ranks[1], 4), '') AS domain,"
        "    NULLIF(substr(p.ranks[2], 4), '') AS phylum,"
        "    NULLIF(substr(p.ranks[3], 4), '') AS class,"
        "    NULLIF(substr(p.ranks[4], 4), '') AS \"order\","
        "    NULLIF(substr(p.ranks[5], 4), '') AS family,"
        "    NULLIF(substr(p.ranks[6], 4), '') AS genus,"
        "    NULLIF(substr(p.ranks[7], 4), '') AS species,"
        "    NULLIF(substr(p.ranks[8], 4), '') AS strain,"
        "    NULL::BIGINT AS ncbi_taxon_id"
        "  FROM (SELECT DISTINCT feature_idx FROM id_map) m"
        "  LEFT JOIN ("
        "    SELECT feature_idx, any_value(ranks) AS ranks"
        "    FROM parsed_taxonomy GROUP BY feature_idx"
        "  ) p ON m.feature_idx = p.feature_idx"
        "  ORDER BY m.feature_idx"
        f") TO '{out}' ({PARQUET_OPTS})"
    )
    conn.execute("DROP TABLE parsed_taxonomy")


def _write_phylogeny(
    conn: duckdb.DuckDBPyConnection,
    tree_path: Path,
    reference_idx: int,
    out: str,
) -> None:
    """Parse a Newick tree and emit one DuckLake row per node, with
    feature_idx populated on tips that match a known read_id."""
    conn.execute(
        "CREATE TEMP TABLE tree_nodes AS SELECT * FROM read_newick(?)",
        [str(tree_path)],
    )

    conn.execute(
        "COPY ("
        f"  SELECT CAST({reference_idx} AS BIGINT) AS reference_idx,"
        "    t.node_index, t.name, t.branch_length, t.edge_id,"
        "    t.parent_index, t.is_tip, m.feature_idx"
        "  FROM tree_nodes t"
        "  LEFT JOIN id_map m ON t.is_tip AND t.name = m.read_id"
        "  ORDER BY t.node_index"
        f") TO '{out}' ({PARQUET_OPTS})",
    )
    conn.execute("DROP TABLE tree_nodes")


def _write_placements(
    conn: duckdb.DuckDBPyConnection,
    jplace_path: Path,
    reference_idx: int,
    out: str,
) -> None:
    """Parse a jplace file and emit one row per (fragment, edge_num).
    Fragments not in id_map are dropped (jplace may carry rows for
    fragments outside this reference's mint scope)."""
    conn.execute(
        "COPY ("
        f"  SELECT CAST({reference_idx} AS BIGINT) AS reference_idx,"
        "    m.feature_idx, j.edge_num,"
        "    j.likelihood, j.like_weight_ratio,"
        "    j.distal_length, j.pendant_length"
        "  FROM read_jplace(?) j"
        "  INNER JOIN id_map m ON j.fragment = m.read_id"
        "  ORDER BY m.feature_idx, j.edge_num"
        f") TO '{out}' ({PARQUET_OPTS})",
        [str(jplace_path)],
    )
