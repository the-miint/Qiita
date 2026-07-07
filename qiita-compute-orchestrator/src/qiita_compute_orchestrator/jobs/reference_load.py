"""Native job: re-key hash_sequences' outputs to feature_idx, write the
six DuckLake-shape staging Parquets the data plane registers.

Reads the upstream Parquets (manifest from hash_sequences, feature_map
from mint-features) and emits the files `register-files` then hands to
the data plane's DoAction. The six staging outputs are:

  - `reference_sequences.parquet`        (feature_idx, sequence_hash, sequence_length_bp)
  - `reference_sequence_chunks/part_*.parquet` (feature_idx, chunk_index, chunk_data)
  - `reference_membership.parquet`       (reference_idx, feature_idx)
  - `reference_taxonomy.parquet`         (if taxonomy_path is set)
  - `reference_phylogeny.parquet`        (if tree_path is set)
  - `reference_placements.parquet`       (if jplace_path is set)

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
    PARQUET_OPTS_CHUNKED,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
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

# Per-batch chunk budget for `_write_reference_sequence_chunks`. The
# writer streams chunk_data straight to the Parquet writer (no sort — see
# that function), so per-batch memory is ~constant in file size no matter
# this value; the budget instead bounds each output part's SIZE. Each
# batch is a contiguous feature_idx range (bin-packed ascending), so a
# part's feature_idx min/max drives DuckLake FILE-level pruning — a
# smaller budget makes those ranges narrower (finer pruning) at the cost
# of more parts and more full-glob sequence_hash re-scans. Bin-packing by
# chunk-count (not feature-count) keeps part sizes even on GG2 backbone
# where feature sizes span 3+ orders of magnitude. Reduced from 50_000
# once the per-batch sort was removed: memory no longer caps it, so it is
# sized for pruning granularity.
_CHUNK_BUDGET_PER_BATCH = 10_000


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
    ]:
        if opt is not None and not opt.exists():
            raise FileNotFoundError(f"{label} not found: {opt}")

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

    sequences_out = validate_parquet_path(sequences_path)
    membership_out = validate_parquet_path(membership_path)
    taxonomy_out = validate_parquet_path(taxonomy_out_path)
    phylogeny_out = validate_parquet_path(phylogeny_out_path)
    placements_out = validate_parquet_path(placements_out_path)

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
            # _build_id_map needs it too. Without this each helper would
            # re-scan the file.
            conn.execute(
                "CREATE TEMP TABLE feature_map AS SELECT * FROM read_parquet(?)",
                [str(inputs.feature_map)],
            )

            # id_map: read_id → feature_idx (via sequence_hash). The
            # taxonomy / phylogeny / placements writes all key off
            # read_id, so this single JOIN is the bridge to feature_idx.
            # Counts also drive the unmapped-hash check below.
            _build_id_map(conn, inputs.manifest)

            _write_reference_sequences(conn, sequences_out)
            written.append(sequences_path)

            _write_reference_sequence_chunks(
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
                # The CLI's DoPut writes Newick / jplace as a chunked
                # `(chunk_index, chunk_data BLOB)` Parquet so the data
                # plane stays schema-agnostic and large blobs stream
                # under bounded memory. miint's `read_newick` /
                # `read_jplace` parse on-disk text/JSON files, so we
                # stitch chunks back into a temp file here.
                newick_path = _unwrap_chunks_to_temp_file(
                    conn,
                    parquet_path=inputs.tree_path,
                    out_path=duckdb_tmp / "tree.nwk",
                )
                _write_phylogeny(conn, newick_path, inputs.reference_idx, phylogeny_out)
                written.append(phylogeny_out_path)

            if inputs.jplace_path is not None:
                jplace_path = _unwrap_chunks_to_temp_file(
                    conn,
                    parquet_path=inputs.jplace_path,
                    out_path=duckdb_tmp / "placement.jplace",
                )
                _write_placements(conn, jplace_path, inputs.reference_idx, placements_out)
                written.append(placements_out_path)

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
            ):
                partial.unlink(missing_ok=True)
            shutil.rmtree(chunks_dir, ignore_errors=True)

    # `staging_dir` is the binding register-files consumes; the YAML's
    # `outputs: [staging_dir]` declaration matches it. The runner's
    # register-files convention picks up flat `*.parquet` files and
    # top-level subdirs of `part_*.parquet` (multi-file tables).
    #
    # `reference_sequence_chunks` re-exposes the feature-keyed chunks dir
    # (written by `_write_reference_sequence_chunks` above) as its own binding.
    # The hash_sequences step's same-named binding is sequence_hash-keyed
    # (pre-minting); this is the feature_idx-keyed re-key, which is what rype
    # needs. host-reference-add's build_rype_index step consumes it — and must
    # run BEFORE register-files, which MOVES these part files into permanent
    # DuckLake storage (data-plane `move_file`). reference-add declares only
    # `staging_dir` as an output, so this extra key is inert there (the runner
    # binds only a step's declared outputs).
    return {"staging_dir": workspace, "reference_sequence_chunks": chunks_dir}


def _unwrap_chunks_to_temp_file(
    conn: duckdb.DuckDBPyConnection,
    *,
    parquet_path: Path,
    out_path: Path,
) -> Path:
    """Stitch a chunked-BLOB upload Parquet back into a temp file.

    Upload shape: `(chunk_index INTEGER, chunk_data BLOB)`. Writes
    `chunk_data` to `out_path` in `chunk_index` order, fetching rows in
    batches so we never materialise the whole BLOB in memory — important
    for jplace inputs that can run into the GB range."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cursor = conn.execute(
        "SELECT chunk_data FROM read_parquet(?) ORDER BY chunk_index",
        [str(parquet_path)],
    )
    with out_path.open("wb") as f:
        while True:
            rows = cursor.fetchmany(1024)
            if not rows:
                break
            for (chunk_data,) in rows:
                if chunk_data is None:
                    raise ValueError(f"{parquet_path} contains a NULL chunk_data")
                f.write(bytes(chunk_data))
    if out_path.stat().st_size == 0:
        raise ValueError(f"{parquet_path} produced an empty file — upload was malformed")
    return out_path


def _build_id_map(
    conn: duckdb.DuckDBPyConnection,
    manifest_path: Path,
) -> None:
    """Join manifest + feature_map (TEMP TABLE pre-loaded by execute) on
    sequence_hash. Raises ValueError if any manifest row lacks a matching
    feature_map row — mint-features is supposed to mint a feature_idx for
    every distinct hash, so a gap means upstream produced inconsistent
    inputs (permanent error)."""
    manifest_count = conn.execute(
        "SELECT count(*) FROM read_parquet(?)",
        [str(manifest_path)],
    ).fetchone()[0]

    conn.execute(
        "CREATE TEMP TABLE id_map AS "
        "SELECT m.read_id, fm.feature_idx,"
        "  m.sequence_hash,"
        "  m.sequence_length_bp "
        "FROM read_parquet(?) m "
        "JOIN feature_map fm "
        "  ON m.sequence_hash = fm.sequence_hash",
        [str(manifest_path)],
    )

    id_map_count = conn.execute("SELECT count(*) FROM id_map").fetchone()[0]
    if id_map_count != manifest_count:
        n_unmapped = manifest_count - id_map_count
        unmapped = conn.execute(
            "SELECT m.sequence_hash FROM read_parquet(?) m "
            "ANTI JOIN id_map x ON m.sequence_hash = x.sequence_hash "
            "LIMIT 10",
            [str(manifest_path)],
        ).fetchall()
        hashes = [str(r[0]) for r in unmapped]
        raise ValueError(f"{n_unmapped} unmapped sequence hash(es) in feature_map: {hashes}")


def _write_reference_sequences(
    conn: duckdb.DuckDBPyConnection,
    out: str,
) -> None:
    """Emit DuckLake's `reference_sequences` shape — one row per unique
    feature_idx with `(feature_idx, sequence_hash, sequence_length_bp)`.
    Pulls everything from id_map (which already carries the per-read
    triple from the manifest × feature_map JOIN); reads sharing a
    canonical hash all carry the same length, so DISTINCT ON
    feature_idx collapses them deterministically."""
    conn.execute(
        "COPY ("
        "  SELECT DISTINCT ON (feature_idx)"
        "    feature_idx, sequence_hash, sequence_length_bp"
        "  FROM id_map"
        "  ORDER BY feature_idx"
        f") TO '{out}' ({PARQUET_OPTS})"
    )


def _write_reference_sequence_chunks(
    conn: duckdb.DuckDBPyConnection,
    reference_sequence_chunks_path: Path,
    out_dir: Path,
) -> None:
    """Re-key hash_sequences' chunks (hash-keyed) to DuckLake's
    `reference_sequence_chunks` schema (feature_idx-keyed), as a
    DIRECTORY of `part_*.parquet` files.

    `reference_sequence_chunks_path` (input) is a DIRECTORY of
    `part_*.parquet` files written by hash_sequences. Read via glob.

    `out_dir` (output) likewise becomes a directory of part files.
    The runner's register-files convention picks up this directory as
    a multi-file DuckLake table (table name = `reference_sequence_chunks`).

    **Never sort chunk_data — stream it.** This matches hash_sequences,
    which writes the same table shape upstream. A whole-file — or even a
    per-batch — `ORDER BY` over 64 KB `chunk_data` rows OOMs DuckDB's caps:
    the sort is a pipeline breaker that buffers the fat rows and can't
    spill them (`memory_limit` is a soft target it overshoots on
    wide-string sorts). An earlier version sorted each batch and OOM'd at
    GG2 scale even after batching, because the parallel sort's working set
    ballooned far past the ~3.2 GB batch input. So each part is a SINGLE
    streaming COPY: the narrow `fmb` CTE (this batch's feature_map subset)
    is the hash-join BUILD side and `chunk_data` rides the PROBE straight
    to the writer — never buffered into a build side or a sort. Peak
    memory is ~1 GB/thread, constant in file size.

    **Batched for pruning, not for memory.** Bin-pack features by chunk
    count into batches walking feature_idx ASCENDING, one `part_NNNNN`
    per batch. Because the batches are disjoint, contiguous feature_idx
    ranges, each part's feature_idx min/max drives DuckLake catalog-level
    FILE pruning (a `WHERE feature_idx IN (...)` DoGet reads only the parts
    whose range overlaps). There is deliberately NO within-part ordering:
    reassembly sorts `chunk_index` in memory per feature and DoGet filters
    by feature_idx, so on-disk order is not load-bearing — which is why
    `_CHUNK_BUDGET_PER_BATCH` is kept small, to keep the per-part ranges
    narrow (finer file pruning) rather than to bound a sort.

    **Memory safety of the re-key.** The `fmb` CTE pre-filters feature_map
    to the batch's hashes, so it (not the 30 GB glob) is the hash-join
    build side; the `WHERE rc.sequence_hash = ANY(?)` keeps the filter on
    the Parquet scan so late materialisation skips chunk_data for
    non-matching rows.

    **Cost tradeoff.** Each batch re-scans the input glob filtered by hash.
    Total chunk_data I/O is constant (each chunk matches exactly one
    batch); only the narrow sequence_hash column is re-read per batch, so
    more (smaller) batches cost little beyond extra part files."""
    parts_glob = str(reference_sequence_chunks_path / "part_*.parquet")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Metadata scan: (feature_idx, sequence_hash, n_chunks) ordered by
    # feature_idx. Only sequence_hash is read from the input Parquet —
    # columnar storage makes the count(*) cheap (~1-2 sec) even though
    # the input is ~30 GB total. JOIN with the small feature_map TEMP
    # TABLE attaches feature_idx; defensive against any hash without a
    # mint (every input hash should have one via _build_id_map's gap
    # check, but this keeps the count semantically correct).
    rows = conn.execute(
        "SELECT fm.feature_idx, rc.sequence_hash, count(*) AS n_chunks "
        "FROM read_parquet(?) rc "
        "JOIN feature_map fm ON rc.sequence_hash = fm.sequence_hash "
        "GROUP BY rc.sequence_hash, fm.feature_idx "
        "ORDER BY fm.feature_idx",
        [parts_glob],
    ).fetchall()

    # Each batch is a list of sequence_hash strings to filter on. Bin-pack
    # in feature_idx order so each output part is a disjoint, contiguous
    # feature_idx range — that range (its Parquet min/max) is what drives
    # DuckLake FILE-level pruning.
    batches: list[list[str]] = []
    current_batch: list[str] = []
    current_chunks = 0
    for _feature_idx, sequence_hash, n_chunks in rows:
        if current_batch and current_chunks + n_chunks > _CHUNK_BUDGET_PER_BATCH:
            batches.append(current_batch)
            current_batch = []
            current_chunks = 0
        current_batch.append(str(sequence_hash))
        current_chunks += n_chunks
    if current_batch:
        batches.append(current_batch)

    if batches:
        for i, batch_hashes in enumerate(batches):
            part_path = out_dir / f"part_{i:05d}.parquet"
            part_out = validate_parquet_path(part_path)
            # SINGLE streaming COPY, re-keyed hash → feature_idx, with NO
            # write-time ORDER BY — chunk_data must never go through a sort
            # (see the docstring; this is the hash_sequences pattern). The
            # `fmb` CTE (this batch's feature_map subset) is the narrow
            # hash-join BUILD side, so chunk_data rides the PROBE straight
            # to the writer, never buffered into a build side or a sort. The
            # `WHERE ... = ANY(...)` on the input column keeps the filter on
            # the Parquet scan so late materialisation skips chunk_data for
            # non-matching rows.
            conn.execute(
                "COPY ("
                "  WITH fmb AS ("
                "    SELECT feature_idx, sequence_hash"
                "    FROM feature_map"
                "    WHERE sequence_hash = ANY(CAST(? AS UUID[]))"
                "  )"
                "  SELECT fmb.feature_idx, rc.chunk_index, rc.chunk_data"
                "  FROM read_parquet(?) rc"
                "  JOIN fmb ON rc.sequence_hash = fmb.sequence_hash"
                "  WHERE rc.sequence_hash = ANY(CAST(? AS UUID[]))"
                f") TO '{part_out}' ({PARQUET_OPTS_CHUNKED})",
                [batch_hashes, parts_glob, batch_hashes],
            )
    else:
        # No minted features → emit one empty part so the directory is
        # non-empty and the runner's `dir.glob('*.parquet')` discovers
        # the multi-file table. register-files would otherwise error
        # on a zero-file directory.
        empty_part = out_dir / "part_00000.parquet"
        empty_out = validate_parquet_path(empty_part)
        conn.execute(
            "COPY ("
            "  SELECT"
            "    CAST(NULL AS BIGINT) AS feature_idx,"
            "    CAST(NULL AS INTEGER) AS chunk_index,"
            "    CAST(NULL AS VARCHAR) AS chunk_data"
            "  WHERE FALSE"
            f") TO '{empty_out}' ({PARQUET_OPTS_CHUNKED})"
        )


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
