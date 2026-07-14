"""Native job: hash + emit a chunked-by-feature reference sequence Parquet.

The CLI's DoPut writes an `upload.parquet` with shape
`(read_id VARCHAR, chunk_index INTEGER, chunk_data VARCHAR)` — sequences
are chunked at the client to keep per-row Parquet width bounded for
genome-scale inputs (single GG2 records run up to ~21 MB).

This step reads that chunked upload and produces:

  - `manifest.parquet` — `(read_id, sequence_hash, sequence_length_bp)`
    One row per upload read.
  - `reference_sequence_chunks.parquet` — `(sequence_hash, chunk_index,
    chunk_data)`. Same 64 KB chunks as the upload, relabeled from
    read_id to canonical sequence_hash. When multiple reads collapse to
    the same canonical hash (a read + its reverse complement), only one
    read's chunks survive — the lex-smallest read_id, deterministically.
  - `annotation_manifest.parquet` — ONLY when an optional `gff_path` is
    supplied. One row per annotated interval, carrying the canonical hash
    of the sub-sequence that interval cuts out of its parent, so the
    interval can be minted a `feature_idx` of its own. See
    `_write_annotation_manifest` for the shape and the coordinate
    conversion.

**Why annotations are hashed HERE.** An annotated interval (a SynDNA insert
on its plasmid, a gene on a chromosome) is quantified as a feature in its own
right, so it needs a `feature_idx` — and `feature_idx` is minted from a
canonical sequence hash. This step is the one place that already holds both
the assembled parent sequences and the hashing machinery, so cutting the
interval out and hashing it is the same job it already does, not a new one.
The extracted bytes are deliberately NOT stored (no `reference_sequences` /
`reference_sequence_chunks` row): they are recoverable from the parent plus
the interval, and a second copy could drift from the first.

**Why the annotation output is unconditional rather than `when:`-gated.** A
step's declared `outputs:` are bound unconditionally, so an output that is
sometimes absent raises a KeyError rather than simply not binding — the
annotation manifest is therefore emitted on every run, zero rows and all.

A `when:` gate *was* available (`when:` is default-ON, but the runner's
`bound.setdefault(...)` idiom supplies a default-OFF anchor, exactly as the
router-build gate does), so this is a choice, not a workaround. It is made for
two reasons: the downstream entries stay unconditional, so there is no
conditional register path to reason about; and nothing extra is scheduled
either way — `hash_sequences` and `load` already run, and
`mint-annotation-features` is an in-process control-plane action, not a SLURM
job. The zero-row *file* costs nothing because the two consumers both short out
on an empty manifest: `mint_annotation_features` early-returns on a footer-only
`count(*)`, and `reference_load` writes no `reference_annotation.parquet` at all
(so a no-GFF reference adds no lake file and no DuckLake snapshot).

**Canonical hashing.** A sequence and its reverse complement describe
the same molecular entity. We compute md5 on BOTH strands and store the
lex-smaller as the canonical `sequence_hash`:

    sequence_hash = LEAST(md5(upper(seq)),
                          md5(sequence_dna_reverse_complement(upper(seq))))::uuid

The stored chunk bytes are NEVER transformed — `chunk_data` is exactly
what the client uploaded. Two strand orientations of the same molecule
get one canonical hash but only one set of chunks survives (the one
whose read_id won the DISTINCT ON). Stored as DuckDB UUID (16 bytes) to
match the wire-side `sequence_hash` and `feature_idx` types — no
VARCHAR md5 hexstring is written anywhere (per the project's
hash-storage rule).

The reverse complement comes from miint's scalar
`sequence_dna_reverse_complement`, which honors full IUPAC ambiguity
codes (A↔T, C↔G, R↔Y, S↔S, W↔W, K↔M, B↔V, D↔H, N↔N) and preserves
non-base characters (e.g. gaps). We `upper()` the input first so case
variation in the upload (`atcg` vs `ATCG`) doesn't desync the hash.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.chunking import canonical_sequence_hash_expr, reassemble_chunks_expr
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    PARQUET_OPTS_CHUNKED,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ._blob_input import resolve_blob_input
from ._feature_load import bin_pack_by_chunks

YAML_STEP_NAME = "hash_sequences"

# DuckDB resource caps for this step. With the read_id-batched
# pipeline below, peak memory per batch ≈ batch_size × avg record
# size (~50K × ~30 KB ≈ 1.5 GB, ~10 GB worst case if a batch lands
# many of GG2's ~21 MB genome tail). `_DUCKDB_MEMORY_GB` is now only the
# OFF-SLURM fallback (local backend / tests); under SLURM the limit tracks
# the real cgroup via `resolve_duckdb_memory_gb()` (SLURM_MEM_PER_NODE), so a
# `--mem-gb` override reaches DuckDB. The 24 GB literal is now only the
# off-SLURM fallback and is intentionally decoupled from the YAML allocation
# (currently mem_gb=32) — under SLURM DuckDB is capped at the actual cgroup, so
# the literal need not equal it. DuckDB owns the whole box here (no in-process
# co-consumer), so it gets the allocation minus headroom.
_DUCKDB_MEMORY_GB = 24
_DUCKDB_THREADS = 8


class Inputs(BaseModel):
    """Typed input contract for hash_sequences.

    `fasta_path` is the workflow-declared input — the runner resolves
    `fasta_upload_idx` → staging path (compute_upload_staging_path on
    the resolved upload row) and injects under this name. The field is
    role-named (matching the fastq_to_parquet `fastq_path` convention)
    rather than the upload-domain-generic `upload_path` because the
    YAML's `inputs:` list IS the kwarg-name for `Inputs.model_validate`;
    the runner's `{prefix}_upload_idx → {prefix}_path` convention
    requires the role name to live on the model.

    `gff_path` is optional and follows the same `*_upload_idx → *_path`
    convention on the remote path; on the local path the runner passes the
    raw absolute path straight through. `resolve_blob_input` accepts either
    shape. Absent → no `annotation_manifest` output.

    `reference_idx` and `work_ticket_idx` are framework-injected scope
    scalars merged by `flatten_native_inputs`. Both are accepted (typed)
    even though this step doesn't consume them — declaring them on the
    Inputs model keeps the contract explicit and matches the
    fastq_to_parquet convention; without the declaration Pydantic
    silently drops them on `model_validate`, which would hide a mis-wired
    scope dispatch.
    """

    fasta_path: Path
    gff_path: Path | None = None
    reference_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Read the chunked upload Parquet; emit manifest + chunks.

    Upload shape: `(read_id, chunk_index, chunk_data)`. Reconstruct each
    read via `string_agg(... ORDER BY chunk_index)`, compute canonical
    hash on both strands, then relabel the upload chunks by sequence_hash
    via JOIN. See module docstring for the canonical-hash semantics."""
    if not inputs.fasta_path.exists():
        raise FileNotFoundError(f"upload parquet not found: {inputs.fasta_path}")

    workspace.mkdir(parents=True, exist_ok=True)
    manifest_path = workspace / "manifest.parquet"
    annotation_manifest_path = workspace / "annotation_manifest.parquet"
    # reference_sequence_chunks is a DIRECTORY of part_*.parquet files
    # rather than a single file — the consumer contract is
    # `read_parquet(dir/part_*.parquet)`. The relabel below writes one
    # part in a single streaming scan; the directory shape is retained so
    # the output can be split into multiple parts later without touching
    # any consumer.
    reference_sequence_chunks_dir = workspace / "reference_sequence_chunks"
    reference_sequence_chunks_dir.mkdir(parents=True, exist_ok=True)
    manifest_out = validate_parquet_path(manifest_path)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )

            # Chunk-budget-batched reconstruction. A single HASH_AGG over
            # the full chunked Parquet buffers every in-flight group's
            # chunks concurrently — for GG2 backbone (~331K reads, max
            # ~21 MB per record, ~40 GB uncompressed sequence data) that
            # exceeds any reasonable DuckDB cap. We batch by chunk-count
            # budget: Python first collects (read_id, n_chunks) for every
            # read (cheap count(*) aggregate, ~10 MB transfer), bin-packs
            # reads into batches each totalling ≤ CHUNK_BUDGET_PER_BATCH
            # chunks, and each batch's string_agg + native md5 runs
            # inside DuckDB. DuckDB still does the heavy lifting (native
            # md5, parallel string_agg); Python only carries the small
            # read-list metadata. Per-batch HASH_AGG state is bounded by
            # CHUNK_BUDGET_PER_BATCH × chunk_size (~3.2 GB).
            #
            # The full Parquet is re-scanned per batch (DuckDB can't
            # prune row-groups since the upload Parquet is single-RG by
            # construction in the data plane writer); the scan dominates
            # per-batch wall time, so the number of batches matters more
            # than the per-batch size. GG2 backbone bin-packs to ~20
            # batches.
            chunks_per_read = conn.execute(
                "SELECT read_id, count(*) AS n_chunks "
                "FROM read_parquet(?) "
                "GROUP BY read_id "
                "ORDER BY read_id",
                [str(inputs.fasta_path)],
            ).fetchall()

            batches = bin_pack_by_chunks(chunks_per_read)

            conn.execute(
                "CREATE TEMP TABLE hashed ("
                "  read_id VARCHAR, "
                "  sequence_hash UUID, "
                "  sequence_length_bp BIGINT"
                ")"
            )

            for batch in batches:
                # `c.read_id = ANY(?)` lets DuckDB take the batch list
                # as a single LIST<VARCHAR> parameter and apply it as a
                # filter during the Parquet scan — no temp table, no
                # per-row INSERT round-trip.
                #
                # Canonical hash = LEAST(forward_md5, reverse_md5). Bytes
                # stay as uploaded; canonical identity lives in the hash.
                conn.execute(
                    "INSERT INTO hashed "
                    "WITH per_read AS ("
                    "  SELECT "
                    "    c.read_id, "
                    f"    {reassemble_chunks_expr('c.')} AS sequence "
                    "  FROM read_parquet(?) c "
                    "  WHERE c.read_id = ANY(?) "
                    "  GROUP BY c.read_id"
                    ") "
                    "SELECT "
                    "  read_id, "
                    # Canonical hash single-sourced in qiita_common.chunking so
                    # assembly ingest derives the identical feature_idx for
                    # identical bytes (shared qiita.feature).
                    f"  {canonical_sequence_hash_expr('sequence')} AS sequence_hash, "
                    "  CAST(length(sequence) AS BIGINT) AS sequence_length_bp "
                    "FROM per_read",
                    [str(inputs.fasta_path), batch],
                )

            # manifest.parquet — one row per upload read.
            conn.execute(
                "COPY ("
                "  SELECT read_id, sequence_hash, sequence_length_bp"
                "  FROM hashed"
                f") TO '{manifest_out}' ({PARQUET_OPTS})"
            )

            # reference_sequence_chunks/part_00000.parquet — relabel the
            # upload chunks from read_id to canonical sequence_hash in a
            # SINGLE streaming scan. When two reads share a canonical hash
            # (a sequence + its reverse complement) we keep ONE — the
            # lex-smaller read_id, deterministically (DISTINCT ON).
            #
            # No write-time ORDER BY. The sequence_hash sort is not
            # load-bearing for any consumer: reference_load re-keys
            # sequence_hash → feature_idx with its own full scan, the data
            # plane's DoGet filters by feature_idx (never sequence_hash),
            # and sequence reassembly sorts chunk_index in-memory per
            # feature. A whole-file ORDER BY (or PARTITION_BY) over 30+ GB
            # of 64 KB chunk_data OOMs DuckDB's caps — its sort can't spill
            # rows that fat, and the partitioned writer either OOMs or
            # shatters the output into tens of thousands of tiny files.
            #
            # The streaming relabel is bounded BY CONSTRUCTION: `canonical`
            # has one narrow row (read_id + uuid) per distinct hash, and
            # because chunks ≥ reads ≥ canonical it is ALWAYS the
            # lower-cardinality join input, so the optimizer builds the
            # hash table on it and chunk_data rides the probe side straight
            # to the writer — never buffered into a build side or a sort.
            # Peak memory is ~1 GB/thread, constant in file size. This
            # replaces the old per-batch loop, which re-scanned the whole
            # upload once per batch AND left the full-`hashed` JOIN free to
            # reorder ahead of the batch filter — at genome scale that
            # materialized the entire file's chunk_data and OOM'd.
            #
            # The `canonical` CTE's ORDER BY is on the narrow hashed table
            # only (no chunk_data) — it is what makes DISTINCT ON pick the
            # lex-smallest read_id deterministically, and it spills cheaply.
            # Empty input writes a valid 0-row part (schema from the
            # projection), keeping the directory non-empty for consumers.
            part_out = validate_parquet_path(reference_sequence_chunks_dir / "part_00000.parquet")
            conn.execute(
                "COPY ("
                "  WITH canonical AS ("
                "    SELECT DISTINCT ON (sequence_hash) read_id, sequence_hash"
                "    FROM hashed"
                "    ORDER BY sequence_hash, read_id"
                "  )"
                "  SELECT cr.sequence_hash, c.chunk_index, c.chunk_data"
                "  FROM read_parquet(?) c"
                "  JOIN canonical cr ON c.read_id = cr.read_id"
                f") TO '{part_out}' ({PARQUET_OPTS_CHUNKED})",
                [str(inputs.fasta_path)],
            )

            gff_file = (
                resolve_blob_input(
                    conn, path=inputs.gff_path, out_path=duckdb_tmp / "annotations.gff3"
                )
                if inputs.gff_path is not None
                else None
            )
            _write_annotation_manifest(
                conn,
                gff_path=gff_file,
                fasta_path=inputs.fasta_path,
                chunks_per_read=chunks_per_read,
                tmp_dir=duckdb_tmp,
                out=validate_parquet_path(annotation_manifest_path),
            )

            conn.execute("DROP TABLE hashed")
        success = True
    finally:
        # On any failure path (interrupted COPY, DuckDB OOM, ...) remove
        # partial Parquets so the launcher's manifest walker doesn't
        # promote a half-written result as the step's output. Best-effort:
        # a hard SIGKILL leaves them behind, but the runner allocates a
        # fresh attempt-N+1 workspace on retry so it doesn't cascade.
        if not success:
            manifest_path.unlink(missing_ok=True)
            annotation_manifest_path.unlink(missing_ok=True)
            shutil.rmtree(reference_sequence_chunks_dir, ignore_errors=True)

    return {
        "manifest": manifest_path,
        "reference_sequence_chunks": reference_sequence_chunks_dir,
        # ALWAYS bound — zero rows when no GFF was supplied. A step's declared
        # outputs are bound unconditionally, so a sometimes-present output is a
        # KeyError, not an absent binding. Emitting a zero-row file rather than
        # gating the downstream entries on a `when:` boolean is deliberate — see
        # the module docstring.
        "annotation_manifest": annotation_manifest_path,
    }


def _write_annotation_manifest(
    conn: duckdb.DuckDBPyConnection,
    *,
    gff_path: Path | None,
    fasta_path: Path,
    chunks_per_read: list[tuple[str, int]],
    tmp_dir: Path,
    out: str,
) -> None:
    """Parse a GFF3 into the annotation manifest: one row per annotated interval,
    carrying the canonical hash of the EXTRACTED sub-sequence so
    `mint-annotation-features` can mint it a feature_idx of its own.

    Emitted shape (`sequence_hash` is the only column minting reads; the rest ride
    along for `reference_load`):

        annotation_id       VARCHAR  -- the interval's identity within the reference
        sequence_hash       UUID     -- canonical hash of the extracted interval
        sequence_length_bp  BIGINT   -- interval length
        parent_read_id      VARCHAR  -- GFF seqid == the parent FASTA read_id
        annotation_type     VARCHAR  -- GFF3 column 3
        strand              VARCHAR
        position            BIGINT   -- 1-based INCLUSIVE  (unchanged from GFF)
        stop_position       BIGINT   -- 1-based EXCLUSIVE  (GFF stop + 1)
        attributes          MAP(VARCHAR, VARCHAR)

    **The closed → half-open conversion happens HERE and nowhere else.** `read_gff`
    emits GFF3's 1-based CLOSED `[start, end]`; every alignment-side consumer
    (`alignment_slice`, `read_alignments`, `qiita_lake.alignment`) speaks 1-based
    HALF-OPEN `[start, stop)`. Both call the column `stop_position`, so nothing
    type-checks the difference and nothing raises — the only symptom of getting it
    wrong is that the interval's last base silently stops being counted. Converting
    once, at ingest, means no downstream consumer ever has to remember.

    Extraction is strand-agnostic ON PURPOSE. A `-` strand annotation is NOT
    reverse-complemented before hashing, because `canonical_sequence_hash_expr`
    already hashes both strands and keeps the lex-smaller — so a feature and its
    reverse complement mint the SAME feature_idx. Revcomping first would be a no-op.

    `gff_path=None` (the no-GFF reference, which is most of them) is handled by
    pointing `read_gff` at a HEADER-ONLY GFF3. That is a real, zero-row GFF source,
    so the empty file is produced by the SAME projection as the populated one and
    cannot acquire a divergent schema — the alternative, a hand-declared empty
    table, is a second schema declaration that drifts the moment `read_gff`'s column
    types change.
    """
    if gff_path is None:
        tmp_dir.mkdir(parents=True, exist_ok=True)
        gff_path = tmp_dir / "_no_annotations.gff3"
        gff_path.write_text("##gff-version 3\n")

    # read_gff's `attributes` is already a MAP(VARCHAR,VARCHAR) — no parsing.
    #
    # The projection is split across two levels deliberately. `annotation_id` is
    # derived from the GFF's own CLOSED coordinates (so an ID-less interval gets the
    # label a human reading the GFF3 file would expect, `seqid:2001-3000`), while
    # the STORED window is half-open. Computing both in one SELECT would leave the
    # fallback silently reading whichever `stop_position` the binder resolved —
    # base column or alias. Naming the raw columns in an inner query removes the
    # question entirely.
    conn.execute(
        "CREATE TEMP TABLE annotation AS "
        "SELECT "
        "  coalesce("
        "    g.attributes['ID'],"
        "    g.seqid || ':' || g.gff_start || '-' || g.gff_stop_closed"
        "  ) AS annotation_id, "
        "  g.seqid AS parent_read_id, "
        "  g.annotation_type, "
        "  g.strand, "
        "  CAST(g.gff_start AS BIGINT) AS position, "
        # THE conversion. GFF3's stop is INCLUSIVE; we store EXCLUSIVE.
        "  CAST(g.gff_stop_closed AS BIGINT) + 1 AS stop_position, "
        "  g.attributes "
        "FROM ("
        "  SELECT seqid, type AS annotation_type, strand, attributes,"
        "         position AS gff_start, stop_position AS gff_stop_closed"
        "  FROM read_gff(?)"
        ") g",
        [str(gff_path)],
    )

    dupes = conn.execute(
        "SELECT annotation_id, count(*) AS n FROM annotation "
        "GROUP BY annotation_id HAVING count(*) > 1 ORDER BY annotation_id LIMIT 5"
    ).fetchall()
    if dupes:
        raise ValueError(
            f"{gff_path} has duplicate annotation IDs (a feature table keys on them): "
            + ", ".join(f"{a!r} x{c}" for a, c in dupes)
        )

    # A seqid must name a sequence we actually hashed, or the annotation has no
    # parent to be an interval OF.
    orphans = conn.execute(
        "SELECT DISTINCT a.parent_read_id FROM annotation a "
        "LEFT JOIN hashed h ON h.read_id = a.parent_read_id "
        "WHERE h.read_id IS NULL ORDER BY 1 LIMIT 5"
    ).fetchall()
    if orphans:
        raise ValueError(
            f"{gff_path} references seqid(s) absent from the FASTA: "
            + ", ".join(repr(o[0]) for o in orphans)
        )

    # The interval must lie inside its parent. `stop_position` is exclusive here, so
    # the legal range is 1 <= position < stop_position <= length + 1.
    bad = conn.execute(
        "SELECT a.annotation_id, a.parent_read_id, a.position, a.stop_position, "
        "       h.sequence_length_bp "
        "FROM annotation a JOIN hashed h ON h.read_id = a.parent_read_id "
        "WHERE a.position < 1 "
        "   OR a.stop_position <= a.position "
        "   OR a.stop_position > h.sequence_length_bp + 1 "
        "ORDER BY a.annotation_id LIMIT 5"
    ).fetchall()
    if bad:
        detail = ", ".join(
            f"{aid!r} on {parent!r} [{pos}, {stop}) vs parent length {plen}"
            for aid, parent, pos, stop, plen in bad
        )
        raise ValueError(f"{gff_path} has interval(s) outside their parent sequence: {detail}")

    # An interval spanning its ENTIRE parent is not a sub-interval — the extracted
    # bytes ARE the parent's bytes, so it canonically hashes to the PARENT's
    # feature_idx. The resulting row would have feature_idx == parent_feature_idx,
    # pointing at a feature that IS in reference_membership and IS indexed — quietly
    # falsifying the invariant the whole annotation design rests on, with nothing
    # raised. NCBI-style GFF3s ship exactly such lines (`region` / `source` covering
    # 1..len), so this is the common case, not a corner one: reject it and tell the
    # caller to drop those rows.
    whole = conn.execute(
        "SELECT a.annotation_id, a.parent_read_id, h.sequence_length_bp "
        "FROM annotation a JOIN hashed h ON h.read_id = a.parent_read_id "
        "WHERE a.position = 1 AND a.stop_position = h.sequence_length_bp + 1 "
        "ORDER BY a.annotation_id LIMIT 5"
    ).fetchall()
    if whole:
        detail = ", ".join(f"{aid!r} spans all {plen} bp of {p!r}" for aid, p, plen in whole)
        raise ValueError(
            f"{gff_path} has interval(s) spanning their ENTIRE parent sequence: {detail}. "
            "Such an interval hashes to its parent's own feature_idx, so it is not a "
            "distinct annotated feature. Drop whole-sequence rows (GFF3 `region` / "
            "`source` lines) before ingest."
        )

    # Reassemble only the ANNOTATED parents, batched by the same chunk budget the
    # main hashing pass uses. A plasmid map annotates a handful of sequences, but the
    # `--gff` contract also advertises a genome's gene coordinates, where the parents
    # are chromosomes — one unbatched string_agg over those is exactly the HASH_AGG
    # blow-up `CHUNK_BUDGET_PER_BATCH` exists to prevent.
    annotated_parents = {
        r[0] for r in conn.execute("SELECT DISTINCT parent_read_id FROM annotation").fetchall()
    }
    conn.execute("CREATE TEMP TABLE parent_sequence (read_id VARCHAR, sequence VARCHAR)")
    for batch in bin_pack_by_chunks(
        [(rid, n) for rid, n in chunks_per_read if rid in annotated_parents]
    ):
        conn.execute(
            "INSERT INTO parent_sequence "
            "SELECT c.read_id, "
            f"       {reassemble_chunks_expr('c.')} AS sequence "
            "FROM read_parquet(?) c "
            "WHERE c.read_id = ANY(?) "
            "GROUP BY c.read_id",
            [str(fasta_path), batch],
        )

    # Cut each interval out of its parent and hash it ONCE, here — both the COPY and
    # any future consumer read the stored column rather than re-evaluating the
    # expression (which is 2 md5 + a reverse-complement per row).
    #
    # NOTE: two annotations with IDENTICAL bases legitimately collapse to ONE
    # sequence_hash, hence one feature_idx. That is not an error and must not be
    # rejected: a bacterial genome carries the 16S rRNA gene in 5-7 byte-identical
    # copies, so refusing it would make `--gff` unusable on essentially every real
    # bacterial genome — while working fine on the SynDNA plasmids that motivated
    # this code. A feature is a SEQUENCE; an annotation is an OCCURRENCE of that
    # sequence at a place. The occurrences stay distinct because `annotation_id`,
    # not `feature_idx`, is the annotation's identity (see the reference_annotation
    # migration); a consumer aggregating coverage over the feature sums across them.
    cut = "substr(p.sequence, a.position, a.stop_position - a.position)"
    conn.execute(
        "CREATE TEMP TABLE annotation_extracted AS "
        f"SELECT a.*, {cut} AS sequence, "
        f"       {canonical_sequence_hash_expr(cut)} AS sequence_hash "
        "FROM annotation a JOIN parent_sequence p ON p.read_id = a.parent_read_id"
    )

    conn.execute(
        "COPY ("
        "  SELECT annotation_id, "
        "         sequence_hash, "
        "         CAST(length(sequence) AS BIGINT) AS sequence_length_bp, "
        "         parent_read_id, annotation_type, strand, position, stop_position, attributes "
        "  FROM annotation_extracted "
        # Genomic order — this file is small (one row per annotated interval) and no
        # consumer keys on the order; it is for the human who opens it.
        "  ORDER BY parent_read_id, position, annotation_id"
        f") TO '{out}' ({PARQUET_OPTS})"
    )
    conn.execute("DROP TABLE annotation_extracted")
    conn.execute("DROP TABLE parent_sequence")
    conn.execute("DROP TABLE annotation")
