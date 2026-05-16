"""Local compute backend — runs DuckDB+miint in-process for dev/test."""

from pathlib import Path

import duckdb
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import ScopeTargetKind, WorkTicketFailureStage
from qiita_common.parquet import validate_parquet_path

from ..backend import ComputeBackend
from ..jobs import flatten_native_inputs, run_native_job
from ..miint import PARQUET_OPTS, ensure_miint_installed, open_conn


class LocalBackend(ComputeBackend):
    """Runs compute jobs in-process using DuckDB+miint. For dev/test only.

    `run_step` dispatches on the step name to an internal Python
    implementation. The SLURM backend is the production analogue; it
    submits the step's container instead and the container does the
    work. The set of step names this backend handles is the union of
    container behaviours every workflow needs in dev/test mode.

    **Canonical-implementation contract:** the per-step helpers below
    (`_run_hash`, `_run_load`, plus the module-level `_write_*` builders)
    are the source of truth for what each step does. When `SlurmBackend`
    is wired, the corresponding container's entrypoint will execute the
    same DuckDB+miint logic — either by importing this module and
    invoking the helper with paths read from `params.json`, or by
    extracting the SQL into a shared `qiita_compute_orchestrator.jobs`
    module that both backends consume. Either way, `LocalBackend` and
    the SLURM container must not drift apart. See docs/architecture.md
    "Backend code-sharing" for the design intent.
    """

    async def run_step(
        self,
        name: str,
        inputs: dict[str, Path],
        workspace: Path,
        *,
        scope_target: dict,
        work_ticket_idx: int,  # noqa: ARG002 — accepted for protocol parity
        container: str | None = None,  # inspected by the runtime guard below; otherwise ignored
        module: str | None = None,  # inspected by the native-dispatch guard below
        entrypoint: str | None = None,  # noqa: ARG002 — LocalBackend ignores entrypoint
        baseline_resources=None,  # noqa: ARG002 — accepted for protocol parity
    ) -> dict[str, Path]:
        """Public backend interface. Translates known internal exceptions
        into typed `BackendFailure` so the runner can classify retriable
        vs permanent. The internal helpers (`_run_hash`, `_run_load`,
        `_write_*`) keep raising plain Python exceptions — they're
        unit-testable in isolation that way; only the run_step boundary
        wraps."""
        if (container is None) == (module is None):
            # Symmetric with SlurmBackend's guard: both None (neither
            # runtime declared) and both set (ambiguous runtime) are
            # contract violations. The wire validator on StepRunRequest
            # catches this upstream; this guard protects direct callers
            # (tests, programmatic submission) so silently preferring
            # one runtime over the other can't happen.
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=name,
                reason="LocalBackend requires exactly one of `container` or `module` on the step",
            )
        if module is not None:
            # Native step: delegate to the framework dispatcher. It
            # validates the module prefix, imports the module,
            # validates raw_inputs via mod.Inputs, invokes
            # mod.execute(inputs, workspace), and maps known exceptions
            # to typed BackendFailure. `flatten_native_inputs` merges
            # the scope-target idx scalars and rejects reserved-key
            # collisions the same way the SLURM launcher does — a job
            # module sees identical raw_inputs regardless of runtime.
            # `step_name=name` plumbs the YAML step name (e.g. "fastq")
            # to all BackendFailures so they match the work_ticket
            # `failure_step_name` contract.
            raw_inputs = flatten_native_inputs(
                {k: str(v) for k, v in inputs.items()},
                step_name=name,
                scope_target=scope_target,
                work_ticket_idx=work_ticket_idx,
            )
            return await run_native_job(module, raw_inputs, workspace, step_name=name)
        # Container path: hash and load both need a reference_idx. The
        # workflow YAML for reference-add is reference-scoped, so this
        # branch only runs under that scope today. Refuse anything else
        # with a typed contract violation instead of silently picking up
        # the wrong scalar.
        if scope_target.get("kind") != ScopeTargetKind.REFERENCE.value:
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=name,
                reason=(
                    f"container step {name!r} requires a reference-scoped ticket; "
                    f"got scope_target.kind={scope_target.get('kind')!r}"
                ),
            )
        reference_idx = scope_target["reference_idx"]
        try:
            if name == "hash":
                manifest = await self._run_hash(inputs["fasta_path"], workspace, reference_idx)
                return {"manifest": manifest}
            if name == "load":
                # Sub-dir so load's reference_*.parquet outputs don't mingle
                # with earlier-step artifacts (manifest, feature_map) in the
                # workspace — register-files globs whatever lives in the dir
                # it's pointed at, so isolation matters.
                staging_dir = await self._run_load(
                    manifest_path=inputs["manifest"],
                    fasta_path=inputs["fasta_path"],
                    feature_map_path=inputs["feature_map"],
                    output_dir=workspace / "staging",
                    reference_idx=reference_idx,
                    taxonomy_path=inputs.get("taxonomy_path"),
                    tree_path=inputs.get("tree_path"),
                    jplace_path=inputs.get("jplace_path"),
                )
                return {"staging_dir": staging_dir}
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=name,
                reason=f"LocalBackend does not implement step {name!r}",
            )
        except BackendFailure:
            # Already classified — propagate without rewrapping.
            raise
        except KeyError as exc:
            # The runner declared an input the YAML expected, but the
            # binding map is missing it. Action-runner contract bug; bad
            # YAML or stale runner. Retry won't help.
            raise BackendFailure(
                kind=FailureKind.CONTRACT_VIOLATION,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=name,
                reason=f"missing required input binding: {exc!s}",
            ) from exc
        except FileNotFoundError as exc:
            raise BackendFailure(
                kind=FailureKind.BAD_INPUT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=name,
                reason=str(exc),
            ) from exc
        except ValueError as exc:
            # FASTA dup-read_id, taxonomy-malformed, unmapped sequence
            # hashes — all data-quality issues. Permanent because the
            # same input always fails the same way.
            raise BackendFailure(
                kind=FailureKind.BAD_INPUT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=name,
                reason=str(exc),
            ) from exc
        except duckdb.Error as exc:
            # Catch-all for SQL-level failures (schema mismatch, missing
            # table, type errors). Likely permanent (workflow code or
            # input shape is wrong); a retry would do the same thing.
            raise BackendFailure(
                kind=FailureKind.UNKNOWN_PERMANENT,
                stage=WorkTicketFailureStage.STEP_RUN,
                step_name=name,
                reason=f"duckdb error: {exc!s}",
            ) from exc

    async def _run_hash(self, fasta_path: Path, output_dir: Path, reference_idx: int) -> Path:
        if not fasta_path.exists():
            raise FileNotFoundError(f"FASTA file not found: {fasta_path}")

        output_dir.mkdir(parents=True, exist_ok=True)
        await ensure_miint_installed()
        manifest_path = output_dir / "manifest.parquet"
        out = validate_parquet_path(manifest_path)

        with open_conn() as conn:
            conn.execute("LOAD miint;")

            # Materialize FASTA into a temp table — single read of the file.
            # DuckDB raises on empty files — treat as zero sequences.
            try:
                conn.execute(
                    "CREATE TEMP TABLE raw_seqs AS "
                    "SELECT read_id, md5(sequence1) AS hash, length(sequence1) AS len "
                    "FROM read_fastx(?)",
                    [str(fasta_path)],
                )
            except duckdb.Error as exc:
                if "Empty file" in str(exc):
                    conn.execute(
                        "CREATE TEMP TABLE raw_seqs (read_id VARCHAR, hash VARCHAR, len BIGINT)"
                    )
                else:
                    raise

            # Reject duplicate read_ids — pure DuckDB, no second FASTA scan.
            dup_result = conn.execute(
                "SELECT read_id FROM raw_seqs GROUP BY read_id HAVING count(*) > 1 LIMIT 10"
            ).fetchall()
            if dup_result:
                dup_ids = sorted(row[0] for row in dup_result)
                raise ValueError(f"FASTA contains duplicate read_id(s): {dup_ids}")

            # Write manifest as Parquet — columns (read_id, sequence_hash, length).
            # The hash column is converted to UUID on the way out so downstream
            # consumers (mint-features, load step) read it as UUID natively.
            # Sorted by sequence_hash because every downstream consumer keys
            # off it: mint-features dedups on it; the load step JOINs on it.
            # reference_idx isn't embedded in the file; the caller's
            # work_ticket.scope_target.reference_idx is the source of truth.
            conn.execute(
                "COPY ("
                "  SELECT read_id,"
                "    CAST(hash AS UUID) AS sequence_hash,"
                "    len AS length"
                "  FROM raw_seqs"
                "  ORDER BY sequence_hash"
                f") TO '{out}' (FORMAT PARQUET, PARQUET_VERSION 'v2', COMPRESSION 'zstd')"
            )
            conn.execute("DROP TABLE raw_seqs")

        return manifest_path

    async def _run_load(
        self,
        *,
        manifest_path: Path,
        fasta_path: Path,
        feature_map_path: Path,
        output_dir: Path,
        reference_idx: int,
        taxonomy_path: Path | None = None,
        tree_path: Path | None = None,
        jplace_path: Path | None = None,
    ) -> Path:
        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_path}")
        if not fasta_path.exists():
            raise FileNotFoundError(f"FASTA file not found: {fasta_path}")
        if not feature_map_path.exists():
            raise FileNotFoundError(f"Feature map not found: {feature_map_path}")
        if jplace_path is not None and not jplace_path.exists():
            raise FileNotFoundError(f"jplace file not found: {jplace_path}")

        output_dir.mkdir(parents=True, exist_ok=True)
        await ensure_miint_installed()

        with open_conn() as conn:
            conn.execute("LOAD miint;")
            conn.execute("SET preserve_insertion_order=false;")
            conn.execute(f"SET temp_directory='{output_dir}/.duckdb_tmp';")

            _build_id_map(conn, manifest_path, feature_map_path)
            _write_sequence_metadata(conn, output_dir)
            _write_sequence_chunks(conn, fasta_path, output_dir)
            _write_membership(conn, output_dir, reference_idx)

            if taxonomy_path is not None:
                _write_taxonomy(conn, taxonomy_path, output_dir, reference_idx)

            if tree_path is not None:
                _write_phylogeny(conn, tree_path, output_dir, reference_idx)

            if jplace_path is not None:
                _write_placements(conn, jplace_path, output_dir, reference_idx)

            conn.execute("DROP TABLE id_map")

        return output_dir


# 16384 rows × ~64 KB chunk_data ≈ 1 GB per row group. Smaller values flush
# more frequently, preventing OOM on genome-heavy references. Empirically
# tuned against GG2 backbone (21 MB max genome, 11 GB FASTA):
#   16384 → 4.2 GB peak RSS (OK), 32768 → OOM on 30 GB machine.
_CHUNK_ROW_GROUP_SIZE = 16384
_PARQUET_OPTS_CHUNKED = f"{PARQUET_OPTS}, ROW_GROUP_SIZE {_CHUNK_ROW_GROUP_SIZE}"
_CHUNK_SIZE = 65536  # 64 KB


def _build_id_map(
    conn: duckdb.DuckDBPyConnection, manifest_path: Path, feature_map_path: Path
) -> None:
    """Build the id_map temp table by joining manifest + feature_map in DuckDB.

    Both files are read directly by DuckDB — no Python-side parsing.
    - manifest_path: Parquet with columns (read_id, sequence_hash, length).
    - feature_map_path: Parquet with columns (sequence_hash, feature_idx).

    Raises ValueError if any manifest entry has no matching feature_idx.
    """
    manifest_count = conn.execute(
        "SELECT count(*) FROM read_parquet(?)",
        [str(manifest_path)],
    ).fetchone()[0]

    conn.execute(
        "CREATE TEMP TABLE id_map AS "
        "SELECT m.read_id, f.feature_idx,"
        "  m.sequence_hash,"
        "  m.length AS sequence_length_bp "
        "FROM read_parquet(?) m "
        "JOIN read_parquet(?) f "
        "  ON m.sequence_hash = f.sequence_hash",
        [str(manifest_path), str(feature_map_path)],
    )

    id_map_count = conn.execute("SELECT count(*) FROM id_map").fetchone()[0]
    if id_map_count != manifest_count:
        n_unmapped = manifest_count - id_map_count
        # Diagnostic ANTI JOIN only on mismatch — second read of the
        # manifest just to surface a useful error message.
        unmapped = conn.execute(
            "SELECT m.sequence_hash FROM read_parquet(?) m "
            "ANTI JOIN id_map x ON m.sequence_hash = x.sequence_hash",
            [str(manifest_path)],
        ).fetchall()
        hashes = [str(r[0]) for r in unmapped[:10]]
        raise ValueError(f"{n_unmapped} unmapped sequence hash(es) in feature_map: {hashes}")


def _write_sequence_metadata(conn: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    """Write reference_sequences.parquet — metadata from id_map (no FASTA read)."""
    out = validate_parquet_path(output_dir / "reference_sequences.parquet")
    conn.execute(
        "COPY ("
        "  SELECT feature_idx,"
        "    sequence_hash,"
        "    CAST(sequence_length_bp AS BIGINT) AS sequence_length_bp"
        "  FROM id_map"
        "  ORDER BY feature_idx"
        f") TO '{out}' ({PARQUET_OPTS})"
    )


def _write_sequence_chunks(
    conn: duckdb.DuckDBPyConnection, fasta_path: Path, output_dir: Path
) -> None:
    """Write reference_sequence_chunks.parquet — sequences chunked at 64 KB.

    Uses the list_transform + UNNEST macro pattern with ROW_GROUP_SIZE 16384
    to force row group flushing before memory pressure builds. Without a small
    ROW_GROUP_SIZE, DuckDB buffers too many rows and OOMs on large references.

    max_batch_bytes='512MB' caps read_fastx memory per batch — prevents it
    from buffering entire genomes (up to 21 MB each) before yielding rows.
    Without this, a single batch spanning many large sequences exhausts memory
    before the first row group is written.
    """
    out = validate_parquet_path(output_dir / "reference_sequence_chunks.parquet")
    conn.execute(
        f"CREATE OR REPLACE MACRO chunk_seq(str) AS "
        f"list_transform("
        f"  range(1, CAST(length(str) + 1 AS BIGINT), {_CHUNK_SIZE}),"
        f"  lambda idx : {{"
        f"    'chunk_index': CAST((idx - 1) / {_CHUNK_SIZE} AS INTEGER),"
        f"    'chunk_data': substring(str, CAST(idx AS BIGINT), {_CHUNK_SIZE})"
        f"  }}"
        f")"
    )
    conn.execute(
        "COPY ("
        "  WITH unnested AS ("
        "    SELECT m.feature_idx,"
        "      UNNEST(chunk_seq(f.sequence1)) AS chunk"
        "    FROM read_fastx(?) f"
        "    JOIN id_map m ON f.read_id = m.read_id"
        "  )"
        "  SELECT feature_idx, chunk.chunk_index, chunk.chunk_data"
        "  FROM unnested"
        f") TO '{out}' ({_PARQUET_OPTS_CHUNKED})",
        [str(fasta_path)],
    )


def _write_membership(
    conn: duckdb.DuckDBPyConnection, output_dir: Path, reference_idx: int
) -> None:
    """Write reference_membership.parquet from id_map."""
    out = validate_parquet_path(output_dir / "reference_membership.parquet")
    conn.execute(
        "COPY ("
        f"  SELECT CAST({reference_idx} AS BIGINT) AS reference_idx, feature_idx"
        "  FROM id_map"
        "  ORDER BY feature_idx"
        f") TO '{out}' ({PARQUET_OPTS})"
    )


def _write_taxonomy(
    conn: duckdb.DuckDBPyConnection,
    taxonomy_path: Path,
    output_dir: Path,
    reference_idx: int,
) -> None:
    """Write reference_taxonomy.parquet from a Parquet input (feature_id, taxonomy).

    Rank extraction and validation done entirely in DuckDB.
    Partial coverage is allowed (not all features need taxonomy).
    """
    out = validate_parquet_path(output_dir / "reference_taxonomy.parquet")

    # Read taxonomy Parquet, join with id_map, split ranks.
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

    # Validate: field count ≤ 8.
    bad = conn.execute(
        "SELECT feature_idx, nranks FROM parsed_taxonomy WHERE nranks > 8 LIMIT 5"
    ).fetchall()
    if bad:
        raise ValueError(f"Taxonomy has >8 semicolon-delimited fields: {bad}")

    # Validate: no blank fields.
    bad = conn.execute(
        "SELECT feature_idx FROM parsed_taxonomy WHERE list_contains(ranks, '') LIMIT 5"
    ).fetchall()
    if bad:
        ids = [r[0] for r in bad]
        raise ValueError(f"Taxonomy contains blank fields for feature_idx: {ids}")

    # Validate: prefix order.
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

    conn.execute(
        "COPY ("
        "  SELECT "
        f"    CAST({reference_idx} AS BIGINT) AS reference_idx,"
        "    feature_idx,"
        "    NULLIF(substr(ranks[1], 4), '') AS domain,"
        "    NULLIF(substr(ranks[2], 4), '') AS phylum,"
        "    NULLIF(substr(ranks[3], 4), '') AS class,"
        "    NULLIF(substr(ranks[4], 4), '') AS \"order\","
        "    NULLIF(substr(ranks[5], 4), '') AS family,"
        "    NULLIF(substr(ranks[6], 4), '') AS genus,"
        "    NULLIF(substr(ranks[7], 4), '') AS species,"
        "    NULLIF(substr(ranks[8], 4), '') AS strain,"
        "    NULL::BIGINT AS ncbi_taxon_id"
        "  FROM parsed_taxonomy"
        "  ORDER BY feature_idx"
        f") TO '{out}' ({PARQUET_OPTS})"
    )

    conn.execute("DROP TABLE parsed_taxonomy")


def _write_phylogeny(
    conn: duckdb.DuckDBPyConnection,
    tree_path: Path,
    output_dir: Path,
    reference_idx: int,
) -> None:
    """Write reference_phylogeny.parquet from Newick with feature_idx on tips.

    Tips with matching sequences get feature_idx populated; tips without
    sequences (and internal nodes) get NULL. No error on unmatched tips.
    """
    out = validate_parquet_path(output_dir / "reference_phylogeny.parquet")

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
    output_dir: Path,
    reference_idx: int,
) -> None:
    """Write reference_placements.parquet from jplace.

    Maps placed fragments to feature_idx via id_map. Fragments not in
    id_map are skipped (they weren't hashed/minted).
    """
    out = validate_parquet_path(output_dir / "reference_placements.parquet")

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
