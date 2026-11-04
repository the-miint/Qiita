"""Tests for the one-off recovery of myloasm provenance from a finished
long-read-assembly 1.0.0/1.0.1 run (`jobs/myloasm_provenance_recover.py`).

The source is a real-shaped `assemble` step output under a scratch tree — myloasm's
`-o` tree from the committed fixture, the splitter's `genomes/`, and the step's
manifest — at the path that the ticket workspace layout gives it. The two data-plane
streams are replaced: the read lookup by the names that myloasm was given (read off
its own mapping file, independently of the miint readers that the job dumps with),
and the run's `assembled_sequence` by what `assembly_hash` itself writes for those
genomes, so the job must reproduce the producer's hashes from the same bytes.
"""

from __future__ import annotations

import asyncio
import gzip
import json
import os
import re
import shutil
import subprocess
import sys
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pytest
from myloasm_fixture import (
    MYLOASM_FIXTURE_ROWS,
    MYLOASM_FIXTURE_VERSIONS,
    myloasm_output_dir,
    myloasm_raw_dir,
)
from qiita_common.duckdb_relation import registered
from qiita_common.myloasm_constants import MYLOASM_TABLES
from qiita_common.workspace import (
    WORK_TICKET_SUBDIR,
    step_attempt_dir,
    step_output_dir,
    ticket_workspace,
)

from qiita_compute_orchestrator.jobs import assembly_hash
from qiita_compute_orchestrator.jobs import myloasm_provenance_recover as job
from qiita_compute_orchestrator.jobs._myloasm_provenance import (
    CONTIG_FEATURE_TABLE,
    READ_NAME_TABLE,
    register_raw_tables,
    write_myloasm_staging,
)

_PREP_SAMPLE = 41
_RUN = 77
_MASK = 5
_SOURCE_TICKET = 4834
_ATTEMPT = 0
_FIRST_FEATURE = 1001
# Reads that the mask passed and the assembler never used: the lookup is the
# prep_sample's whole pass-set, and the job must keep only the names that the dump uses.
_UNUSED_READS = [("m84001/1/ccs", 1), ("m84001/2/ccs", 2)]

_READ_SCHEMA = pa.schema([("read_id", pa.string()), ("sequence_idx", pa.int64())])
# `sequence_hash` as the data plane streams it: canonical text, not UUID (see the
# job's CAST).
_LAKE_SCHEMA = pa.schema(
    [
        ("feature_idx", pa.int64()),
        ("sequence_hash", pa.string()),
        ("sequence_length_bp", pa.int64()),
    ]
)
_REPO_ROOT = Path(__file__).resolve().parents[3]
_SPLIT_SCRIPT = _REPO_ROOT / "workflows" / "long-read-assembly" / "myloasm_split.py"
_MANIFEST_WRITER = _REPO_ROOT / "workflows" / "_shared" / "manifest_writer.py"


def _assemble_output(out: Path, version: str) -> Path:
    """A finished 1.0.0/1.0.1 `assemble` step's output for a myloasm run, built as
    that step built it: myloasm's `-o` tree under `assembler/`, the splitter's
    `genomes/`, and the step's `manifest.json` over both. Those versions wrote no
    provenance dump, so there is none here."""
    asm = myloasm_output_dir(out / "assembler", version)
    genomes = out / "genomes"
    genomes.mkdir(parents=True)
    for argv in (
        [
            _SPLIT_SCRIPT,
            asm / "assembly_primary.fa",
            genomes / "circular.fa",
            genomes / "noLCG.fa",
            genomes / "contig_attributes.tsv",
        ],
        [_MANIFEST_WRITER, out, "genomes_dir=genomes"],
    ):
        result = subprocess.run(
            [sys.executable, *map(str, argv)], capture_output=True, text=True, env=dict(os.environ)
        )
        if result.returncode != 0:
            raise RuntimeError(f"{argv[0].name} failed: {result.stderr}")
    return out


# myloasm's decoration of an input read's name in its own files: an `r<N>-` index
# prefix, and a `+split<N>` suffix on a piece of a split read.
_MYLOASM_NAME_DECORATION = re.compile(r"^r\d+-|\+split\d+$")


def _input_read_names(asm: Path) -> list[tuple[str, int]]:
    """`(read_id, sequence_idx)` for every read that myloasm was given, as the run's
    pass-set holds them: each query name in myloasm's own mapping file with its
    decoration removed, numbered in name order. Read from the file, not through a
    miint reader, so the job's reader-derived names must match the input."""
    with gzip.open(asm / "3-mapping" / "map_to_unitigs.paf.gz", "rt") as paf:
        names = {_MYLOASM_NAME_DECORATION.sub("", line.split("\t", 1)[0]) for line in paf}
    return [(name, i) for i, name in enumerate(sorted(names), start=1)]


def _producer_features(genomes: Path, work: Path) -> dict[str, tuple[int, str, int]]:
    """`contig_id -> (feature_idx, sequence_hash, length)` as the run's lake rows hold
    them: `assembly_hash` run over the genomes, one feature per canonical hash as
    minting assigns them, the hash as the data plane streams it (canonical text)."""
    bins = work / "refined_bins"
    bins.mkdir(parents=True)
    out = asyncio.run(
        assembly_hash.execute(
            assembly_hash.Inputs(
                genomes_dir=genomes,
                refined_bins_dir=bins,
                prep_sample_idx=_PREP_SAMPLE,
                work_ticket_idx=1,
            ),
            work / "ws",
        )
    )
    with duckdb.connect() as conn:
        rows = conn.execute(
            "SELECT b.contig_id, CAST(m.sequence_hash AS VARCHAR), m.sequence_length_bp, "
            "dense_rank() OVER (ORDER BY m.sequence_hash) "
            "FROM read_parquet(?) m JOIN read_parquet(?) b USING (read_id) ORDER BY 1",
            [str(out["manifest"]), str(out["bin_map"])],
        ).fetchall()
    return {c: (_FIRST_FEATURE + rank - 1, h, n) for c, h, n, rank in rows}


@dataclass(frozen=True)
class Source:
    """One finished source run: its scratch root, its read names, and the run's lake
    features (`contig_id -> (feature_idx, sequence_hash, length)`)."""

    version: str
    scratch: Path
    output: Path
    read_names: list[tuple[str, int]]
    features: dict[str, tuple[int, str, int]]


def _output_dir(scratch: Path) -> Path:
    return step_output_dir(
        step_attempt_dir(
            ticket_workspace(scratch / WORK_TICKET_SUBDIR, _SOURCE_TICKET), "assemble", _ATTEMPT
        )
    )


@pytest.fixture(scope="module", params=MYLOASM_FIXTURE_VERSIONS)
def built(request, tmp_path_factory) -> Source:
    root = tmp_path_factory.mktemp(f"myloasm-{request.param}")
    scratch = root / "scratch"
    output = _assemble_output(_output_dir(scratch), request.param)
    names = _input_read_names(output / "assembler")
    features = _producer_features(output / "genomes", root / "producer")
    return Source(request.param, scratch, output, names, features)


@pytest.fixture
def source(built, tmp_path) -> Source:
    """A private copy of the built source, for tests that damage it."""
    scratch = tmp_path / "scratch"
    shutil.copytree(built.scratch, scratch)
    return Source(built.version, scratch, _output_dir(scratch), built.read_names, built.features)


def _reader(schema: pa.Schema, rows: list[dict]) -> pa.RecordBatchReader:
    return pa.RecordBatchReader.from_batches(
        schema, [pa.RecordBatch.from_pylist(rows, schema=schema)] if rows else []
    )


def _install(monkeypatch, source: Source, *, reads=None, lake=None) -> dict:
    """Point the job at `source`'s scratch root and replace both streams. Returns
    what the job asked the streams for."""
    asked: dict = {}
    read_rows = [
        {"read_id": r, "sequence_idx": i}
        for r, i in (source.read_names + _UNUSED_READS if reads is None else reads)
    ]
    lake_rows = [
        {"feature_idx": f, "sequence_hash": h, "sequence_length_bp": n}
        for f, h, n in (sorted(set(source.features.values())) if lake is None else lake)
    ]

    @asynccontextmanager
    async def _reads(conn, *, prep_sample_idx, mask_idx, relation="masked_reads", columns=None):
        asked["reads"] = (prep_sample_idx, mask_idx, columns)
        with registered(conn, relation, _reader(_READ_SCHEMA, read_rows)):
            yield relation

    @asynccontextmanager
    async def _lake(conn, *, prep_sample_idx, processing_idx, relation="assembled_lengths"):
        asked["lake"] = (prep_sample_idx, processing_idx)
        with registered(conn, relation, _reader(_LAKE_SCHEMA, lake_rows)):
            yield relation

    monkeypatch.setattr(
        job, "get_settings", lambda: SimpleNamespace(path_scratch=str(source.scratch))
    )
    monkeypatch.setattr(job, "open_read_masked_stream", _reads)
    monkeypatch.setattr(job, "open_assembled_sequence_stream", _lake)
    return asked


def _inputs(**overrides) -> job.Inputs:
    return job.Inputs(
        **{
            "prep_sample_idx": _PREP_SAMPLE,
            "work_ticket_idx": 9001,
            "assembly_processing_idx": _RUN,
            "assembly_mask_idx": _MASK,
            "source_work_ticket_idx": _SOURCE_TICKET,
            "assemble_attempt": _ATTEMPT,
            **overrides,
        }
    )


def _run(workspace: Path, **overrides) -> dict[str, Path]:
    return asyncio.run(job.execute(_inputs(**overrides), workspace))


def _expected(source: Source, tmp_path: Path) -> Path:
    """The same run staged through the shared transform directly, from the container's
    own dump script: what the recovery must reproduce."""
    raw = myloasm_raw_dir(tmp_path / "expected", source.version, sequence_idx_names=False)
    staging = tmp_path / "expected" / "staging"
    staging.mkdir()
    with duckdb.connect() as conn:
        register_raw_tables(conn, raw)
        conn.execute(f"CREATE TEMP TABLE {READ_NAME_TABLE} (read_id VARCHAR, sequence_idx BIGINT)")
        conn.executemany(f"INSERT INTO {READ_NAME_TABLE} VALUES (?, ?)", source.read_names)
        conn.execute(
            f"CREATE TEMP TABLE {CONTIG_FEATURE_TABLE} (contig_id VARCHAR, feature_idx BIGINT)"
        )
        conn.executemany(
            f"INSERT INTO {CONTIG_FEATURE_TABLE} VALUES (?, ?)",
            [(c, f) for c, (f, _, _) in source.features.items()],
        )
        write_myloasm_staging(
            conn, staging_dir=staging, prep_sample_idx=_PREP_SAMPLE, processing_idx=_RUN
        )
    return staging


def _staged_parquet(staging: Path) -> list[Path]:
    return sorted(staging.glob("*.parquet")) if staging.is_dir() else []


def test_recovers_the_six_tables_the_transform_stages_for_the_run(monkeypatch, source, tmp_path):
    asked = _install(monkeypatch, source)
    outputs = _run(tmp_path / "ws")

    staging = outputs["staging_dir"]
    assert staging.parent == tmp_path / "ws", "the staging dir must be a step output"
    # register-files loads everything in the staging dir, so it holds the six
    # tables and nothing else (no raw dump, no spill).
    assert sorted(p.name for p in staging.iterdir()) == sorted(
        f"{t}.parquet" for t in MYLOASM_TABLES
    )
    # Only the two columns that the lookup binds cross the wire.
    assert asked["reads"] == (_PREP_SAMPLE, _MASK, ["read_id", "sequence_idx"])
    assert asked["lake"] == (_PREP_SAMPLE, _RUN)

    expected = _expected(source, tmp_path)
    with duckdb.connect() as conn:
        for table, rows in MYLOASM_FIXTURE_ROWS.items():
            got, want = staging / f"{table}.parquet", expected / f"{table}.parquet"
            (n,) = conn.execute(f"SELECT count(*) FROM '{got}'").fetchone()
            assert n == rows, table
            for a, b in ((got, want), (want, got)):
                diff = conn.execute(f"FROM '{a}' EXCEPT ALL FROM '{b}'").fetchall()
                assert diff == [], f"{table}: {a.parent.name} has rows {b.parent.name} lacks"
            keys = conn.execute(
                f"SELECT DISTINCT prep_sample_idx, processing_idx FROM '{got}'"
            ).fetchall()
            assert keys == [(_PREP_SAMPLE, _RUN)], table


def test_the_source_tree_is_left_as_it_was(monkeypatch, source, tmp_path):
    """The source is another ticket's finished output; reading it writes nothing."""
    before = sorted(p.relative_to(source.scratch) for p in source.scratch.rglob("*"))
    _install(monkeypatch, source)
    _run(tmp_path / "ws")
    assert sorted(p.relative_to(source.scratch) for p in source.scratch.rglob("*")) == before


def _drop_manifest(source: Source) -> None:
    (source.output / "manifest.json").unlink()


def _unlist_primary(source: Source) -> None:
    manifest = source.output / "manifest.json"
    body = json.loads(manifest.read_text())
    body["files"] = [f for f in body["files"] if f["path"] != "assembler/assembly_primary.fa"]
    manifest.write_text(json.dumps(body))


def _grow_a_genome_file(source: Source) -> None:
    with (source.output / "genomes" / "noLCG.fa").open("a") as f:
        f.write("\n")


def _drop_assembler_tree(source: Source) -> None:
    shutil.rmtree(source.output / "assembler")


def _empty_the_run_log(source: Source) -> None:
    # Not in the manifest's checked files, so only the dump can catch it.
    (source.output / "assembler" / "myloasm.log").write_text("")


@pytest.mark.parametrize(
    ("damage", "error", "match"),
    [
        pytest.param(_drop_manifest, FileNotFoundError, "manifest", id="no-manifest"),
        pytest.param(_unlist_primary, ValueError, "assembly_primary.fa", id="primary-unlisted"),
        pytest.param(_grow_a_genome_file, ValueError, "noLCG.fa", id="changed-since-finish"),
        pytest.param(_drop_assembler_tree, FileNotFoundError, "assembler", id="no-tree"),
        # 1.0.2's dump refuses an empty run log; so does the recovery's.
        pytest.param(
            _empty_the_run_log, ValueError, "read_myloasm_run returned no rows", id="no-run"
        ),
    ],
)
def test_a_source_that_is_not_the_finished_step_output_is_refused(
    monkeypatch, source, tmp_path, damage, error, match
):
    damage(source)
    _install(monkeypatch, source)
    with pytest.raises(error, match=match):
        _run(tmp_path / "ws")
    assert _staged_parquet(tmp_path / "ws" / "myloasm_staging") == []


@pytest.mark.skipif(os.geteuid() == 0, reason="root traverses a mode-000 directory")
def test_a_source_that_cannot_be_looked_at_raises_rather_than_reading_as_missing(
    monkeypatch, source, tmp_path
):
    _install(monkeypatch, source)
    locked = source.output.parent
    locked.chmod(0)
    try:
        with pytest.raises(PermissionError):
            _run(tmp_path / "ws")
    finally:
        locked.chmod(0o755)
    assert _staged_parquet(tmp_path / "ws" / "myloasm_staging") == []


def test_a_source_attempt_that_does_not_exist_is_refused(monkeypatch, source, tmp_path):
    _install(monkeypatch, source)
    with pytest.raises(FileNotFoundError, match=f"attempt-{_ATTEMPT + 1}"):
        _run(tmp_path / "ws", assemble_attempt=_ATTEMPT + 1)


def test_a_read_the_lookup_does_not_resolve_fails_and_stages_nothing(monkeypatch, source, tmp_path):
    _install(monkeypatch, source, reads=source.read_names[1:])
    with pytest.raises(ValueError, match="no sequence_idx"):
        _run(tmp_path / "ws")
    assert _staged_parquet(tmp_path / "ws" / "myloasm_staging") == []


@pytest.mark.parametrize(
    ("lake_of", "match"),
    [
        # A genome contig whose bytes the run's lake rows do not carry: the tree is
        # not the registered attempt's.
        pytest.param(
            lambda f: sorted(set(f.values()))[1:], "not in the run's", id="contig-missing"
        ),
        # A lake feature that no genome contig hashes to.
        pytest.param(
            lambda f: [*sorted(set(f.values())), (9999, str(uuid.UUID(int=1)), 10)],
            "no genome contig",
            id="feature-extra",
        ),
    ],
)
def test_genome_contigs_must_be_exactly_the_runs_lake_features(
    monkeypatch, source, tmp_path, lake_of, match
):
    _install(monkeypatch, source, lake=lake_of(source.features))
    with pytest.raises(ValueError, match=match):
        _run(tmp_path / "ws")
    assert _staged_parquet(tmp_path / "ws" / "myloasm_staging") == []


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("assembly_processing_idx", 0),
        ("assembly_mask_idx", 0),
        ("source_work_ticket_idx", 0),
        ("assemble_attempt", -1),
    ],
)
def test_inputs_refuse_an_identifier_out_of_range(field, bad):
    with pytest.raises(ValueError):
        _inputs(**{field: bad})
