"""Shared test constants and builders for qiita-compute-orchestrator."""

import gzip
from pathlib import Path

import duckdb

# Canonical test sequences shared across hash and load job tests.
TEST_SEQUENCES = {
    "seq1": "ATCGATCGATCG",
    "seq2": "GCTAGCTAGCTA",
    "seq3": "AAATTTTCCCGGG",
    "seq4": "TTTTAAAACCCC",
    "seq5": "GGGGCCCCAAAA",
}


def write_fasta(path: Path, records: list[tuple[str, str]]) -> Path:
    """Write `(read_id, sequence)` records as FASTA at `path`, gzipped when it ends
    in `.gz`, creating its directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "".join(f">{read_id}\n{seq}\n" for read_id, seq in records)
    if path.suffix == ".gz":
        with gzip.open(path, "wt") as fh:
            fh.write(text)
    else:
        path.write_text(text)
    return path


def write_chunked_blob_upload(dest: Path, payload: bytes) -> Path:
    """Write the `(chunk_index INTEGER, chunk_data BLOB)` Parquet the data
    plane's DoPut writer produces for a chunked-BLOB upload.

    Two chunks, inserted out of order, so a reassembly that ignores
    `chunk_index` and takes row order produces the wrong bytes rather than
    passing by luck. Every step that resolves a `*_upload_idx` handle meets
    this shape, so the well-formed envelope is built here rather than per test
    module. `test_blob_input` writes two deliberately malformed ones inline
    (no chunks; a NULL `chunk_data`) — those are the shapes under test there,
    not copies of this.
    """
    half = max(1, len(payload) // 2)
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE up (chunk_index INTEGER, chunk_data BLOB)")
        conn.execute("INSERT INTO up VALUES (1, ?), (0, ?)", [payload[half:], payload[:half]])
        conn.execute(f"COPY up TO '{dest}' (FORMAT PARQUET)")
    return dest


# `plan()` is applied by the CP as a down-size only while it is below the step's
# YAML baseline (runner/_dispatch.py), so the allocation a shard actually gets is
# the hint clamped at that baseline. Read from the workflow rather than restated in
# each test: a baseline raised in the YAML has to move both index builders' clamps,
# and a literal in either would keep passing against the old one.
def shard_yaml_baseline_gb(step_name: str) -> int:
    """`build-shard-index`'s declared `mem_gb` for `step_name`.

    Also asserts the step has escalation headroom under the action ceiling, because
    the callers' clamp elides that term: `_resolve_step_resources` applies the hint
    only when `hint < resolved < ceiling`, and it coincides with a plain
    `hint < baseline` only while the baseline sits under the ceiling. Closing that
    gap in the YAML would make the tests' clamp diverge from production silently.
    """
    import yaml

    repo_root = Path(__file__).resolve().parents[2]
    data = yaml.safe_load((repo_root / "workflows/build-shard-index/1.0.0.yaml").read_text())
    for step in data["steps"]:
        if step.get("step") == step_name:
            mem_gb = step["baseline_resources"]["mem_gb"]
            ceiling = data["action_ceiling"]["mem_gb"]
            assert mem_gb < ceiling, (
                f"build-shard-index {step_name} baseline mem_gb={mem_gb} has no headroom "
                f"under ceiling={ceiling}; the down-size would not apply at all and the "
                "callers' clamp no longer matches runner/_dispatch.py"
            )
            return mem_gb
    raise AssertionError(f"build-shard-index declares no {step_name} step")


# The backbone of the conftest `krepp_built_index` fixture; the index numbers its edges
# edge_id = node_index, as Qiita numbers an undecorated tree.
KREPP_TREE_NEWICK = "((g0:1,g1:1):1,(g2:1,g3:1):1);\n"


def krepp_recorded_version(index_dir: Path) -> str:
    """The `krepp version:` an index's metadata texts record, read with DuckDB's
    read_text rather than krepp_build's parser, so a test comparing against it does
    not grade that parser with itself. Raises unless every partition records one and
    the same version."""
    import duckdb

    versions = duckdb.sql(
        "SELECT DISTINCT regexp_extract(content, '(?m)^krepp version: ([^\\r\\n]*)', 1)"
        f" FROM read_text('{index_dir}/metadata*.txt')"
    ).fetchall()
    assert len(versions) == 1 and versions[0][0], versions
    return versions[0][0]


def krepp_genomes() -> dict[str, str]:
    """The four genomes the conftest `krepp_built_index` index is built from: tips g0..g3
    of KREPP_TREE_NEWICK, 20 kb each, random with a fixed seed."""
    import random

    rng = random.Random(1)
    return {f"g{i}": "".join(rng.choice("ACGT") for _ in range(20000)) for i in range(4)}
