"""Shared test constants and builders for qiita-compute-orchestrator."""

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
