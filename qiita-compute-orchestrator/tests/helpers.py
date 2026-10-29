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


_MYLOASM_FIXTURES = Path(__file__).resolve().parent / "data" / "myloasm"
# Files myloasm itself writes gzipped; every other fixture file is gzipped for storage only.
_MYLOASM_NATIVE_GZ = frozenset({"map_to_unitigs.paf.gz", "read_coverages.txt.gz"})
MYLOASM_FIXTURE_VERSIONS = ("0.6.0", "0.7.0")


def myloasm_output_dir(dest: Path, version: str) -> Path:
    """Lay out the committed myloasm fixture at `dest` the way myloasm writes its `-o`
    directory, for myloasm `version` (see `tests/data/myloasm/README.md`)."""
    import gzip
    import shutil

    if version not in MYLOASM_FIXTURE_VERSIONS:
        raise ValueError(f"no myloasm {version} fixture; have {MYLOASM_FIXTURE_VERSIONS}")
    sources = [_MYLOASM_FIXTURES / "ecoli_sim"]
    if version == "0.6.0":
        sources.append(_MYLOASM_FIXTURES / "ecoli_sim_0.6.0")
    for source in sources:
        for path in source.rglob("*"):
            if not path.is_file():
                continue
            target = dest / path.relative_to(source)
            target.parent.mkdir(parents=True, exist_ok=True)
            if path.name in _MYLOASM_NATIVE_GZ:
                shutil.copyfile(path, target)
            else:
                with gzip.open(path, "rb") as src, target.with_suffix("").open("wb") as out:
                    shutil.copyfileobj(src, out)
    return dest


# The first sequence_idx `myloasm_raw_dir` gives a renamed read; any value well clear of
# a small fixture's other identifiers does.
MYLOASM_FIRST_SEQUENCE_IDX = 9_000_000_000


def myloasm_raw_dir(dest: Path, version: str, *, sequence_idx_names: bool) -> Path:
    """The raw provenance dump the assemble step writes (`<table>.parquet` per miint
    reader, verbatim), built from the committed myloasm fixture.

    With `sequence_idx_names`, every read name is first replaced by an integer, the
    way a run whose assembler input was named by `sequence_idx` reports them: names
    sorted, numbered from `MYLOASM_FIRST_SEQUENCE_IDX`. `myloasm_read_name_map`
    returns the same numbering for the original names."""
    from qiita_common.myloasm_constants import MYLOASM_TABLES

    from qiita_compute_orchestrator.miint import open_miint_conn

    asm_dir = myloasm_output_dir(dest / "assembler", version)
    raw = dest / "raw"
    raw.mkdir(parents=True)
    with open_miint_conn() as conn:
        for table, spec in MYLOASM_TABLES.items():
            conn.execute(
                f"COPY (FROM {spec.reader}('{asm_dir}')) TO '{raw / f'{table}.parquet'}' "
                "(FORMAT PARQUET)"
            )
        if sequence_idx_names:
            conn.execute(
                "CREATE TEMP TABLE name_map AS SELECT read_id, "
                "CAST(sequence_idx AS VARCHAR) AS seq FROM (" + _read_name_map_sql(raw) + ")"
            )
            renames = {
                "myloasm_mapping": ["read_id"],
                "myloasm_layout": ["read_id"],
                "myloasm_split": ["read_id"],
                "myloasm_link": ["from_read_id", "to_read_id"],
            }
            for table, columns in renames.items():
                path = raw / f"{table}.parquet"
                replace = ", ".join(
                    f"(SELECT seq FROM name_map n WHERE n.read_id = t.{c}) AS {c}" for c in columns
                )
                conn.execute(
                    f"COPY (SELECT * REPLACE ({replace}) FROM '{path}' t) "
                    f"TO '{path}.tmp' (FORMAT PARQUET)"
                )
                Path(f"{path}.tmp").replace(path)
    return raw


def myloasm_read_name_map(raw: Path) -> list[tuple[str, int]]:
    """(original read name, sequence_idx) for every read the fixture's raw dump at
    `raw` names, numbered as `myloasm_raw_dir(..., sequence_idx_names=True)` does."""
    with duckdb.connect() as conn:
        return conn.execute(_read_name_map_sql(raw) + " ORDER BY read_id").fetchall()


def _read_name_map_sql(raw: Path) -> str:
    """(read_id, sequence_idx) over every read name in a raw dump, sorted names
    numbered from `MYLOASM_FIRST_SEQUENCE_IDX`."""
    return (
        "SELECT read_id, "
        f"{MYLOASM_FIRST_SEQUENCE_IDX} - 1 + row_number() OVER (ORDER BY read_id) AS sequence_idx "
        "FROM ("
        f"  SELECT read_id FROM '{raw}/myloasm_mapping.parquet'"
        f"  UNION SELECT read_id FROM '{raw}/myloasm_layout.parquet'"
        f"  UNION SELECT read_id FROM '{raw}/myloasm_split.parquet'"
        f"  UNION SELECT from_read_id FROM '{raw}/myloasm_link.parquet'"
        f"  UNION SELECT to_read_id FROM '{raw}/myloasm_link.parquet'"
        ") WHERE read_id IS NOT NULL"
    )
