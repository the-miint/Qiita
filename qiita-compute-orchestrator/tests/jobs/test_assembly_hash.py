"""Isolated unit tests for `assembly_hash.execute` — the container-FASTA ->
manifest / hash-keyed-chunks / bin_map head of the assembly-storage tail.

Runs against the team-mirror miint build (conftest stages it): the job reads FASTA
with miint `read_fastx` and chunks with `sequence_split`. Calls execute() directly.
Covers: happy path (LCG + MAG, synthetic read_ids, hash-keyed chunks, dedup of
identical contigs), synthetic-id disambiguation of a contig id reused across bins,
and empty -> StepNoData.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from uuid import UUID

import duckdb
import pytest
from qiita_common.backend_failure import StepNoData

from qiita_compute_orchestrator.jobs.assembly_hash import Inputs, execute


def _run(inputs: Inputs, workspace) -> dict:
    return asyncio.run(execute(inputs, workspace))


def _fasta(path, records: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f">{cid}\n{seq}\n" for cid, seq in records.items()))


def _layout(tmp_path):
    genomes = tmp_path / "genomes"
    refined = tmp_path / "refined"
    genomes.mkdir(parents=True)
    refined.mkdir(parents=True)
    return genomes, refined


def _canonical(seq: str) -> str:
    """Mirror the shared canonical form: LEAST(upper, revcomp(upper))."""
    rc = seq.translate(str.maketrans("ACGTRYSWKMBDHVN", "TGCAYRSWMKVHDBN"))[::-1].upper()
    return min(seq.upper(), rc)


def _hash(seq: str) -> UUID:
    return UUID(hashlib.md5(_canonical(seq).encode()).hexdigest())


def _rows(parquet, cols: str, order: str):
    with duckdb.connect(":memory:") as con:
        return con.execute(
            f"SELECT {cols} FROM read_parquet('{parquet}') ORDER BY {order}"
        ).fetchall()


def test_happy_path_manifest_bin_map_and_chunks(tmp_path):
    genomes, refined = _layout(tmp_path)
    # circular.fa is a single multi-FASTA of circular contigs; each record is its
    # OWN LCG genome, so its bin_id IS the contig id (from the read, not a filename)
    # — no per-contig split step.
    _fasta(genomes / "circular.fa", {"c1": "AAAACCCCGGGGTTTT", "c2": "GGGGAAAATTTTCCCC"})
    _fasta(refined / "bin.1.fa", {"x1": "ACGTACGTACGTACGT", "x2": "TTTTGGGGCCCCAAAA"})

    out = _run(
        Inputs(
            genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=42, work_ticket_idx=7
        ),
        tmp_path / "ws",
    )

    # manifest: synthetic read_id kind:bin_id:contig, canonical hash, length.
    manifest = _rows(
        out["manifest"],
        "read_id, CAST(sequence_hash AS VARCHAR), sequence_length_bp",
        "read_id",
    )
    assert manifest == sorted(
        [
            ("LCG:c1:c1", str(_hash("AAAACCCCGGGGTTTT")), 16),
            ("LCG:c2:c2", str(_hash("GGGGAAAATTTTCCCC")), 16),
            ("MAG:bin.1:x1", str(_hash("ACGTACGTACGTACGT")), 16),
            ("MAG:bin.1:x2", str(_hash("TTTTGGGGCCCCAAAA")), 16),
        ]
    )

    # bin_map: kind + bin_id per synthetic read_id. Each LCG contig is its own bin
    # (bin_id == contig id); the MAG's contigs share the file's bin_id.
    bin_map = _rows(out["bin_map"], "read_id, kind, bin_id", "read_id")
    assert bin_map == sorted(
        [
            ("LCG:c1:c1", "LCG", "c1"),
            ("LCG:c2:c2", "LCG", "c2"),
            ("MAG:bin.1:x1", "MAG", "bin.1"),
            ("MAG:bin.1:x2", "MAG", "bin.1"),
        ]
    )

    # chunks: a directory of part_*.parquet keyed by sequence_hash; reassembled
    # chunk_data equals the canonical bytes.
    chunks_dir = out["assembly_chunks"]
    assert chunks_dir.is_dir()
    parts = sorted(chunks_dir.glob("part_*.parquet"))
    assert parts
    glob = str(chunks_dir / "part_*.parquet")
    with duckdb.connect(":memory:") as con:
        cols = {
            c[0]: c[1]
            for c in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [glob]).fetchall()
        }
        assert cols == {"sequence_hash": "UUID", "chunk_index": "INTEGER", "chunk_data": "VARCHAR"}
        reassembled = dict(
            con.execute(
                "SELECT CAST(sequence_hash AS VARCHAR), "
                "string_agg(chunk_data, '' ORDER BY chunk_index) "
                "FROM read_parquet(?) GROUP BY sequence_hash",
                [glob],
            ).fetchall()
        )
    assert reassembled[str(_hash("AAAACCCCGGGGTTTT"))] == "AAAACCCCGGGGTTTT"
    assert reassembled[str(_hash("ACGTACGTACGTACGT"))] == "ACGTACGTACGTACGT"


def test_identical_contigs_dedup_to_one_chunk_set(tmp_path):
    """Two contigs (different bins) with identical bytes collapse to ONE
    sequence_hash in the chunks, but BOTH keep their manifest + bin_map rows (so
    write-assembly-membership records both bins for the shared feature)."""
    genomes, refined = _layout(tmp_path)
    _fasta(refined / "bin.1.fa", {"ctg": "ACGTACGTACGTACGT"})
    _fasta(refined / "bin.2.fa", {"ctg": "ACGTACGTACGTACGT"})

    out = _run(
        Inputs(genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=1, work_ticket_idx=1),
        tmp_path / "ws",
    )
    # Same raw contig id "ctg" in two bins — synthetic read_ids disambiguate.
    manifest = _rows(out["manifest"], "read_id", "read_id")
    assert manifest == [("MAG:bin.1:ctg",), ("MAG:bin.2:ctg",)]

    glob = str(out["assembly_chunks"] / "part_*.parquet")
    with duckdb.connect(":memory:") as con:
        distinct_hashes = con.execute(
            f"SELECT count(DISTINCT sequence_hash) FROM read_parquet('{glob}')"
        ).fetchone()[0]
    assert distinct_hashes == 1


def test_lcg_only_no_mag(tmp_path):
    """LCG-only: a circular genome but NO refined MAGs (empty refined_bins_dir)
    stores successfully — the single-file circular.fa LCG path + COALESCE bin_id
    run without any MAG row alongside them (bin_id resolves to the contig id)."""
    genomes, refined = _layout(tmp_path)
    _fasta(genomes / "circular.fa", {"c1": "AAAACCCCGGGGTTTT"})

    out = _run(
        Inputs(genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=5, work_ticket_idx=9),
        tmp_path / "ws",
    )
    bin_map = _rows(out["bin_map"], "read_id, kind, bin_id", "read_id")
    assert bin_map == [("LCG:c1:c1", "LCG", "c1")]


@pytest.mark.skipif(
    os.environ.get("QIITA_ASSEMBLY_STRESS") != "1",
    reason="heavy (~800 MB fixture); opt in with QIITA_ASSEMBLY_STRESS=1",
)
def test_pass2_stays_bounded_at_scale(tmp_path, monkeypatch):
    """Regression for the pass-2 memory blow-up: the dedup must NOT carry the
    sequence payload through a sort. The old `DISTINCT ON (sequence_hash) …,
    sequence ORDER BY …` sorted every row's full contig bytes (~6x amplification)
    and OOM'd at ~1 GB of assembled input under a few-GB cap; the narrow-dedup +
    streaming-chunk rewrite completes with ~constant memory regardless of input.

    Here: ~800 MB of distinct random contigs under a 3 GB DuckDB cap — the old
    query OOMs (0.8 GB x ~6 > 3 GB), the new one stays ~1.8 GB. Opt-in because the
    fixture is large (the orchestrator suite has no slow tier to exclude it from).
    """
    import qiita_compute_orchestrator.jobs.assembly_hash as ahmod

    genomes, refined = _layout(tmp_path)
    # 400 x 2 MB distinct random contigs (random bytes → no accidental dedup).
    lut = bytes(b"ACGT"[i & 3] for i in range(256))
    with open(genomes / "circular.fa", "wb") as f:
        for i in range(400):
            f.write(b">ctg%06d\n" % i)
            f.write(os.urandom(2_000_000).translate(lut))
            f.write(b"\n")

    # Constrain DuckDB to a cap the old payload-carrying sort would exceed.
    monkeypatch.setattr(ahmod, "_DUCKDB_MEMORY_GB", 3)

    out = _run(
        Inputs(genomes_dir=genomes, refined_bins_dir=refined, prep_sample_idx=1, work_ticket_idx=1),
        tmp_path / "ws",
    )
    # Completed under the cap; every contig is distinct → 400 hashes, none deduped.
    n_contigs = _rows(out["manifest"], "count(*)", "1")[0][0]
    chunks_glob = str(out["assembly_chunks"] / "part_*.parquet")
    n_hashes = _rows(chunks_glob, "count(DISTINCT sequence_hash)", "1")[0][0]
    assert n_contigs == 400
    assert n_hashes == 400


def test_no_contigs_is_no_data(tmp_path):
    genomes, refined = _layout(tmp_path)
    with pytest.raises(StepNoData):
        _run(
            Inputs(
                genomes_dir=genomes,
                refined_bins_dir=refined,
                prep_sample_idx=1,
                work_ticket_idx=1,
            ),
            tmp_path / "ws",
        )
