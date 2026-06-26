"""Real-miint contract pins for the `COPY ... TO ... (FORMAT FASTQ)` writer the
admin masked-read export builds on (the `qiita-admin masked-read-export` CLI
streams the data plane's `read_masked` view and writes per-sample FASTQ locally
via this COPY).

These run against the team-mirror miint build (staged by the session-autouse
`_stage_miint_extension` fixture in tests/conftest.py; `open_miint_conn` is
LOAD-only). They pin the facts the CLI's FASTQ path depends on that the upstream
`docs/copy-formats.md` summary does not spell out — verified empirically against
the build, not paraphrased:

  * The writer requires the **verbatim** `read_id`, `sequence1`, `qual1`
    columns (and `sequence2`, `qual2` for paired) — the exact column names the
    `read_masked` view emits. Aliasing `read_id` away raises a BinderException,
    so the CLI must select the view's columns by name, not rename them.
  * `qual1`/`qual2` are `UTINYINT[]` (phred-decoded, as `read_fastx` emits) and
    are written back as ASCII phred+33 (Q40 -> 'I', Q30 -> '?').
  * Paired rows (`sequence2` set) into a single output path are a hard error;
    the writer demands the `{ORIENTATION}` placeholder (split R1/R2 files) or
    `INTERLEAVE true` (one interleaved file).
  * `{ORIENTATION}` expands to exactly `R1` / `R2`, so a `<stem>.{ORIENTATION}.fastq.gz`
    target yields `<stem>.R1.fastq.gz` + `<stem>.R2.fastq.gz` — the export filename
    spec (`<biosample_accession>.<run>.<pool>.<prep>.R1.fastq.gz`).
  * `COMPRESSION 'gzip'` gzip-compresses the output (option-driven — it gzips even
    when the path ends in `.gz.partial`, not just `.gz`), so the CLI writes
    `<stem>.fastq.gz` and may stage to a `.partial` sibling before the atomic rename.

If a future miint build changes any of these, this file fails — the signal to
re-pin the CLI's COPY SQL.
"""

from __future__ import annotations

import gzip
from pathlib import Path

import duckdb
import pytest

from qiita_compute_orchestrator.miint import open_miint_conn


def _qual(values: list[int]) -> str:
    """A UTINYINT[] phred-quality literal."""
    return "[" + ",".join(str(v) for v in values) + "]::UTINYINT[]"


def _gz_lines(path: Path) -> list[str]:
    """Assert the file is gzip (magic 1f8b) and return its decompressed lines."""
    assert path.read_bytes()[:2] == b"\x1f\x8b", "expected gzip magic"
    with gzip.open(path, "rt") as fh:
        return fh.read().splitlines()


def _seed_single_end(conn: duckdb.DuckDBPyConnection) -> None:
    """One single-end read: sequence2/qual2 NULL throughout (a single-end sample
    in read_masked)."""
    conn.execute(
        f"CREATE OR REPLACE TABLE masked AS SELECT * FROM (VALUES "
        f"('readS', 'GGGGCCCC', {_qual([40] * 8)}, NULL::VARCHAR, NULL::UTINYINT[])"
        f") t(read_id, sequence1, qual1, sequence2, qual2)"
    )


def _seed_paired(conn: duckdb.DuckDBPyConnection) -> None:
    """Two paired reads (sequence2 set) — a paired sample in read_masked."""
    conn.execute(
        f"CREATE OR REPLACE TABLE masked AS SELECT * FROM (VALUES "
        f"('readP1', 'ACGTACGT', {_qual([30, 31, 32, 33, 34, 35, 36, 37])}, "
        f"          'TTGGCCAA', {_qual([20, 21, 22, 23, 24, 25, 26, 27])}),"
        f"('readP2', 'AAAACCCC', {_qual([38] * 8)}, "
        f"          'GGGGTTTT', {_qual([28] * 8)})"
        f") t(read_id, sequence1, qual1, sequence2, qual2)"
    )


def test_fastq_writer_requires_verbatim_read_id_column(tmp_path: Path) -> None:
    """Aliasing the id column away raises a BinderException naming `read_id`.
    Pins that the CLI selects the read_masked columns by their literal names."""
    with open_miint_conn() as conn:
        _seed_single_end(conn)
        with pytest.raises(duckdb.Error, match="read_id"):
            conn.execute(
                f"COPY (SELECT read_id AS id, sequence1, qual1 FROM masked) "
                f"TO '{tmp_path}/x.fastq' (FORMAT FASTQ)"
            )


def test_single_end_fastq_write_encodes_qual_phred33(tmp_path: Path) -> None:
    """SE write (read_id, sequence1, qual1) -> standard 4-line FASTQ; UTINYINT[]
    qual is ASCII phred+33 (Q40 -> 'I')."""
    out = tmp_path / "se.fastq"
    with open_miint_conn() as conn:
        _seed_single_end(conn)
        conn.execute(
            f"COPY (SELECT read_id, sequence1, qual1 FROM masked) TO '{out}' (FORMAT FASTQ)"
        )
    assert out.read_text().splitlines() == ["@readS", "GGGGCCCC", "+", "IIIIIIII"]


def test_paired_rows_into_single_path_demand_orientation(tmp_path: Path) -> None:
    """A row with sequence2 set into one file errors, naming {ORIENTATION} /
    INTERLEAVE — pins that the CLI must split paired output."""
    with open_miint_conn() as conn:
        _seed_paired(conn)
        with pytest.raises(duckdb.Error, match="ORIENTATION"):
            conn.execute(
                f"COPY (SELECT read_id, sequence1, qual1, sequence2, qual2 FROM masked) "
                f"TO '{tmp_path}/pe.fastq' (FORMAT FASTQ)"
            )


def test_orientation_placeholder_writes_R1_R2(tmp_path: Path) -> None:
    """`{ORIENTATION}` expands to R1/R2, so a `<stem>.{ORIENTATION}.fastq` target
    produces exactly `<stem>.R1.fastq` + `<stem>.R2.fastq` with the mates split
    (sequence1 -> R1, sequence2 -> R2), each phred+33 encoded."""
    stem = "SAMN1.5.7.42"
    with open_miint_conn() as conn:
        _seed_paired(conn)
        conn.execute(
            f"COPY (SELECT read_id, sequence1, qual1, sequence2, qual2 FROM masked) "
            f"TO '{tmp_path}/{stem}.{{ORIENTATION}}.fastq' (FORMAT FASTQ)"
        )
    r1 = tmp_path / f"{stem}.R1.fastq"
    r2 = tmp_path / f"{stem}.R2.fastq"
    assert r1.is_file() and r2.is_file()
    # R1 carries sequence1 (Q30..37 -> '?@ABCDEF'); R2 carries sequence2.
    assert r1.read_text().splitlines()[:4] == ["@readP1", "ACGTACGT", "+", "?@ABCDEF"]
    assert r2.read_text().splitlines()[:4] == ["@readP1", "TTGGCCAA", "+", "56789:;<"]
    # Both mates of both reads are present (4 lines per record, 2 records each).
    assert len(r1.read_text().splitlines()) == 8
    assert len(r2.read_text().splitlines()) == 8


def test_single_end_fastq_gzip_writes_gzip(tmp_path: Path) -> None:
    """`COMPRESSION 'gzip'` writes a gzip stream (the CLI's single-end path) — the
    `.gz.partial` extension is incidental, the option drives compression."""
    out = tmp_path / "se.fastq.gz.partial"
    with open_miint_conn() as conn:
        _seed_single_end(conn)
        conn.execute(
            f"COPY (SELECT read_id, sequence1, qual1 FROM masked) "
            f"TO '{out}' (FORMAT FASTQ, COMPRESSION 'gzip')"
        )
    assert _gz_lines(out) == ["@readS", "GGGGCCCC", "+", "IIIIIIII"]


def test_orientation_gzip_writes_R1_R2_gzip(tmp_path: Path) -> None:
    """The CLI's paired path: `{ORIENTATION}` + `COMPRESSION 'gzip'` together yield
    gzip-compressed `<stem>.R1.fastq.gz` + `<stem>.R2.fastq.gz`, mates split."""
    stem = "SAMN1.5.7.42"
    with open_miint_conn() as conn:
        _seed_paired(conn)
        conn.execute(
            f"COPY (SELECT read_id, sequence1, qual1, sequence2, qual2 FROM masked) "
            f"TO '{tmp_path}/{stem}.{{ORIENTATION}}.fastq.gz' (FORMAT FASTQ, COMPRESSION 'gzip')"
        )
    r1 = tmp_path / f"{stem}.R1.fastq.gz"
    r2 = tmp_path / f"{stem}.R2.fastq.gz"
    assert r1.is_file() and r2.is_file()
    assert _gz_lines(r1)[:4] == ["@readP1", "ACGTACGT", "+", "?@ABCDEF"]
    assert _gz_lines(r2)[:4] == ["@readP1", "TTGGCCAA", "+", "56789:;<"]


def test_interleave_option_writes_single_file(tmp_path: Path) -> None:
    """INTERLEAVE true writes one file with R1,R2 interleaved per read — the
    documented alternative to split output (the CLI uses split, but pin that the
    option exists so a build drop is caught)."""
    out = tmp_path / "interleaved.fastq"
    with open_miint_conn() as conn:
        _seed_paired(conn)
        conn.execute(
            f"COPY (SELECT read_id, sequence1, qual1, sequence2, qual2 FROM masked) "
            f"TO '{out}' (FORMAT FASTQ, INTERLEAVE true)"
        )
    lines = out.read_text().splitlines()
    # Two paired reads -> four records interleaved (R1 then R2 per read).
    assert lines[0:2] == ["@readP1", "ACGTACGT"]
    assert lines[4:6] == ["@readP1", "TTGGCCAA"]
    assert len(lines) == 16
