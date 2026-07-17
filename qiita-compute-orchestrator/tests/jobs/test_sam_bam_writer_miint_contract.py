"""Real-miint contract pins for the `COPY ... TO ... (FORMAT SAM|BAM)` writer.

The headline: it is an **alignment** writer, not a reads writer — it does not put
the bases in the file. Anything needing sequences in a SAM/BAM must look elsewhere
(today: `lima_export`, which is why that job carries a pysam dependency).

These run against the team-mirror miint build (staged by the session-autouse
`_stage_miint_extension` fixture in tests/conftest.py; `open_miint_conn` is
LOAD-only). Everything here was verified empirically against the build on
2026-07-16, not paraphrased from upstream's `docs/copy-formats.md` — which
describes the writer only as taking "HTSlib-style columns" and does not say that
the reads themselves are not among them.

**Why this file exists.** `CLAUDE.local.md` says to reach for a miint writer before
hand-rolling one, and `COPY ... TO (FORMAT BAM)` looks exactly like the tool for
the job. It is not: it is an ALIGNMENT writer. Without this pin, the next person to
read `lima_export` sees a non-miint sequence writer and a `pysam` dependency and
"fixes" it back to a miint COPY — which would produce a BAM lima happily reads and
finds no bases in, i.e. a chain that silently masks every read `twist_no_adaptor`.

Pinned here:
  * The writer NEVER emits SEQ/QUAL — not for an unmapped record and not for a
    fully mapped one (the anti-vacuity control). The reads are simply not part of
    its output, under any column naming.
  * `REFERENCE_LENGTHS` is mandatory and its table must be non-empty, so a uBAM
    (no `@SQ` header at all) is not expressible.
  * There is no read-group option, so `@RG ... DS:READTYPE=CCS` — the field lima
    keys on to treat the reads as CCS — cannot be written at all.

If a future miint build fixes any of these, this file fails. That is the signal to
revisit `lima_export`'s pysam writer (and drop the dep), not a spurious failure.
"""

from __future__ import annotations

import duckdb
import pytest

from qiita_compute_orchestrator.miint import open_miint_conn

# The alignment columns the writer demands, discovered by following its own
# BinderExceptions. `position` must be BIGINT (a UINTEGER is rejected).
_ALIGNMENT_COLS = (
    "'123' AS read_id, {flags}::USMALLINT AS flags, {ref} AS reference, "
    "{pos}::BIGINT AS position, 60::UTINYINT AS mapq, {cigar} AS cigar, "
    "'*'::VARCHAR AS mate_reference, 0::BIGINT AS mate_position, 0::BIGINT AS template_length"
)
_SEQ = "ACGTACGTAA"
_QUAL = "[40,40,40,40,40,40,40,40,40,40]::UTINYINT[]"


def _unmapped(seq_col: str, qual_col: str) -> str:
    cols = _ALIGNMENT_COLS.format(flags=4, ref="'*'::VARCHAR", pos=0, cigar="'*'::VARCHAR")
    return f"SELECT {cols}, '{_SEQ}' AS {seq_col}, {_QUAL} AS {qual_col}"


def _mapped() -> str:
    cols = _ALIGNMENT_COLS.format(flags=0, ref="'dummy'::VARCHAR", pos=1, cigar="'10M'::VARCHAR")
    return f"SELECT {cols}, '{_SEQ}' AS sequence1, {_QUAL} AS qual1"


@pytest.fixture
def conn():
    with open_miint_conn() as c:
        c.execute(
            "CREATE TABLE reflens AS "
            "SELECT 'dummy'::VARCHAR AS reference, 10::BIGINT AS reference_length"
        )
        yield c


def _records(path) -> list[str]:
    return [ln for ln in path.read_text().splitlines() if not ln.startswith("@")]


@pytest.mark.parametrize(
    "seq_col,qual_col",
    [
        ("sequence1", "qual1"),  # what read_fastx / read.parquet call them
        ("sequence", "quality"),
        ("seq", "qual"),
        ("sequence", "qual"),
    ],
)
def test_sam_writer_never_emits_seq_or_qual(conn, tmp_path, seq_col, qual_col):
    """The finding that forces pysam. The writer ACCEPTS a sequence column without
    complaint and then drops it: SEQ and QUAL both land as `*`. There is no column
    naming that carries the bases through."""
    out = tmp_path / f"{seq_col}.sam"
    conn.execute(
        f"COPY ({_unmapped(seq_col, qual_col)}) TO '{out}' "
        "(FORMAT SAM, REFERENCE_LENGTHS 'reflens')"
    )
    (record,) = _records(out)
    seq, qual = record.split("\t")[9:11]
    assert (seq, qual) == ("*", "*"), record
    assert _SEQ not in record


def test_sam_writer_drops_seq_even_for_a_mapped_record(conn, tmp_path):
    """THE ANTI-VACUITY CONTROL. Without this, the test above proves only that an
    UNMAPPED record has no SEQ — which would be an artifact of our own record, not a
    property of the writer. A fully mapped record (real @SQ reference, `10M` cigar,
    flags=0) is the case the writer is actually built for, and its bases are dropped
    too. So the writer is reference/coordinate-only by design, and no amount of
    fixing up our record makes it write a uBAM."""
    out = tmp_path / "mapped.sam"
    conn.execute(f"COPY ({_mapped()}) TO '{out}' (FORMAT SAM, REFERENCE_LENGTHS 'reflens')")
    (record,) = _records(out)
    fields = record.split("\t")
    assert fields[1:6] == ["0", "dummy", "1", "60", "10M"], "the control really is mapped"
    assert fields[9:11] == ["*", "*"], record


def test_bam_writer_requires_a_non_empty_reference_table(conn, tmp_path):
    """A uBAM has no `@SQ` header. The writer cannot express one: REFERENCE_LENGTHS
    is mandatory, and an empty table is rejected rather than yielding a bare header."""
    conn.execute(
        "CREATE TABLE empty_refs AS "
        "SELECT '*'::VARCHAR AS reference, 1::BIGINT AS reference_length WHERE false"
    )
    with pytest.raises(duckdb.Error, match="REFERENCE_LENGTHS"):
        conn.execute(f"COPY ({_mapped()}) TO '{tmp_path / 'a.bam'}' (FORMAT BAM)")
    with pytest.raises(duckdb.Error, match="empty"):
        conn.execute(
            f"COPY ({_mapped()}) TO '{tmp_path / 'b.bam'}' "
            "(FORMAT BAM, REFERENCE_LENGTHS 'empty_refs')"
        )


@pytest.mark.parametrize("option", ["READ_GROUP 'x'", "RG 'x'", "HEADER 'x'", "EXTRA_HEADER 'x'"])
def test_bam_writer_has_no_read_group_option(conn, tmp_path, option):
    """`@RG` with `DS:READTYPE=CCS` is the field lima keys on to treat the reads as
    CCS, and the writer exposes no way to emit a read group at all."""
    with pytest.raises(duckdb.Error, match="Unknown option"):
        conn.execute(
            f"COPY ({_mapped()}) TO '{tmp_path / 'c.bam'}' "
            f"(FORMAT BAM, REFERENCE_LENGTHS 'reflens', {option})"
        )
