"""Isolated unit tests for `qc.execute` (miint chain seams stubbed).

`qc` is a pure `reads.parquet -> qc_reads.parquet` transform keyed by the
already-minted `sequence_idx` (a fastp-equivalent QC: adapter trim -> optional
polyG -> length/quality filter). It is drop-only and `sequence_idx`-preserving,
so the output is a subset of the minted range (benign gaps).

The real `trim_adapters` / `trim_adapters_pe` / `trim_polyg` / `filter_read`
calls need the miint extension, so they're exercised in `test_qc_smoke.py`; here
the two layout seams (`_run_qc_se`, `_run_qc_pe`) are stubbed and we assert the
orchestration:

  - single-end (`sequence2 IS NULL`) and paired-end rows are routed to the
    matching seam;
  - the surviving output is the union of both seams' kept rows, sorted by
    `sequence_idx`, with the 6-column schema preserved and SE rows' R2 NULL;
  - polyG gating: `apply_polyg` is True only for 2-color instruments;
  - the parsed adapter set is threaded to both seams as a constant `VARCHAR[]`;
  - empty (but valid) output when every read is dropped;
  - fail-fast on missing reads / missing or empty adapter FASTA.

The `write_reads` / `read_survivors` fixtures (tests/jobs/conftest.py) own the
fastq_to_parquet 6-col schema.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
import pytest

# Independent oracle for the preserved output schema (deliberately NOT shared
# with the writer fixture, so a drift in either is caught).
_READS_SCHEMA = ["sequence_idx", "read_id", "sequence1", "qual1", "sequence2", "qual2"]


def _schema(path: Path) -> list[str]:
    with duckdb.connect(":memory:") as conn:
        return [
            d[0] for d in conn.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 0").description
        ]


def _rows(path: Path) -> list[tuple]:
    """(sequence_idx, sequence2 IS NULL) per row, sorted — to confirm SE rows
    keep a NULL R2 and PE rows keep a non-NULL R2 through the union/COPY."""
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            f"SELECT sequence_idx, sequence2 IS NULL FROM read_parquet('{path}') "
            "ORDER BY sequence_idx"
        ).fetchall()


def _adapter_fasta(tmp_path: Path, *adapters: str, name: str = "adapters.fasta") -> Path:
    p = tmp_path / name
    p.write_text("".join(f">{i}\n{a}\n" for i, a in enumerate(adapters)))
    return p


_AD = "AGATCGGAAGAGC"  # standard TruSeq adapter prefix


def test_qc_routes_se_pe_and_unions(tmp_path, monkeypatch, write_reads, read_survivors):
    """SE rows go to the SE seam, PE rows to the PE seam; the COPY output is the
    union of both seams' kept rows, sorted, with the 6-col schema preserved and
    SE rows carrying a NULL R2."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(
        tmp_path / "reads.parquet",
        [
            (10, "se1", "AAAA", None),  # SE
            (20, "pe1", "AAAA", "TTTT"),  # PE
            (30, "se2", "CCCC", None),  # SE -> dropped by the SE seam below
            (40, "pe2", "GGGG", "CCCC"),  # PE
        ],
    )

    seen: dict = {}

    def fake_se(conn, src_view, dest_table, *, adapters_sql, apply_polyg):
        seen["se_ids"] = sorted(
            r[0] for r in conn.execute(f"SELECT sequence_idx FROM {src_view}").fetchall()
        )
        seen["se_adapters_sql"] = adapters_sql
        seen["se_polyg"] = apply_polyg
        # keep 10, drop 30 — SE inserts NULL R2
        conn.execute(
            f"INSERT INTO {dest_table} "
            "SELECT sequence_idx, read_id, sequence1, qual1, NULL::VARCHAR, NULL::UTINYINT[] "
            f"FROM {src_view} WHERE sequence_idx = 10"
        )

    def fake_pe(conn, src_view, dest_table, *, adapters_sql, apply_polyg):
        seen["pe_ids"] = sorted(
            r[0] for r in conn.execute(f"SELECT sequence_idx FROM {src_view}").fetchall()
        )
        seen["pe_adapters_sql"] = adapters_sql
        seen["pe_polyg"] = apply_polyg
        # keep both PE rows
        conn.execute(
            f"INSERT INTO {dest_table} "
            "SELECT sequence_idx, read_id, sequence1, qual1, sequence2, qual2 "
            f"FROM {src_view}"
        )

    monkeypatch.setattr(qc, "_run_qc_se", fake_se)
    monkeypatch.setattr(qc, "_run_qc_pe", fake_pe)

    inputs = qc.Inputs(
        reads=reads,
        adapter_fasta=_adapter_fasta(tmp_path, _AD),
        instrument_model=None,
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    out = asyncio.run(qc.execute(inputs, tmp_path / "ws"))

    assert seen["se_ids"] == [10, 30]
    assert seen["pe_ids"] == [20, 40]
    assert read_survivors(out["qc_reads"]) == [10, 20, 40]
    assert _schema(out["qc_reads"]) == _READS_SCHEMA
    # 10 is SE (R2 NULL); 20, 40 are PE (R2 not NULL).
    assert _rows(out["qc_reads"]) == [(10, True), (20, False), (40, False)]


@pytest.mark.parametrize(
    "model,expected",
    [
        ("NextSeq 550", True),
        ("Illumina NovaSeq 6000", True),
        ("MiniSeq", True),
        ("nextseq2000", True),  # case-insensitive
        ("Illumina MiSeq", False),
        ("HiSeq 2500", False),
        (None, False),
        ("", False),
    ],
)
def test_is_two_color(model, expected):
    from qiita_compute_orchestrator.jobs import qc

    assert qc._is_two_color(model) is expected


def test_qc_polyg_gate_threaded_to_seams(tmp_path, monkeypatch, write_reads):
    """`apply_polyg` reflects `_is_two_color(instrument_model)` and is passed to
    both seams (here: a 2-color NextSeq -> True)."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(
        tmp_path / "reads.parquet", [(10, "se", "AAAA", None), (20, "pe", "AA", "TT")]
    )
    seen: dict = {}

    def rec(key):
        def _f(conn, src_view, dest_table, *, adapters_sql, apply_polyg):
            seen[key] = apply_polyg

        return _f

    monkeypatch.setattr(qc, "_run_qc_se", rec("se"))
    monkeypatch.setattr(qc, "_run_qc_pe", rec("pe"))

    inputs = qc.Inputs(
        reads=reads,
        adapter_fasta=_adapter_fasta(tmp_path, _AD),
        instrument_model="NextSeq 550",
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    asyncio.run(qc.execute(inputs, tmp_path / "ws"))
    assert seen == {"se": True, "pe": True}


def test_qc_adapters_threaded_as_constant(tmp_path, monkeypatch, write_reads):
    """Both adapter sequences from the FASTA reach the seams as a constant
    `VARCHAR[]` literal."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(tmp_path / "reads.parquet", [(10, "se", "AAAA", None)])
    seen: dict = {}

    def rec(conn, src_view, dest_table, *, adapters_sql, apply_polyg):
        seen["adapters_sql"] = adapters_sql

    monkeypatch.setattr(qc, "_run_qc_se", rec)
    monkeypatch.setattr(qc, "_run_qc_pe", lambda *a, **k: None)

    inputs = qc.Inputs(
        reads=reads,
        adapter_fasta=_adapter_fasta(tmp_path, "AGATCGGAAGAGC", "CTGTCTCTTATA"),
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    asyncio.run(qc.execute(inputs, tmp_path / "ws"))
    sql = seen["adapters_sql"]
    assert "AGATCGGAAGAGC" in sql
    assert "CTGTCTCTTATA" in sql
    assert sql.endswith("::VARCHAR[]")


def test_qc_empty_output_when_all_dropped(tmp_path, monkeypatch, write_reads, read_survivors):
    """A sample where QC drops every read is valid: an empty (0-row) but
    well-formed qc_reads.parquet, schema intact."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(
        tmp_path / "reads.parquet", [(10, "se", "AAAA", None), (20, "pe", "AA", "TT")]
    )
    monkeypatch.setattr(qc, "_run_qc_se", lambda *a, **k: None)  # insert nothing
    monkeypatch.setattr(qc, "_run_qc_pe", lambda *a, **k: None)

    inputs = qc.Inputs(
        reads=reads,
        adapter_fasta=_adapter_fasta(tmp_path, _AD),
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    out = asyncio.run(qc.execute(inputs, tmp_path / "ws"))
    assert out["qc_reads"].exists()
    assert read_survivors(out["qc_reads"]) == []
    assert _schema(out["qc_reads"]) == _READS_SCHEMA


def test_qc_missing_reads_raises(tmp_path):
    from qiita_compute_orchestrator.jobs import qc

    inputs = qc.Inputs(
        reads=tmp_path / "nope.parquet",
        adapter_fasta=_adapter_fasta(tmp_path, _AD),
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    with pytest.raises(FileNotFoundError):
        asyncio.run(qc.execute(inputs, tmp_path / "ws"))


def test_qc_missing_adapter_fasta_raises(tmp_path, write_reads):
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(tmp_path / "reads.parquet", [(10, "se", "AAAA", None)])
    inputs = qc.Inputs(
        reads=reads,
        adapter_fasta=tmp_path / "nope.fasta",
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    with pytest.raises(FileNotFoundError):
        asyncio.run(qc.execute(inputs, tmp_path / "ws"))


def test_qc_empty_adapter_fasta_raises(tmp_path, write_reads):
    """An adapter FASTA with no records is a misconfiguration — fail fast (QC is
    always-on with a required adapter set)."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(tmp_path / "reads.parquet", [(10, "se", "AAAA", None)])
    empty = tmp_path / "empty.fasta"
    empty.write_text("")
    inputs = qc.Inputs(reads=reads, adapter_fasta=empty, prep_sample_idx=5, work_ticket_idx=1)
    with pytest.raises(ValueError):
        asyncio.run(qc.execute(inputs, tmp_path / "ws"))


def test_read_adapter_fasta_single_multi_and_wrapped(tmp_path):
    """`_read_adapter_fasta` reads adapter sequences via miint's read_fastx —
    one per record, in file order, with wrapped (multi-line) sequences joined.
    Pins the read_fastx `sequence1` contract the job relies on (real miint)."""
    from qiita_compute_orchestrator.jobs import qc
    from qiita_compute_orchestrator.miint import open_miint_conn

    p = tmp_path / "a.fasta"
    p.write_text(">0\nAGATCGGAAGAGC\n>1\nCTGTCT\nCTTATA\n")  # 2nd record wrapped
    with open_miint_conn() as conn:
        assert qc._read_adapter_fasta(conn, p) == ["AGATCGGAAGAGC", "CTGTCTCTTATA"]


def test_read_adapter_fasta_empty_raises(tmp_path):
    """read_fastx throws on an empty/blank file; the reader re-raises ValueError
    (-> BAD_INPUT) rather than leaking the duckdb error."""
    from qiita_compute_orchestrator.jobs import qc
    from qiita_compute_orchestrator.miint import open_miint_conn

    p = tmp_path / "a.fasta"
    p.write_text("\n\n")
    with open_miint_conn() as conn, pytest.raises(ValueError):
        qc._read_adapter_fasta(conn, p)
