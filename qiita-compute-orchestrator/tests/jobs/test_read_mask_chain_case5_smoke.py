"""Real-miint case-5 chain test: syndna -> lima -> qc, un-adaptered spike-ins.

This is the test that would have caught the zero-count bug the reorder fixes. In
case 5 (`syndna_is_twisted == False`) the SynDNA spike-ins carry NO Twist adaptor.
Before the fix, lima ran first and marked them `twist_no_adaptor`; every later step
only re-classified still-`pass` rows, so the spike-in count was STRUCTURALLY zero.

Here syndna runs FIRST (on the raw reads), then lima processes only the still-`pass`
(biological) reads. The assertion: an un-adaptered spike-in ends the chain as
`spikein_syndna`, NOT `twist_no_adaptor`. The old order fails this; the new order
passes it. lima itself is simulated (a container binary) — the fragile part is the
partial-mask threading and reason preservation, which is exercised for real.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
from qiita_common.duckdb_miint import miint_connect_config, miint_install_sql
from qiita_common.models import ReadMaskReason

_SPIKEIN = "ACGGTTACGATCGGATCACTGACTGCATTAGCC" * 12  # the reference sequence
_BIO_INSERT = "ACTACTACTA" * 13  # 130 nt, adaptor-free, > min_length
_ADAPTER = "AGATCGGAAGAGC"
_FEATURE = 77


def _q(seq: str) -> list[int]:
    return [35] * len(seq)


def _build_syndna_index(tmp_path: Path) -> Path:
    conn = duckdb.connect(":memory:", config=miint_connect_config())
    conn.execute(miint_install_sql())
    conn.execute("LOAD miint;")
    conn.execute(
        "CREATE TABLE chunks AS SELECT CAST(? AS BIGINT) feature_idx, "
        "CAST(0 AS INTEGER) chunk_index, CAST(? AS VARCHAR) chunk_data",
        [_FEATURE, _SPIKEIN],
    )
    conn.execute(
        "CREATE TABLE bmap AS SELECT DISTINCT feature_idx, "
        "CAST(feature_idx AS VARCHAR) bucket_name FROM chunks"
    )
    ryxdi = tmp_path / "syndna.ryxdi"
    status = conn.execute(
        "SELECT status FROM rype_index_create(?, ?, mapping_table := 'bmap', "
        "k := 64, w := 25, orient := TRUE)",
        ["chunks", str(ryxdi)],
    ).fetchone()[0]
    assert status == "ok", status
    conn.close()
    return ryxdi


def _write_reads(path: Path, rows: list[tuple[int, str]]) -> Path:
    values = ", ".join(
        "(CAST(5 AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
        "CAST(? AS UTINYINT[]), CAST(NULL AS VARCHAR), CAST(NULL AS UTINYINT[]))"
        for _ in rows
    )
    params: list = []
    for sidx, seq in rows:
        params.extend([sidx, f"r{sidx}", seq, _q(seq)])
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t("
            "prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )
    return path


def _adapters(tmp_path: Path) -> Path:
    p = tmp_path / "adapters.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE a(feature_idx BIGINT, sequence VARCHAR)")
        conn.execute("INSERT INTO a VALUES (0, ?)", [_ADAPTER])
        conn.execute(f"COPY a TO '{p}' (FORMAT PARQUET)")
    return p


def _reasons(path: Path) -> dict[int, str]:
    with duckdb.connect(":memory:") as conn:
        return dict(
            conn.execute(f"SELECT sequence_idx, reason FROM read_parquet('{path}')").fetchall()
        )


def test_case5_chain_spike_in_survives_as_spikein_not_twist_no_adaptor(tmp_path):
    from qiita_compute_orchestrator.jobs import lima_export, lima_mask, qc, syndna

    # read 1: biological — adaptor + insert. read 2: spike-in — a slice of the
    # reference, carrying NO Twist adaptor (the case-5 signature).
    bio_read = _BIO_INSERT + _ADAPTER
    spike_read = _SPIKEIN[50:250]
    reads = _write_reads(tmp_path / "reads.parquet", [(1, bio_read), (2, spike_read)])
    index = _build_syndna_index(tmp_path)

    # syndna FIRST: marks the spike-in on the raw reads, before lima can drop it.
    partial = asyncio.run(
        syndna.execute(
            syndna.Inputs(reads=reads, syndna_rype_path=index, work_ticket_idx=1),
            tmp_path / "ws_syndna",
        )
    )["partial_mask"]
    assert _reasons(partial) == {
        1: ReadMaskReason.PASS.value,
        2: ReadMaskReason.SPIKEIN_SYNDNA.value,
    }

    # lima_export: only the still-`pass` read reaches lima (spike-in excluded).
    exported = asyncio.run(
        lima_export.execute(
            lima_export.Inputs(
                reads=reads,
                lima_args="--hifi-preset ASYMMETRIC --neighbors",
                partial_mask=partial,
                work_ticket_idx=1,
            ),
            tmp_path / "ws_export",
        )
    )
    names = [
        ln[1:] for ln in exported["lima_in_fastq"].read_text().splitlines() if ln.startswith("@")
    ]
    assert names == ["1"], "the spike-in must NOT be exported to lima"

    # simulate lima: it kept read 1 and clipped the adaptor down to the insert.
    lima_out = tmp_path / "lima_out.fastq"
    lima_out.write_text(f"@1 bc=3,3\n{_BIO_INSERT}\n+\n{'I' * len(_BIO_INSERT)}\n")

    # lima_mask: read 1 -> pass (adaptor trimmed); read 2 carried as spikein_syndna.
    partial = asyncio.run(
        lima_mask.execute(
            lima_mask.Inputs(
                reads=reads, lima_out_fastq=lima_out, partial_mask=partial, work_ticket_idx=1
            ),
            tmp_path / "ws_limamask",
        )
    )["partial_mask"]
    assert _reasons(partial)[2] == ReadMaskReason.SPIKEIN_SYNDNA.value

    # qc: classifies the biological read; carries the spike-in verdict verbatim.
    qc_mask = asyncio.run(
        qc.execute(
            qc.Inputs(
                reads=reads,
                adapter_parquet=_adapters(tmp_path),
                partial_mask=partial,
                instrument_model="Illumina MiSeq",
                work_ticket_idx=1,
            ),
            tmp_path / "ws_qc",
        )
    )["qc_mask"]

    final = _reasons(qc_mask)
    # THE FIX: the un-adaptered spike-in is spikein_syndna, not twist_no_adaptor.
    assert final[2] == ReadMaskReason.SPIKEIN_SYNDNA.value
    assert final[2] != ReadMaskReason.TWIST_NO_ADAPTOR.value
    # ...and the biological read passed QC.
    assert final[1] == ReadMaskReason.PASS.value
