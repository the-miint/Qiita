"""Isolated unit tests for `pacbio_ingest.execute` — the container-output → two
DuckLake Parquets tail of the pacbio-processing workflow.

Calls execute() directly. Covers: happy path (LCG + MAG contigs + CheckM +
DAS_Tool provenance); LCG-only partial store (no MAGs → success, empty quality);
no genomes at all → StepNoData.
"""

from __future__ import annotations

import asyncio

import duckdb
import pytest
from qiita_common.backend_failure import StepNoData

from qiita_compute_orchestrator.jobs.pacbio_ingest import Inputs, execute


def _run(inputs: Inputs, workspace) -> dict:
    return asyncio.run(execute(inputs, workspace))


def _fasta(path, records: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f">{cid}\n{seq}\n" for cid, seq in records.items()))


def _layout(tmp_path):
    genomes = tmp_path / "genomes"
    refined = tmp_path / "refined"
    checkm = tmp_path / "checkm"
    (genomes / "LCG").mkdir(parents=True)
    refined.mkdir(parents=True)
    checkm.mkdir(parents=True)
    return genomes, refined, checkm


def _rows(parquet, cols: str, order: str = "genome_local_id, contig_id"):
    con = duckdb.connect(":memory:")
    out = con.execute(f"SELECT {cols} FROM read_parquet('{parquet}') ORDER BY {order}").fetchall()
    con.close()
    return out


def test_happy_path_lcg_mag_and_quality(tmp_path):
    genomes, refined, checkm = _layout(tmp_path)
    _fasta(genomes / "LCG" / "circ1.fna", {"circ1_contig": "AAAACCCC"})
    _fasta(refined / "bin.1.fa", {"bin1_c1": "GGGG", "bin1_c2": "TTTTTT"})
    (checkm / "checkm_quality.tsv").write_text(
        "genome_local_id\tmarker_lineage\tcompleteness\tcontamination\t"
        "strain_heterogeneity\tgenome_size\tn_contigs\n"
        "bin.1\tk__Bacteria\t95.5\t1.2\t0.0\t10000\t2\n"
    )
    (refined / "das_tool_scores.tsv").write_text(
        "genome_local_id\tdas_tool_score\tsource_binner\nbin.1\t0.87\tmetabat2\n"
    )

    ws = tmp_path / "ws"
    out = _run(
        Inputs(
            genomes_dir=genomes,
            refined_bins_dir=refined,
            checkm_dir=checkm,
            assembler="hifiasm_meta",
            prep_sample_idx=42,
            work_ticket_idx=7,
        ),
        ws,
    )
    staging = out["genome_staging_dir"]

    ag = _rows(
        staging / "assembled_genome.parquet",
        "kind, genome_local_id, contig_id, sequence, length_bp, prep_sample_idx, assembler",
    )
    assert ag == [
        ("MAG", "bin.1", "bin1_c1", "GGGG", 4, 42, "hifiasm_meta"),
        ("MAG", "bin.1", "bin1_c2", "TTTTTT", 6, 42, "hifiasm_meta"),
        ("LCG", "circ1", "circ1_contig", "AAAACCCC", 8, 42, "hifiasm_meta"),
    ]

    gq = _rows(
        staging / "genome_quality.parquet",
        "kind, genome_local_id, completeness, contamination, genome_size, "
        "n_contigs, das_tool_score, source_binner, prep_sample_idx",
        order="genome_local_id",
    )
    assert gq == [("MAG", "bin.1", 95.5, 1.2, 10000, 2, 0.87, "metabat2", 42)]


def test_lcg_only_is_success_with_empty_quality(tmp_path):
    genomes, refined, checkm = _layout(tmp_path)
    _fasta(genomes / "LCG" / "circ1.fna", {"c1": "ACGTACGT"})
    # No MAG bins, no checkm table.

    out = _run(
        Inputs(
            genomes_dir=genomes,
            refined_bins_dir=refined,
            checkm_dir=checkm,
            prep_sample_idx=1,
            work_ticket_idx=1,
        ),
        tmp_path / "ws",
    )
    staging = out["genome_staging_dir"]
    ag = _rows(staging / "assembled_genome.parquet", "kind, genome_local_id")
    assert ag == [("LCG", "circ1")]
    # genome_quality.parquet exists but is empty.
    con = duckdb.connect(":memory:")
    n = con.execute(
        f"SELECT count(*) FROM read_parquet('{staging / 'genome_quality.parquet'}')"
    ).fetchone()[0]
    con.close()
    assert n == 0


def test_no_genomes_is_no_data(tmp_path):
    genomes, refined, checkm = _layout(tmp_path)
    with pytest.raises(StepNoData):
        _run(
            Inputs(
                genomes_dir=genomes,
                refined_bins_dir=refined,
                checkm_dir=checkm,
                prep_sample_idx=1,
                work_ticket_idx=1,
            ),
            tmp_path / "ws",
        )
