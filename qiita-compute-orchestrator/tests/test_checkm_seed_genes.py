"""Pin how `checkm.sh` hands CheckM the gene_call step's genes.

CheckM calls Prodigal on a bin unless `<out>/bins/<bin_id>/genes.faa` exists and is
not empty; `seed_genes` puts gene_call's `genes.faa` / `genes.gff` there before each
run. These tests run that function under bash, and check that the names that it reads
are the ones that gene_call writes and the workflow binds.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import yaml
from qiita_common.assembly_constants import KIND_LCG, KIND_MAG, KIND_UNBINNED

from qiita_compute_orchestrator.jobs import gene_call

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW_DIR = _REPO_ROOT / "workflows" / "long-read-assembly"
_CHECKM_SH = _WORKFLOW_DIR / "checkm.sh"
_WORKFLOW_YAML = _WORKFLOW_DIR / "1.0.2.yaml"


def _function(name: str) -> str:
    """The text of shell function `name` in checkm.sh, from its opening line to the `}`
    that closes it at column 0."""
    lines = _CHECKM_SH.read_text().splitlines()
    start = lines.index(f"{name}() {{")
    end = lines.index("}", start)
    return "\n".join(lines[start : end + 1])


def _seed(gene_calls: Path, kind: str, genomes: Path, out: Path) -> subprocess.CompletedProcess:
    script = "\n".join(
        [
            "set -euo pipefail",
            f"GENE_CALLS_DIR='{gene_calls}'",
            _function("seed_genes"),
            f"seed_genes '{kind}' '{genomes}' '{out}'",
        ]
    )
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=False)


def _subject(gene_calls: Path, kind: str, bin_id: str) -> None:
    d = gene_calls / kind / bin_id
    d.mkdir(parents=True)
    (d / "genes.faa").write_text(f">{bin_id}_1\nM*\n")
    (d / "genes.gff").write_text(f"##gff-version  3\n# {bin_id}\n")


def test_each_subjects_genes_land_where_checkm_looks(tmp_path) -> None:
    """Files go to `<out>/bins/<bin_id>/`, byte for byte; a genome that gene_call found
    no genes for gets no directory, so CheckM calls Prodigal on it."""
    gene_calls, genomes, out = tmp_path / "gc", tmp_path / "genomes", tmp_path / "ck"
    genomes.mkdir()
    for stem in ("bin.1", "s0.ctg000001c", "bin.2"):
        (genomes / f"{stem}.fa").write_text(">c\nACGT\n")
    _subject(gene_calls, KIND_MAG, "bin.1")
    _subject(gene_calls, KIND_MAG, "s0.ctg000001c")

    result = _seed(gene_calls, KIND_MAG, genomes, out)

    assert result.returncode == 0, result.stderr
    assert sorted(p.name for p in (out / "bins").iterdir()) == ["bin.1", "s0.ctg000001c"]
    for stem in ("bin.1", "s0.ctg000001c"):
        for name in ("genes.faa", "genes.gff"):
            seeded = (out / "bins" / stem / name).read_bytes()
            assert seeded == (gene_calls / KIND_MAG / stem / name).read_bytes()


def test_only_the_named_kind_is_seeded(tmp_path) -> None:
    gene_calls, genomes, out = tmp_path / "gc", tmp_path / "genomes", tmp_path / "ck"
    genomes.mkdir()
    (genomes / "c1.fa").write_text(">c1\nACGT\n")
    _subject(gene_calls, KIND_LCG, "c1")

    result = _seed(gene_calls, KIND_UNBINNED, genomes, out)

    assert result.returncode == 0, result.stderr
    assert not out.exists()


def test_genes_for_a_genome_checkm_does_not_score_stop_the_step(tmp_path) -> None:
    """A bin id that gene_call wrote and that names no genome means the two disagree on bin ids,
    and CheckM would call Prodigal on every subject."""
    gene_calls, genomes, out = tmp_path / "gc", tmp_path / "genomes", tmp_path / "ck"
    genomes.mkdir()
    (genomes / "bin.1.fa").write_text(">c\nACGT\n")
    _subject(gene_calls, KIND_MAG, "bin_1")

    result = _seed(gene_calls, KIND_MAG, genomes, out)

    assert result.returncode == 1
    assert "bin_1" in result.stderr


def test_every_checkm_run_is_seeded_with_its_own_kind() -> None:
    """Each run_checkm call names one of the kinds that gene_call writes under, once each,
    and run_checkm seeds before lineage_wf calls Prodigal."""
    calls = [
        ln.split()[1]
        for ln in _CHECKM_SH.read_text().splitlines()
        if ln.strip().startswith("run_checkm ")
    ]
    assert sorted(calls) == sorted([KIND_MAG, KIND_LCG, KIND_UNBINNED])
    body = _function("run_checkm")
    assert body.index("seed_genes ") < body.index("lineage_wf")


def test_checkm_reads_the_directory_gene_call_writes() -> None:
    """checkm.sh reads `<input>/<dir>`: the input is a gene_call output that the 1.0.2 checkm
    step declares, and the directory is the one that gene_call writes CheckM's files to."""
    m = re.search(r'^GENE_CALLS_DIR="\$\(qiita_input (\w+)\)/(\w+)"$', _CHECKM_SH.read_text(), re.M)
    assert m, "checkm.sh no longer sets GENE_CALLS_DIR from a step input"
    binding, subdir = m.groups()
    steps = {
        s["step"]: s for s in yaml.safe_load(_WORKFLOW_YAML.read_text())["steps"] if "step" in s
    }
    assert binding in steps["gene_call"]["outputs"]
    assert binding in steps["checkm"]["inputs"]
    assert subdir == gene_call.CHECKM_DIR
    order = list(steps)
    assert order.index("gene_call") < order.index("checkm")
