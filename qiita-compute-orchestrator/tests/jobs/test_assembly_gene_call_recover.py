"""Tests for the one-off `assembly-gene-call-recover` jobs (`gene_call_recover`,
`assembly_load_recover`) and the source lookup they share (`_recover_source`).

The source is a completed long-read-assembly ticket's workspace under a tmp
PATH_SCRATCH: each step attempt's `output/manifest.json` with its `outputs` map, and
mint-features' map in its attempt directory. The paths are spelled out here rather than
built with `qiita_common.workspace`, so the layout of workspaces already on disk is
pinned independently of the helpers the jobs use. `gene_call.execute` and
`assembly_load.execute` are replaced by recorders: what is pinned is the `Inputs`
each wrapper hands them (the two jobs have their own tests).
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from qiita_compute_orchestrator.jobs import (
    _recover_source,
    assembly_load,
    assembly_load_recover,
    gene_call,
    gene_call_recover,
)

_SOURCE_TICKET = 4834
_RUN = 77
# The tables a recovery registers, spelled out rather than read from the job.
_STAGED_TABLES = ("assembly_membership", "bin_quality", "gene_call", "gene_call_contig")
_PREP_SAMPLE = 41
_TICKET = 9001
_ATTEMPTS = {
    "assemble": 1,
    "bin_refine": 0,
    "checkm": 2,
    "assembly_hash": 0,
    "mint-features": 0,
}
# Each step's outputs, as the step's manifest maps them (relative to its output dir).
_STEP_OUTPUTS = {
    "assemble": {"genomes_dir": "genomes"},
    "bin_refine": {"refined_bins_dir": "refined_bins"},
    "checkm": {"checkm_dir": "checkm"},
    "assembly_hash": {
        "manifest": "manifest.parquet",
        "assembly_chunks": "chunks",
        "bin_map": "bin_map.parquet",
    },
}


def _output_dir(scratch: Path, step: str) -> Path:
    return scratch / "ticket" / str(_SOURCE_TICKET) / step / f"attempt-{_ATTEMPTS[step]}" / "output"


def _mint_features_map(scratch: Path) -> Path:
    attempt = _ATTEMPTS["mint-features"]
    return (
        scratch
        / "ticket"
        / str(_SOURCE_TICKET)
        / "mint-features"
        / f"attempt-{attempt}"
        / "feature_map.parquet"
    )


@pytest.fixture
def scratch(tmp_path, monkeypatch) -> Path:
    """A completed source ticket's workspace, with PATH_SCRATCH pointed at it."""
    root = tmp_path / "scratch"
    for step, outputs in _STEP_OUTPUTS.items():
        out = _output_dir(root, step)
        out.mkdir(parents=True)
        for rel in outputs.values():
            target = out / rel
            if target.suffix:
                target.write_bytes(b"x")
            else:
                target.mkdir()
        (out / "manifest.json").write_text(json.dumps({"files": [], "outputs": outputs}))
    feature_map = _mint_features_map(root)
    feature_map.parent.mkdir(parents=True)
    feature_map.write_bytes(b"x")
    monkeypatch.setattr(
        _recover_source, "get_settings", lambda: SimpleNamespace(path_scratch=str(root))
    )
    return root


@pytest.fixture
def recorded(monkeypatch) -> dict:
    """Replace both jobs' `execute` with a recorder of the Inputs and workspace."""
    seen: dict = {}

    def _recorder(name: str, result: dict):
        async def _execute(inputs, workspace):
            seen[name] = (inputs, workspace)
            return result

        return _execute

    monkeypatch.setattr(gene_call, "execute", _recorder("gene_call", {"gene_calls": Path("g")}))

    async def _load(inputs, workspace):
        """Records, and stages what assembly_load stages: every table it writes."""
        seen["assembly_load"] = (inputs, workspace)
        staging = workspace / "assembly_staging"
        (staging / "assembled_sequence_chunks").mkdir(parents=True)
        for table in (*_STAGED_TABLES, "assembled_sequence", "myloasm_run"):
            (staging / f"{table}.parquet").touch()
        return {"staging_dir": staging}

    monkeypatch.setattr(assembly_load, "execute", _load)
    return seen


def _gene_call_inputs(**overrides) -> gene_call_recover.Inputs:
    fields = {
        "prep_sample_idx": _PREP_SAMPLE,
        "work_ticket_idx": _TICKET,
        "source_work_ticket_idx": _SOURCE_TICKET,
        "assemble_attempt": _ATTEMPTS["assemble"],
        "bin_refine_attempt": _ATTEMPTS["bin_refine"],
    }
    fields.update(overrides)
    return gene_call_recover.Inputs(**fields)


def _load_inputs(tmp_path: Path, **overrides) -> assembly_load_recover.Inputs:
    fields = {
        "gene_calls": tmp_path / "gene_calls",
        "gene_feature_map": tmp_path / "gene_feature_map.parquet",
        "subject_genome_map": tmp_path / "subject_genome_map.parquet",
        "prep_sample_idx": _PREP_SAMPLE,
        "work_ticket_idx": _TICKET,
        "assembly_processing_idx": _RUN,
        "source_work_ticket_idx": _SOURCE_TICKET,
        "assemble_attempt": _ATTEMPTS["assemble"],
        "bin_refine_attempt": _ATTEMPTS["bin_refine"],
        "checkm_attempt": _ATTEMPTS["checkm"],
        "assembly_hash_attempt": _ATTEMPTS["assembly_hash"],
        "mint_features_attempt": _ATTEMPTS["mint-features"],
    }
    fields.update(overrides)
    return assembly_load_recover.Inputs(**fields)


def test_gene_call_runs_over_the_source_tickets_step_outputs(scratch, recorded, tmp_path):
    out = asyncio.run(gene_call_recover.execute(_gene_call_inputs(), tmp_path / "ws"))
    assert out == {"gene_calls": Path("g")}
    inputs, workspace = recorded["gene_call"]
    assert workspace == tmp_path / "ws"
    assert inputs == gene_call.Inputs(
        genomes_dir=(_output_dir(scratch, "assemble") / "genomes").resolve(),
        refined_bins_dir=(_output_dir(scratch, "bin_refine") / "refined_bins").resolve(),
        prep_sample_idx=_PREP_SAMPLE,
        work_ticket_idx=_TICKET,
    )


def test_assembly_load_runs_over_the_source_outputs_under_the_runs_processing_idx(
    scratch, recorded, tmp_path
):
    out = asyncio.run(assembly_load_recover.execute(_load_inputs(tmp_path), tmp_path / "ws"))
    assert out == {"staging_dir": tmp_path / "ws" / "assembly_staging"}
    # Only the run-keyed tables the recovery changes are left to register: the
    # sequences and the myloasm provenance are already in the lake.
    assert sorted(p.name for p in out["staging_dir"].iterdir()) == sorted(
        f"{t}.parquet" for t in _STAGED_TABLES
    )
    inputs, workspace = recorded["assembly_load"]
    assert workspace == tmp_path / "ws"
    hashed = _output_dir(scratch, "assembly_hash")
    assert inputs == assembly_load.Inputs(
        manifest=(hashed / "manifest.parquet").resolve(),
        feature_map=_mint_features_map(scratch),
        assembly_chunks=(hashed / "chunks").resolve(),
        bin_map=(hashed / "bin_map.parquet").resolve(),
        genomes_dir=(_output_dir(scratch, "assemble") / "genomes").resolve(),
        checkm_dir=(_output_dir(scratch, "checkm") / "checkm").resolve(),
        refined_bins_dir=(_output_dir(scratch, "bin_refine") / "refined_bins").resolve(),
        gene_calls=tmp_path / "gene_calls",
        gene_feature_map=tmp_path / "gene_feature_map.parquet",
        subject_genome_map=tmp_path / "subject_genome_map.parquet",
        processing_idx=_RUN,
        prep_sample_idx=_PREP_SAMPLE,
        work_ticket_idx=_TICKET,
    )


@pytest.mark.parametrize("step", ["assemble", "bin_refine", "checkm", "assembly_hash"])
def test_a_missing_step_manifest_is_refused(scratch, recorded, tmp_path, step):
    manifest = _output_dir(scratch, step) / "manifest.json"
    manifest.unlink()
    with pytest.raises(FileNotFoundError, match=str(manifest)):
        asyncio.run(assembly_load_recover.execute(_load_inputs(tmp_path), tmp_path / "ws"))
    assert "assembly_load" not in recorded


@pytest.mark.parametrize(
    ("step", "rel"),
    [("assemble", "genomes"), ("bin_refine", "refined_bins"), ("assembly_hash", "bin_map.parquet")],
)
def test_a_missing_step_output_is_refused(scratch, recorded, tmp_path, step, rel):
    target = _output_dir(scratch, step) / rel
    target.unlink() if target.suffix else target.rmdir()
    with pytest.raises(FileNotFoundError, match=str(target)):
        asyncio.run(assembly_load_recover.execute(_load_inputs(tmp_path), tmp_path / "ws"))
    assert "assembly_load" not in recorded


def test_a_missing_mint_features_map_is_refused(scratch, recorded, tmp_path):
    feature_map = _mint_features_map(scratch)
    feature_map.unlink()
    with pytest.raises(FileNotFoundError, match=str(feature_map)):
        asyncio.run(assembly_load_recover.execute(_load_inputs(tmp_path), tmp_path / "ws"))


def test_an_attempt_that_does_not_exist_is_refused(scratch, recorded, tmp_path):
    with pytest.raises(FileNotFoundError, match="attempt-5"):
        asyncio.run(
            gene_call_recover.execute(_gene_call_inputs(assemble_attempt=5), tmp_path / "ws")
        )
    assert "gene_call" not in recorded


def test_a_manifest_that_does_not_name_the_output_is_refused(scratch, recorded, tmp_path):
    manifest = _output_dir(scratch, "assemble") / "manifest.json"
    manifest.write_text(json.dumps({"files": [], "outputs": {}}))
    with pytest.raises(ValueError, match="names no output 'genomes_dir'"):
        asyncio.run(gene_call_recover.execute(_gene_call_inputs(), tmp_path / "ws"))


def test_a_manifest_output_outside_the_attempt_is_refused(scratch, recorded, tmp_path):
    manifest = _output_dir(scratch, "assemble") / "manifest.json"
    manifest.write_text(json.dumps({"files": [], "outputs": {"genomes_dir": "../../../x"}}))
    with pytest.raises(ValueError, match="resolves outside"):
        asyncio.run(gene_call_recover.execute(_gene_call_inputs(), tmp_path / "ws"))


@pytest.mark.skipif(os.geteuid() == 0, reason="root traverses a mode-000 directory")
def test_a_source_that_cannot_be_looked_at_raises_rather_than_reading_as_missing(
    scratch, recorded, tmp_path
):
    locked = _output_dir(scratch, "assemble").parent
    locked.chmod(0)
    try:
        with pytest.raises(PermissionError):
            asyncio.run(gene_call_recover.execute(_gene_call_inputs(), tmp_path / "ws"))
    finally:
        locked.chmod(0o755)


@pytest.mark.parametrize(
    ("field", "bad"),
    [
        ("assembly_processing_idx", 0),
        ("source_work_ticket_idx", 0),
        ("assemble_attempt", -1),
        ("bin_refine_attempt", -1),
        ("checkm_attempt", -1),
        ("assembly_hash_attempt", -1),
        ("mint_features_attempt", -1),
    ],
)
def test_inputs_refuse_an_identifier_out_of_range(tmp_path, field, bad):
    with pytest.raises(ValidationError):
        _load_inputs(tmp_path, **{field: bad})
