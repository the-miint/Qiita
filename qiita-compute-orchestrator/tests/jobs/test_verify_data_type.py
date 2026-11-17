"""verify_data_type: the data-type verdict table and a real-miint run over marker reads."""

from __future__ import annotations

import asyncio
import json
import random
from pathlib import Path

import duckdb
import pytest
from pydantic import TypeAdapter
from qiita_common.backend_failure import StepNoData
from qiita_common.models import MarkerClass, QcCheck, QcCheckResult, QcSeverity
from qiita_common.qc_metrics import validate_qc_metrics

from qiita_compute_orchestrator.jobs import verify_data_type
from qiita_compute_orchestrator.jobs.verify_data_type import _verdict

_MARKERS = Path(__file__).parent / "data" / "markers"
_REF_IDX = {cls: 100 + i for i, cls in enumerate(MarkerClass)}
_READ_LEN = 100
_RANDOM_READ_SEED = 7


def _fractions(**by_class: float) -> dict[MarkerClass, float]:
    return {cls: by_class.get(cls.value, 0.0) for cls in MarkerClass}


@pytest.mark.parametrize("protocol", ["short_read_amplicon", "long_read_amplicon"])
@pytest.mark.parametrize(
    ("top", "severity", "reason"),
    [
        (0.90, QcSeverity.INFO, None),
        (0.8999, QcSeverity.FAIL, "data_type_mismatch"),
        (0.0, QcSeverity.FAIL, "data_type_mismatch"),
    ],
)
def test_amplicon_fails_below_ninety_percent_in_the_top_class(protocol, top, severity, reason):
    verdict = _verdict(protocol, 5000, _fractions(marker_16s=top))
    assert (verdict.severity, verdict.reason) == (severity, reason)
    assert (verdict.measured_value, verdict.threshold) == (top, 0.90)


@pytest.mark.parametrize("protocol", ["short_read_metagenomics", "long_read_metagenomics"])
@pytest.mark.parametrize(
    ("rrna", "severity", "reason"),
    [
        (0.049, QcSeverity.INFO, None),
        (0.05, QcSeverity.FAIL, "data_type_mismatch"),
        (0.0, QcSeverity.INFO, None),
    ],
)
def test_metagenomics_fails_at_five_percent_rrna(protocol, rrna, severity, reason):
    fractions = _fractions(marker_16s=rrna / 2, marker_23s_28s=rrna / 2)
    verdict = _verdict(protocol, 5000, fractions)
    assert (verdict.severity, verdict.reason) == (severity, reason)
    assert (verdict.measured_value, verdict.threshold) == (pytest.approx(rrna), 0.05)


def test_transcriptomics_is_informational_whatever_the_fractions():
    verdict = _verdict("short_read_transcriptomics", 5000, _fractions(marker_16s=0.01))
    assert (verdict.severity, verdict.reason) == (QcSeverity.INFO, None)
    assert (verdict.measured_value, verdict.threshold) == (None, None)


@pytest.mark.parametrize("protocol", ["short_read_amplicon", "short_read_metagenomics"])
def test_fewer_than_a_thousand_reads_is_insufficient_not_a_fail(protocol):
    verdict = _verdict(protocol, 999, _fractions(marker_16s=1.0))
    assert (verdict.severity, verdict.reason) == (QcSeverity.INFO, "insufficient_reads")
    assert (verdict.measured_value, verdict.threshold) == (999, 1000)
    assert _verdict(protocol, 1000, _fractions(marker_16s=1.0)).reason != "insufficient_reads"


def test_an_unregistered_protocol_raises_even_with_few_reads():
    with pytest.raises(ValueError, match="made_up_protocol"):
        _verdict("made_up_protocol", 10, _fractions())


def _marker_sequences(cls: MarkerClass) -> list[str]:
    lines = (_MARKERS / f"{cls.value}.fasta").read_text().splitlines()
    return [line for line in lines if not line.startswith(">")]


def _marker_reads(cls: MarkerClass, n: int) -> list[str]:
    seqs = _marker_sequences(cls)
    return [seqs[i % len(seqs)][5 * i : 5 * i + _READ_LEN] for i in range(n)]


def _random_reads(n: int) -> list[str]:
    rng = random.Random(_RANDOM_READ_SEED)
    return ["".join(rng.choice("ACGT") for _ in range(_READ_LEN)) for _ in range(n)]


def _write_reads(write_reads, path: Path, sequences: list[str], mate2: str | None = None) -> Path:
    rows = [(i + 1, f"r{i}", s, mate2) for i, s in enumerate(sequences)]
    return write_reads(path, rows)


def _inputs(reads: Path, protocol: str = "short_read_amplicon") -> verify_data_type.Inputs:
    refs = {f"{cls.value}_ref": _MARKERS / f"{cls.value}.fasta" for cls in MarkerClass}
    idxs = {f"{cls.value}_reference_idx": str(idx) for cls, idx in _REF_IDX.items()}
    return verify_data_type.Inputs(
        reads=reads,
        prep_protocol=protocol,
        prep_sample_idx=5,
        work_ticket_idx=9,
        **refs,
        **idxs,
    )


def _run(reads: Path, workspace: Path, protocol: str = "short_read_amplicon", **refs) -> Path:
    inputs = _inputs(reads, protocol).model_copy(update=refs)
    out = asyncio.run(verify_data_type.execute(inputs, workspace))
    assert set(out) == {"qc_checks"}
    return out["qc_checks"]


def _entry(path: Path) -> QcCheckResult:
    (entry,) = TypeAdapter(list[QcCheckResult]).validate_json(path.read_bytes())
    return entry


def test_fractions_and_dominant_class_over_marker_and_random_reads(tmp_path, write_reads):
    sequences = (
        _marker_reads(MarkerClass.MARKER_16S, 20)
        + _marker_reads(MarkerClass.MARKER_18S, 10)
        + _marker_reads(MarkerClass.MARKER_23S_28S, 10)
        + _marker_reads(MarkerClass.MARKER_5S_5_8S, 10)
        + _random_reads(10)
    )
    entry = _entry(_run(_write_reads(write_reads, tmp_path / "reads.parquet", sequences), tmp_path))

    assert (entry.prep_sample_idx, entry.check) == (5, QcCheck.DATA_TYPE)
    metrics = validate_qc_metrics(QcCheck.DATA_TYPE, entry.metrics)
    assert metrics["n_sampled"] == 60
    assert metrics["dominant_class"] == "marker_16s"
    assert metrics["fractions"] == {
        "marker_16s": pytest.approx(20 / 60),
        "marker_18s": pytest.approx(10 / 60),
        "marker_23s_28s": pytest.approx(10 / 60),
        "marker_5s_5_8s": pytest.approx(10 / 60),
        "unclassified": pytest.approx(10 / 60),
    }
    assert metrics["marker_reference_idx"] == {c.value: i for c, i in _REF_IDX.items()}
    assert (entry.severity, entry.reason) == (QcSeverity.INFO, "insufficient_reads")


def test_a_read_hitting_two_classes_with_equal_scores_goes_to_the_first(tmp_path, write_reads):
    sequences = _marker_reads(MarkerClass.MARKER_18S, 20)
    reads = _write_reads(write_reads, tmp_path / "reads.parquet", sequences)
    twin = _MARKERS / "marker_18s.fasta"
    metrics = _entry(_run(reads, tmp_path, marker_16s_ref=twin)).metrics
    assert metrics["dominant_class"] == "marker_16s"
    assert metrics["fractions"]["marker_16s"] == 1.0
    assert metrics["fractions"]["marker_18s"] == 0.0


def test_random_reads_hit_no_class(tmp_path, write_reads):
    reads = _write_reads(write_reads, tmp_path / "reads.parquet", _random_reads(40))
    metrics = _entry(_run(reads, tmp_path)).metrics
    assert metrics["dominant_class"] is None
    assert metrics["fractions"]["unclassified"] == 1.0


def test_sample_is_capped_at_sample_size(tmp_path, write_reads, monkeypatch):
    monkeypatch.setattr(verify_data_type, "SAMPLE_SIZE", 30)
    sequences = _marker_reads(MarkerClass.MARKER_16S, 50)
    reads = _write_reads(write_reads, tmp_path / "reads.parquet", sequences)
    assert _entry(_run(reads, tmp_path)).metrics["n_sampled"] == 30


def test_two_runs_write_identical_bytes(tmp_path, write_reads, monkeypatch):
    monkeypatch.setattr(verify_data_type, "SAMPLE_SIZE", 30)
    sequences = _marker_reads(MarkerClass.MARKER_16S, 25) + _marker_reads(
        MarkerClass.MARKER_18S, 25
    )
    reads = _write_reads(write_reads, tmp_path / "reads.parquet", sequences)
    first = _run(reads, tmp_path / "a").read_bytes()
    second = _run(reads, tmp_path / "b").read_bytes()
    assert first == second


def test_the_second_mate_is_ignored(tmp_path, write_reads):
    sequences = _marker_reads(MarkerClass.MARKER_16S, 20)
    plain = _write_reads(write_reads, tmp_path / "plain.parquet", sequences)
    paired = _write_reads(
        write_reads, tmp_path / "paired.parquet", sequences, mate2=_random_reads(1)[0]
    )
    assert (
        _entry(_run(plain, tmp_path / "a")).metrics == _entry(_run(paired, tmp_path / "b")).metrics
    )


def test_a_sample_with_no_reads_ends_as_no_data(tmp_path):
    reads = tmp_path / "reads.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT 1::BIGINT AS sequence_idx, 'A' AS sequence1 WHERE false)"
            f" TO '{reads}' (FORMAT PARQUET)"
        )
    with pytest.raises(StepNoData):
        _run(reads, tmp_path)
    assert not (tmp_path / "qc_checks.json").exists()


def test_an_unregistered_protocol_fails_the_job(tmp_path, write_reads):
    reads = _write_reads(write_reads, tmp_path / "reads.parquet", _random_reads(5))
    with pytest.raises(ValueError, match="made_up_protocol"):
        _run(reads, tmp_path, protocol="made_up_protocol")


def test_the_output_is_a_single_json_list(tmp_path, write_reads):
    reads = _write_reads(write_reads, tmp_path / "reads.parquet", _random_reads(5))
    assert len(json.loads(_run(reads, tmp_path).read_text())) == 1
