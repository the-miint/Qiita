"""verify_data_type: the data-type verdict table and a real-miint run over marker reads."""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
from pathlib import Path

import duckdb
import pytest
import yaml
from pydantic import TypeAdapter
from qiita_common.backend_failure import StepNoData
from qiita_common.models import QcCheck, QcCheckResult, QcSeverity
from qiita_common.qc_metrics import MarkerClass, validate_qc_metrics

from qiita_compute_orchestrator.jobs import verify_data_type
from qiita_compute_orchestrator.jobs.verify_data_type import _verdict
from qiita_compute_orchestrator.miint import open_miint_conn

_MARKERS = Path(__file__).parent / "data" / "markers"
_WORKFLOW = Path(__file__).resolve().parents[3] / "workflows" / "verify-data-type" / "1.0.0.yaml"
_REF_IDX = {cls: 100 + i for i, cls in enumerate(MarkerClass)}
_READ_LEN = 100
_RANDOM_READ_SEED = 7


_N = 5000


def _counts(**by_class: int) -> dict[MarkerClass, int]:
    return {cls: by_class.get(cls.value, 0) for cls in MarkerClass}


@pytest.mark.parametrize("protocol", ["short_read_amplicon", "long_read_amplicon"])
@pytest.mark.parametrize(
    ("by_class", "severity", "reason", "measured", "threshold"),
    [
        ({"marker_16s": 5000}, QcSeverity.INFO, None, 1.0, 0.90),
        ({"marker_16s": 4500}, QcSeverity.INFO, None, 1.0, 0.90),
        ({"marker_16s": 2250, "marker_18s": 250}, QcSeverity.INFO, None, 0.9, 0.90),
        (
            {"marker_16s": 2200, "marker_18s": 300},
            QcSeverity.FAIL,
            "data_type_mismatch",
            0.88,
            0.90,
        ),
        (
            {"marker_16s": 1250, "marker_18s": 1250},
            QcSeverity.FAIL,
            "data_type_mismatch",
            0.5,
            0.90,
        ),
        (
            {c.value: 1000 for c in MarkerClass},
            QcSeverity.FAIL,
            "data_type_mismatch",
            0.25,
            0.90,
        ),
        ({"marker_16s": 2500}, QcSeverity.INFO, None, 1.0, 0.90),
        ({"marker_16s": 2499}, QcSeverity.INFO, "no_rrna_marker", 0.4998, 0.5),
        ({"marker_16s": 500}, QcSeverity.INFO, "no_rrna_marker", 0.1, 0.5),
        ({}, QcSeverity.INFO, "no_rrna_marker", 0.0, 0.5),
    ],
)
def test_amplicon_verdict_table(protocol, by_class, severity, reason, measured, threshold):
    verdict = _verdict(protocol, _N, _counts(**by_class))
    assert (verdict.severity, verdict.reason) == (severity, reason)
    assert (verdict.measured_value, verdict.threshold) == (pytest.approx(measured), threshold)


def test_amplicon_top_class_at_exactly_ninety_percent_is_not_a_fail():
    """477/530 is exactly 0.9 and grades info; 476/530 is below it and fails."""
    counts = _counts(marker_16s=477, marker_18s=53)
    verdict = _verdict("long_read_amplicon", 1000, counts)
    assert (verdict.severity, verdict.reason) == (QcSeverity.INFO, None)
    counts = _counts(marker_16s=476, marker_18s=54)
    assert _verdict("long_read_amplicon", 1000, counts).severity == QcSeverity.FAIL


def test_amplicon_exactly_half_classified_is_graded_not_no_rrna_marker():
    assert _verdict("short_read_amplicon", 1000, _counts(marker_16s=500)).reason is None
    assert _verdict("short_read_amplicon", 1000, _counts(marker_16s=499)).reason == "no_rrna_marker"


@pytest.mark.parametrize("protocol", ["short_read_metagenomics", "long_read_metagenomics"])
@pytest.mark.parametrize(
    ("rrna_n", "severity", "reason"),
    [
        (245, QcSeverity.INFO, None),
        (250, QcSeverity.FAIL, "data_type_mismatch"),
        (0, QcSeverity.INFO, None),
    ],
)
def test_metagenomics_fails_at_five_percent_rrna(protocol, rrna_n, severity, reason):
    counts = _counts(marker_16s=rrna_n // 2, marker_23s_28s=rrna_n - rrna_n // 2)
    verdict = _verdict(protocol, _N, counts)
    assert (verdict.severity, verdict.reason) == (severity, reason)
    assert (verdict.measured_value, verdict.threshold) == (pytest.approx(rrna_n / _N), 0.05)


def test_metagenomics_at_exactly_five_percent_of_a_non_round_sample_fails():
    verdict = _verdict("short_read_metagenomics", 1020, _counts(marker_16s=51))
    assert verdict.severity == QcSeverity.FAIL
    assert _verdict("short_read_metagenomics", 1020, _counts(marker_16s=50)).severity == (
        QcSeverity.INFO
    )


def test_transcriptomics_is_informational_whatever_the_counts():
    verdict = _verdict("short_read_transcriptomics", _N, _counts(marker_16s=50))
    assert (verdict.severity, verdict.reason) == (QcSeverity.INFO, None)
    assert (verdict.measured_value, verdict.threshold) == (None, None)


@pytest.mark.parametrize("protocol", ["short_read_amplicon", "short_read_metagenomics"])
def test_fewer_than_a_thousand_reads_is_insufficient_not_a_fail(protocol):
    verdict = _verdict(protocol, 999, _counts(marker_16s=999))
    assert (verdict.severity, verdict.reason) == (QcSeverity.INFO, "insufficient_reads")
    assert (verdict.measured_value, verdict.threshold) == (None, None)
    assert _verdict(protocol, 1000, _counts(marker_16s=1000)).reason != "insufficient_reads"


def test_an_unregistered_protocol_raises_even_with_few_reads():
    with pytest.raises(ValueError, match="made_up_protocol"):
        _verdict("made_up_protocol", 10, _counts())


def test_sample_order_is_md5_of_the_sequence_idx():
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TABLE r AS SELECT i::BIGINT AS sequence_idx, 'A' AS sequence1"
            " FROM range(1, 101) t(i)"
        )
        conn.execute(verify_data_type._SAMPLE_SQL.format(reads="r", limit=10))
        got = [
            row[0] for row in conn.execute("SELECT read_id FROM sample ORDER BY read_id").fetchall()
        ]
    want = sorted(
        sorted(range(1, 101), key=lambda i: hashlib.md5(str(i).encode()).hexdigest())[:10]
    )
    assert got == want


def _marker_sequences(cls: MarkerClass) -> list[str]:
    with open_miint_conn() as conn:
        rows = conn.execute(
            "SELECT sequence1 FROM read_fastx(?)", [str(_MARKERS / f"{cls.value}.fasta")]
        ).fetchall()
    return [sequence for (sequence,) in rows]


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


@pytest.mark.parametrize(("reverse", "winner"), [(False, "marker_16s"), (True, "marker_18s")])
def test_a_read_hitting_two_classes_with_equal_scores_goes_to_the_first(
    tmp_path, write_reads, monkeypatch, reverse, winner
):
    reads = _write_reads(
        write_reads, tmp_path / "reads.parquet", _marker_reads(MarkerClass.MARKER_18S, 20)
    )
    if reverse:
        class_counts = verify_data_type._class_counts

        def reversed_counts(conn, refs):
            return class_counts(conn, dict(reversed(refs.items())))

        monkeypatch.setattr(verify_data_type, "_class_counts", reversed_counts)
    twin = _MARKERS / "marker_18s.fasta"
    metrics = _entry(_run(reads, tmp_path, marker_16s_ref=twin)).metrics
    assert metrics["dominant_class"] == winner
    assert metrics["fractions"][winner] == 1.0


def test_a_read_goes_to_its_higher_scoring_class_whatever_the_class_order(tmp_path, write_reads):
    degraded = tmp_path / "degraded.fasta"
    lines = []
    for i, seq in enumerate(_marker_sequences(MarkerClass.MARKER_18S)):
        mutated = "".join("A" if j % 7 == 0 and base != "A" else base for j, base in enumerate(seq))
        lines.append(f">d{i}\n{mutated}\n")
    degraded.write_text("".join(lines))
    reads = _write_reads(
        write_reads, tmp_path / "reads.parquet", _marker_reads(MarkerClass.MARKER_18S, 20)
    )
    metrics = _entry(_run(reads, tmp_path, marker_16s_ref=degraded)).metrics
    assert metrics["dominant_class"] == "marker_18s"
    assert metrics["fractions"]["marker_18s"] == 1.0


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


def test_an_unregistered_protocol_fails_before_any_input_is_read(tmp_path):
    with pytest.raises(ValueError, match="made_up_protocol"):
        _run(tmp_path / "absent.parquet", tmp_path, protocol="made_up_protocol")


def test_the_output_is_a_single_json_list(tmp_path, write_reads):
    reads = _write_reads(write_reads, tmp_path / "reads.parquet", _random_reads(5))
    assert len(json.loads(_run(reads, tmp_path).read_text())) == 1


def test_inputs_marker_fields_match_the_marker_classes_and_the_workflow():
    fields = set(verify_data_type.Inputs.model_fields)
    refs = {f"{c.value}_ref" for c in MarkerClass}
    idxs = {f"{c.value}_reference_idx" for c in MarkerClass}
    assert {f for f in fields if f.startswith("marker_") and f.endswith("_ref")} == refs
    assert {f for f in fields if f.endswith("_reference_idx")} == idxs

    workflow = yaml.safe_load(_WORKFLOW.read_text())
    step = workflow["steps"][0]
    assert set(step["inputs"]) == {"reads"} | refs
    assert set(workflow["context_schema"]["required"]) == idxs
    assert set(workflow["context_schema"]["properties"]) == idxs
    assert set(step["params"].values()) >= idxs


def test_amplicon_with_mostly_unclassified_reads_is_info_no_rrna_marker(
    tmp_path, write_reads, monkeypatch
):
    monkeypatch.setattr(verify_data_type, "MIN_SAMPLED_READS", 10)
    sequences = _marker_reads(MarkerClass.MARKER_16S, 8) + _random_reads(32)
    reads = _write_reads(write_reads, tmp_path / "reads.parquet", sequences)
    entry = _entry(_run(reads, tmp_path))
    assert (entry.severity, entry.reason) == (QcSeverity.INFO, "no_rrna_marker")
    assert (entry.measured_value, entry.threshold) == (pytest.approx(0.2), 0.5)


def test_amplicon_split_across_classes_fails(tmp_path, write_reads, monkeypatch):
    monkeypatch.setattr(verify_data_type, "MIN_SAMPLED_READS", 10)
    sequences = _marker_reads(MarkerClass.MARKER_16S, 20) + _marker_reads(
        MarkerClass.MARKER_18S, 20
    )
    reads = _write_reads(write_reads, tmp_path / "reads.parquet", sequences)
    entry = _entry(_run(reads, tmp_path))
    assert (entry.severity, entry.reason) == (QcSeverity.FAIL, "data_type_mismatch")


def test_a_read_covering_under_half_a_marker_is_not_a_hit(tmp_path, write_reads):
    marker = _marker_reads(MarkerClass.MARKER_16S, 20)
    partial = [s[:30] + r[30:] for s, r in zip(marker[:10], _random_reads(10), strict=True)]
    reads = _write_reads(write_reads, tmp_path / "reads.parquet", marker + partial)
    metrics = _entry(_run(reads, tmp_path)).metrics
    assert metrics["fractions"]["marker_16s"] == pytest.approx(20 / 30)
    assert metrics["fractions"]["unclassified"] == pytest.approx(10 / 30)
