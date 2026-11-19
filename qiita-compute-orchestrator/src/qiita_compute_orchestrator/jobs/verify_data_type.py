"""Native job: sample a prep_sample's R1 reads, classify them against one SortMeRNA
reference per marker class, and write the `data_type` QC check."""

from __future__ import annotations

from fractions import Fraction
from pathlib import Path
from typing import NamedTuple

from pydantic import BaseModel, TypeAdapter
from qiita_common.backend_failure import StepNoData
from qiita_common.models import QcCheck, QcCheckResult, QcSeverity
from qiita_common.parquet import validate_parquet_path
from qiita_common.qc_metrics import (
    DATA_TYPE_THRESHOLD_KINDS,
    MARKER_REF_BINDINGS,
    MARKER_REFERENCE_IDX_KEYS,
    DataTypeMetrics,
    DataTypeThresholdKind,
    MarkerClass,
)

from ..miint import apply_duckdb_settings, duckdb_tmp_dir, open_miint_conn, resolve_duckdb_memory_gb
from ..read_source import bind_step_reads

YAML_STEP_NAME = "verify_data_type"
QC_CHECKS_FILENAME = "qc_checks.json"

SAMPLE_SIZE = 10_000
MIN_SAMPLED_READS = 1_000
# Amplicon: under AMPLICON_MIN_CLASSIFIED of reads classified is no rRNA marker at all (ITS,
# say), not a mismatch; above it the top class must hold AMPLICON_MIN_TOP of them. Exact
# fractions so a boundary count is never decided by float rounding.
AMPLICON_MIN_CLASSIFIED = Fraction(1, 2)
AMPLICON_MIN_TOP = Fraction(9, 10)
METAGENOMIC_MAX_RRNA = Fraction(1, 20)

# Keep in step with amplicon_deblur's `coverage >= 50`.
_MIN_COVERAGE = 50

_DUCKDB_FALLBACK_MEMORY_GB = 6
_DUCKDB_THREADS = 2

# md5 is fixed across DuckDB versions; hash() is not, and the subsample must be reproducible.
_SAMPLE_SQL = """
CREATE OR REPLACE TABLE sample AS
SELECT sequence_idx AS read_id, sequence1
FROM {reads}
WHERE length(sequence1) > 0
ORDER BY md5(CAST(sequence_idx AS VARCHAR)), sequence_idx
LIMIT {limit}
"""

_CLASS_HIT_SQL = """
INSERT INTO class_hit
SELECT {ordinal}, read_id, score, coverage
FROM align_sortmerna_rrna('sample', subject_table := 'marker_subject', num_alignments := 1)
WHERE aligned = 1
"""

# Each read goes to its highest-scoring class, a tie to the first in `refs`; coverage is
# judged on that winning hit.
_CLASS_COUNTS_SQL = """
SELECT ordinal, count(*) FROM (
    SELECT ordinal, coverage,
           row_number() OVER (PARTITION BY read_id ORDER BY score DESC, ordinal) AS rn
    FROM class_hit
)
WHERE rn = 1 AND coverage >= {coverage}
GROUP BY ordinal
"""


class Inputs(BaseModel):
    reads: Path
    marker_16s_ref: Path
    marker_18s_ref: Path
    marker_23s_28s_ref: Path
    marker_5s_5_8s_ref: Path
    marker_16s_reference_idx: int
    marker_18s_reference_idx: int
    marker_23s_28s_reference_idx: int
    marker_5s_5_8s_reference_idx: int
    prep_protocol: str
    prep_sample_idx: int
    work_ticket_idx: int


class Verdict(NamedTuple):
    severity: QcSeverity
    reason: str | None
    measured_value: float | None
    threshold: float | None


def _threshold_kind(protocol: str) -> DataTypeThresholdKind:
    kind = DATA_TYPE_THRESHOLD_KINDS.get(protocol)
    if kind is None:
        raise ValueError(f"no data-type threshold for prep_protocol {protocol!r}")
    return kind


def _verdict(protocol: str, n_sampled: int, counts: dict[MarkerClass, int]) -> Verdict:
    """The data-type verdict for `protocol` from per-class read counts; an unregistered
    protocol raises."""
    kind = _threshold_kind(protocol)
    if n_sampled < MIN_SAMPLED_READS:
        return Verdict(QcSeverity.INFO, "insufficient_reads", None, None)
    if kind is DataTypeThresholdKind.AMPLICON:
        classified_n = sum(counts.values())
        if Fraction(classified_n, n_sampled) < AMPLICON_MIN_CLASSIFIED:
            return Verdict(
                QcSeverity.INFO,
                "no_rrna_marker",
                classified_n / n_sampled,
                float(AMPLICON_MIN_CLASSIFIED),
            )
        top = Fraction(max(counts.values()), classified_n)
        return _graded(top < AMPLICON_MIN_TOP, top, AMPLICON_MIN_TOP)
    if kind is DataTypeThresholdKind.METAGENOMIC:
        rrna = Fraction(sum(counts.values()), n_sampled)
        return _graded(rrna >= METAGENOMIC_MAX_RRNA, rrna, METAGENOMIC_MAX_RRNA)
    return Verdict(QcSeverity.INFO, None, None, None)


def _graded(failed: bool, measured: Fraction, threshold: Fraction) -> Verdict:
    if failed:
        return Verdict(QcSeverity.FAIL, "data_type_mismatch", float(measured), float(threshold))
    return Verdict(QcSeverity.INFO, None, float(measured), float(threshold))


def _class_counts(conn, refs: dict[MarkerClass, Path]) -> dict[MarkerClass, int]:
    """Reads per class, each read assigned to its best-scoring class in SQL, not by the
    aligner's tie order; a read whose best hit covers under `_MIN_COVERAGE`% is unclassified."""
    conn.execute(
        "CREATE OR REPLACE TABLE class_hit "
        "(ordinal INTEGER, read_id BIGINT, score INTEGER, coverage DOUBLE)"
    )
    for ordinal, ref in enumerate(refs.values()):
        conn.execute(
            "CREATE OR REPLACE TABLE marker_subject AS "
            f"SELECT read_id, sequence1 FROM read_fastx('{validate_parquet_path(ref)}')"
        )
        conn.execute(_CLASS_HIT_SQL.format(ordinal=ordinal))
    classes = list(refs)
    counts = dict.fromkeys(refs, 0)
    for ordinal, n in conn.execute(_CLASS_COUNTS_SQL.format(coverage=_MIN_COVERAGE)).fetchall():
        counts[classes[ordinal]] = n
    return counts


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Classify a subsample of the reads and write the `data_type` QC check. StepNoData
    when the prep_sample has no reads."""
    _threshold_kind(inputs.prep_protocol)
    refs = {cls: getattr(inputs, MARKER_REF_BINDINGS[cls]) for cls in MarkerClass}
    for ref in refs.values():
        if not ref.exists():
            raise FileNotFoundError(f"marker reference not found: {ref}")

    workspace.mkdir(parents=True, exist_ok=True)
    memory_gb = resolve_duckdb_memory_gb(_DUCKDB_FALLBACK_MEMORY_GB, threads=_DUCKDB_THREADS)
    with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=_DUCKDB_THREADS)
        async with bind_step_reads(
            conn,
            reads=inputs.reads,
            work_ticket_idx=inputs.work_ticket_idx,
            workspace=duckdb_tmp,
        ) as reads_rel:
            conn.execute(_SAMPLE_SQL.format(reads=reads_rel, limit=int(SAMPLE_SIZE)))
        (n_sampled,) = conn.execute("SELECT count(*) FROM sample").fetchone()
        if n_sampled == 0:
            raise StepNoData(
                step_name=YAML_STEP_NAME, reason="the prep_sample has no reads to classify"
            )
        counts = _class_counts(conn, refs)

    fractions = {cls: n / n_sampled for cls, n in counts.items()}
    unclassified = (n_sampled - sum(counts.values())) / n_sampled
    dominant = max(counts, key=counts.get) if any(counts.values()) else None
    metrics = DataTypeMetrics.model_validate(
        {
            "n_sampled": n_sampled,
            "fractions": {
                **{cls.value: f for cls, f in fractions.items()},
                "unclassified": unclassified,
            },
            "dominant_class": dominant,
            "marker_reference_idx": {
                cls.value: getattr(inputs, MARKER_REFERENCE_IDX_KEYS[cls]) for cls in MarkerClass
            },
        }
    )
    verdict = _verdict(inputs.prep_protocol, n_sampled, counts)
    check = QcCheckResult(
        prep_sample_idx=inputs.prep_sample_idx,
        check=QcCheck.DATA_TYPE,
        severity=verdict.severity,
        reason=verdict.reason,
        measured_value=verdict.measured_value,
        threshold=verdict.threshold,
        metrics=metrics.model_dump(mode="json"),
    )
    out = workspace / QC_CHECKS_FILENAME
    out.write_bytes(TypeAdapter(list[QcCheckResult]).dump_json([check]))
    return {"qc_checks": out}
