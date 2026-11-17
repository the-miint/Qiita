"""Native job: sample a prep_sample's R1 reads, classify them against one SortMeRNA
reference per marker class, and write the `data_type` QC check."""

from __future__ import annotations

from pathlib import Path
from typing import NamedTuple

from pydantic import BaseModel, TypeAdapter
from qiita_common.backend_failure import StepNoData
from qiita_common.models import QcCheck, QcCheckResult, QcSeverity
from qiita_common.parquet import validate_parquet_path
from qiita_common.qc_metrics import DataTypeMetrics, MarkerClass

from ..miint import apply_duckdb_settings, duckdb_tmp_dir, open_miint_conn, resolve_duckdb_memory_gb
from ..read_source import bind_step_reads

YAML_STEP_NAME = "verify_data_type"
QC_CHECKS_FILENAME = "qc_checks.json"

SAMPLE_SIZE = 10_000
MIN_SAMPLED_READS = 1_000
# Amplicon: under AMPLICON_MIN_CLASSIFIED_FRACTION of reads classified is no rRNA marker at all
# (ITS, say), not a mismatch; above it the top class must hold AMPLICON_MIN_TOP_FRACTION of them.
AMPLICON_MIN_CLASSIFIED_FRACTION = 0.5
AMPLICON_MIN_TOP_FRACTION = 0.90
METAGENOMIC_MAX_RRNA_FRACTION = 0.05

# Keep in step with amplicon_deblur's `coverage >= 50`; no e-value cut, as the library's
# differ from the CLI's.
_MIN_COVERAGE = 50

_DUCKDB_FALLBACK_MEMORY_GB = 6
_DUCKDB_THREADS = 2

_AMPLICON = frozenset({"short_read_amplicon", "long_read_amplicon"})
_METAGENOMIC = frozenset({"short_read_metagenomics", "long_read_metagenomics"})
_TRANSCRIPTOMIC = frozenset({"short_read_transcriptomics"})

# hash() spreads the sample across the read set; sequence_idx breaks ties so it is reproducible.
_SAMPLE_SQL = """
CREATE OR REPLACE TABLE sample AS
SELECT sequence_idx AS read_id, sequence1
FROM {reads}
WHERE length(sequence1) > 0
ORDER BY hash(sequence_idx), sequence_idx
LIMIT {limit}
"""

# Subject ids carry the class; a read tied across classes goes to the first in table order.
_SUBJECT_SQL = "SELECT '{cls}:' || read_id AS read_id, sequence1 FROM read_fastx('{ref}')"

_CLASS_COUNTS_SQL = """
SELECT split_part(ref_name, ':', 1) AS cls, count(*)
FROM align_sortmerna_rrna('sample', subject_table := 'marker_subject', num_alignments := 1)
WHERE aligned = 1 AND coverage >= {coverage}
GROUP BY cls
"""


class Inputs(BaseModel):
    reads: Path | None = None
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


def _require_thresholded(protocol: str) -> None:
    if protocol not in _AMPLICON | _METAGENOMIC | _TRANSCRIPTOMIC:
        raise ValueError(f"no data-type threshold for prep_protocol {protocol!r}")


def _verdict(protocol: str, n_sampled: int, fractions: dict[MarkerClass, float]) -> Verdict:
    """The data-type verdict for `protocol`; an unregistered protocol raises."""
    _require_thresholded(protocol)
    if n_sampled < MIN_SAMPLED_READS:
        return Verdict(QcSeverity.INFO, "insufficient_reads", None, None)
    if protocol in _AMPLICON:
        classified = sum(fractions.values())
        if classified < AMPLICON_MIN_CLASSIFIED_FRACTION:
            return Verdict(
                QcSeverity.INFO, "no_rrna_marker", classified, AMPLICON_MIN_CLASSIFIED_FRACTION
            )
        top = max(fractions.values()) / classified
        return _graded(top < AMPLICON_MIN_TOP_FRACTION, top, AMPLICON_MIN_TOP_FRACTION)
    if protocol in _METAGENOMIC:
        rrna = sum(fractions.values())
        return _graded(rrna >= METAGENOMIC_MAX_RRNA_FRACTION, rrna, METAGENOMIC_MAX_RRNA_FRACTION)
    return Verdict(QcSeverity.INFO, None, None, None)


def _graded(failed: bool, measured: float, threshold: float) -> Verdict:
    if failed:
        return Verdict(QcSeverity.FAIL, "data_type_mismatch", measured, threshold)
    return Verdict(QcSeverity.INFO, None, measured, threshold)


def _class_counts(conn, refs: dict[MarkerClass, Path]) -> dict[MarkerClass, int]:
    """Reads per class, each read assigned to its best-scoring class."""
    conn.execute(
        "CREATE OR REPLACE TABLE marker_subject AS "
        + " UNION ALL ".join(
            _SUBJECT_SQL.format(cls=cls.value, ref=validate_parquet_path(ref))
            for cls, ref in refs.items()
        )
    )
    counts = dict.fromkeys(refs, 0)
    for cls, n in conn.execute(_CLASS_COUNTS_SQL.format(coverage=_MIN_COVERAGE)).fetchall():
        counts[MarkerClass(cls)] = n
    return counts


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Classify a sample of the reads and write the `data_type` QC check. StepNoData
    when the prep_sample has no reads."""
    _require_thresholded(inputs.prep_protocol)
    refs = {cls: getattr(inputs, f"{cls}_ref") for cls in MarkerClass}
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
                cls.value: getattr(inputs, f"{cls}_reference_idx") for cls in MarkerClass
            },
        }
    )
    verdict = _verdict(inputs.prep_protocol, n_sampled, fractions)
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
