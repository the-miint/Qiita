"""Native job: sample a prep_sample's R1 reads, classify them against one SortMeRNA
reference per marker class, and write the `data_type` QC check."""

from __future__ import annotations

from pathlib import Path
from typing import NamedTuple

from pydantic import BaseModel, TypeAdapter
from qiita_common.backend_failure import StepNoData
from qiita_common.models import (
    DataTypeMetrics,
    MarkerClass,
    QcCheck,
    QcCheckResult,
    QcSeverity,
)
from qiita_common.parquet import validate_parquet_path

from ..miint import apply_duckdb_settings, duckdb_tmp_dir, open_miint_conn, resolve_duckdb_memory_gb

YAML_STEP_NAME = "verify_data_type"
QC_CHECKS_FILENAME = "qc_checks.json"

SAMPLE_SIZE = 10_000
MIN_SAMPLED_READS = 1_000
AMPLICON_MIN_TOP_FRACTION = 0.90
METAGENOMIC_MAX_RRNA_FRACTION = 0.05

# The same hit test as amplicon_deblur; no e-value cut, as the library's differ from the
# CLI's. A read hitting several classes goes to its best score, ties to the first class.
_MIN_COVERAGE = 50

_DUCKDB_FALLBACK_MEMORY_GB = 6
_DUCKDB_THREADS = 2

_AMPLICON = frozenset({"short_read_amplicon", "long_read_amplicon"})
_METAGENOMIC = frozenset({"short_read_metagenomics", "long_read_metagenomics"})
_TRANSCRIPTOMIC = frozenset({"short_read_transcriptomics"})

_SAMPLE_SQL = """
CREATE OR REPLACE TABLE sample AS
SELECT sequence_idx AS read_id, sequence1
FROM read_parquet('{reads}')
WHERE length(sequence1) > 0
ORDER BY hash(sequence_idx), sequence_idx
LIMIT {limit}
"""

_CLASS_COUNTS_SQL = """
WITH hits AS ({branches}),
best AS (
    SELECT ordinal, row_number() OVER (PARTITION BY read_id ORDER BY score DESC, ordinal) AS rank
    FROM hits
)
SELECT ordinal, count(*) FROM best WHERE rank = 1 GROUP BY ordinal
"""

_HITS_BRANCH_SQL = """
SELECT {ordinal} AS ordinal, read_id, max(score) AS score
FROM align_sortmerna_rrna('sample', ref_paths := ['{ref}'])
WHERE aligned = 1 AND coverage >= {coverage}
GROUP BY read_id
"""


class Inputs(BaseModel):
    """Typed input contract; `*_reference_idx` arrive as strings and are coerced."""

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


def _verdict(protocol: str, n_sampled: int, fractions: dict[MarkerClass, float]) -> Verdict:
    """The data-type verdict for `protocol`; an unregistered protocol raises."""
    if protocol not in _AMPLICON | _METAGENOMIC | _TRANSCRIPTOMIC:
        raise ValueError(f"no data-type threshold for prep_protocol {protocol!r}")
    if n_sampled < MIN_SAMPLED_READS:
        return Verdict(QcSeverity.INFO, "insufficient_reads", n_sampled, MIN_SAMPLED_READS)
    if protocol in _AMPLICON:
        top = max(fractions.values())
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
    branches = " UNION ALL ".join(
        _HITS_BRANCH_SQL.format(
            ordinal=ordinal, ref=validate_parquet_path(ref), coverage=_MIN_COVERAGE
        )
        for ordinal, ref in enumerate(refs.values())
    )
    classes = list(refs)
    counts = dict.fromkeys(classes, 0)
    for ordinal, n in conn.execute(_CLASS_COUNTS_SQL.format(branches=branches)).fetchall():
        counts[classes[ordinal]] = n
    return counts


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Classify a sample of the reads and write the `data_type` QC check. StepNoData
    when the sample has no reads."""
    if not inputs.reads.exists():
        raise FileNotFoundError(f"reads parquet not found: {inputs.reads}")
    refs = {cls: getattr(inputs, f"{cls}_ref") for cls in MarkerClass}
    for ref in refs.values():
        if not ref.exists():
            raise FileNotFoundError(f"marker reference not found: {ref}")

    workspace.mkdir(parents=True, exist_ok=True)
    memory_gb = resolve_duckdb_memory_gb(_DUCKDB_FALLBACK_MEMORY_GB, threads=_DUCKDB_THREADS)
    with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=_DUCKDB_THREADS)
        conn.execute(
            _SAMPLE_SQL.format(reads=validate_parquet_path(inputs.reads), limit=int(SAMPLE_SIZE))
        )
        (n_sampled,) = conn.execute("SELECT count(*) FROM sample").fetchone()
        if n_sampled == 0:
            raise StepNoData(step_name=YAML_STEP_NAME, reason="the sample has no reads to classify")
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
