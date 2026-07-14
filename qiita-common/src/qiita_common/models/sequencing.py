"""Sequencing-run / sequenced-pool / sequenced-sample import models.

Bodies and responses for the sequencing-ingestion surface: a sequencing_run
row, one sequenced_pool per lane, and one sequenced_sample (atomically with
its parent prep_sample, prep_sample_to_study links, and prep_sample_metadata
rows) per pool item. Also carries the sequence-range allocator and the mask
definition (read-filtering config identity) models.
"""

from typing import Annotated, Any, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
    model_validator,
)
from pydantic.types import Base64Bytes

from qiita_common.auth_constants import MAX_NAME_LENGTH, MAX_VERSION_LENGTH, SystemRole
from qiita_common.models._base import PatchRequestModel
from qiita_common.models.biosample import GlobalMetadataEntry, MetadataChecklistRef
from qiita_common.models.reference import Platform
from qiita_common.models.work_ticket import WorkTicketState


class SequencingRunCreateRequest(BaseModel):
    """Body for POST /api/v1/sequencing-run.

    `instrument_run_id` is the instrument-assigned identifier and must be
    unique across the system; collision surfaces as 409. `extra_metadata`
    is a free-form JSON object (stored as JSONB).
    """

    model_config = ConfigDict(extra="forbid")

    instrument_run_id: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    platform: Platform
    instrument_model: str | None = None
    instrument_serial: str | None = None
    run_performed_at: AwareDatetime | None = None
    extra_metadata: dict[str, Any] | None = None


class SequencingRunCreateResponse(BaseModel):
    """Returned by POST /api/v1/sequencing-run on success."""

    sequencing_run_idx: Annotated[int, Field(gt=0)]


class SequencingRunResponse(BaseModel):
    """Returned by GET /api/v1/sequencing-run/{sequencing_run_idx}.

    The caller-visible view of a `qiita.sequencing_run` row (the column set
    `repositories.sequencing_run.fetch_sequencing_run` selects, with the row's
    `idx` surfaced as `sequencing_run_idx`). `instrument_model` is the field the
    `submit-host-filter-pool` fan-out reads to forward QC's polyG gate per sample;
    it is nullable (non-bcl runs may not record it).
    """

    sequencing_run_idx: Annotated[int, Field(gt=0)]
    instrument_run_id: str
    platform: Platform
    instrument_model: str | None = None
    instrument_serial: str | None = None
    run_performed_at: AwareDatetime | None = None
    extra_metadata: dict[str, Any] | None = None
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime
    retired: bool
    retired_by_idx: Annotated[int, Field(gt=0)] | None = None
    retired_at: AwareDatetime | None = None
    retire_reason: str | None = None


# same-pattern-ok: per-key wire shape; parallels StudyLookupByAccessionRequest
class SequencingRunLookupByInstrumentRunIdRequest(BaseModel):
    """Resolves a list of instrument_run_id values to sequencing_run idxs in
    one round trip. Body-shaped (not query-params) so a long id list cannot
    exceed nginx's default URL-line cap.
    """

    model_config = ConfigDict(extra="forbid")

    instrument_run_ids: list[Annotated[str, Field(min_length=1)]] = Field(
        min_length=1, max_length=10_000
    )


# same-pattern-ok: per-key wire shape; parallels StudyLookupByAccessionResponse
class SequencingRunLookupByInstrumentRunIdResponse(BaseModel):
    """`resolved` maps each found instrument_run_id to its sequencing_run_idx.
    `missing` lists ids that did not resolve, in input order (deduped).
    """

    model_config = ConfigDict(extra="forbid")

    resolved: dict[str, Annotated[int, Field(gt=0)]]
    missing: list[str]


class SequencedPoolCreateRequest(BaseModel):
    """Body for POST /api/v1/sequencing-run/{sequencing_run_idx}/sequenced-pool.

    `run_preflight_blob` is the run preflight (typically a SQLite file)
    after post-sequencing info has been doped into it.
    Pydantic's Base64Bytes decodes the JSON string field as
    base64 on receive — a plain `bytes` field would otherwise treat the
    incoming string as UTF-8 and the encoded payload would land in BYTEA
    instead of the decoded blob. `run_preflight_filename` is the
    originating file name on disk.

    The preflight is an optional, co-populated pair: send both
    `run_preflight_blob` and `run_preflight_filename` or neither. A
    half-populated pair is rejected (422). When present, each must be
    non-empty (`min_length=1`).
    """

    model_config = ConfigDict(extra="forbid")

    run_preflight_blob: Base64Bytes | None = Field(default=None, min_length=1)
    run_preflight_filename: str | None = Field(default=None, min_length=1)
    extra_metadata: dict[str, Any] | None = None

    @model_validator(mode="after")
    def run_preflight_pair_consistent(self):
        if (self.run_preflight_blob is None) != (self.run_preflight_filename is None):
            raise ValueError(
                "run_preflight_blob and run_preflight_filename must both be"
                " provided or both be omitted"
            )
        return self


class SequencedPoolCreateResponse(BaseModel):
    """Returned by POST /api/v1/sequencing-run/{idx}/sequenced-pool on success."""

    sequenced_pool_idx: Annotated[int, Field(gt=0)]


class PoolReadMetrics(BaseModel):
    """Compute-on-read read-metric rollup for a sequenced_pool.

    The four counts are SUMS over the pool's NON-retired sequenced_samples
    (each NULL until at least one sample in the pool has been processed);
    `fraction_passing_quality_filter` is recomputed from the summed counts via
    `_fraction_passing_quality_filter` — NOT a mean of per-sample fractions — and
    is None when raw is absent or 0. `sample_count` is the pool's non-retired
    sequenced_sample total; `samples_with_metrics` is how many of those carry
    read counts, so a partial rollup (some samples still unprocessed) is
    interpretable rather than looking complete."""

    raw_read_count_r1r2: int | None
    biological_read_count_r1r2: int | None
    quality_filtered_read_count_r1r2: int | None
    # SynDNA spike-ins, disjoint from biological (added in the lab, not a molecule
    # from the sample). Always 0/NULL for protocols that carry no spike-in.
    spikein_read_count_r1r2: int | None
    sample_count: int
    samples_with_metrics: int

    @computed_field  # type: ignore[prop-decorator]
    @property
    def fraction_passing_quality_filter(self) -> float | None:
        """Pool quality_filtered / raw, recomputed from the SUMMED counts (see
        `_fraction_passing_quality_filter`)."""
        return _fraction_passing_quality_filter(
            self.raw_read_count_r1r2, self.quality_filtered_read_count_r1r2
        )


class SequencedPoolResponse(BaseModel):
    """Returned by GET /api/v1/sequencing-run/{R}/sequenced-pool/{P}.

    The pool's caller-visible metadata (the BYTEA `run_preflight_blob` is
    omitted — only `run_preflight_filename` is surfaced) plus the compute-on-read
    read-metric rollup. There is no stored pool-level metric: `read_metrics`
    is aggregated from the constituent sequenced_samples at request time, so it
    never drifts when a sample is re-processed or deleted."""

    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    sequencing_run_idx: Annotated[int, Field(gt=0)]
    run_preflight_filename: str | None
    extra_metadata: dict[str, Any] | None
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime
    read_metrics: PoolReadMetrics


class SampleQCReport(BaseModel):
    """One pool member's persisted QC reports, as carried in PoolQCReport.samples.

    `raw_qc_report` / `filtered_qc_report` are the verbatim qc_report.json
    documents the native `qc_report` job emitted at each point (the
    `{point, layout, read_pairs, mates: {r1, r2}}` shape), or None for a sample
    not yet processed by fastq-to-parquet/1.2.0. `sequenced_pool_item_id` is the
    sample's per-pool item id (lane+barcode); `prep_sample_idx` identifies the
    sample. The blobs are surfaced as-is — the pool report does not re-derive
    them, only merges copies into `merged`."""

    prep_sample_idx: Annotated[int, Field(gt=0)]
    sequenced_pool_item_id: str | None
    raw_qc_report: dict[str, Any] | None
    filtered_qc_report: dict[str, Any] | None


class MateQCAggregate(BaseModel):
    """One mate's (r1 or r2) QC summary pooled across a pool's samples.

    Counts (`reads`, `total_bases`) are plain sums. The means
    (`mean_quality`, `gc_content`, `n_content`, `mean_length`) are
    base- or read-weighted pools of the per-sample means — each per-sample mean
    is multiplied back out by its weight (total_bases for the content/quality
    means, reads for the mean length) to recover the underlying sum, those are
    summed, then divided by the pooled weight. Recovering the sum from a stored
    ratio carries negligible float error, acceptable for a human-facing report.
    A mean is None when no contributing sample carried it. The three histograms
    are per-bucket sums of the per-sample histograms (string bucket keys, the
    same keying the per-sample qc_report uses)."""

    reads: int
    total_bases: int
    mean_quality: float | None
    gc_content: float | None
    n_content: float | None
    min_length: int | None
    max_length: int | None
    mean_length: float | None
    quality_histogram: dict[str, int]
    gc_histogram: dict[str, int]
    length_histogram: dict[str, int]


class PointQCAggregate(BaseModel):
    """One report point (raw or filtered) pooled across a pool's samples.

    `samples` is how many of the pool's samples carried a report at this point;
    `read_pairs` sums their per-sample read_pairs. `mates.r1` is present whenever
    any contributing sample had r1; `mates.r2` is None for an all-single-end
    pool (no sample carried an r2 block)."""

    samples: int
    read_pairs: int
    mates: dict[str, MateQCAggregate | None]


class MergedQCAggregate(BaseModel):
    """Run-level (pool-wide) merge of the per-sample QC reports — the
    multiqc-equivalent summary. `raw` / `filtered` is None when no sample in the
    pool carried a report at that point."""

    raw: PointQCAggregate | None
    filtered: PointQCAggregate | None


def _merge_qc_point(reports: list[dict[str, Any]]) -> PointQCAggregate | None:
    """Pool a list of per-sample qc_report.json documents for ONE point into a
    single PointQCAggregate, or None when the list is empty (no sample carried a
    report at this point).

    Each `report` is one sample's `{point, layout, read_pairs, mates: {r1, r2}}`
    document. The two mates merge independently; an absent mate (None) is
    skipped, so r2 is only present when at least one sample had it."""
    if not reports:
        return None
    mates: dict[str, MateQCAggregate | None] = {}
    for mate in ("r1", "r2"):
        per_sample = [r["mates"][mate] for r in reports if r["mates"].get(mate) is not None]
        mates[mate] = _merge_mate(per_sample) if per_sample else None
    return PointQCAggregate(
        samples=len(reports),
        read_pairs=sum(r["read_pairs"] for r in reports),
        mates=mates,
    )


def _weighted_mean(
    per_sample: list[dict[str, Any]], value_key: str, weight_key: str
) -> float | None:
    """Pool a stored per-sample ratio (`value_key`) weighted by `weight_key`.

    Recovers each sample's underlying sum as ratio * weight, sums those, and
    divides by the pooled weight. None when no sample carries the ratio or the
    pooled weight is 0."""
    num = 0.0
    denom = 0
    for s in per_sample:
        v = s.get(value_key)
        w = s.get(weight_key)
        if v is None or not w:
            continue
        num += v * w
        denom += w
    return num / denom if denom else None


def _merge_histograms(per_sample: list[dict[str, Any]], key: str) -> dict[str, int]:
    """Per-bucket sum of the per-sample `key` histograms (string bucket keys),
    returned ordered by numeric bucket value for a stable response."""
    merged: dict[str, int] = {}
    for s in per_sample:
        for bucket, count in (s.get(key) or {}).items():
            merged[bucket] = merged.get(bucket, 0) + count
    return {k: merged[k] for k in sorted(merged, key=int)}


def _merge_mate(per_sample: list[dict[str, Any]]) -> MateQCAggregate:
    """Pool a list of per-sample mate summaries (each the r1/r2 block of a
    qc_report) into one MateQCAggregate. See MateQCAggregate for the weighting."""
    min_lengths = [s["min_length"] for s in per_sample if s.get("min_length") is not None]
    max_lengths = [s["max_length"] for s in per_sample if s.get("max_length") is not None]
    return MateQCAggregate(
        reads=sum(s["reads"] for s in per_sample),
        total_bases=sum(s["total_bases"] for s in per_sample),
        mean_quality=_weighted_mean(per_sample, "mean_quality", "total_bases"),
        gc_content=_weighted_mean(per_sample, "gc_content", "total_bases"),
        n_content=_weighted_mean(per_sample, "n_content", "total_bases"),
        min_length=min(min_lengths) if min_lengths else None,
        max_length=max(max_lengths) if max_lengths else None,
        mean_length=_weighted_mean(per_sample, "mean_length", "reads"),
        quality_histogram=_merge_histograms(per_sample, "quality_histogram"),
        gc_histogram=_merge_histograms(per_sample, "gc_histogram"),
        length_histogram=_merge_histograms(per_sample, "length_histogram"),
    )


def merge_qc_reports(samples: list[SampleQCReport]) -> MergedQCAggregate:
    """Merge a pool's per-sample QC reports into the run-level MergedQCAggregate.

    Raw and filtered points merge independently over the samples that carry that
    point; a point with no reports yields None. Pure (no DB) so it is unit-tested
    directly from constructed SampleQCReport lists."""
    return MergedQCAggregate(
        raw=_merge_qc_point([s.raw_qc_report for s in samples if s.raw_qc_report is not None]),
        filtered=_merge_qc_point(
            [s.filtered_qc_report for s in samples if s.filtered_qc_report is not None]
        ),
    )


class PoolQCReport(BaseModel):
    """Returned by GET /api/v1/sequencing-run/{R}/sequenced-pool/{P}/qc-report.

    The pool's merged (multiqc-equivalent) QC report: the read-metric rollup
    (reused from the pool metadata endpoint), the run-level `merged` aggregate of
    every per-sample QC report, and the per-sample `samples` detail. Everything
    is compute-on-read — `merged` is aggregated from the constituent
    sequenced_samples' persisted reports at request time, so it never drifts when
    a sample is re-processed or deleted. `samples_with_qc_report` counts how many
    of the pool's non-retired samples carry at least a raw report, so a partial
    pool (some samples still unprocessed) is interpretable."""

    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    sequencing_run_idx: Annotated[int, Field(gt=0)]
    sample_count: int
    samples_with_qc_report: int
    read_metrics: PoolReadMetrics
    merged: MergedQCAggregate
    samples: list[SampleQCReport]


class PoolCompletionStatus(BaseModel):
    """Returned by GET /api/v1/sequencing-run/{R}/sequenced-pool/{P}/completion.

    The pool's end-to-end processing rollup over two stages:

    `demux_state` — the pool-scoped bcl-convert (demux) work ticket's state, the
    stage that stores each sample's reads once. One of completed / in_flight /
    no_data / failed / not_submitted (precedence highest-first when more than one
    bcl-convert ticket exists; not_submitted when there is none).

    The per-sample buckets below — the HOST-MASKING stage: each non-retired
    sequenced_sample classified by the state of its read-mask work tickets (any
    version) and tallied into five mutually-exclusive buckets (precedence,
    highest first, so a sample appears in exactly one):
      completed     — has at least one COMPLETED read-mask ticket.
      in_flight     — no COMPLETED ticket but at least one PENDING/QUEUED/
                      PROCESSING (e.g. a re-submitted retry); work is ongoing.
      no_data       — no COMPLETED and nothing in flight, but at least one
                      NO_DATA (an empty/blank well — a terminal outcome that is
                      NOT a failure). Outranks failed so a sample carrying both a
                      no_data and a stale failed ticket counts as no_data.
      failed        — no COMPLETED, nothing in flight, no NO_DATA, but at least
                      one FAILED.
      not_submitted — no read-mask ticket at all (e.g. a sample a partial
                      submit-host-filter-pool fan-out never reached).

    `complete` is the host-masking done flag: the pool is non-empty and every
    sample is in a terminal-accounted state — COMPLETED or NO_DATA (so a plate
    of real data with empty wells still reaches `complete=True`, and a
    zero-sample pool reads `complete=False`, not vacuously true). `fully_processed`
    is the end-to-end flag: demux completed AND `complete`. Everything is
    compute-on-read over the work_ticket table, so it never drifts when a sample
    is re-processed, re-submitted, or deleted."""

    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    sequencing_run_idx: Annotated[int, Field(gt=0)]
    demux_state: Literal["completed", "in_flight", "no_data", "failed", "not_submitted"]
    sample_count: Annotated[int, Field(ge=0)]
    samples_completed: Annotated[int, Field(ge=0)]
    samples_in_flight: Annotated[int, Field(ge=0)]
    samples_no_data: Annotated[int, Field(ge=0)]
    samples_failed: Annotated[int, Field(ge=0)]
    samples_not_submitted: Annotated[int, Field(ge=0)]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def complete(self) -> bool:
        """True when the pool has samples and every one is in a terminal-
        accounted state for HOST-MASKING: a COMPLETED read-mask ticket or a
        NO_DATA (empty-well) outcome. Says nothing about demux — see
        `fully_processed` for the end-to-end signal."""
        return (
            self.sample_count > 0
            and (self.samples_completed + self.samples_no_data) == self.sample_count
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def fully_processed(self) -> bool:
        """End-to-end done-and-clean flag: the pool's demux COMPLETED and every
        sample finished host-masking (`complete`). The single signal for
        "this pool is fully processed without error."""
        return self.demux_state == "completed" and self.complete


class SequencedPoolPreflightResponse(BaseModel):
    """Returned by GET /api/v1/sequencing-run/{R}/sequenced-pool/{P}/preflight.

    The SA-only read route the bcl-convert prep step calls to materialize
    the sample sheet from the pool's run preflight blob. `Base64Bytes`
    handles the wire-format base64 encoding of the BYTEA column; the
    attribute is raw bytes after deserialization.

    The route 404s if the pool has no preflight (both blob and filename
    NULL on the pool row), so this response model treats both as
    non-nullable.
    """

    model_config = ConfigDict(extra="forbid")

    run_preflight_blob: Base64Bytes
    run_preflight_filename: str = Field(min_length=1)


class SequencedPoolPreflightUpdateLaneRequest(BaseModel):
    """Body for POST /sequencing-run/{R}/sequenced-pool/{P}/preflight/update-lane.

    Bulk-reassigns the `lane` column inside the pool's run-preflight SQLite blob,
    delegating to `run_preflight.update_lane`: every platform-sample row whose
    current lane equals `from_lane` (NULL is a value) is moved to `to_lane`.

    `platform` selects the platform-specific sample table update_lane targets —
    `"illumina"` (illumina_sample) or `"tellseq"` (tellseq_sample). This is the
    run_preflight platform-table key, NOT qiita's `Platform` enum: TellSeq is an
    Illumina library-prep protocol rather than a sequencing platform, so the two
    value sets deliberately differ — do not substitute `Platform` here.

    `from_lane` / `to_lane` are the source and target lane numbers; either may be
    None to match (or clear to) a NULL lane, but they must differ — an identical
    pair is a no-op and is rejected so the SQLite change_log never gains spurious
    entries. A non-null lane must be >= 1, since update_lane reserves -1 as the
    NULL sentinel in its `COALESCE(lane, -1)` comparisons.

    `reason` is the required audit string; update_lane records one change_log row
    per reassigned sample carrying it.
    """

    model_config = ConfigDict(extra="forbid")

    platform: Literal["illumina", "tellseq"]
    from_lane: Annotated[int, Field(ge=1)] | None = None
    to_lane: Annotated[int, Field(ge=1)] | None = None
    reason: str = Field(min_length=1)

    @field_validator("reason")
    @classmethod
    def reason_not_blank(cls, v: str) -> str:
        # min_length=1 alone admits a whitespace-only string, which update_lane
        # would write verbatim into the immutable change_log audit trail — a
        # meaningless record that defeats the point of requiring a reason. Reject
        # blank/whitespace-only (the value is otherwise stored unchanged).
        if not v.strip():
            raise ValueError("reason must not be blank or whitespace-only")
        return v

    @model_validator(mode="after")
    def lanes_must_differ(self):
        if self.from_lane == self.to_lane:
            raise ValueError("from_lane and to_lane are identical; no lane change requested")
        return self


class SequencedPoolPreflightUpdateLaneResponse(BaseModel):
    """Returned by POST .../preflight/update-lane on success.

    `rows_updated` is the number of platform-sample rows whose lane was
    reassigned — 0 when no row currently sits at `from_lane`."""

    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    rows_updated: Annotated[int, Field(ge=0)]


class SequencedPoolDeleteResponse(BaseModel):
    """Summary of a full sequenced_pool purge across Postgres, DuckLake, and disk.

    The `*_deleted` counts (sequenced_sample … work_ticket) are the rows removed
    by the FK-ordered Postgres cascade. `read_rows_deleted` /
    `read_mask_rows_deleted` are the DuckLake rows the data plane purged for the
    pool's prep_samples (the reads its bcl-convert run wrote, plus any masks over
    them); `staged_reads_reaped` is the number of durable on-disk
    `reads/{prep_sample_idx}/read.parquet` copies removed. The three new counts
    default to 0 so a CP-only/dev deploy (no data plane / no shared scratch)
    still constructs.

    The parent `sequencing_run` is intentionally retained (a run may hold other
    pools); biosample rows are also retained — a biosample is a physical sample
    independent of any single prep and is not pool-owned. `prep_sample_deleted`
    therefore never drives biosample GC. See the route docstring (DELETE
    /sequencing-run/{R}/sequenced-pool/{P}) and the delete-cascade action."""

    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    sequenced_sample_deleted: int
    prep_sample_deleted: int
    metadata_deleted: int
    field_exception_deleted: int
    study_link_deleted: int
    work_ticket_deleted: int
    read_rows_deleted: int = 0
    read_mask_rows_deleted: int = 0
    staged_reads_reaped: int = 0


class SequencedSampleCreateRequest(BaseModel):
    """Body for the sequenced-sample composer POST.

    Atomically creates a prep_sample row (with processing_kind='sequenced'),
    its 1:1 sequenced_sample subtype row, one prep_sample_to_study link
    for `primary_study_idx` plus one per entry in `secondary_study_idxs`,
    and one prep_sample_metadata row per metadata entry (resolved against
    prep_sample_global_field by display_name).

    `primary_study_idx` owns the per-display_name prep_sample_study_field
    rows the composer writes for `metadata`; secondary studies see those
    values through the global field slot but do not own the field row.
    The asymmetry is forced by the schema: a prep_sample has at most one
    prep_sample_study_field per global_field_idx, so exactly one of the
    linked studies must be designated. `secondary_study_idxs` must not
    contain `primary_study_idx`; duplicate entries within it are
    collapsed (order-preserving) rather than rejected.

    `metadata` keys must match seeded prep_sample_global_field display_name
    values; unknown names surface as a single 422 listing every bad key.
    The two ENA accession fields are nullable: a sample may already carry
    ENA accessions when it is created (e.g. ingesting already-submitted
    data), or have them written back later after an ENA submission.
    """

    model_config = ConfigDict(extra="forbid")

    biosample_idx: Annotated[int, Field(gt=0)]
    prep_protocol_idx: Annotated[int, Field(gt=0)]
    owner_idx: Annotated[int, Field(gt=0)]
    sequenced_pool_item_id: str = Field(min_length=1)
    primary_study_idx: Annotated[int, Field(gt=0)]
    secondary_study_idxs: list[Annotated[int, Field(gt=0)]] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)
    metadata_checklist_name: str | None = Field(default=None, min_length=1)
    ena_experiment_accession: str | None = Field(default=None, max_length=50)
    ena_run_accession: str | None = Field(default=None, max_length=50)

    @model_validator(mode="after")
    def dedupe_secondary_study_idxs(self):
        # Collapse duplicate secondary studies (order-preserving). A study
        # repeated in secondary_study_idxs is a benign caller convenience,
        # not a conflict, so normalize rather than reject; primary appearing
        # in secondary remains the genuine error, caught next.
        self.secondary_study_idxs = list(dict.fromkeys(self.secondary_study_idxs))
        return self

    @model_validator(mode="after")
    def primary_not_in_secondary(self):
        if self.primary_study_idx in self.secondary_study_idxs:
            raise ValueError(
                f"primary_study_idx ({self.primary_study_idx}) must not appear"
                " in secondary_study_idxs"
            )
        return self


class SequencedSampleCreateResponse(BaseModel):
    """Returned by the sequenced-sample composer POST on success."""

    prep_sample_idx: Annotated[int, Field(gt=0)]
    sequenced_sample_idx: Annotated[int, Field(gt=0)]


def _fraction_passing_quality_filter(
    raw_read_count_r1r2: int | None, quality_filtered_read_count_r1r2: int | None
) -> float | None:
    """quality_filtered / raw — the share of raw reads surviving the full QC +
    host-filter pipeline. Computed on read so it can never drift from the counts.
    None when either bound is absent or raw is 0 (no division). Shared
    by the per-sample (SequencedSampleResponse) and pool-rollup (PoolReadMetrics)
    surfaces; the pool passes its SUMMED counts here, so the pool fraction is
    recomputed from the sums, never a mean of per-sample fractions."""
    if raw_read_count_r1r2 is None or quality_filtered_read_count_r1r2 is None:
        return None
    if raw_read_count_r1r2 == 0:
        return None
    return quality_filtered_read_count_r1r2 / raw_read_count_r1r2


class SequencedSampleResponse(BaseModel):
    """Returned by GET /api/v1/sequenced-sample/{sequenced_sample_idx}.

    Carries every caller-visible column from the sequenced_sample subtype
    row plus the controlling supertype prep_sample row, and embeds a dict
    of every globally-linked metadata value the prep_sample carries,
    keyed on prep_sample_global_field.internal_name. Purely-local
    metadata and metadata whose prep_sample_to_study link has been
    retired are excluded -- both surface as
    prep_sample_metadata.global_field_idx IS NULL via the existing
    schema triggers and are filtered out by the read.

    `effective_updated_at` = GREATEST(prep_sample.updated_at,
    sequenced_sample.updated_at) — a single timestamp that bumps on a
    write to either table, used as the source for the ETag header on
    the GET and the If-Match contract on a future PATCH.
    `caller_system_role` carries the caller's principal.system_role
    verbatim from the database.
    """

    sequenced_sample_idx: Annotated[int, Field(gt=0)]
    prep_sample_idx: Annotated[int, Field(gt=0)]
    biosample_idx: Annotated[int, Field(gt=0)]
    owner_idx: Annotated[int, Field(gt=0)]
    prep_protocol_idx: Annotated[int, Field(gt=0)]
    metadata_checklist: MetadataChecklistRef | None
    sequenced_pool_idx: int | None
    sequenced_pool_item_id: str | None
    ena_experiment_accession: str | None
    ena_run_accession: str | None
    last_submission_at: AwareDatetime | None
    submission_error: str | None
    # Per-stage read counts, both-mates (R1+R2) totals. NULL until the
    # sample is processed by fastq-to-parquet/1.2.0 (the persist-read-metrics
    # action writes them). By the DB CHECK: quality_filtered <= biological and
    # biological + spikein <= raw. `spikein` (SynDNA) is DISJOINT from biological —
    # a spike-in is added in the lab, not a molecule from the sample — and is 0 for
    # protocols that carry none. `qc_*` and `twist_no_adaptor` reads count toward
    # raw only.
    raw_read_count_r1r2: int | None
    biological_read_count_r1r2: int | None
    quality_filtered_read_count_r1r2: int | None
    spikein_read_count_r1r2: int | None
    last_metadata_change_at: AwareDatetime | None
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime
    effective_updated_at: AwareDatetime
    retired: bool
    retired_by_idx: int | None
    retired_at: AwareDatetime | None
    retire_reason: str | None
    global_metadata: dict[str, GlobalMetadataEntry]
    caller_system_role: SystemRole

    @computed_field  # type: ignore[prop-decorator]
    @property
    def fraction_passing_quality_filter(self) -> float | None:
        """quality_filtered / raw for this sample — see
        `_fraction_passing_quality_filter`. Computed on read, never stored, so it
        can't drift from the counts."""
        return _fraction_passing_quality_filter(
            self.raw_read_count_r1r2, self.quality_filtered_read_count_r1r2
        )


class SequencedSamplePatchRequest(PatchRequestModel):
    """Body for PATCH /api/v1/sequenced-sample/{sequenced_sample_idx}.

    Carries the four subtype-table columns editable after creation: the
    two ENA accessions (which may also be set at create time) and the
    submission-tracking pair. Supertype prep_sample fields
    (owner_idx, metadata_checklist_idx) and identity-level columns
    (sequenced_pool_idx, sequenced_pool_item_id) are intentionally
    out of scope; the former will land via a future
    PATCH /prep-sample/{idx} endpoint, the latter are not editable.
    Inherits extra="forbid" and the at_least_one_field rule from
    PatchRequestModel.
    """

    ena_experiment_accession: str | None = Field(default=None, max_length=50)
    ena_run_accession: str | None = Field(default=None, max_length=50)
    last_submission_at: AwareDatetime | None = None
    submission_error: str | None = None


# ---------------------------------------------------------------------------
# Sequence-range allocator
# ---------------------------------------------------------------------------


class SequenceRangeMintRequest(BaseModel):
    """Body for POST /api/v1/sequence-range.

    Allocates `count` contiguous sequence_idx values for `prep_sample_idx`.
    Both idx fields are positive integers; the route layer additionally
    enforces `count <= Settings.max_sequence_mint_count`. Service-account
    callers with `sequence_range:mint` only — humans never mint.

    `work_ticket_idx` records WHICH ticket minted the range, which is what lets a
    reads job tell its own crashed attempt (safe to reuse the orphaned range) from
    a different ticket re-ingesting an already-loaded sample (must not reuse — the
    reads would double). See qiita.sequence_range.minted_by_work_ticket_idx.
    """

    model_config = ConfigDict(extra="forbid")

    prep_sample_idx: Annotated[int, Field(gt=0)]
    count: Annotated[int, Field(gt=0)]
    work_ticket_idx: Annotated[int, Field(gt=0)]


class SequenceRange(BaseModel):
    """Returned by POST /api/v1/sequence-range (201) and
    GET /api/v1/sequence-range/{prep_sample_idx} (200).

    The pair (sequence_idx_start, sequence_idx_stop) is inclusive on
    both ends — `stop - start + 1` is the count of sequence_idx values
    reserved for raw reads belonging to this prep_sample.

    `minted_by_work_ticket_idx` is the ticket that minted the range. A reads job
    reuses an existing range on a mint-409 ONLY when this matches its own ticket
    (a retry of the same step); a different ticket means the reads are already
    registered and reuse would duplicate them. NULL = provenance unknown (minted
    before the column existed, or not unambiguously attributable at backfill), and
    callers treat NULL as not-mine — fail closed.

    `minted_by_work_ticket_state` is that ticket's current state, joined on the
    read-back (NULL on the mint's own response, and NULL when the minter is unknown
    or its row is gone). Ownership alone is not sufficient to reuse a range: if the
    minting ticket already COMPLETED, its reads are registered in the lake, so even
    the same ticket must not re-mint over them. Callers refuse on `completed`.
    """

    prep_sample_idx: Annotated[int, Field(gt=0)]
    sequence_idx_start: Annotated[int, Field(gt=0)]
    sequence_idx_stop: Annotated[int, Field(gt=0)]
    minted_by_work_ticket_idx: int | None = None
    minted_by_work_ticket_state: WorkTicketState | None = None
    created_at: AwareDatetime


# ---------------------------------------------------------------------------
# Mask definition (read-filtering config identity)
# ---------------------------------------------------------------------------


class MaskDefinitionMintRequest(BaseModel):
    """Body for POST /api/v1/mask-definition.

    Mints (or returns the existing) mask_idx for a read-filtering config.
    `params` is the full config blob — host references, QC settings, etc.;
    its canonical-JSON SHA-256 is the dedup key, so the same config always
    resolves to the same mask_idx fleet-wide (idempotent mint). Service-account
    callers with `read_masked:doget` only — humans never mint masks.
    """

    model_config = ConfigDict(extra="forbid")

    filter_workflow: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    filter_version: str = Field(min_length=1, max_length=MAX_VERSION_LENGTH)
    # The config blob the mask_idx is deduplicated on. Must be a JSON object so
    # the hash is over a stable, named-key structure (not a bare scalar/array).
    params: dict[str, Any]


class MaskDefinition(BaseModel):
    """Returned by POST /api/v1/mask-definition (200/201).

    `mask_idx` is the filtering-config discriminator that tags the data plane's
    read_mask / read_masked rows. The same `params` (canonically hashed) always
    yields the same `mask_idx`.
    """

    mask_idx: Annotated[int, Field(gt=0)]
    filter_workflow: str
    filter_version: str
    params: dict[str, Any]
    created_at: AwareDatetime


class MaskDefinitionDeleteResponse(BaseModel):
    """Returned by DELETE /api/v1/mask-definition/{mask_idx}.

    `rows_deleted` is the DuckLake `read_mask` row count the data plane removed
    (0 on an idempotent re-run); the Postgres `mask_definition` row is purged
    last. Referencing `work_ticket` rows detach automatically via the
    `ON DELETE SET NULL` FK."""

    mask_idx: Annotated[int, Field(gt=0)]
    rows_deleted: int


class ReadMaskedDoGetTicketRequest(BaseModel):
    """Body for POST /api/v1/read-masked/ticket/doget.

    Signs a Flight DoGet ticket scoped to a single (prep_sample_idx, mask_idx)
    on the data plane's `read_masked` view. Both identifiers are mandatory: the
    data plane's empty-filter path would dump every sample's pass reads across
    every mask, so the route never signs an unfiltered read_masked ticket
    (the mandatory-filter invariant).
    """

    model_config = ConfigDict(extra="forbid")

    prep_sample_idx: Annotated[int, Field(gt=0)]
    mask_idx: Annotated[int, Field(gt=0)]


class MaskedReadExportSample(BaseModel):
    """One sample in a sequenced_pool's masked-read export manifest.

    `biosample_accession` is None until the biosample is submitted to NCBI. The
    per-sample output filename
    `<biosample_accession>.<sequencing_run_idx>.<sequenced_pool_idx>.<prep_sample_idx>`
    requires it, so the export refuses a sample whose accession is still None —
    the manifest surfaces the None rather than silently dropping the sample, and
    the export route/CLI fails loudly on it. The pool-wide
    `sequencing_run_idx`/`sequenced_pool_idx` live on the manifest, not repeated
    per row.

    `mask_state` is the sample's per-`(mask_idx, prep_sample)` completion gate
    (`qiita.mask_sample.state`): `'completed'` (fully masked — exportable),
    `'pending'` (a block-mask is mid-flight; a covering block hasn't finished, so
    the read_mask is partial — NOT exportable, the ticket route 409s), or `None`
    (no gate row — the per-sample read-mask path or an unmasked sample; the
    all-or-nothing per-sample write means it is exportable). The CLI reads it to
    report which samples it will skip before minting per-sample tickets.
    """

    prep_sample_idx: Annotated[int, Field(gt=0)]
    biosample_accession: str | None
    mask_state: str | None = None


class MaskedReadExportManifest(BaseModel):
    """Returned by GET /admin/sequenced-pool/{sequenced_pool_idx}/masked-read-export.

    The roster of a sequenced_pool's non-retired samples to export under a given
    `mask_idx`, with each sample's filename parts. The caller then mints a
    per-sample DoGet ticket (POST /admin/masked-read-export/ticket) and streams
    each sample's read_masked rows from the data plane, writing parquet/fastq
    locally. system_admin + admin:masked_read_export only.
    """

    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    sequencing_run_idx: Annotated[int, Field(gt=0)]
    mask_idx: Annotated[int, Field(gt=0)]
    samples: list[MaskedReadExportSample]


class MaskedReadExportTicketRequest(BaseModel):
    """Body for POST /admin/masked-read-export/ticket.

    Mints a Flight DoGet ticket scoped to one (prep_sample_idx, mask_idx) on the
    data plane's read_masked view — the human (system_admin) counterpart to the
    service-account POST /read-masked/ticket/doget. Minted just-in-time per
    sample by the export CLI. Both identifiers mandatory (the data plane's
    empty-filter path would dump every sample's pass reads). system_admin +
    admin:masked_read_export only.
    """

    model_config = ConfigDict(extra="forbid")

    prep_sample_idx: Annotated[int, Field(gt=0)]
    mask_idx: Annotated[int, Field(gt=0)]
