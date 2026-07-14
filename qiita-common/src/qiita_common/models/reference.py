"""Reference-database models: staging lifecycle, reference metadata, indexes,
terminology / field-data enums, and the closed sequencing-platform and
access-tier enums shared with the biosample, study, and sequencing surfaces."""

from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, Field, model_validator

from qiita_common.auth_constants import MAX_NAME_LENGTH, MAX_VERSION_LENGTH


class ReferenceStatus(StrEnum):
    """Lifecycle states of a reference database during staging.

    Mirrored DB-side by the `status` column on `qiita.reference`, which is a
    plain `TEXT` + `CHECK` column (not a Postgres `CREATE TYPE` ENUM) — so this
    enum is intentionally not covered by the parity tests. Keep this set and
    the matching `CHECK` list in sync by hand.
    """

    PENDING = "pending"
    HASHING = "hashing"
    MINTING = "minting"
    LOADING = "loading"
    # `indexing` is entered only by the host-reference-add workflow, after
    # `loading`, while the rype index is built. Regular references skip it
    # (loading → active directly), so `loading` keeps both outgoing edges.
    INDEXING = "indexing"
    ACTIVE = "active"
    FAILED = "failed"


class ReadMaskReason(StrEnum):
    """Why a read is kept or dropped by a read mask.

    One value per row of the DuckLake `read_mask` table (the `reason` column).
    `pass` survives the mask (its recorded trims are applied by the `read_masked`
    view); every other value excludes the read from `read_masked`. The `qc_*`
    values come from the `qc` step's `filter_read` fail reasons; the `host_*`
    values come from the `host_filter` step's rype / minimap2 hits;
    `twist_no_adaptor` comes from the long-read `lima` adapter chain.

    Reason precedence (privacy-critical): a read that both fails QC and hits the
    host filter records the `host_*` hit, so a host/human read can never leak
    through a code path that only inspects `qc_*`. Host classification runs only
    on the QC-pass subset, so `host_*` only ever overrides `pass`. Each step in
    the chain classifies only rows still `pass` and falls through to the incoming
    reason, so an earlier verdict is never overwritten by a later step.

    `twist_no_adaptor` marks a HiFi read in which lima found no Twist adaptor. Such
    a read is not a library molecule from this run — it is artifactual, not a real
    read whose adapter ligation merely failed. It therefore counts toward `raw`
    only, and is excluded from the biological bucket along with the `qc_*` values.

    `spikein_syndna` marks a SynDNA spike-in: added in the lab, so not a molecule
    from the sample. It is excluded from `biological` and carries its OWN count
    bucket — so the read accounting balances, since a spike-in read leaves
    `biological` and must be accounted somewhere. Its rows are RETAINED in
    `read_mask`, so a later per-insert quantification can re-derive exactly those
    reads without a re-ingest. (That quantification is COVERAGE DEPTH, not this
    count — see the SynDNA cell-count issue.)

    Note the biological predicate is a WHITELIST (`pass` + `host_*`), not
    `NOT LIKE 'qc_%'`: a new reason must be classified explicitly, never bucketed
    as biological by default.

    Backs a DuckLake VARCHAR column, NOT a Postgres `CREATE TYPE ... AS ENUM`.
    Per the enum-parity carve-out in CLAUDE.md (a StrEnum backed by a
    non-Postgres column is a valid, deliberate choice), it has no `ENUM_PAIRS`
    entry and is out of scope for the parity test.
    """

    PASS = "pass"
    QC_TOO_SHORT = "qc_too_short"
    QC_TOO_LONG = "qc_too_long"
    QC_LOW_QUALITY = "qc_low_quality"
    QC_TOO_MANY_N = "qc_too_many_n"
    HOST_RYPE = "host_rype"
    HOST_MINIMAP2 = "host_minimap2"
    TWIST_NO_ADAPTOR = "twist_no_adaptor"
    SPIKEIN_SYNDNA = "spikein_syndna"


class ReadMaskBucket(StrEnum):
    """Which `sequenced_sample` read-count column a `ReadMaskReason` contributes to.

    Every reason counts toward `raw` (it is a row in the mask). Beyond that:

    * `BIOLOGICAL` — a molecule from the sample that survived the technical
      filters. `pass` plus the `host_*` hits: a human read is still a biological
      read, just one we deplete. `quality_filtered` is the `pass` SUBSET of this
      bucket, not a bucket of its own.
    * `SPIKEIN` — added in the lab. Disjoint from BIOLOGICAL, with its own column,
      so the accounting balances: a spike-in read leaves BIOLOGICAL and has to be
      counted somewhere. (This is a MASKING metric. The cell-count model consumes
      per-insert COVERAGE DEPTH, not this read count.)
    * `RAW_ONLY` — counted nowhere else. QC failures, and `twist_no_adaptor`
      (a read with no Twist adaptor is artifactual, not a library molecule).

    This map is a WHITELIST, deliberately. The predicate it replaced was
    `reason NOT LIKE 'qc_%'` — fail-OPEN, so every reason added since would have
    been silently counted as biological. `test_read_mask_buckets` fails on any
    unclassified reason, so a new one must be placed here on purpose.
    """

    BIOLOGICAL = "biological"
    SPIKEIN = "spikein"
    RAW_ONLY = "raw_only"


READ_MASK_BUCKET: dict[ReadMaskReason, ReadMaskBucket] = {
    ReadMaskReason.PASS: ReadMaskBucket.BIOLOGICAL,
    ReadMaskReason.HOST_RYPE: ReadMaskBucket.BIOLOGICAL,
    ReadMaskReason.HOST_MINIMAP2: ReadMaskBucket.BIOLOGICAL,
    ReadMaskReason.SPIKEIN_SYNDNA: ReadMaskBucket.SPIKEIN,
    ReadMaskReason.QC_TOO_SHORT: ReadMaskBucket.RAW_ONLY,
    ReadMaskReason.QC_TOO_LONG: ReadMaskBucket.RAW_ONLY,
    ReadMaskReason.QC_LOW_QUALITY: ReadMaskBucket.RAW_ONLY,
    ReadMaskReason.QC_TOO_MANY_N: ReadMaskBucket.RAW_ONLY,
    ReadMaskReason.TWIST_NO_ADAPTOR: ReadMaskBucket.RAW_ONLY,
}


def read_mask_reason_sql_list(bucket: ReadMaskBucket) -> str:
    """A SQL `IN (...)` list of the reasons in `bucket`, sorted for determinism.

    The single source of truth for the count predicates in
    `qiita_control_plane.actions.library._read_mask_counts`. The data plane's
    `mask_metrics_counts` (Rust) MUST emit the same lists — it cannot import this,
    so the two are kept in lockstep by `test_rust_reason_lists_match_the_python_bucket_map`
    (NOT the block e2e test, whose fixture emits no `spikein_syndna` rows), which asserts
    both paths produce identical counts."""
    reasons = sorted(r.value for r, b in READ_MASK_BUCKET.items() if b is bucket)
    return ", ".join(f"'{r}'" for r in reasons)


class GenomeSource(StrEnum):
    """Controlled vocabulary for a genome's provenance (`qiita.genome.source`).

    `genbank` / `refseq` are the two NCBI assembly repositories; `qiita` marks a
    genome derived from a qiita sample itself — those rows additionally carry
    the originating `prep_sample_idx` on `qiita.genome` (enforced by the
    `genome_qiita_origin_check` biconditional CHECK: prep_sample_idx set iff
    source = 'qiita'). Extend deliberately (a new migration + a new value here);
    ingest rejects anything outside the set.

    Backs the `qiita.genome.source` column, which is plain `TEXT` + `CHECK`
    (`genome_source_check`), NOT a Postgres `CREATE TYPE ... AS ENUM`. Per the
    enum-parity carve-out in CLAUDE.md, it has no `ENUM_PAIRS` entry and is out
    of scope for the parity test; the light `test_genome_schema` CHECK↔StrEnum
    guard catches drift instead. Keep this set and the CHECK list in sync by hand.
    """

    GENBANK = "genbank"
    REFSEQ = "refseq"
    QIITA = "qiita"


class TerminologyStatus(StrEnum):
    """Lifecycle states of a terminology row.

    Mirrors the Postgres `qiita.terminology_status` enum. `loading` while a
    load is in flight; `active` when the load is complete and the row
    reflects a consistent terminology version; `failed` when a load aborted
    and the row's contents may be inconsistent with the source.
    """

    LOADING = "loading"
    ACTIVE = "active"
    FAILED = "failed"


class TerminologyTermObsoletionKind(StrEnum):
    """Reason a terminology_term row was marked obsolete on the most
    recent load.

    Mirrors the Postgres `qiita.terminology_term_obsoletion_kind` enum.
    `source_deprecated` when the source vocabulary deprecates the term;
    `source_merged` when the source merges this term into another;
    `silently_dropped` when the term disappears from a reload without a
    recorded replacement.
    """

    SOURCE_DEPRECATED = "source_deprecated"
    SOURCE_MERGED = "source_merged"
    SILENTLY_DROPPED = "silently_dropped"


class FieldDataType(StrEnum):
    """Closed set of value kinds a biosample/prep_sample field may carry.

    Mirrors the Postgres `qiita.field_data_type` enum. Members map 1:1 to the
    value_* columns on the EAV metadata tables: a field with this data_type
    must have its value written into the matching value_* column. The match
    is enforced at write time by the biosample_metadata_apply_field_contract
    trigger (and its prep-sample twin).
    """

    TEXT = "text"
    NUMERIC = "numeric"
    BOOLEAN = "boolean"
    DATE = "date"
    TERMINOLOGY = "terminology"


class Platform(StrEnum):
    """Closed set of sequencing platforms recognized by the system.

    Mirrors the Postgres `qiita.platform` enum. Values are the canonical
    platform names from ENA's SRA XSD, lowercased for Postgres convention,
    so downstream submission paths can map 1:1 without a translation
    table. New values may be added as additional platforms come online;
    existing values cannot be removed once any row references them.
    """

    ILLUMINA = "illumina"
    PACBIO_SMRT = "pacbio_smrt"
    OXFORD_NANOPORE = "oxford_nanopore"
    DNBSEQ = "dnbseq"
    LS454 = "ls454"
    ION_TORRENT = "ion_torrent"
    COMPLETE_GENOMICS = "complete_genomics"


class Tier(StrEnum):
    """Closed set of access-tier values used for user-to-study access levels
    and for data-visibility requirements.

    Mirrors the Postgres `qiita.tier` enum. Members are listed in ascending
    privilege order; a higher tier implies all lower tiers' privileges.
    `study_access` rows cannot carry `'public'` — a principal with no
    `study_access` row has effective tier `'public'` by absence.
    """

    PUBLIC = "public"
    VIEWER = "viewer"
    MEMBER = "member"
    ADMIN = "admin"


ReferenceKind = Literal["sequence_reference", "taxonomy_authority", "artifact_sequence_set"]
"""Kinds of reference, mirroring the `qiita.reference.kind` TEXT/CHECK column
(NOT a Postgres ENUM — see the reference migrations). `artifact_sequence_set` is
an indexless set of artifact sequences (e.g. the canonical adapter set the QC
step trims against): ingested through the same kind-agnostic reference-add flow,
but carries no taxonomy and builds no rype/minimap2 index."""


class ReferenceCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    version: str = Field(min_length=1, max_length=MAX_VERSION_LENGTH)
    kind: ReferenceKind
    # Orthogonal to `kind`: a host reference is still a sequence_reference,
    # but is used as a negative filter (reads matching it are removed). The
    # rype index built for it is wired in as rype's `negative_index`.
    is_host: bool = False


class ReferenceResponse(BaseModel):
    reference_idx: Annotated[int, Field(gt=0)]
    name: str
    version: str
    kind: ReferenceKind
    status: ReferenceStatus
    is_host: bool
    # `created_by_idx` is the canonical owner reference, FK to qiita.principal.
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime


class PrepProtocolResponse(BaseModel):
    """A prep protocol an operator picks for `submit-bcl-convert
    --prep-protocol-idx`. Mirrors a `qiita.prep_protocol` row; the table's PK
    column is `idx`, surfaced here as `prep_protocol_idx` to match the FK column
    name on `prep_sample`."""

    prep_protocol_idx: Annotated[int, Field(gt=0)]
    name: str
    description: str | None
    retired: bool
    # `created_by_idx` is the canonical owner reference, FK to qiita.principal.
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime


# The minimap2 `.mmi` subject index. DUAL-PURPOSE: the second host-filter pass AND a
# per-shard analysis-alignment index the sharded aligner consumes. Use this neutral
# name in the analysis-reference (sharding / alignment) context;
# HOST_FILTER_INDEX_TYPE_MINIMAP2 below is the SAME value, aliased for the
# host-filter context so that code reads in host-filter terms.
INDEX_TYPE_MINIMAP2 = "minimap2"

# The two search-index types a host reference must carry to host-filter: a rype
# `.ryxdi` minimizer index (first pass) and a minimap2 `.mmi` (second pass). Shared
# so the runner's "must carry both" gate (_resolve_host_filter_indexes) and the
# CLI's submit-host-filter-pool pre-check resolve the same pair instead of pinning
# the literals independently and drifting.
HOST_FILTER_INDEX_TYPE_RYPE = "rype"
HOST_FILTER_INDEX_TYPE_MINIMAP2 = INDEX_TYPE_MINIMAP2
HOST_FILTER_REQUIRED_INDEX_TYPES = frozenset(
    {HOST_FILTER_INDEX_TYPE_RYPE, HOST_FILTER_INDEX_TYPE_MINIMAP2}
)

# The bowtie2 subject index (`.bt2` set), an ANALYSIS-alignment index the sharded
# aligner consumes. bowtie2 is analysis-only (unlike dual-purpose rype/minimap2), so
# it is deliberately NOT in HOST_FILTER_REQUIRED_INDEX_TYPES. Mirrors the
# `reference_index.index_type` CHECK allow-list (plain TEXT+CHECK, no Postgres ENUM
# twin — see CLAUDE.md "Enum parity").
INDEX_TYPE_BOWTIE2 = "bowtie2"

# The whole-reference rype ROUTER `.ryxdi`: a single multi-bucket rype index
# over the entire reference, one bucket per shard (`bucket_name = str(shard_id)`),
# that one `rype_classify` pass turns into the `read_to_shard` table the sharded
# aligners need. Analysis-only (like bowtie2) — NOT in
# HOST_FILTER_REQUIRED_INDEX_TYPES. Written by the `build_routing_index` native
# job with `shard_id` NULL (whole-reference, not per-shard).
#
# Admitted into the `reference_index.index_type` CHECK allow-list by
# 20260711000000_reference_index_rype_router_type.sql: the sharded reference-add
# path builds the router and registers the row. Previously the router was
# native-job-only and its path passed directly to the align job, so no row
# carried it yet.
INDEX_TYPE_RYPE_ROUTER = "rype_router"


class ReferenceIndex(BaseModel):
    """A built search index for a reference (e.g. a rype `.ryxdi` directory).

    The control plane tracks *where* the index lives and *how* it was built;
    the authoritative manifest (buckets, minimizer params, etc.) lives inside
    the index artifact itself. Mirrors the `qiita.reference_index` row. There
    may be more than one index per reference (different `index_type`, or — once
    references can grow — newer generations of the same type).

    A sharded *analysis* index writes one row per shard (`shard_id` 0..N-1); an
    unsharded whole-reference index (a host `rype`/`minimap2`) has `shard_id`
    None. Like `index_type`, `shard_id` carries no allow-list here — the DB
    CHECK (`shard_id IS NULL OR shard_id >= 0`) is authoritative."""

    reference_index_idx: Annotated[int, Field(gt=0)]
    reference_idx: Annotated[int, Field(gt=0)]
    index_type: str
    fs_path: str
    params: dict[str, Any]
    created_at: AwareDatetime
    shard_id: int | None = None


class ReferenceShardIndexStatus(BaseModel):
    """Returned by GET /api/v1/reference/{idx}/shard-index-status.

    Observability for a sharded analysis reference's fan-out build (the
    `plan-shards` -> N x `build-shard-index` -> `finalize-shard` pipeline).
    Makes a reference wedged in `indexing` on a permanently-failed shard
    visible so an operator can redrive the offending ticket.

    `expected_shards` is N, the shard count the planner assigned (COUNT(DISTINCT
    shard_id) over `reference_membership`). `registered_shards` maps each
    expected `index_type` to how many of the N shards have a registered
    `reference_index` row: a type whose shards all completed reads `N`, a wedged
    type reads `< N`, and a wholly-failed type reads `0` (still keyed, so it is
    visible rather than silently absent). The reference reaches `active` only
    once every expected type reaches N — the same count `finalize_shard` gates
    on. `failed_shard_tickets` counts this reference's build-shard-index work
    tickets in `failed`; those are what an operator redrives to unwedge the
    build (its `finalize_shard` re-counts and, as the last observer, flips
    `active`).

    An unsharded reference — or one whose sharding fanned out zero shards (no
    genome-bearing features) — reads `expected_shards=0`, empty
    `registered_shards`, and zero `failed_shard_tickets`."""

    reference_idx: Annotated[int, Field(gt=0)]
    expected_shards: Annotated[int, Field(ge=0)]
    registered_shards: dict[str, Annotated[int, Field(ge=0)]]
    failed_shard_tickets: Annotated[int, Field(ge=0)]


class ReferenceArtifactPurgeResponse(BaseModel):
    """Result of the orchestrator's on-disk reference-artifact cleanup.

    `removed` is True when a `{path_derived}/references/{idx}` directory was
    found and deleted, False when nothing was there (idempotent no-op). `path`
    echoes the directory the orchestrator targeted so the control plane can log
    exactly what was removed."""

    reference_idx: Annotated[int, Field(gt=0)]
    path: str
    removed: bool


class ReferenceDeleteResponse(BaseModel):
    """Summary of a full reference purge across Postgres, DuckLake, and disk.

    Counts are the Postgres rows removed by the cascade; `orphan_feature_count`
    is the subset of this reference's features that no other reference still
    claimed (and so were deleted from `qiita.feature` and the DuckLake
    sequence tables). `artifacts_removed` reflects the orchestrator cleanup."""

    reference_idx: Annotated[int, Field(gt=0)]
    membership_deleted: int
    index_deleted: int
    work_ticket_deleted: int
    orphan_feature_count: int
    artifacts_removed: bool


# `genome_source` / `genome_source_id` and the `genome_fields_consistent`
# validator predate the Parquet refactor (commit 3cac813); under the
# path-based contract genome metadata flows through `genome_map.parquet`
# and the half-set check is enforced at the qiita.genome NOT NULL
# constraint instead (covered by
# test_library_mint_features_genome_map_with_null_source_id_fails). The
# fields and validator are kept so any caller that builds the model with
# genome data still gets the validator's protection.
class FeatureHashEntry(BaseModel):
    sequence_hash: UUID
    genome_source: GenomeSource | None = None
    genome_source_id: str | None = None

    @model_validator(mode="after")
    def genome_fields_consistent(self):
        if (self.genome_source is None) != (self.genome_source_id is None):
            raise ValueError("genome_source and genome_source_id must both be set or both be null")
        return self


# Valid status transitions for references.
VALID_STATUS_TRANSITIONS: dict[ReferenceStatus, set[ReferenceStatus]] = {
    ReferenceStatus.PENDING: {ReferenceStatus.HASHING, ReferenceStatus.FAILED},
    ReferenceStatus.HASHING: {ReferenceStatus.MINTING, ReferenceStatus.FAILED},
    ReferenceStatus.MINTING: {ReferenceStatus.LOADING, ReferenceStatus.FAILED},
    # `loading` keeps its direct `→ active` edge for regular references (which
    # never build an index); the host-reference-add workflow instead routes
    # `loading → indexing → active` while it builds the rype index.
    ReferenceStatus.LOADING: {
        ReferenceStatus.INDEXING,
        ReferenceStatus.ACTIVE,
        ReferenceStatus.FAILED,
    },
    ReferenceStatus.INDEXING: {ReferenceStatus.ACTIVE, ReferenceStatus.FAILED},
    # ACTIVE is a terminal success state. To remediate a broken active reference,
    # delete it and re-create. No direct transition to FAILED — that path is only
    # for in-progress references that encounter errors during ingestion.
    ReferenceStatus.ACTIVE: set(),
    ReferenceStatus.FAILED: {ReferenceStatus.PENDING},
}


class ReferenceStatusUpdate(BaseModel):
    status: ReferenceStatus
