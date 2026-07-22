"""Pydantic models for ENA/SRA study metadata resolved via miint
`read_ena` / `read_ena_attributes`, or the plain-HTTP fallback (see
`qiita_control_plane.ena_import`).

`read_ena` returns an ALL-VARCHAR relation — every ENA Portal API field,
including the numeric ones, arrives as text (see
`duckdb-miint/docs/insdc_ena.md`). These models are the boundary where that
untyped text becomes a typed value: the handful of numeric fields (`tax_id`,
`read_count`, `base_count`, `fastq_bytes`) are coerced at construction time,
and a non-blank value that fails to parse raises rather than silently
becoming `None`/`0` — an unresolved/garbled upstream field must fail loud,
never disappear.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field, field_validator


def _split_semicolon_list(value: str | list[str] | None) -> list[str]:
    """ENA Portal API TSV fields that carry one value per run file
    (`fastq_ftp` / `fastq_aspera` / `fastq_md5` — two entries for a
    paired-end run, one for single-end) are semicolon-separated VARCHAR from
    `read_ena`. Split and drop empty segments; a value that already arrives
    pre-split (e.g. from a resolver that parses eagerly) passes through
    unchanged."""
    if value is None:
        return []
    if isinstance(value, list):
        return [item.strip() for item in value if item and item.strip()]
    return [part.strip() for part in value.split(";") if part.strip()]


def _coerce_optional_int(value: str | int | None) -> int | None:
    """Coerce a `read_ena` ALL-VARCHAR numeric field to `int`. Blank/None
    means "ENA has no value for this run" and is preserved as `None`; a
    non-blank value that fails to parse as an integer is a data-corruption
    signal from upstream and must fail loud, never silently become `None`
    or `0`."""
    if value is None or isinstance(value, int):
        return value
    text = value.strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError as exc:
        raise ValueError(f"{value!r} is not a valid integer") from exc


class EnaStudyHeader(BaseModel):
    """One study's header metadata — `read_ena(accession, result='study')`.
    Field set matches `ENAParser::DefaultFields("study")`
    (`duckdb-miint/src/ena_parser.cpp`)."""

    study_accession: str = Field(min_length=1)
    secondary_study_accession: str | None = None
    study_title: str | None = None
    study_description: str | None = None
    center_name: str | None = None
    first_public: str | None = None
    last_updated: str | None = None
    scientific_name: str | None = None
    tax_id: int | None = None

    @field_validator("tax_id", mode="before")
    @classmethod
    def _coerce_tax_id(cls, v: str | int | None) -> int | None:
        return _coerce_optional_int(v)


class EnaRunRecord(BaseModel):
    """One row per sequencing run — `read_ena(accession)` (default
    `result='read_run'`) — joining run/experiment/sample/study accessions
    with the library-prep and fastq-file fields the registration layer
    needs. One row per run; the sample it belongs to is `sample_accession`.
    """

    run_accession: str = Field(min_length=1)
    experiment_accession: str = Field(min_length=1)
    sample_accession: str = Field(min_length=1)
    study_accession: str = Field(min_length=1)
    library_layout: str | None = None
    library_strategy: str | None = None
    library_source: str | None = None
    library_selection: str | None = None
    # ENA's controlled-vocabulary instrument platform (ILLUMINA,
    # OXFORD_NANOPORE, PACBIO_SMRT, ...). Carried through unmapped -- the
    # registration layer (ena_import.platform_mapping) maps it to
    # qiita_common.models.Platform, fail-loud on an unrecognized value.
    instrument_platform: str | None = None
    fastq_ftp: list[str] = Field(default_factory=list)
    fastq_aspera: list[str] = Field(default_factory=list)
    fastq_bytes: list[int] = Field(default_factory=list)
    fastq_md5: list[str] = Field(default_factory=list)
    read_count: int | None = None
    base_count: int | None = None

    @field_validator("fastq_ftp", "fastq_aspera", "fastq_md5", mode="before")
    @classmethod
    def _coerce_fastq_lists(cls, v: str | list[str] | None) -> list[str]:
        return _split_semicolon_list(v)

    @field_validator("fastq_bytes", mode="before")
    @classmethod
    def _coerce_fastq_bytes(cls, v: str | list[str] | list[int] | None) -> list[int]:
        parts: list[str | int] = _split_semicolon_list(v) if isinstance(v, str | type(None)) else v
        try:
            return [int(part) for part in parts]
        except (ValueError, TypeError) as exc:
            raise ValueError(f"fastq_bytes={v!r} contains a non-integer segment") from exc

    @field_validator("read_count", "base_count", mode="before")
    @classmethod
    def _coerce_counts(cls, v: str | int | None) -> int | None:
        return _coerce_optional_int(v)


class EnaSampleAttributes(BaseModel):
    """One BioSample's submitter-defined tag -> value attribute map —
    `read_ena_attributes(accession)`, pivoted from its (sample_accession,
    tag, value) row shape into one map per sample (see
    `ena_import.resolver.pivot_sample_attributes`)."""

    sample_accession: str = Field(min_length=1)
    attributes: dict[str, str] = Field(default_factory=dict)

    @field_validator("attributes")
    @classmethod
    def _validate_tags(cls, v: dict[str, str]) -> dict[str, str]:
        for tag, value in v.items():
            if not tag or not tag.strip():
                raise ValueError(f"attribute tag must be a non-empty string; got {tag!r}")
            if not isinstance(value, str):
                raise ValueError(f"attribute value for tag {tag!r} must be a string; got {value!r}")
        return v


class SourceArchive(StrEnum):
    """Public archive an `ena_import`-registered `sequenced_sample` row's
    metadata (and, once the download workflow lands, its read bytes) was
    resolved from.

    Mirrored by the `qiita.sequenced_sample.source_archive` TEXT/CHECK
    constraint (db/migrations/20260721000000_sequenced_sample_ena_provenance.sql)
    — not a Postgres ENUM; same carve-out as `UploadStatus` / `ReferenceStatus`;
    see CLAUDE.md "Enum parity". Keep both sides in sync by hand."""

    ENA = "ena"
    SRA = "sra"


class ResolverKind(StrEnum):
    """Which `qiita_control_plane.ena_import.EnaResolver` implementation
    produced a `sequenced_sample` row's imported metadata — matches
    `qiita_control_plane.ena_import.BACKEND_MIINT` / `BACKEND_HTTP`.

    Mirrored by the `qiita.sequenced_sample.resolver_kind` TEXT/CHECK
    constraint (db/migrations/20260721000000_sequenced_sample_ena_provenance.sql)
    — not a Postgres ENUM; same carve-out as `UploadStatus` / `ReferenceStatus`;
    see CLAUDE.md "Enum parity". Keep both sides in sync by hand."""

    MIINT = "miint"
    HTTP = "http"
