"""Pydantic models for INSDC study metadata resolved via miint `read_ena` /
`read_ena_attributes`.

`read_ena` returns typed columns (duckdb-miint#178): numeric fields arrive as
`int | None`, and per-file fields arrive as `list[...]`. These models validate
the typed data at construction.
"""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class EnaStudyHeader(BaseModel):
    """One study's header metadata — `read_ena(accession, result='study')`.
    Field set matches `ENAParser::DefaultFields("study")`."""

    study_accession: str = Field(min_length=1)
    secondary_study_accession: str | None = None
    study_title: str | None = None
    study_description: str | None = None
    center_name: str | None = None
    first_public: str | None = None
    last_updated: str | None = None
    scientific_name: str | None = None
    tax_id: int | None = None


class EnaRunRecord(BaseModel):
    """One sequencing run — `read_ena(accession)` (default `result='read_run'`) —
    joining run/experiment/sample/study accessions with the library-prep and
    fastq-file fields registration needs. Its sample is `sample_accession`.
    """

    run_accession: str = Field(min_length=1)
    experiment_accession: str = Field(min_length=1)
    sample_accession: str = Field(min_length=1)
    study_accession: str = Field(min_length=1)
    library_layout: str | None = None
    library_strategy: str | None = None
    library_source: str | None = None
    library_selection: str | None = None
    # ENA's controlled-vocabulary platform (ILLUMINA, OXFORD_NANOPORE, ...),
    # carried through unmapped -- ena_import.platform_mapping maps it to
    # Platform, fail-loud on an unrecognized value.
    instrument_platform: str | None = None
    fastq_ftp: list[str] = Field(default_factory=list)
    fastq_aspera: list[str] = Field(default_factory=list)
    fastq_bytes: list[int] = Field(default_factory=list)
    fastq_md5: list[str] = Field(default_factory=list)
    read_count: int | None = None
    base_count: int | None = None


class EnaSampleAttributes(BaseModel):
    """One BioSample's submitter-defined tag -> value attribute map —
    `read_ena_attributes(accession)`, pivoted from its (sample_accession, tag,
    value) row shape into one map per sample."""

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
