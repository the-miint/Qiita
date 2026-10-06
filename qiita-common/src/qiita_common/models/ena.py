"""Pydantic models for INSDC study metadata resolved via miint `read_ena` /
`read_ena_attributes`.

`read_ena` returns typed columns (duckdb-miint#178): numeric fields arrive as
`int | None`, and per-file fields arrive as `list[...]`. These models validate
the typed data at construction.

`status` is required on both `EnaStudyHeader` and `EnaRunRecord`: ENA Portal's
`/search` endpoint (what `read_ena` queries) returns only public records by
default, so a non-`public` value in a returned row, or one this codebase does
not recognize, must fail loud rather than default to "assume public".
"""

from __future__ import annotations

from enum import IntEnum, StrEnum

from pydantic import BaseModel, Field, field_validator


class EnaStatus(StrEnum):
    """ENA's per-record availability status, as `read_ena` reports it."""

    PUBLIC = "public"
    SUPPRESSED = "suppressed"


class EnaBrowserStatus(IntEnum):
    """Numeric `status` values ENA's Browser API (`summary/{accession}`) reports.

    This is a *different* status surface from `EnaStatus` above: the Portal
    `/search` endpoint `read_ena` queries never returns a non-public record at
    all, so re-checking a run Qiita already holds has to go through the Browser
    API instead, which reports every record regardless of status.

    Verified live (2026-09-30): `PRJEB1` and `ERR000130` both report `5` with
    `statusDescription: "suppressed"`; `SRR096342` reports `4` /
    `"public"`. ENA documents four further statuses -- private, permanently
    suppressed, temporarily suppressed, replaced, withdrawn -- but no numeric
    code for any of them has turned up in a live response, and the Browser
    API's own OpenAPI spec declares `status` a bare `int32` with no enum.
    Guessing a code that later collides with a different real status would
    silently misclassify a run, so `parse_ena_browser_status` raises on
    anything outside this set rather than extending it speculatively.
    """

    PUBLIC = 4
    SUPPRESSED = 5


class UnknownEnaBrowserStatusError(RuntimeError):
    """The Browser API reported a numeric `status` this codebase does not
    recognize (see `EnaBrowserStatus`). Raised rather than guessed at."""


def parse_ena_browser_status(*, status: int, description: str) -> str | None:
    """Map one Browser API `(status, statusDescription)` pair to the value
    `sequenced_sample.ena_status` should hold: `None` for `PUBLIC` (available),
    else ENA's own `description` verbatim. Raises `UnknownEnaBrowserStatusError`
    for any `status` code not in `EnaBrowserStatus`."""
    try:
        parsed = EnaBrowserStatus(status)
    except ValueError:
        raise UnknownEnaBrowserStatusError(
            f"ENA Browser API reported status={status!r} ({description!r}), which"
            " this codebase does not recognize -- see EnaBrowserStatus"
        ) from None
    return None if parsed is EnaBrowserStatus.PUBLIC else description


class EnaStudyHeader(BaseModel):
    """One study's header metadata — `read_ena(accession, result='study')`.
    Field set matches `ENAParser::DefaultFields("study")` plus `status`."""

    study_accession: str = Field(min_length=1)
    status: EnaStatus
    secondary_study_accession: str | None = None
    study_title: str | None = None
    study_description: str | None = None
    center_name: str | None = None
    first_public: str | None = None
    last_updated: str | None = None
    scientific_name: str | None = None
    tax_id: int | None = None

    @field_validator("secondary_study_accession")
    @classmethod
    def _normalize_blank_to_none(cls, v: str | None) -> str | None:
        # read_ena reports a missing secondary accession as "", not NULL --
        # normalize so a consumer's `is not None` means "ENA reported one".
        if v is not None and not v.strip():
            return None
        return v


class EnaRunRecord(BaseModel):
    """One sequencing run — `read_ena(accession)` (default `result='read_run'`) —
    joining run/experiment/sample/study accessions with the library-prep and
    fastq-file fields registration needs. Its sample is `sample_accession`.
    """

    run_accession: str = Field(min_length=1)
    experiment_accession: str = Field(min_length=1)
    sample_accession: str = Field(min_length=1)
    # Submitter-supplied and not guaranteed present: ENA returns it empty for
    # DDBJ-brokered samples (SAMD01818724).
    sample_alias: str | None = None
    study_accession: str = Field(min_length=1)
    status: EnaStatus
    library_layout: str | None = None
    library_strategy: str | None = None
    library_source: str | None = None
    library_selection: str | None = None
    # ENA's controlled-vocabulary platform (ILLUMINA, OXFORD_NANOPORE, ...),
    # carried through unmapped -- ena_import.platform_mapping maps it to
    # Platform, fail-loud on an unrecognized value.
    instrument_platform: str | None = None
    # NULL when ENA reports the field empty (e.g. a run with no generated FASTQ).
    fastq_ftp: list[str] | None = None
    fastq_aspera: list[str] | None = None
    fastq_bytes: list[int] | None = None
    fastq_md5: list[str] | None = None
    read_count: int | None = None
    base_count: int | None = None

    @field_validator("library_layout", "library_strategy", "library_source", "library_selection")
    @classmethod
    def _normalize_blank_library_to_none(cls, v: str | None) -> str | None:
        # read_ena reports a missing library field as "", not NULL -- normalize
        # so a consumer's `is not None` means "ENA reported one", as on
        # EnaStudyHeader.
        if v is not None and not v.strip():
            return None
        return v


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
