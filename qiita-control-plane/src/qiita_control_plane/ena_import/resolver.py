"""Resolver contract for ENA/SRA study metadata.

`EnaResolver` is the locked seam: given a validated study accession, resolve its
header, its runs (one row per run, joined to its sample), and its per-sample
attributes as typed `qiita_common.models.ena` models — never a raw dict, never an
empty result for an accession that fails to resolve. Two implementations share it:
`MiintEnaResolver` (default, DuckDB + miint) and `HttpEnaResolver` (experimental
plain-HTTP fallback). Swapping is config-level via `get_resolver`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from qiita_common.models.ena import EnaRunRecord, EnaSampleAttributes, EnaStudyHeader


class EnaAccessionNotFoundError(RuntimeError):
    """A well-formed, validated accession resolved to zero rows from ENA. Raised
    rather than returning an empty list so an operator sees a clear "not found"
    reason instead of a silent no-op import."""


def pivot_sample_attributes(
    columns: list[str], rows: list[list[str]] | list[tuple[str, ...]]
) -> list[EnaSampleAttributes]:
    """Pivot the narrow `(sample_accession, tag, value)` rows (as returned by both
    `read_ena_attributes` and the ENA Browser XML API) into one `EnaSampleAttributes`
    per distinct sample, preserving first-seen order. Shared by both resolvers so they
    agree on the pivot, not just the wire shape."""
    sample_idx = columns.index("sample_accession")
    tag_idx = columns.index("tag")
    value_idx = columns.index("value")

    grouped: dict[str, dict[str, str]] = {}
    order: list[str] = []
    for row in rows:
        sample_accession = row[sample_idx]
        if sample_accession not in grouped:
            grouped[sample_accession] = {}
            order.append(sample_accession)
        grouped[sample_accession][row[tag_idx]] = row[value_idx]

    return [
        EnaSampleAttributes(sample_accession=accession, attributes=grouped[accession])
        for accession in order
    ]


class EnaResolver(ABC):
    """Abstract resolver for one ENA/SRA study's metadata. `accession` is always the
    STUDY accession; every method validates it and raises `InvalidEnaAccessionError`
    (wrong kind/shape) or `EnaAccessionNotFoundError` (well-formed but nothing found)
    rather than returning empty -- with one carve-out, see `resolve_sample_attributes`."""

    @abstractmethod
    def resolve_study_header(self, accession: str) -> EnaStudyHeader:
        """Resolve one study's header metadata."""

    @abstractmethod
    def resolve_runs(self, accession: str) -> list[EnaRunRecord]:
        """Resolve every run (one row per run, joined to its sample) under
        a study accession."""

    @abstractmethod
    def resolve_sample_attributes(self, accession: str) -> list[EnaSampleAttributes]:
        """Resolve every sample's submitter-defined attribute map under a study.

        Carve-out from the class "never empty" rule: a sample can genuinely carry ZERO
        submitter-defined attributes (a real, common ENA/DDBJ shape). Callers reach
        this only after `resolve_runs` has proven the samples real, so an empty
        attribute result returns [] rather than raising. Existence checks preceding the
        attribute fetch still raise on zero rows -- only the fetch itself does not."""
