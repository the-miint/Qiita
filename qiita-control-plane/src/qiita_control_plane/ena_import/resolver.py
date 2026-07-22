"""Resolver contract for ENA/SRA study metadata.

`EnaResolver` is the seam this architecture locks: given a validated
study accession, resolve its header, its runs (one row per run, joined to
its sample), and its per-sample attributes as typed
`qiita_common.models.ena` models — never a raw dict, and never an empty
result for an accession that fails to resolve. Two implementations share
this contract: `MiintEnaResolver` (default — drives DuckDB + the miint
`read_ena`/`read_ena_attributes` table functions) and `HttpEnaResolver`
(experimental fallback — plain ENA Portal API + Browser XML, off by
default). Swapping between them is config-level
(`qiita_control_plane.ena_import.get_resolver`), never a callers change.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from qiita_common.models.ena import EnaRunRecord, EnaSampleAttributes, EnaStudyHeader


class EnaAccessionNotFoundError(RuntimeError):
    """A well-formed, validated accession resolved to zero rows from ENA.

    Never surfaced as an empty list: an accession that exists but is
    genuinely empty (e.g. a withdrawn study) still needs an operator to see
    a clear "not found" reason rather than silently importing nothing, and
    a typo'd-but-well-shaped accession must not look like "study has no
    data"."""


def pivot_sample_attributes(
    columns: list[str], rows: list[list[str]] | list[tuple[str, ...]]
) -> list[EnaSampleAttributes]:
    """Pivot the narrow `(sample_accession, tag, value)` row shape both
    `read_ena_attributes` and the ENA Browser XML API return into one
    `EnaSampleAttributes` per distinct sample, preserving first-seen sample
    order. Shared by `MiintEnaResolver` and `HttpEnaResolver` so the two
    implementations agree on the pivot, not just the wire shape."""
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
    """Abstract resolver for one ENA/SRA study's metadata. `accession` is
    always the STUDY accession (see `ena_import.accession`); every method
    validates it and raises `InvalidEnaAccessionError` (wrong kind/shape) or
    `EnaAccessionNotFoundError` (well-formed but nothing found) rather than
    returning an empty result -- with one deliberate carve-out, see
    `resolve_sample_attributes` below."""

    @abstractmethod
    def resolve_study_header(self, accession: str) -> EnaStudyHeader:
        """Resolve one study's header metadata."""

    @abstractmethod
    def resolve_runs(self, accession: str) -> list[EnaRunRecord]:
        """Resolve every run (one row per run, joined to its sample) under
        a study accession."""

    @abstractmethod
    def resolve_sample_attributes(self, accession: str) -> list[EnaSampleAttributes]:
        """Resolve every sample's submitter-defined attribute map under a
        study accession.

        Carve-out from the class docstring's "never empty" rule: a sample
        (or every sample in the study) can genuinely carry ZERO
        submitter-defined attributes -- a real, common ENA/DDBJ shape, not
        a resolution failure. Because callers only reach this method after
        `resolve_runs` has already proven the study's samples are real
        (see `ena_import.batch._process_one_study`), an empty attribute
        result here returns fewer/no entries rather than raising
        `EnaAccessionNotFoundError`. Existence checks that precede the
        attribute fetch (e.g. `HttpEnaResolver`'s sample-list search) still
        raise on zero rows -- only the attribute fetch itself does not."""
