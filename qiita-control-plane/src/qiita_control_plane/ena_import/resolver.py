"""Shared ENA/SRA metadata-resolver helpers.

`MiintEnaResolver` (miint `read_ena` / `read_ena_attributes`) is the resolver; the
error type and the sample-attribute pivot below are shared with it. Given a
validated study accession it resolves the header, the runs (one row per run,
joined to its sample), and the per-sample attributes as typed
`qiita_common.models.ena` models — never a raw dict, never an empty result for an
accession that fails to resolve.
"""

from __future__ import annotations

from qiita_common.models.ena import EnaSampleAttributes


class EnaAccessionNotFoundError(RuntimeError):
    """A well-formed, validated accession resolved to zero rows from ENA. Raised
    rather than returning an empty list so an operator sees a clear "not found"
    reason instead of a silent no-op import."""


def pivot_sample_attributes(
    columns: list[str], rows: list[list[str]] | list[tuple[str, ...]]
) -> list[EnaSampleAttributes]:
    """Pivot the narrow `(sample_accession, tag, value)` rows (as returned by
    `read_ena_attributes`) into one `EnaSampleAttributes` per distinct sample,
    preserving first-seen order."""
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
