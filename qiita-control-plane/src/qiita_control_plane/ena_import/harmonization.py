"""ENA sample attributes -> the biosample import's metadata dict.

`attribute_mapping.map_ena_attributes` splits one BioSample's attributes into a
curated set landing on a `biosample_global_field` (cross-study comparable) and
everything else, retained as study-local rather than dropped. The halves go to
`repositories.biosample.resolve_or_import_biosample_by_ena_accession` as two
separate dicts; this module holds no SQL.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .attribute_mapping import map_ena_attributes

# The import composer enforces this required global field. ENA carries no NCBI
# host taxon id -- its `host` is submitter free text -- so the honest value is
# the missing-value marker rather than a guess or a weakened gate.
HOST_TAXON_ID_DISPLAY_NAME = "host taxon id"
HOST_TAXON_ID_UNKNOWN = "not provided"


@dataclass(frozen=True)
class HarmonizationResult:
    """One biosample's harmonization outcome.

    `mapped_count`: attributes written as globally-linked metadata.
    `retained_unmapped`: raw ENA tags written as study-local metadata (not
    dropped).
    """

    mapped_count: int
    retained_unmapped: list[str] = field(default_factory=list)


def build_biosample_metadata(
    attributes: dict[str, str],
) -> tuple[dict[str, str], dict[str, str], HarmonizationResult]:
    """Split one BioSample's ENA attributes into `(global_metadata,
    local_metadata, result)` for the import.

    The two dicts cannot be merged: an unmapped tag is often spelled exactly
    like a global field the mapping declined (ENA's environmental-context tags
    are), and the import resolves any key naming a global to that global.
    """
    mapped, unmapped = map_ena_attributes(attributes)
    global_metadata = {**mapped, HOST_TAXON_ID_DISPLAY_NAME: HOST_TAXON_ID_UNKNOWN}
    return (
        global_metadata,
        dict(unmapped),
        HarmonizationResult(mapped_count=len(mapped), retained_unmapped=sorted(unmapped)),
    )
