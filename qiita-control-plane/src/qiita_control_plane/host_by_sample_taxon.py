"""The table of biosample taxa with an implied host (or none), keyed by NCBI taxon id.

The host is not derivable from the taxonomy tree (`qiita.terminology_term` stores no
lineage, and `human gut metagenome` is not a descendant of *Homo sapiens*), so the
judgment is an explicit table.
"""

from __future__ import annotations

from collections.abc import Iterable

from qiita_common.models import (
    NCBI_TAXONOMY_HUMAN_TERM_ID,
    NCBI_TAXONOMY_METAGENOME_TERM_ID,
    NCBI_TAXONOMY_MOUSE_TERM_ID,
)

# Keyed on the biosample's own taxon, which for a metagenome names the environment it was
# drawn from. The value is the host that environment implies; None means it has no host.
#
# Not exhaustive over NCBI: a taxon absent here is unresolved, not assumed hostless, and
# the submit path aborts on it. Add a row only when the environment implies the host.
# A new row is a code change plus a deploy; if rows start landing often, move the table to
# a seeded lookup table rather than growing this dict.
HOST_BY_SAMPLE_TAXON: dict[str, str | None] = {
    # A human gut metagenome is, by construction, drawn from a human gut.
    "408170": NCBI_TAXONOMY_HUMAN_TERM_ID,
    # Seawater has no host: a decision ('not applicable'), not a gap, so such
    # samples are not host-depleted.
    "1561972": None,
    "646099": NCBI_TAXONOMY_HUMAN_TERM_ID,  # human metagenome: drawn from a human
    "539655": NCBI_TAXONOMY_HUMAN_TERM_ID,  # human skin metagenome: drawn from human skin
    "410661": NCBI_TAXONOMY_MOUSE_TERM_ID,  # mouse gut metagenome: drawn from a mouse gut
    "540485": NCBI_TAXONOMY_MOUSE_TERM_ID,  # mouse skin metagenome: drawn from mouse skin
    "410658": None,  # soil metagenome: bulk soil, no organism it was taken from
    "412755": None,  # marine sediment metagenome: sediment, not an organism
    "556182": None,  # freshwater sediment metagenome: sediment, not an organism
    "408172": None,  # marine metagenome: open water, like seawater
    "1504975": None,  # salt marsh metagenome: wetland sediment and water
    "1671699": None,  # sand metagenome: mineral substrate
    "527640": None,  # microbial mat metagenome: free-living community on a surface
    "496921": None,  # stromatolite metagenome: lithified microbial mat
    "1260732": None,  # coal metagenome: geologic deposit
    # Absent on purpose: the bare `metagenome` root names no environment, so it implies
    # no host. Engineered environments (bioreactor, activated sludge, oil field) are
    # absent too, since ENA can record a host on them.
}

# Taxa that name an environment, never a host, whether or not the table gives them a host.
NON_HOST_TAXA: frozenset[str] = frozenset(HOST_BY_SAMPLE_TAXON) | {NCBI_TAXONOMY_METAGENOME_TERM_ID}


def implied_hosts(taxon_ids: Iterable[str]) -> dict[str, str | None]:
    """The table restricted to `taxon_ids`; absent means unresolved, None no host."""
    return {t: HOST_BY_SAMPLE_TAXON[t] for t in taxon_ids if t in HOST_BY_SAMPLE_TAXON}
