"""ENA run metadata -> curated `qiita.prep_protocol` name mapping.

Maps every resolved run to one of the five seeded prep_protocol rows
(`short_read_metagenomics`, `short_read_transcriptomics`, `long_read_metagenomics`,
`short_read_amplicon`, `long_read_amplicon`) — no new protocol is minted here.

The read-length bucket (short vs. long) is derived from the run's *mapped*
`qiita.platform`, not the raw ENA string, so protocol selection rides the same closed
platform mapping the sequencing_run/sequenced_pool grouping uses -- one source of truth
for short- vs. long-read.
"""

from __future__ import annotations

from qiita_common.models import Platform

# qiita.platform members classified as short-read.
_SHORT_READ_PLATFORMS = frozenset(
    {
        Platform.ILLUMINA,
        Platform.DNBSEQ,
        Platform.ION_TORRENT,
        Platform.COMPLETE_GENOMICS,
        Platform.LS454,
    }
)
# qiita.platform members classified as long-read.
_LONG_READ_PLATFORMS = frozenset({Platform.PACBIO_SMRT, Platform.OXFORD_NANOPORE})

_SHORT_READ = "short_read"
_LONG_READ = "long_read"


class UnmappableEnaLibraryStrategyError(ValueError):
    """Raised when an ENA run's (library_strategy, library_source, mapped platform) has
    no curated prep_protocol name -- including when the category is recognized but no
    protocol exists for its read-length bucket (there is no `long_read_transcriptomics`).
    Carries the offending strategy/source."""

    def __init__(self, library_strategy: str | None, library_source: str | None) -> None:
        self.library_strategy = library_strategy
        self.library_source = library_source
        super().__init__(
            "no curated prep_protocol mapping for ENA library_strategy="
            f"{library_strategy!r}, library_source={library_source!r}"
        )


def _read_length_bucket(platform: Platform) -> str:
    """Return "short_read" / "long_read" for a mapped qiita.platform. The two frozensets
    partition every current Platform member; one absent from both raises
    NotImplementedError rather than silently defaulting to a bucket."""
    if platform in _LONG_READ_PLATFORMS:
        return _LONG_READ
    if platform in _SHORT_READ_PLATFORMS:
        return _SHORT_READ
    raise NotImplementedError(
        f"no short/long read-length bucket defined for qiita.platform={platform!r}"
    )


def map_ena_run_to_prep_protocol_name(
    *,
    library_strategy: str | None,
    library_source: str | None,
    platform: Platform,
) -> str:
    """Map one ENA run's (library_strategy, library_source, mapped platform) to a
    curated prep_protocol name.

    Category dispatch (case-insensitive, stripped):
      - strategy == "AMPLICON" -> {read_length}_amplicon
      - strategy == "WGS" or source == "METAGENOMIC" -> {read_length}_metagenomics
      - strategy == "RNA-SEQ" or source in {"TRANSCRIPTOMIC", "METATRANSCRIPTOMIC"}
        -> short_read_transcriptomics (long-read raises -- no such protocol curated)

    Raises `UnmappableEnaLibraryStrategyError` when the pair matches no category or the
    matched category has no protocol for the read-length bucket. Never silently buckets.
    """
    read_length = _read_length_bucket(platform)
    strategy = (library_strategy or "").strip().upper()
    source = (library_source or "").strip().upper()

    if strategy == "AMPLICON":
        return f"{read_length}_amplicon"

    if strategy == "WGS" or source == "METAGENOMIC":
        return f"{read_length}_metagenomics"

    if strategy == "RNA-SEQ" or source in {"TRANSCRIPTOMIC", "METATRANSCRIPTOMIC"}:
        if read_length == _LONG_READ:
            # No long_read_transcriptomics protocol is curated -- fail loud.
            raise UnmappableEnaLibraryStrategyError(library_strategy, library_source)
        return "short_read_transcriptomics"

    raise UnmappableEnaLibraryStrategyError(library_strategy, library_source)
