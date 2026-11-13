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
    """Raised when an ENA run's library_selection contradicts its strategy."""

    def __init__(
        self,
        library_strategy: str | None,
        library_source: str | None,
        library_selection: str | None,
    ) -> None:
        self.library_strategy = library_strategy
        self.library_source = library_source
        self.library_selection = library_selection
        super().__init__(
            f"ENA library_selection={library_selection!r} marks an amplicon library but "
            f"library_strategy={library_strategy!r} (library_source={library_source!r}) "
            "is not AMPLICON; ask the depositor to correct the ENA record, "
            "or register the run by hand"
        )


class UnsupportedEnaLibraryStrategyError(ValueError):
    """No curated prep_protocol exists for this strategy/source on this platform."""

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
    library_selection: str | None,
    platform: Platform,
) -> str:
    """Map one ENA run to a curated prep_protocol name.

    Raises `UnsupportedEnaLibraryStrategyError` when no protocol exists for it, and
    `UnmappableEnaLibraryStrategyError` when a supported non-AMPLICON run has selection "PCR".
    """
    read_length = _read_length_bucket(platform)
    strategy = (library_strategy or "").strip().upper()
    source = (library_source or "").strip().upper()
    selection = (library_selection or "").strip().upper()

    if strategy == "AMPLICON":
        return f"{read_length}_amplicon"

    if strategy == "WGS" or source == "METAGENOMIC":
        category = "metagenomics"
    elif strategy == "RNA-SEQ" or source in {"TRANSCRIPTOMIC", "METATRANSCRIPTOMIC"}:
        category = "transcriptomics"
    else:
        raise UnsupportedEnaLibraryStrategyError(library_strategy, library_source)

    if category == "transcriptomics" and read_length == _LONG_READ:
        raise UnsupportedEnaLibraryStrategyError(library_strategy, library_source)

    if selection == "PCR":
        raise UnmappableEnaLibraryStrategyError(library_strategy, library_source, library_selection)

    return f"{read_length}_{category}"
