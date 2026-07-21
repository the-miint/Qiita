"""ENA run metadata -> curated `qiita.prep_protocol` name mapping (T02, owner
decision R4): map every resolved ENA run to one of the five system-admin
curated prep_protocol rows seeded by
`db/migrations/20260501000010_prep_protocol_prep_sample_field.sql`
(`short_read_metagenomics`, `short_read_transcriptomics`,
`long_read_metagenomics`, `short_read_amplicon`, `long_read_amplicon`) --
no new protocol is minted by this ticket.

The read-length bucket (short vs. long) is derived from the run's *mapped*
`qiita.platform` (`ena_import.platform_mapping.map_ena_platform`), not the
raw ENA string, so protocol selection rides the exact same closed platform
mapping the sequencing_run/sequenced_pool grouping uses -- one source of
truth for "is this platform short- or long-read".
"""

from __future__ import annotations

from qiita_common.models import Platform

# Every qiita.platform member currently classified as short-read.
_SHORT_READ_PLATFORMS = frozenset(
    {
        Platform.ILLUMINA,
        Platform.DNBSEQ,
        Platform.ION_TORRENT,
        Platform.COMPLETE_GENOMICS,
        Platform.LS454,
    }
)
# Every qiita.platform member currently classified as long-read.
_LONG_READ_PLATFORMS = frozenset({Platform.PACBIO_SMRT, Platform.OXFORD_NANOPORE})

_SHORT_READ = "short_read"
_LONG_READ = "long_read"


class UnmappableEnaLibraryStrategyError(ValueError):
    """Raised when an ENA run's (library_strategy, library_source, mapped
    platform) has no mapping to a curated prep_protocol name -- including
    the case where the (strategy/source) category is recognized but no
    curated protocol exists for its read-length bucket (there is no
    `long_read_transcriptomics` among the five curated protocols). Carries
    the offending strategy/source so the caller can see exactly what
    failed to resolve."""

    def __init__(self, library_strategy: str | None, library_source: str | None) -> None:
        self.library_strategy = library_strategy
        self.library_source = library_source
        super().__init__(
            "no curated prep_protocol mapping for ENA library_strategy="
            f"{library_strategy!r}, library_source={library_source!r}"
        )


def _read_length_bucket(platform: Platform) -> str:
    """Return "short_read" / "long_read" for a mapped qiita.platform.

    The two frozensets above partition every current Platform member; a
    future Platform value that isn't added to either set raises
    NotImplementedError rather than silently defaulting to one bucket."""
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
    """Map one ENA run's (library_strategy, library_source, mapped platform)
    to a curated prep_protocol name.

    Category dispatch (case-insensitive, stripped):
      - library_strategy == "AMPLICON" -> {read_length}_amplicon
      - library_strategy == "WGS" or library_source == "METAGENOMIC"
        -> {read_length}_metagenomics
      - library_strategy == "RNA-SEQ" or library_source in
        {"TRANSCRIPTOMIC", "METATRANSCRIPTOMIC"} -> short_read_transcriptomics
        (long-read never maps here -- no long_read_transcriptomics protocol
        is curated; raises instead)

    Raises `UnmappableEnaLibraryStrategyError`, carrying the offending
    strategy/source, when the pair matches no category above OR when the
    matched category has no curated protocol for the run's read-length
    bucket. Never silently buckets an unrecognized strategy into one of
    the five names.
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
            # No long_read_transcriptomics protocol is curated -- fail loud
            # rather than silently bucket into a metagenomics/amplicon slot.
            raise UnmappableEnaLibraryStrategyError(library_strategy, library_source)
        return "short_read_transcriptomics"

    raise UnmappableEnaLibraryStrategyError(library_strategy, library_source)
