"""ENA `instrument_platform` -> `qiita.platform` mapping.

Every run is mapped up front so the registration composer can group runs by platform
and mint one `sequencing_run` + `sequenced_pool` per distinct platform. Case-normalized,
closed-table lookup, never a default: an unrecognized value is a hard stop rather than
risk minting under the wrong platform bucket.
"""

from __future__ import annotations

from qiita_common.models import Platform


class UnmappableEnaPlatformError(ValueError):
    """Raised when an ENA `instrument_platform` value (blank, missing, or absent from
    `_ENA_PLATFORM_TO_QIITA`) has no `Platform` mapping. Carries the raw value."""

    def __init__(self, instrument_platform: str | None) -> None:
        self.instrument_platform = instrument_platform
        super().__init__(
            f"no qiita.platform mapping for ENA instrument_platform={instrument_platform!r}"
        )


# ENA/INSDC's instrument_platform controlled vocabulary mapped to qiita.platform.
# BGISEQ is ENA's legacy name for DNBSEQ; both map to Platform.DNBSEQ. CAPILLARY has no
# qiita.platform counterpart and is deliberately absent (it raises like any other
# uncovered value).
_ENA_PLATFORM_TO_QIITA: dict[str, Platform] = {
    "ILLUMINA": Platform.ILLUMINA,
    "PACBIO_SMRT": Platform.PACBIO_SMRT,
    "OXFORD_NANOPORE": Platform.OXFORD_NANOPORE,
    "BGISEQ": Platform.DNBSEQ,
    "DNBSEQ": Platform.DNBSEQ,
    "LS454": Platform.LS454,
    "ION_TORRENT": Platform.ION_TORRENT,
    "COMPLETE_GENOMICS": Platform.COMPLETE_GENOMICS,
}


def map_ena_platform(instrument_platform: str | None) -> Platform:
    """Map an ENA `instrument_platform` value to `Platform` by case-normalized exact
    match against the closed table. Raises `UnmappableEnaPlatformError` on `None`,
    blank, or any value outside it -- never a default."""
    if instrument_platform is None or not instrument_platform.strip():
        raise UnmappableEnaPlatformError(instrument_platform)
    key = instrument_platform.strip().upper()
    mapped = _ENA_PLATFORM_TO_QIITA.get(key)
    if mapped is None:
        raise UnmappableEnaPlatformError(instrument_platform)
    return mapped
