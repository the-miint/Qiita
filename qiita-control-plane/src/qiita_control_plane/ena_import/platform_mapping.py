"""ENA `instrument_platform` -> `qiita.platform` mapping (the per-platform
grouping design): every ENA run is mapped to a `qiita_common.models.Platform` up front so
the registration composer can group runs by platform and mint one
`sequencing_run` + `sequenced_pool` per distinct platform.

Positional, case-normalized, closed-table lookup -- never a default. An ENA
`instrument_platform` value this table doesn't recognize is a hard stop: the
registration composer must not silently mint a sequencing_run under the
wrong platform bucket, or worse, guess.
"""

from __future__ import annotations

from qiita_common.models import Platform


class UnmappableEnaPlatformError(ValueError):
    """Raised when an ENA `instrument_platform` value (blank, missing, or
    simply not in `_ENA_PLATFORM_TO_QIITA`) has no mapping to
    `qiita_common.models.Platform`. Carries the offending raw value so the
    caller can see exactly what failed to resolve."""

    def __init__(self, instrument_platform: str | None) -> None:
        self.instrument_platform = instrument_platform
        super().__init__(
            f"no qiita.platform mapping for ENA instrument_platform={instrument_platform!r}"
        )


# ENA/INSDC's controlled vocabulary for instrument_platform (the SRA.common.xsd
# PLATFORM group ENA's Portal API surfaces via read_run's instrument_platform
# field), mapped to qiita.platform (db/migrations/20260501000009_sequencing_run.sql).
# BGISEQ is ENA's legacy name for the same platform family DNBSEQ later
# renamed to; both map to Platform.DNBSEQ. CAPILLARY has no qiita.platform
# counterpart and is deliberately absent from this table -- it raises, same
# as any other value the table doesn't cover.
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
    """Map an ENA `instrument_platform` value to `qiita_common.models.Platform`.

    Case-normalized (stripped + upper-cased) exact match against the closed
    table above. Raises `UnmappableEnaPlatformError` on `None`, blank, or any
    value outside the table -- never falls back to a default platform.
    """
    if instrument_platform is None or not instrument_platform.strip():
        raise UnmappableEnaPlatformError(instrument_platform)
    key = instrument_platform.strip().upper()
    mapped = _ENA_PLATFORM_TO_QIITA.get(key)
    if mapped is None:
        raise UnmappableEnaPlatformError(instrument_platform)
    return mapped
