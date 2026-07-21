"""Tests for `ena_import.platform_mapping.map_ena_platform` (T02, R3):
positional, case-normalized mapping from ENA's `instrument_platform` string
to `qiita_common.models.Platform`. Fail-loud on anything outside the closed
table -- never a default."""

import pytest
from qiita_common.models import Platform

from qiita_control_plane.ena_import.platform_mapping import (
    UnmappableEnaPlatformError,
    map_ena_platform,
)


@pytest.mark.parametrize(
    ("instrument_platform", "expected"),
    [
        ("ILLUMINA", Platform.ILLUMINA),
        ("PACBIO_SMRT", Platform.PACBIO_SMRT),
        ("OXFORD_NANOPORE", Platform.OXFORD_NANOPORE),
        ("BGISEQ", Platform.DNBSEQ),
        ("DNBSEQ", Platform.DNBSEQ),
        ("LS454", Platform.LS454),
        ("ION_TORRENT", Platform.ION_TORRENT),
        ("COMPLETE_GENOMICS", Platform.COMPLETE_GENOMICS),
    ],
)
def test_map_ena_platform_known_values(instrument_platform, expected):
    assert map_ena_platform(instrument_platform) is expected


def test_map_ena_platform_is_case_insensitive():
    assert map_ena_platform("illumina") is Platform.ILLUMINA
    assert map_ena_platform("  Illumina  ") is Platform.ILLUMINA


def test_map_ena_platform_bgiseq_and_dnbseq_agree():
    """BGISEQ is ENA's legacy name for the platform family DNBSEQ later
    renamed to; both must resolve to the same qiita.platform so a study
    mixing the two spellings still groups into one sequencing_run."""
    assert map_ena_platform("BGISEQ") == map_ena_platform("DNBSEQ")


def test_map_ena_platform_rejects_none():
    with pytest.raises(UnmappableEnaPlatformError, match="None"):
        map_ena_platform(None)


def test_map_ena_platform_rejects_blank():
    with pytest.raises(UnmappableEnaPlatformError):
        map_ena_platform("   ")


def test_map_ena_platform_rejects_unknown_value():
    with pytest.raises(UnmappableEnaPlatformError, match="CAPILLARY") as excinfo:
        map_ena_platform("CAPILLARY")
    assert excinfo.value.instrument_platform == "CAPILLARY"


def test_map_ena_platform_rejects_typo():
    with pytest.raises(UnmappableEnaPlatformError):
        map_ena_platform("ILUMINA")
