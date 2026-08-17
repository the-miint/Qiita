"""Tests for `ena_import.protocol_mapping.map_ena_run_to_prep_protocol_name`: maps every
ENA run to one of the five curated prep_protocol names, fail-loud -- never silently
bucketed, even for a recognized category with no curated protocol for its read-length."""

import pytest
from qiita_common.models import Platform

from qiita_control_plane.ena_import.protocol_mapping import (
    UnmappableEnaLibraryStrategyError,
    map_ena_run_to_prep_protocol_name,
)


@pytest.mark.parametrize(
    ("library_strategy", "library_source", "platform", "expected"),
    [
        # Metagenomics -- WGS strategy or METAGENOMIC source, either arm.
        ("WGS", "GENOMIC", Platform.ILLUMINA, "short_read_metagenomics"),
        ("WGS", "GENOMIC", Platform.OXFORD_NANOPORE, "long_read_metagenomics"),
        ("OTHER", "METAGENOMIC", Platform.ILLUMINA, "short_read_metagenomics"),
        ("OTHER", "METAGENOMIC", Platform.PACBIO_SMRT, "long_read_metagenomics"),
        # Amplicon.
        ("AMPLICON", "GENOMIC", Platform.ILLUMINA, "short_read_amplicon"),
        ("AMPLICON", "GENOMIC", Platform.OXFORD_NANOPORE, "long_read_amplicon"),
        ("AMPLICON", "GENOMIC", Platform.PACBIO_SMRT, "long_read_amplicon"),
        # Transcriptomics -- short-read only (no long_read_transcriptomics).
        ("RNA-Seq", "TRANSCRIPTOMIC", Platform.ILLUMINA, "short_read_transcriptomics"),
        ("OTHER", "TRANSCRIPTOMIC", Platform.ILLUMINA, "short_read_transcriptomics"),
        ("OTHER", "METATRANSCRIPTOMIC", Platform.DNBSEQ, "short_read_transcriptomics"),
        # Case-insensitivity + whitespace tolerance.
        ("amplicon", " genomic ", Platform.ILLUMINA, "short_read_amplicon"),
        ("wgs", None, Platform.ILLUMINA, "short_read_metagenomics"),
    ],
)
def test_map_ena_run_to_prep_protocol_name(library_strategy, library_source, platform, expected):
    assert (
        map_ena_run_to_prep_protocol_name(
            library_strategy=library_strategy,
            library_source=library_source,
            platform=platform,
        )
        == expected
    )


def test_long_read_transcriptomic_run_raises_no_curated_protocol():
    """RNA-Seq/transcriptomic on long-read has no curated protocol (only
    short_read_transcriptomics is seeded) -- must raise, not fall into another bucket."""
    with pytest.raises(UnmappableEnaLibraryStrategyError) as excinfo:
        map_ena_run_to_prep_protocol_name(
            library_strategy="RNA-Seq",
            library_source="TRANSCRIPTOMIC",
            platform=Platform.OXFORD_NANOPORE,
        )
    assert excinfo.value.library_strategy == "RNA-Seq"
    assert excinfo.value.library_source == "TRANSCRIPTOMIC"


def test_unrecognized_strategy_raises_and_surfaces_strategy():
    with pytest.raises(UnmappableEnaLibraryStrategyError, match="ChIP-Seq") as excinfo:
        map_ena_run_to_prep_protocol_name(
            library_strategy="ChIP-Seq",
            library_source="GENOMIC",
            platform=Platform.ILLUMINA,
        )
    assert excinfo.value.library_strategy == "ChIP-Seq"
    assert excinfo.value.library_source == "GENOMIC"


def test_none_strategy_and_source_raises():
    with pytest.raises(UnmappableEnaLibraryStrategyError):
        map_ena_run_to_prep_protocol_name(
            library_strategy=None,
            library_source=None,
            platform=Platform.ILLUMINA,
        )
