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
    ("library_strategy", "library_source", "library_selection", "platform", "expected"),
    [
        # Metagenomics -- WGS strategy or METAGENOMIC source, either arm.
        ("WGS", "GENOMIC", None, Platform.ILLUMINA, "short_read_metagenomics"),
        ("WGS", "GENOMIC", None, Platform.OXFORD_NANOPORE, "long_read_metagenomics"),
        ("OTHER", "METAGENOMIC", None, Platform.ILLUMINA, "short_read_metagenomics"),
        ("OTHER", "METAGENOMIC", None, Platform.PACBIO_SMRT, "long_read_metagenomics"),
        # Amplicon.
        ("AMPLICON", "GENOMIC", None, Platform.ILLUMINA, "short_read_amplicon"),
        ("AMPLICON", "GENOMIC", None, Platform.OXFORD_NANOPORE, "long_read_amplicon"),
        ("AMPLICON", "GENOMIC", None, Platform.PACBIO_SMRT, "long_read_amplicon"),
        # Transcriptomics -- short-read only (no long_read_transcriptomics).
        ("RNA-Seq", "TRANSCRIPTOMIC", None, Platform.ILLUMINA, "short_read_transcriptomics"),
        ("OTHER", "TRANSCRIPTOMIC", None, Platform.ILLUMINA, "short_read_transcriptomics"),
        ("OTHER", "METATRANSCRIPTOMIC", None, Platform.DNBSEQ, "short_read_transcriptomics"),
        # Case-insensitivity + whitespace tolerance.
        ("amplicon", " genomic ", None, Platform.ILLUMINA, "short_read_amplicon"),
        ("wgs", None, None, Platform.ILLUMINA, "short_read_metagenomics"),
        # PCR selection only refuses outside AMPLICON; RANDOM PCR and RT-PCR are distinct.
        ("AMPLICON", "METAGENOMIC", "PCR", Platform.ILLUMINA, "short_read_amplicon"),
        ("WGS", "METAGENOMIC", "RANDOM PCR", Platform.ILLUMINA, "short_read_metagenomics"),
        ("RNA-Seq", "TRANSCRIPTOMIC", "RT-PCR", Platform.ILLUMINA, "short_read_transcriptomics"),
    ],
)
def test_map_ena_run_to_prep_protocol_name(
    library_strategy, library_source, library_selection, platform, expected
):
    assert (
        map_ena_run_to_prep_protocol_name(
            library_strategy=library_strategy,
            library_source=library_source,
            library_selection=library_selection,
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
            library_selection=None,
            platform=Platform.OXFORD_NANOPORE,
        )
    assert excinfo.value.library_strategy == "RNA-Seq"
    assert excinfo.value.library_source == "TRANSCRIPTOMIC"


def test_unrecognized_strategy_raises_and_surfaces_strategy():
    with pytest.raises(UnmappableEnaLibraryStrategyError, match="ChIP-Seq") as excinfo:
        map_ena_run_to_prep_protocol_name(
            library_strategy="ChIP-Seq",
            library_source="GENOMIC",
            library_selection=None,
            platform=Platform.ILLUMINA,
        )
    assert excinfo.value.library_strategy == "ChIP-Seq"
    assert excinfo.value.library_source == "GENOMIC"


def test_none_strategy_and_source_raises():
    with pytest.raises(UnmappableEnaLibraryStrategyError):
        map_ena_run_to_prep_protocol_name(
            library_strategy=None,
            library_source=None,
            library_selection=None,
            platform=Platform.ILLUMINA,
        )


@pytest.mark.parametrize(
    ("library_strategy", "library_source", "library_selection", "platform"),
    [
        ("OTHER", "METAGENOMIC", "PCR", Platform.ION_TORRENT),
        ("RNA-Seq", "METAGENOMIC", "PCR", Platform.ILLUMINA),
        ("RNA-Seq", "TRANSCRIPTOMIC", "PCR", Platform.ILLUMINA),
        ("WGS", "METAGENOMIC", " pcr ", Platform.ILLUMINA),
        ("OTHER", "METAGENOMIC", "PCR", Platform.PACBIO_SMRT),
    ],
    ids=["PRJNA1127492", "PRJNA1256458", "rna-seq-transcriptomic", "wgs-padded", "long-read"],
)
def test_pcr_selection_on_a_non_amplicon_strategy_raises(
    library_strategy, library_source, library_selection, platform
):
    with pytest.raises(
        UnmappableEnaLibraryStrategyError,
        match="is not AMPLICON; ask the depositor to correct the ENA record, or register",
    ) as excinfo:
        map_ena_run_to_prep_protocol_name(
            library_strategy=library_strategy,
            library_source=library_source,
            library_selection=library_selection,
            platform=platform,
        )
    assert excinfo.value.library_strategy == library_strategy
    assert excinfo.value.library_source == library_source
    assert excinfo.value.library_selection == library_selection
