"""Shared field-by-field assertions against the PRJNA48739 fixture data, run by
the resolver test suite to pin the resolver's contract."""

from __future__ import annotations

from qiita_common.models.ena import EnaRunRecord, EnaSampleAttributes, EnaStudyHeader


def assert_prjna48739_study_header(header: EnaStudyHeader) -> None:
    assert header.study_accession == "PRJNA48739"
    assert header.secondary_study_accession == "SRP005461"
    assert header.study_title == "Streptococcus pneumoniae GA17570 genome sequencing project"
    assert header.center_name == "Institute for Genome Sciences"
    assert header.scientific_name == "Streptococcus pneumoniae GA17570"
    assert header.tax_id == 760791


def assert_prjna48739_runs(runs: list[EnaRunRecord]) -> None:
    assert len(runs) == 2
    by_accession = {run.run_accession: run for run in runs}

    single = by_accession["SRR096342"]
    assert single.experiment_accession == "SRX039368"
    assert single.sample_accession == "SAMN00199006"
    assert single.study_accession == "PRJNA48739"
    assert single.library_layout == "SINGLE"
    assert single.library_strategy == "WGS"
    assert single.library_source == "GENOMIC"
    assert single.library_selection == "RANDOM"
    assert single.instrument_platform == "LS454"
    assert single.fastq_ftp == ["ftp.sra.ebi.ac.uk/vol1/fastq/SRR096/SRR096342/SRR096342.fastq.gz"]
    assert single.fastq_bytes == [89054035]
    assert single.fastq_md5 == ["791595268ae7a965664652bde3444a2b"]
    assert single.read_count == 298966
    assert single.base_count == 158722947

    paired = by_accession["SRR096343"]
    assert paired.library_layout == "PAIRED"
    assert paired.instrument_platform == "LS454"
    assert paired.fastq_ftp == [
        "ftp.sra.ebi.ac.uk/vol1/fastq/SRR096/SRR096343/SRR096343.fastq.gz",
        "ftp.sra.ebi.ac.uk/vol1/fastq/SRR096/SRR096343/SRR096343_1.fastq.gz",
        "ftp.sra.ebi.ac.uk/vol1/fastq/SRR096/SRR096343/SRR096343_2.fastq.gz",
    ]
    assert paired.fastq_bytes == [5686490, 22054785, 24627105]
    assert paired.read_count == 238252
    assert paired.base_count == 87391853


def assert_prjna48739_sample_attributes(attrs: list[EnaSampleAttributes]) -> None:
    assert len(attrs) == 1
    sample = attrs[0]
    assert sample.sample_accession == "SAMN00199006"
    assert sample.attributes["strain"] == "GA17570"
    assert sample.attributes["organism"] == "Streptococcus pneumoniae GA17570"
    assert sample.attributes["ENA-FIRST-PUBLIC"] == "2011-01-25"
    assert len(sample.attributes) == 7
