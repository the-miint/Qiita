"""Tests for the ENA/SRA study-metadata Pydantic models (`models.ena`).

`read_ena` (see `duckdb-miint/docs/insdc_ena.md`) returns an ALL-VARCHAR
relation — every field, including the numeric ones, arrives as text. These
models are the boundary that coerces the handful of numeric fields
(`tax_id`, `read_count`, `base_count`, `fastq_bytes`) and fails loud on
garbage rather than silently propagating a bad string.
"""

import pytest
from pydantic import ValidationError


def test_ena_study_header_minimal():
    from qiita_common.models.ena import EnaStudyHeader

    header = EnaStudyHeader(study_accession="PRJEB11419")
    assert header.study_accession == "PRJEB11419"
    assert header.secondary_study_accession is None
    assert header.tax_id is None


def test_ena_study_header_full():
    from qiita_common.models.ena import EnaStudyHeader

    header = EnaStudyHeader(
        study_accession="PRJEB11419",
        secondary_study_accession="ERP012803",
        study_title="Human gut microbiome",
        study_description="A cohort study",
        center_name="EBI",
        first_public="2015-06-01",
        last_updated="2015-06-02",
        scientific_name="human gut metagenome",
        tax_id="408170",
    )
    assert header.secondary_study_accession == "ERP012803"
    assert header.tax_id == 408170


def test_ena_study_header_rejects_empty_accession():
    from qiita_common.models.ena import EnaStudyHeader

    with pytest.raises(ValidationError):
        EnaStudyHeader(study_accession="")


def test_ena_study_header_rejects_garbage_tax_id():
    from qiita_common.models.ena import EnaStudyHeader

    with pytest.raises(ValidationError):
        EnaStudyHeader(study_accession="PRJEB11419", tax_id="not-a-number")


def test_ena_study_header_blank_tax_id_is_none():
    """A blank VARCHAR (ENA has no value for this run) means "missing", not
    a parse failure — only a non-blank unparseable value fails loud."""
    from qiita_common.models.ena import EnaStudyHeader

    header = EnaStudyHeader(study_accession="PRJEB11419", tax_id="")
    assert header.tax_id is None


def test_ena_run_record_minimal_requires_accessions():
    from qiita_common.models.ena import EnaRunRecord

    with pytest.raises(ValidationError):
        EnaRunRecord(
            run_accession="ERR1074767",
            experiment_accession="",
            sample_accession="SAMEA3610311",
            study_accession="PRJEB11419",
        )


def test_ena_run_record_field_by_field():
    from qiita_common.models.ena import EnaRunRecord

    run = EnaRunRecord(
        run_accession="ERR1074767",
        experiment_accession="ERX1111111",
        sample_accession="SAMEA3610311",
        study_accession="PRJEB11419",
        library_layout="PAIRED",
        library_strategy="WGS",
        library_source="METAGENOMIC",
        library_selection="RANDOM",
        fastq_ftp=(
            "ftp.sra.ebi.ac.uk/vol1/fastq/ERR107/ERR1074767_1.fastq.gz;"
            "ftp.sra.ebi.ac.uk/vol1/fastq/ERR107/ERR1074767_2.fastq.gz"
        ),
        fastq_aspera=(
            "fasp.sra.ebi.ac.uk:/vol1/fastq/ERR107/ERR1074767_1.fastq.gz;"
            "fasp.sra.ebi.ac.uk:/vol1/fastq/ERR107/ERR1074767_2.fastq.gz"
        ),
        fastq_bytes="123456;234567",
        fastq_md5="d41d8cd98f00b204e9800998ecf8427e;098f6bcd4621d373cade4e832627b4f6",
        read_count="1000",
        base_count="150000",
    )
    assert run.run_accession == "ERR1074767"
    assert run.experiment_accession == "ERX1111111"
    assert run.sample_accession == "SAMEA3610311"
    assert run.study_accession == "PRJEB11419"
    assert run.library_layout == "PAIRED"
    assert run.library_strategy == "WGS"
    assert run.library_source == "METAGENOMIC"
    assert run.library_selection == "RANDOM"
    assert run.fastq_ftp == [
        "ftp.sra.ebi.ac.uk/vol1/fastq/ERR107/ERR1074767_1.fastq.gz",
        "ftp.sra.ebi.ac.uk/vol1/fastq/ERR107/ERR1074767_2.fastq.gz",
    ]
    assert run.fastq_aspera == [
        "fasp.sra.ebi.ac.uk:/vol1/fastq/ERR107/ERR1074767_1.fastq.gz",
        "fasp.sra.ebi.ac.uk:/vol1/fastq/ERR107/ERR1074767_2.fastq.gz",
    ]
    assert run.fastq_bytes == [123456, 234567]
    assert run.fastq_md5 == [
        "d41d8cd98f00b204e9800998ecf8427e",
        "098f6bcd4621d373cade4e832627b4f6",
    ]
    assert run.read_count == 1000
    assert run.base_count == 150000


def test_ena_run_record_single_end_lists_are_single_element():
    from qiita_common.models.ena import EnaRunRecord

    run = EnaRunRecord(
        run_accession="ERR1074767",
        experiment_accession="ERX1111111",
        sample_accession="SAMEA3610311",
        study_accession="PRJEB11419",
        fastq_ftp="ftp.sra.ebi.ac.uk/vol1/fastq/ERR107/ERR1074767.fastq.gz",
        fastq_bytes="123456",
        fastq_md5="d41d8cd98f00b204e9800998ecf8427e",
        read_count="1000",
        base_count="150000",
    )
    assert run.fastq_ftp == ["ftp.sra.ebi.ac.uk/vol1/fastq/ERR107/ERR1074767.fastq.gz"]
    assert run.fastq_bytes == [123456]


def test_ena_run_record_blank_optional_fields_default_empty():
    from qiita_common.models.ena import EnaRunRecord

    run = EnaRunRecord(
        run_accession="ERR1074767",
        experiment_accession="ERX1111111",
        sample_accession="SAMEA3610311",
        study_accession="PRJEB11419",
    )
    assert run.fastq_ftp == []
    assert run.fastq_bytes == []
    assert run.read_count is None
    assert run.base_count is None


def test_ena_run_record_rejects_garbage_fastq_bytes():
    from qiita_common.models.ena import EnaRunRecord

    with pytest.raises(ValidationError):
        EnaRunRecord(
            run_accession="ERR1074767",
            experiment_accession="ERX1111111",
            sample_accession="SAMEA3610311",
            study_accession="PRJEB11419",
            fastq_bytes="not-a-number",
        )


def test_ena_run_record_rejects_garbage_read_count():
    from qiita_common.models.ena import EnaRunRecord

    with pytest.raises(ValidationError):
        EnaRunRecord(
            run_accession="ERR1074767",
            experiment_accession="ERX1111111",
            sample_accession="SAMEA3610311",
            study_accession="PRJEB11419",
            read_count="unknown",
        )


def test_ena_sample_attributes_pivot():
    from qiita_common.models.ena import EnaSampleAttributes

    attrs = EnaSampleAttributes(
        sample_accession="SAMEA3610311",
        attributes={
            "collection date": "2013-01-01",
            "geographic location (country and/or sea)": "USA",
        },
    )
    assert attrs.sample_accession == "SAMEA3610311"
    assert attrs.attributes["collection date"] == "2013-01-01"
    assert len(attrs.attributes) == 2


def test_ena_sample_attributes_rejects_empty_sample_accession():
    from qiita_common.models.ena import EnaSampleAttributes

    with pytest.raises(ValidationError):
        EnaSampleAttributes(sample_accession="", attributes={})


def test_ena_sample_attributes_rejects_blank_tag():
    from qiita_common.models.ena import EnaSampleAttributes

    with pytest.raises(ValidationError):
        EnaSampleAttributes(sample_accession="SAMEA3610311", attributes={"": "USA"})


def test_ena_sample_attributes_defaults_to_empty_map():
    from qiita_common.models.ena import EnaSampleAttributes

    attrs = EnaSampleAttributes(sample_accession="SAMEA3610311")
    assert attrs.attributes == {}
