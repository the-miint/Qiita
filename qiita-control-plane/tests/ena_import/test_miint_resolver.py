"""Tests for `MiintEnaResolver`, driving DuckDB + the miint
`read_ena` / `read_ena_attributes` table functions.

Network-free: the module-level query functions (`_query_ena_*`) are monkeypatched by
fully-qualified name. Fixtures under `fixtures/` are real rows from public study
PRJNA48739."""

import json
from pathlib import Path

import pytest
from qiita_common.models.ena import EnaRunRecord, EnaSampleAttributes, EnaStudyHeader

from qiita_control_plane.ena_import.resolver import EnaAccessionNotFoundError

FIXTURES = Path(__file__).parent / "fixtures"

_QUERY_STUDY = "qiita_control_plane.ena_import.miint_resolver._query_ena_study_header"
_QUERY_RUNS = "qiita_control_plane.ena_import.miint_resolver._query_ena_runs"
_QUERY_ATTRS = "qiita_control_plane.ena_import.miint_resolver._query_ena_sample_attributes"


def _load_fixture(name: str) -> tuple[list[str], list[list[str]]]:
    data = json.loads((FIXTURES / name).read_text())
    return data["columns"], data["rows"]


# Field-by-field assertions against the PRJNA48739 fixture data, pinning the
# resolver's contract. Inlined here (miint is the sole resolver, so the old
# cross-resolver "shared contract" module had a single importer).
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


def test_resolve_study_header_maps_fields(monkeypatch):
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    columns, rows = _load_fixture("study_header.json")
    monkeypatch.setattr(_QUERY_STUDY, lambda accession: (columns, rows))

    header = MiintEnaResolver().resolve_study_header("PRJNA48739")

    assert_prjna48739_study_header(header)
    assert header.first_public == "2013-05-31"


def test_resolve_study_header_zero_rows_is_not_found(monkeypatch):
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    monkeypatch.setattr(_QUERY_STUDY, lambda accession: (["study_accession"], []))

    with pytest.raises(EnaAccessionNotFoundError, match="PRJEB00000000"):
        MiintEnaResolver().resolve_study_header("PRJEB00000000")


def test_resolve_study_header_rejects_non_study_accession(monkeypatch):
    from qiita_control_plane.ena_import.accession import InvalidEnaAccessionError
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    monkeypatch.setattr(_QUERY_STUDY, lambda accession: pytest.fail("must not query"))

    with pytest.raises(InvalidEnaAccessionError):
        MiintEnaResolver().resolve_study_header("SAMEA3610311")


def test_resolve_runs_maps_field_by_field(monkeypatch):
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    columns, rows = _load_fixture("runs.json")
    monkeypatch.setattr(_QUERY_RUNS, lambda accession: (columns, rows))

    runs = MiintEnaResolver().resolve_runs("PRJNA48739")

    assert_prjna48739_runs(runs)


def test_resolve_runs_zero_rows_is_not_found(monkeypatch):
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    monkeypatch.setattr(_QUERY_RUNS, lambda accession: (["run_accession"], []))

    with pytest.raises(EnaAccessionNotFoundError, match="PRJEB00000000"):
        MiintEnaResolver().resolve_runs("PRJEB00000000")


def test_resolve_sample_attributes_pivots_by_sample(monkeypatch):
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    columns, rows = _load_fixture("sample_attributes.json")
    monkeypatch.setattr(_QUERY_ATTRS, lambda accession: (columns, rows))

    attrs = MiintEnaResolver().resolve_sample_attributes("PRJNA48739")

    assert_prjna48739_sample_attributes(attrs)


def test_resolve_sample_attributes_zero_rows_returns_empty_list(monkeypatch):
    """Real DDBJ shape (PRJDB40364's SAMD01818724 has zero attributes): a 0-row
    read_ena_attributes result is "no attributes", not "nonexistent" -- must NOT raise."""
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    monkeypatch.setattr(_QUERY_ATTRS, lambda accession: (["sample_accession", "tag", "value"], []))

    attrs = MiintEnaResolver().resolve_sample_attributes("PRJDB40364")

    assert attrs == []


def test_resolve_runs_rejects_empty_accession(monkeypatch):
    from qiita_control_plane.ena_import.accession import InvalidEnaAccessionError
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    monkeypatch.setattr(_QUERY_RUNS, lambda accession: pytest.fail("must not query"))

    with pytest.raises(InvalidEnaAccessionError):
        MiintEnaResolver().resolve_runs("")


# ---------------------------------------------------------------------------
# Service-side connect contract: LOAD-only, never INSTALL. The resolver runs
# inside the CP service (qiita-api, whose $HOME is /dev/null), so it must use the
# staged LOAD-only helper and never reach an INSTALL path (see
# qiita_control_plane.miint; the analog test is tests/test_miint_connect.py).
# ---------------------------------------------------------------------------


def test_resolver_binds_the_staged_helper_not_the_client_installer():
    """The resolver must bind `connect_with_miint_staged` (LOAD-only) and not the
    client-side `connect_with_miint` (INSTALL): a service-side INSTALL resolves
    `$HOME/.duckdb` and dies on qiita-api's `/dev/null` home. httpfs rides along
    via `miint_load_sql` (pinned in qiita-common), so there is nothing extra to
    load here."""
    from qiita_control_plane import miint as miint_module
    from qiita_control_plane.ena_import import miint_resolver

    assert miint_resolver.connect_with_miint_staged is miint_module.connect_with_miint_staged
    assert not hasattr(miint_resolver, "connect_with_miint")
