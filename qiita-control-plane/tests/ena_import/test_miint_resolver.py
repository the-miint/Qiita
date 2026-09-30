"""Tests for `MiintEnaResolver`, driving DuckDB + the miint
`read_ena` / `read_ena_attributes` table functions.

Network-free: the module-level query functions (`_query_ena_*`) are monkeypatched by
fully-qualified name. Fixtures under `fixtures/` are real rows from public study
PRJNA48739."""

import json
from pathlib import Path

import pytest
from pydantic import ValidationError
from qiita_common.models.ena import EnaRunRecord, EnaSampleAttributes, EnaStatus, EnaStudyHeader

from qiita_control_plane.ena_import import miint_resolver
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
    assert header.status is EnaStatus.PUBLIC


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
    assert single.status is EnaStatus.PUBLIC

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


def _group_fixture_rows(columns, rows):
    """Group the recorded narrow `(sample_accession, tag, value)` fixture the way
    `_query_ena_sample_attributes`' SQL now does, so the fixture keeps its
    recorded-from-ENA shape while the fake matches what DuckDB returns."""
    sample_i, tag_i, value_i = (columns.index(c) for c in ("sample_accession", "tag", "value"))
    grouped: dict[str, dict[str, str]] = {}
    for row in rows:
        grouped.setdefault(row[sample_i], {})[row[tag_i]] = row[value_i]
    return sorted(grouped.items())


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
    """PRJEB99999999 is a genuine zero-row Portal response (see the fixture's
    `_source`), not a synthetic one -- proving the not-found path against a
    real "doesn't exist" shape, including the `status` column now always
    requested."""
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    columns, rows = _load_fixture("study_header_not_found.json")
    monkeypatch.setattr(_QUERY_STUDY, lambda accession: (columns, rows))

    with pytest.raises(EnaAccessionNotFoundError, match="PRJEB99999999"):
        MiintEnaResolver().resolve_study_header("PRJEB99999999")


def test_resolve_study_header_rejects_non_study_accession(monkeypatch):
    from qiita_common.ena_accession import InvalidEnaAccessionError

    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    monkeypatch.setattr(_QUERY_STUDY, lambda accession: pytest.fail("must not query"))

    with pytest.raises(InvalidEnaAccessionError):
        MiintEnaResolver().resolve_study_header("SAMEA3610311")


def test_resolve_runs_maps_field_by_field(monkeypatch):
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    columns, rows = _load_fixture("ena_runs.json")
    monkeypatch.setattr(_QUERY_RUNS, lambda accession: (columns, rows))

    runs = MiintEnaResolver().resolve_ena_runs("PRJNA48739")

    assert_prjna48739_runs(runs)


def test_resolve_runs_zero_rows_is_not_found(monkeypatch):
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    monkeypatch.setattr(_QUERY_RUNS, lambda accession: (["run_accession"], []))

    with pytest.raises(EnaAccessionNotFoundError, match="PRJEB00000000"):
        MiintEnaResolver().resolve_ena_runs("PRJEB00000000")


def test_resolve_runs_includes_a_suppressed_run(monkeypatch):
    """A non-public run is not filtered here -- the resolver's job is to report
    ENA's status faithfully; refusing/excluding it is the batch driver's and
    registration's job."""
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    columns, rows = _load_fixture("ena_runs_suppressed.json")
    monkeypatch.setattr(_QUERY_RUNS, lambda accession: (columns, rows))

    runs = MiintEnaResolver().resolve_ena_runs("PRJNA48739")

    assert len(runs) == 1
    assert runs[0].status is EnaStatus.SUPPRESSED


def test_resolve_runs_rejects_an_unrecognized_status(monkeypatch):
    """A status value outside EnaStatus's recognized vocabulary must fail loud
    via Pydantic, not silently pass through as public."""
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    columns, rows = _load_fixture("ena_runs_unknown_status.json")
    monkeypatch.setattr(_QUERY_RUNS, lambda accession: (columns, rows))

    with pytest.raises(ValidationError):
        MiintEnaResolver().resolve_ena_runs("PRJNA48739")


class _FakeCapturingConnection:
    """Fakes `connect_with_miint_staged()`'s context-manager + `execute` shape,
    capturing the last call's SQL params so a test can assert on what fields
    were requested without a live DuckDB+miint session."""

    def __init__(self) -> None:
        self.captured_params: dict | None = None

    def __enter__(self) -> _FakeCapturingConnection:
        return self

    def __exit__(self, *exc_info: object) -> bool:
        return False

    def execute(self, _sql: str, params: dict) -> _FakeCapturingConnection:
        self.captured_params = params
        return self

    @property
    def description(self) -> list[tuple[str]]:
        return [("study_accession",)]

    def fetchall(self) -> list[tuple]:
        return []


def test_query_ena_study_header_requests_exactly_the_model_fields(monkeypatch):
    fake = _FakeCapturingConnection()
    monkeypatch.setattr(miint_resolver, "connect_with_miint_staged", lambda: fake)

    miint_resolver._query_ena_study_header("PRJNA48739")

    assert fake.captured_params["fields"].split(",") == list(EnaStudyHeader.model_fields)


def test_query_ena_runs_requests_exactly_the_model_fields(monkeypatch):
    fake = _FakeCapturingConnection()
    monkeypatch.setattr(miint_resolver, "connect_with_miint_staged", lambda: fake)

    miint_resolver._query_ena_runs("PRJNA48739")

    assert fake.captured_params["fields"].split(",") == list(EnaRunRecord.model_fields)


def test_resolve_sample_attributes_pivots_by_sample(monkeypatch):
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    columns, rows = _load_fixture("sample_attributes.json")
    monkeypatch.setattr(_QUERY_ATTRS, lambda accession: _group_fixture_rows(columns, rows))

    attrs = MiintEnaResolver().resolve_sample_attributes("PRJNA48739")

    assert_prjna48739_sample_attributes(attrs)


def test_resolve_sample_attributes_zero_rows_returns_empty_list(monkeypatch):
    """Real DDBJ shape (PRJDB40364's SAMD01818724 has zero attributes): a 0-row
    read_ena_attributes result is "no attributes", not "nonexistent" -- must NOT raise."""
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    monkeypatch.setattr(_QUERY_ATTRS, lambda accession: [])

    attrs = MiintEnaResolver().resolve_sample_attributes("PRJDB40364")

    assert attrs == []


def test_resolve_runs_rejects_empty_accession(monkeypatch):
    from qiita_common.ena_accession import InvalidEnaAccessionError

    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    monkeypatch.setattr(_QUERY_RUNS, lambda accession: pytest.fail("must not query"))

    with pytest.raises(InvalidEnaAccessionError):
        MiintEnaResolver().resolve_ena_runs("")


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
