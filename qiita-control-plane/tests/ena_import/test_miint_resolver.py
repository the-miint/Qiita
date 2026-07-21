"""Tests for `MiintEnaResolver` (T01-2): the default `EnaResolver`
implementation, driving DuckDB + the miint `read_ena` / `read_ena_attributes`
table functions.

Network-free: the DuckDB+miint session is a private, module-level query
function (`_query_ena_study_header` / `_query_ena_runs` /
`_query_ena_sample_attributes`), monkeypatched here by fully-qualified name
— mirrors `qiita_control_plane.runner._stream_masked_reads_to_fastq` (see
`tests/test_read_ingest_resolvers.py`), the established seam for
connect_with_miint()-touching code. Fixtures under `fixtures/` are real rows
recorded from a small, long-lived public ENA study
(PRJNA48739 — see the module docstring on the live system test for why)."""

import json
from pathlib import Path

import pytest

from qiita_control_plane.ena_import.resolver import EnaAccessionNotFoundError

from ._resolver_contract_checks import (
    assert_prjna48739_runs,
    assert_prjna48739_sample_attributes,
    assert_prjna48739_study_header,
)

FIXTURES = Path(__file__).parent / "fixtures"

_QUERY_STUDY = "qiita_control_plane.ena_import.miint_resolver._query_ena_study_header"
_QUERY_RUNS = "qiita_control_plane.ena_import.miint_resolver._query_ena_runs"
_QUERY_ATTRS = "qiita_control_plane.ena_import.miint_resolver._query_ena_sample_attributes"


def _load_fixture(name: str) -> tuple[list[str], list[list[str]]]:
    data = json.loads((FIXTURES / name).read_text())
    return data["columns"], data["rows"]


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


def test_resolve_sample_attributes_zero_rows_is_not_found(monkeypatch):
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    monkeypatch.setattr(_QUERY_ATTRS, lambda accession: (["sample_accession", "tag", "value"], []))

    with pytest.raises(EnaAccessionNotFoundError, match="PRJEB00000000"):
        MiintEnaResolver().resolve_sample_attributes("PRJEB00000000")


def test_resolve_runs_rejects_empty_accession(monkeypatch):
    from qiita_control_plane.ena_import.accession import InvalidEnaAccessionError
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    monkeypatch.setattr(_QUERY_RUNS, lambda accession: pytest.fail("must not query"))

    with pytest.raises(InvalidEnaAccessionError):
        MiintEnaResolver().resolve_runs("")
