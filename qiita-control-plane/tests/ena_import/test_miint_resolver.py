"""Tests for `MiintEnaResolver`: the default `EnaResolver`, driving DuckDB + the miint
`read_ena` / `read_ena_attributes` table functions.

Network-free: the module-level query functions (`_query_ena_*`) are monkeypatched by
fully-qualified name. Fixtures under `fixtures/` are real rows from public study
PRJNA48739."""

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
# httpfs install-once lock: INSTALL runs at most once per process; LOAD runs per
# connection.
# ---------------------------------------------------------------------------


class _FakeConnection:
    """Records every SQL string passed to `.execute`; nothing else is needed here."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, sql: str, *args, **kwargs) -> _FakeConnection:
        self.executed.append(sql)
        return self


def test_httpfs_install_runs_at_most_once_across_repeated_calls(monkeypatch):
    from qiita_control_plane.ena_import import miint_resolver

    # Reset the module-level once-flag so this test is order-independent.
    monkeypatch.setattr(miint_resolver, "_httpfs_installed", False)

    connections: list[_FakeConnection] = []

    def _fake_connect_with_miint() -> _FakeConnection:
        con = _FakeConnection()
        connections.append(con)
        return con

    monkeypatch.setattr(miint_resolver, "connect_with_miint", _fake_connect_with_miint)

    miint_resolver._open_ena_connection()
    miint_resolver._open_ena_connection()

    assert len(connections) == 2
    all_executed = [sql for con in connections for sql in con.executed]
    install_calls = [sql for sql in all_executed if "INSTALL httpfs" in sql]
    load_calls = [sql for sql in all_executed if "LOAD httpfs" in sql]
    assert len(install_calls) == 1
    # LOAD is per-connection -- once per call, regardless of the INSTALL cache.
    assert len(load_calls) == 2
