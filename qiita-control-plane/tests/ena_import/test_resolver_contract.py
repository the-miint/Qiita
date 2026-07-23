"""Tests for the `EnaResolver` ABC -- the interface contract both implementations share.
No implementation is bound here."""

import inspect

import pytest


def test_ena_resolver_cannot_be_instantiated_directly():
    from qiita_control_plane.ena_import.resolver import EnaResolver

    with pytest.raises(TypeError, match="abstract"):
        EnaResolver()


def test_ena_resolver_declares_exactly_the_three_abstract_methods():
    from qiita_control_plane.ena_import.resolver import EnaResolver

    assert EnaResolver.__abstractmethods__ == frozenset(
        {"resolve_study_header", "resolve_runs", "resolve_sample_attributes"}
    )


def test_resolve_study_header_signature():
    from qiita_control_plane.ena_import.resolver import EnaResolver

    sig = inspect.signature(EnaResolver.resolve_study_header)
    assert list(sig.parameters) == ["self", "accession"]


def test_resolve_runs_signature():
    from qiita_control_plane.ena_import.resolver import EnaResolver

    sig = inspect.signature(EnaResolver.resolve_runs)
    assert list(sig.parameters) == ["self", "accession"]


def test_resolve_sample_attributes_signature():
    from qiita_control_plane.ena_import.resolver import EnaResolver

    sig = inspect.signature(EnaResolver.resolve_sample_attributes)
    assert list(sig.parameters) == ["self", "accession"]


def test_incomplete_subclass_cannot_be_instantiated():
    """A subclass implementing only part of the contract stays abstract -- stops a
    resolver from silently returning None/empty for an unimplemented method."""
    from qiita_control_plane.ena_import.resolver import EnaResolver

    class _Partial(EnaResolver):
        def resolve_study_header(self, accession):
            raise NotImplementedError

    with pytest.raises(TypeError, match="abstract"):
        _Partial()


def test_complete_subclass_can_be_instantiated():
    from qiita_control_plane.ena_import.resolver import EnaResolver

    class _Complete(EnaResolver):
        def resolve_study_header(self, accession):
            raise NotImplementedError

        def resolve_runs(self, accession):
            raise NotImplementedError

        def resolve_sample_attributes(self, accession):
            raise NotImplementedError

    assert _Complete() is not None


def test_ena_accession_not_found_error_is_a_runtime_error():
    from qiita_control_plane.ena_import.resolver import EnaAccessionNotFoundError

    assert issubclass(EnaAccessionNotFoundError, RuntimeError)


def test_pivot_sample_attributes_groups_by_sample():
    from qiita_common.models.ena import EnaSampleAttributes

    from qiita_control_plane.ena_import.resolver import pivot_sample_attributes

    columns = ["sample_accession", "tag", "value"]
    rows = [
        ["SAMEA1", "collection date", "2013-01-01"],
        ["SAMEA1", "country", "USA"],
        ["SAMEA2", "collection date", "2014-02-02"],
    ]
    result = pivot_sample_attributes(columns, rows)
    assert result == [
        EnaSampleAttributes(
            sample_accession="SAMEA1",
            attributes={"collection date": "2013-01-01", "country": "USA"},
        ),
        EnaSampleAttributes(
            sample_accession="SAMEA2", attributes={"collection date": "2014-02-02"}
        ),
    ]


def test_pivot_sample_attributes_empty_rows_returns_empty_list():
    from qiita_control_plane.ena_import.resolver import pivot_sample_attributes

    assert pivot_sample_attributes(["sample_accession", "tag", "value"], []) == []
