"""Tests for `HttpEnaResolver`: the plain-ENA-Portal-API fallback. Network-free via
`httpx.MockTransport` against recorded Portal-TSV / Browser-XML fixtures for study
PRJNA48739 (shared with `test_miint_resolver.py` via `_resolver_contract_checks.py`).
Also covers the `get_resolver` factory."""

from pathlib import Path

import httpx
import pytest

from qiita_control_plane.ena_import.resolver import EnaAccessionNotFoundError

from ._resolver_contract_checks import (
    assert_prjna48739_runs,
    assert_prjna48739_sample_attributes,
    assert_prjna48739_study_header,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _fixture_text(name: str) -> str:
    return (FIXTURES / name).read_text()


def _handler(request: httpx.Request) -> httpx.Response:
    url = str(request.url)
    if "result=study" in url:
        return httpx.Response(200, text=_fixture_text("portal_study.tsv"))
    if "result=read_run" in url:
        return httpx.Response(200, text=_fixture_text("portal_read_run.tsv"))
    if "result=sample" in url:
        return httpx.Response(200, text=_fixture_text("portal_sample_accessions.tsv"))
    if "/browser/api/xml/" in url:
        return httpx.Response(200, text=_fixture_text("browser_samples.xml"))
    raise AssertionError(f"unexpected URL: {url}")


def _empty_handler(request: httpx.Request) -> httpx.Response:
    url = str(request.url)
    if "/browser/api/xml/" in url:
        return httpx.Response(200, text='<?xml version="1.0"?><SAMPLE_SET></SAMPLE_SET>')
    # header row only — zero data rows.
    header_by_result = {
        "result=study": "study_accession\n",
        "result=read_run": "run_accession\n",
        "result=sample": "sample_accession\n",
    }
    for marker, body in header_by_result.items():
        if marker in url:
            return httpx.Response(200, text=body)
    raise AssertionError(f"unexpected URL: {url}")


def _no_attributes_handler(request: httpx.Request) -> httpx.Response:
    """A real sample with zero `<SAMPLE_ATTRIBUTE>` elements -- live DDBJ shape
    (PRJDB40364's SAMD01818724)."""
    url = str(request.url)
    if "result=sample" in url:
        return httpx.Response(200, text="sample_accession\nSAMD01818724\n")
    if "/browser/api/xml/" in url:
        return httpx.Response(
            200,
            text='<?xml version="1.0"?>'
            '<SAMPLE_SET><SAMPLE accession="SAMD01818724"></SAMPLE></SAMPLE_SET>',
        )
    raise AssertionError(f"unexpected URL: {url}")


def _resolver_with(handler):
    from qiita_control_plane.ena_import.http_resolver import HttpEnaResolver

    return HttpEnaResolver(http_client=httpx.Client(transport=httpx.MockTransport(handler)))


def test_resolve_study_header_maps_fields():
    resolver = _resolver_with(_handler)
    header = resolver.resolve_study_header("PRJNA48739")

    assert_prjna48739_study_header(header)


def test_resolve_study_header_zero_rows_is_not_found():
    resolver = _resolver_with(_empty_handler)
    with pytest.raises(EnaAccessionNotFoundError, match="PRJEB00000000"):
        resolver.resolve_study_header("PRJEB00000000")


def test_resolve_runs_maps_field_by_field():
    resolver = _resolver_with(_handler)
    runs = resolver.resolve_runs("PRJNA48739")

    assert_prjna48739_runs(runs)


def test_resolve_runs_zero_rows_is_not_found():
    resolver = _resolver_with(_empty_handler)
    with pytest.raises(EnaAccessionNotFoundError, match="PRJEB00000000"):
        resolver.resolve_runs("PRJEB00000000")


def test_resolve_sample_attributes_pivots_by_sample():
    resolver = _resolver_with(_handler)
    attrs = resolver.resolve_sample_attributes("PRJNA48739")

    assert_prjna48739_sample_attributes(attrs)


def test_resolve_sample_attributes_zero_samples_is_not_found():
    """Zero rows from `result=sample` means the study has no samples -- raises."""
    resolver = _resolver_with(_empty_handler)
    with pytest.raises(EnaAccessionNotFoundError, match="PRJEB00000000"):
        resolver.resolve_sample_attributes("PRJEB00000000")


def test_resolve_sample_attributes_zero_attributes_for_real_sample_returns_empty_list():
    """A real sample with zero attributes is "no attributes", not "nonexistent" --
    unlike the zero-samples case, this must NOT raise."""
    resolver = _resolver_with(_no_attributes_handler)

    attrs = resolver.resolve_sample_attributes("PRJDB40364")

    assert attrs == []


def test_resolve_study_header_rejects_non_study_accession():
    from qiita_control_plane.ena_import.accession import InvalidEnaAccessionError

    resolver = _resolver_with(lambda request: pytest.fail("must not query"))
    with pytest.raises(InvalidEnaAccessionError):
        resolver.resolve_study_header("SAMEA3610311")


# ---------------------------------------------------------------------------
# get_resolver factory
# ---------------------------------------------------------------------------


def test_get_resolver_defaults_to_miint():
    from qiita_control_plane.ena_import import get_resolver
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    assert isinstance(get_resolver(), MiintEnaResolver)


def test_get_resolver_miint_backend():
    from qiita_control_plane.ena_import import get_resolver
    from qiita_control_plane.ena_import.miint_resolver import MiintEnaResolver

    assert isinstance(get_resolver(backend="miint"), MiintEnaResolver)


def test_get_resolver_http_backend():
    from qiita_control_plane.ena_import import get_resolver
    from qiita_control_plane.ena_import.http_resolver import HttpEnaResolver

    assert isinstance(get_resolver(backend="http"), HttpEnaResolver)


def test_get_resolver_raises_on_unknown_backend():
    from qiita_control_plane.ena_import import get_resolver

    with pytest.raises(ValueError, match="unknown ENA resolver backend"):
        get_resolver(backend="carrier-pigeon")
