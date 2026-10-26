"""Tests for `qiita_common.http_status`."""

import pytest

from qiita_common.http_status import is_transient_error_text, is_transient_http_status


@pytest.mark.parametrize("status", [500, 502, 503, 504, 599, 408, 429])
def test_transient_statuses(status):
    assert is_transient_http_status(status) is True


@pytest.mark.parametrize("status", [200, 400, 401, 403, 404])
def test_permanent_statuses(status):
    assert is_transient_http_status(status) is False


@pytest.mark.parametrize(
    "text", ["read_ena failed: Connection refused (URL: x)", "Operation TIMED OUT", "curl error 7"]
)
def test_transient_error_text(text):
    assert is_transient_error_text(text) is True


@pytest.mark.parametrize("text", ["no sequence data", "malformed FASTQ record", "HTTP 404"])
def test_permanent_error_text(text):
    assert is_transient_error_text(text) is False
