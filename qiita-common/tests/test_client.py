"""Tests for ControlPlaneClient (Phase I auth params + Phase B method surface)."""

from pathlib import Path

import httpx
import pytest


def test_client_importable():
    """ControlPlaneClient must be importable."""
    from qiita_common.client import ControlPlaneClient

    assert ControlPlaneClient is not None


def test_client_has_required_methods():
    """ControlPlaneClient must expose the reference-management methods."""
    from qiita_common.client import ControlPlaneClient

    client = ControlPlaneClient(
        base_url="http://localhost:8080", api_token="qk_test"
    )
    assert callable(client.create_reference)
    assert callable(client.mint_features)
    assert callable(client.update_reference_status)
    assert callable(client.register_files)
    assert callable(client.get_doget_ticket)


def test_client_raises_when_both_token_and_token_path_set(tmp_path):
    from qiita_common.client import ControlPlaneClient

    p = tmp_path / "tok"
    p.write_text("qk_from_file")
    with pytest.raises(ValueError, match="mutually exclusive"):
        ControlPlaneClient(
            "http://localhost:8080",
            api_token="qk_inline",
            api_token_path=p,
        )


def test_client_raises_when_neither_token_nor_token_path_set():
    from qiita_common.client import ControlPlaneClient

    with pytest.raises(ValueError, match="exactly one"):
        ControlPlaneClient("http://localhost:8080")


def test_client_reads_token_from_path(tmp_path):
    from qiita_common.client import ControlPlaneClient

    p = tmp_path / "tok"
    p.write_text("qk_from_file\n")  # trailing newline should be stripped
    client = ControlPlaneClient(
        "http://localhost:8080", api_token_path=p
    )
    # Internal access for testing the loaded value.
    assert client._token == "qk_from_file"


def test_client_attaches_authorization_header(tmp_path):
    from qiita_common.client import ControlPlaneClient

    p = tmp_path / "tok"
    p.write_text("qk_AAAA")
    client = ControlPlaneClient("http://localhost:8080", api_token_path=p)
    assert client._http.headers["Authorization"] == "Bearer qk_AAAA"


def test_client_repr_redacts_token(tmp_path):
    from qiita_common.client import ControlPlaneClient

    p = tmp_path / "tok"
    p.write_text("qk_BBBB")
    client = ControlPlaneClient("http://localhost:8080", api_token_path=p)
    s = repr(client)
    assert "qk_BBBB" not in s
    assert "<redacted>" in s


def test_client_with_explicit_http_client_skips_header_setup():
    """Caller-supplied http_client takes precedence — they own the auth setup."""
    from qiita_common.client import ControlPlaneClient

    # Caller-controlled headers; we still require api_token to satisfy the
    # constructor contract, but the actual outgoing requests follow the
    # injected client's headers.
    custom = httpx.AsyncClient(
        base_url="http://localhost:8080",
        headers={"Authorization": "Bearer caller-supplied"},
    )
    client = ControlPlaneClient(
        "http://localhost:8080",
        api_token="qk_unused",
        http_client=custom,
    )
    assert client._http is custom
    assert client._http.headers["Authorization"] == "Bearer caller-supplied"
