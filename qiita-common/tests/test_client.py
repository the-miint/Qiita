"""Tests for ControlPlaneClient (auth params + user-management method surface)."""

import json

import httpx
import pytest


def test_client_importable():
    """ControlPlaneClient must be importable."""
    from qiita_common.client import ControlPlaneClient

    assert ControlPlaneClient is not None


def test_client_has_required_methods():
    """ControlPlaneClient must expose the reference-management methods.

    The mint/membership/register methods are thin wrappers over the
    /api/v1/library/{name} dispatch endpoint; verifying they're callable
    catches the construction surface, the dispatch shape itself is
    covered by the integration suite (test_library_dispatch.py)."""
    from qiita_common.client import ControlPlaneClient

    client = ControlPlaneClient(base_url="http://localhost:8080", api_token="qk_test")
    assert callable(client.create_reference)
    assert callable(client.mint_features)
    assert callable(client.write_membership)
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
    client = ControlPlaneClient("http://localhost:8080", api_token_path=p)
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


# ---------------------------------------------------------------------------
# Library-dispatch wrappers — verify URL + envelope without a live server
# ---------------------------------------------------------------------------


def _capture_transport(captured: list, response_outputs: dict):
    """An httpx MockTransport that records every request and returns a
    canned `{"outputs": ...}` body."""

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            200,
            content=json.dumps({"outputs": response_outputs}).encode(),
            headers={"content-type": "application/json"},
        )

    return httpx.MockTransport(handler)


async def test_mint_features_posts_to_library_dispatch(tmp_path):
    """client.mint_features must POST /api/v1/library/mint-features with
    the {scope_target, inputs:{manifest_path, output_dir}} envelope and
    unwrap `outputs` for the caller — catches URL/shape drift at
    unit-test time."""
    from pathlib import Path

    from qiita_common.client import ControlPlaneClient

    captured: list[httpx.Request] = []
    transport = _capture_transport(
        captured,
        {
            "feature_map_path": "/workspace/feature_map.parquet",
            "minted": 1,
            "reused": 0,
        },
    )
    custom = httpx.AsyncClient(
        base_url="http://localhost:8080",
        transport=transport,
        headers={"Authorization": "Bearer qk_x"},
    )
    client = ControlPlaneClient(
        "http://localhost:8080",
        api_token="qk_unused",
        http_client=custom,
    )

    manifest = tmp_path / "manifest.parquet"
    manifest.write_bytes(b"")  # content irrelevant — captured by mock
    resp = await client.mint_features(
        reference_idx=42,
        manifest_path=manifest,
        output_dir=Path("/workspace"),
    )

    assert len(captured) == 1
    req = captured[0]
    assert req.method == "POST"
    assert req.url.path == "/api/v1/library/mint-features"
    body = json.loads(req.content)
    assert body["scope_target"] == {"kind": "reference", "reference_idx": 42}
    assert body["inputs"]["manifest_path"] == str(manifest)
    assert body["inputs"]["output_dir"] == "/workspace"
    # Returned model is built from the unwrapped `outputs` field.
    assert resp.feature_map_path == "/workspace/feature_map.parquet"
    assert resp.minted == 1
    assert resp.reused == 0


async def test_write_membership_posts_to_library_dispatch():
    """client.write_membership must POST /api/v1/library/write-membership
    with inputs.feature_map_path."""
    from pathlib import Path

    from qiita_common.client import ControlPlaneClient

    captured: list[httpx.Request] = []
    transport = _capture_transport(captured, {"linked": 3, "already_linked": 0})
    custom = httpx.AsyncClient(
        base_url="http://localhost:8080",
        transport=transport,
        headers={"Authorization": "Bearer qk_x"},
    )
    client = ControlPlaneClient(
        "http://localhost:8080",
        api_token="qk_unused",
        http_client=custom,
    )

    resp = await client.write_membership(
        reference_idx=7, feature_map_path=Path("/workspace/feature_map.parquet")
    )

    assert captured[0].url.path == "/api/v1/library/write-membership"
    body = json.loads(captured[0].content)
    assert body["scope_target"] == {"kind": "reference", "reference_idx": 7}
    assert body["inputs"]["feature_map_path"] == "/workspace/feature_map.parquet"
    assert resp.linked == 3
    assert resp.already_linked == 0


async def test_register_files_posts_to_library_dispatch():
    """client.register_files must POST /api/v1/library/register-files."""
    from qiita_common.client import ControlPlaneClient

    captured: list[httpx.Request] = []
    transport = _capture_transport(captured, {"registered": ["/data/x.parquet"]})
    custom = httpx.AsyncClient(
        base_url="http://localhost:8080",
        transport=transport,
        headers={"Authorization": "Bearer qk_x"},
    )
    client = ControlPlaneClient(
        "http://localhost:8080",
        api_token="qk_unused",
        http_client=custom,
    )

    resp = await client.register_files(
        reference_idx=11,
        staging_dir="/data/staging",
        files={"x.parquet": "reference_sequences"},
    )

    assert captured[0].url.path == "/api/v1/library/register-files"
    body = json.loads(captured[0].content)
    assert body["scope_target"] == {"kind": "reference", "reference_idx": 11}
    assert body["inputs"] == {
        "staging_dir": "/data/staging",
        "files": {"x.parquet": "reference_sequences"},
    }
    assert resp.registered == ["/data/x.parquet"]
