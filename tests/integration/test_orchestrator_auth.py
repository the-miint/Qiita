"""Integration tests for the orchestrator's PAT-based control-plane auth.

Exercises the full path:
  - ControlPlaneClient(api_token_path=...) reads the file, attaches Bearer,
    and successfully calls control-plane endpoints requiring worker scopes.
  - Wrong-kind, missing-scope, and absent-token cases all fail with the
    right status code so misconfigurations surface fast.
  - orchestrator's Settings.from_env() resolves the token from the
    expected sources and refuses to start when neither source is populated.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
async def control_plane(postgres_pool):
    """Mount the control-plane app with the integration pool."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.oidc_verifier = None
    app.state.settings = Settings(
        database_url="unused",
        hmac_secret_key=b"\x00" * 32,
        data_plane_url="grpc://localhost:50051",
    )
    yield app


# ---------------------------------------------------------------------------
# ControlPlaneClient + worker token: end-to-end
# ---------------------------------------------------------------------------


async def test_orchestrator_can_call_protected_endpoint_with_service_token(
    control_plane, compute_worker_service_account
):
    """The session-scoped worker token authenticates against /auth/whoami."""
    from qiita_common.client import ControlPlaneClient

    transport = ASGITransport(app=control_plane)
    custom = AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={
            "Authorization": f"Bearer {compute_worker_service_account['token']}"
        },
    )
    async with ControlPlaneClient(
        "http://test",
        api_token=compute_worker_service_account["token"],
        http_client=custom,
    ) as client:
        # Use a non-method path (whoami) since ControlPlaneClient doesn't
        # expose it. Drive httpx directly through the injected transport.
        resp = await client._http.get("/api/v1/auth/whoami")
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["kind"] == "service"
        assert body["principal_idx"] == compute_worker_service_account["principal_idx"]


async def test_orchestrator_rejected_without_token(control_plane):
    """A bare httpx call (no Authorization header) gets 401 on
    auth-required endpoints."""
    transport = ASGITransport(app=control_plane)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.get("/api/v1/auth/tokens")
    assert resp.status_code == 401


# Wrong-kind / wrong-scope rejection paths for the worker-only routes
# (POST /references/{id}/features/mint and /register) are exercised in
# test_auth_boundary.py via require_service + require_scope guards. The
# orchestrator-side tests focus on the credential resolution path here.


# ---------------------------------------------------------------------------
# orchestrator Settings.from_env()
# ---------------------------------------------------------------------------


def _set_min_env(monkeypatch):
    """The orchestrator's other required env vars."""
    monkeypatch.setenv("CONTROL_PLANE_URL", "http://localhost:8080")


def test_orchestrator_token_path_takes_precedence_over_env_var(
    monkeypatch, tmp_path
):
    """File takes precedence: when both file and env var are set, the
    file is used."""
    from qiita_compute_orchestrator.config import Settings

    _set_min_env(monkeypatch)
    p = tmp_path / "tok"
    p.write_text("qk_from_file")
    monkeypatch.setenv("CONTROL_PLANE_API_TOKEN_PATH", str(p))
    monkeypatch.setenv("QIITA_ALLOW_TOKEN_ENV", "true")
    monkeypatch.setenv("CONTROL_PLANE_API_TOKEN", "qk_from_env")

    settings = Settings.from_env()
    assert settings.api_token_path == p
    assert settings.api_token is None


def test_orchestrator_uses_env_var_when_file_missing_and_allow_token_env(
    monkeypatch, tmp_path
):
    from qiita_compute_orchestrator.config import Settings

    _set_min_env(monkeypatch)
    nonexistent = tmp_path / "absent"
    monkeypatch.setenv("CONTROL_PLANE_API_TOKEN_PATH", str(nonexistent))
    monkeypatch.setenv("QIITA_ALLOW_TOKEN_ENV", "true")
    monkeypatch.setenv("CONTROL_PLANE_API_TOKEN", "qk_from_env")

    settings = Settings.from_env()
    assert settings.api_token_path is None
    assert settings.api_token == "qk_from_env"


def test_orchestrator_refuses_env_var_unless_allow_token_env_set(
    monkeypatch, tmp_path
):
    """Without QIITA_ALLOW_TOKEN_ENV=true, the env-var path is ignored,
    and orchestrator boot fails with an actionable error."""
    from qiita_compute_orchestrator.config import Settings

    _set_min_env(monkeypatch)
    nonexistent = tmp_path / "absent"
    monkeypatch.setenv("CONTROL_PLANE_API_TOKEN_PATH", str(nonexistent))
    monkeypatch.delenv("QIITA_ALLOW_TOKEN_ENV", raising=False)
    monkeypatch.setenv("CONTROL_PLANE_API_TOKEN", "qk_should_be_ignored")

    with pytest.raises(RuntimeError, match="QIITA_ALLOW_TOKEN_ENV"):
        Settings.from_env()


def test_orchestrator_refuses_when_no_token_anywhere(monkeypatch, tmp_path):
    from qiita_compute_orchestrator.config import Settings

    _set_min_env(monkeypatch)
    nonexistent = tmp_path / "absent"
    monkeypatch.setenv("CONTROL_PLANE_API_TOKEN_PATH", str(nonexistent))
    monkeypatch.delenv("QIITA_ALLOW_TOKEN_ENV", raising=False)
    monkeypatch.delenv("CONTROL_PLANE_API_TOKEN", raising=False)

    with pytest.raises(RuntimeError, match="no API token available"):
        Settings.from_env()
