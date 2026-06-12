"""Tests for qiita_compute_orchestrator.config.

Focus: the asymmetric token-resolution surface. `Settings.from_env()`
is called eagerly by the FastAPI lifespan (where `cp_to_co_token` is
required — it's the inbound `POST /step/*` bearer). It's *also*
called lazily by `get_settings()` from the SLURM-launcher / CLI path,
where `cp_to_co_token` is irrelevant because the launcher never serves
that route. The flag below threads that distinction.
"""

from __future__ import annotations

import pytest

from qiita_compute_orchestrator.config import (
    Settings,
    _settings_ctx,
    get_settings,
    install_settings,
)


@pytest.fixture(autouse=True)
def _reset_settings_ctx():
    """Clear the install_settings ContextVar between tests so one test's
    install doesn't leak into the next test's no-install fallback path."""
    token = _settings_ctx.set(None)
    try:
        yield
    finally:
        _settings_ctx.reset(token)


def test_from_env_default_requires_cp_to_co_token(monkeypatch, tmp_path):
    """The orchestrator service path. With no readable token file and
    no env-var fallback, `from_env()` must raise — boot-time fail-fast."""
    # Remove both file and env-var sources for cp_to_co.
    monkeypatch.setenv("CP_TO_CO_TOKEN_PATH", str(tmp_path / "missing-cp-to-co.token"))
    monkeypatch.delenv("CP_TO_CO_TOKEN", raising=False)
    # Keep co_to_cp resolvable so this test isolates the cp_to_co path.
    monkeypatch.setenv("QIITA_ALLOW_TOKEN_ENV", "true")
    monkeypatch.setenv("CO_TO_CP_TOKEN", "test-co-to-cp-token")
    with pytest.raises(RuntimeError, match="CP↔CO"):
        Settings.from_env()


def test_from_env_skips_cp_to_co_token_when_not_required(monkeypatch, tmp_path):
    """The launcher / CLI path. Same missing-token surface, but
    `require_cp_to_co_token=False` returns Settings with an empty
    `cp_to_co_token` instead of raising. `co_to_cp_token` still
    resolves because the launcher does need it for outbound calls."""
    monkeypatch.setenv("CP_TO_CO_TOKEN_PATH", str(tmp_path / "missing-cp-to-co.token"))
    monkeypatch.delenv("CP_TO_CO_TOKEN", raising=False)
    monkeypatch.setenv("QIITA_ALLOW_TOKEN_ENV", "true")
    monkeypatch.setenv("CO_TO_CP_TOKEN", "test-co-to-cp-token")
    settings = Settings.from_env(require_cp_to_co_token=False)
    assert settings.cp_to_co_token == ""
    assert settings.co_to_cp_token == "test-co-to-cp-token"


def test_get_settings_no_install_fallback_skips_cp_to_co_token(monkeypatch, tmp_path):
    """End-to-end: with no `install_settings` and an unresolvable
    cp_to_co token, `get_settings()` must NOT raise — that's the
    SLURM-launcher path, where the no-install fallback is supposed to
    pass `require_cp_to_co_token=False` internally.

    This is the behavior that lets `SlurmBackend.submit_step` drop
    `CP_TO_CO_TOKEN` from the per-job env. If this test starts
    failing, the deploy host is back to needing the inbound bearer
    in every SLURM job's `scontrol show job` output."""
    monkeypatch.setenv("CP_TO_CO_TOKEN_PATH", str(tmp_path / "missing-cp-to-co.token"))
    monkeypatch.delenv("CP_TO_CO_TOKEN", raising=False)
    monkeypatch.setenv("QIITA_ALLOW_TOKEN_ENV", "true")
    monkeypatch.setenv("CO_TO_CP_TOKEN", "test-co-to-cp-token")
    settings = get_settings()
    assert settings.cp_to_co_token == ""
    assert settings.co_to_cp_token == "test-co-to-cp-token"


def test_get_settings_returns_installed_value(monkeypatch):
    """When `install_settings` was called (the orchestrator service
    lifespan path), `get_settings()` returns the cached value
    verbatim — no re-resolution from env."""
    sentinel = Settings(
        backend_type="local",
        path_scratch="/tmp/sentinel",
        path_derived="/tmp/sentinel-derived",
        cp_to_co_token="cached-cp-to-co",
        cp_url="https://sentinel.invalid",
        co_to_cp_token="cached-co-to-cp",
        slurm=None,
    )
    install_settings(sentinel)
    assert get_settings() is sentinel


def test_from_env_path_derived_explicit(monkeypatch):
    """PATH_DERIVED is the derived-artifact ROOT (NOT .../images). Resolved on
    every backend and leniently — no absolute/exists assertion — mirroring
    path_scratch, because native index builders (build_rype_index,
    build_minimap2_index) derive `{path_derived}/references/{idx}/...` under
    both LocalBackend and SLURM. Distinct from the strict slurm-only
    path_derived_images (which is PATH_DERIVED/images)."""
    monkeypatch.setenv("PATH_DERIVED", "/scratch/persistent")
    monkeypatch.setenv("QIITA_ALLOW_TOKEN_ENV", "true")
    monkeypatch.setenv("CO_TO_CP_TOKEN", "t")
    settings = Settings.from_env(require_cp_to_co_token=False)
    assert settings.path_derived == "/scratch/persistent"


def test_from_env_path_derived_dev_fallback(monkeypatch):
    """With no PATH_DERIVED, path_derived falls back under TMPDIR — the
    dev/local posture (no fail-fast)."""
    monkeypatch.delenv("PATH_DERIVED", raising=False)
    monkeypatch.setenv("TMPDIR", "/tmp/xyz")
    monkeypatch.setenv("QIITA_ALLOW_TOKEN_ENV", "true")
    monkeypatch.setenv("CO_TO_CP_TOKEN", "t")
    settings = Settings.from_env(require_cp_to_co_token=False)
    assert settings.path_derived == "/tmp/xyz/qiita/derived"
