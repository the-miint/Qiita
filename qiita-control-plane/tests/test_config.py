"""Tests for control plane Settings."""

import pytest


def test_settings_has_database_url(monkeypatch):
    """Settings must expose a database_url field read from DATABASE_URL env var."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", "dev-secret")

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.database_url == "postgresql://u:p@localhost:5432/db"


def test_settings_requires_database_url(monkeypatch):
    """Settings must raise if DATABASE_URL is missing."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("HMAC_SECRET_KEY", "dev-secret")

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        Settings.from_env()


def test_settings_has_hmac_secret_key(monkeypatch):
    """Settings must expose hmac_secret_key."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", "dev-secret")

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.hmac_secret_key == "dev-secret"


def test_settings_requires_hmac_secret_key(monkeypatch):
    """Settings must raise if HMAC_SECRET_KEY is missing."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.delenv("HMAC_SECRET_KEY", raising=False)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="HMAC_SECRET_KEY"):
        Settings.from_env()
