"""Tests for control plane Settings."""

import base64
import secrets

import pytest

# A valid base64-encoded 32-byte secret for tests.
_TEST_SECRET_B64 = base64.b64encode(secrets.token_bytes(32)).decode()


def test_settings_has_database_url(monkeypatch):
    """Settings must expose a database_url field read from DATABASE_URL env var."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", _TEST_SECRET_B64)

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.database_url == "postgresql://u:p@localhost:5432/db"


def test_settings_requires_database_url(monkeypatch):
    """Settings must raise if DATABASE_URL is missing."""
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("HMAC_SECRET_KEY", _TEST_SECRET_B64)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        Settings.from_env()


def test_settings_hmac_key_is_bytes(monkeypatch):
    """Settings.hmac_secret_key must be decoded bytes, not the raw base64 string."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    raw_bytes = secrets.token_bytes(32)
    monkeypatch.setenv("HMAC_SECRET_KEY", base64.b64encode(raw_bytes).decode())

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.hmac_secret_key == raw_bytes
    assert isinstance(settings.hmac_secret_key, bytes)


def test_settings_rejects_invalid_base64_hmac(monkeypatch):
    """Settings must raise if HMAC_SECRET_KEY is not valid base64."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", "not!!!valid%%%base64")

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="base64"):
        Settings.from_env()


def test_settings_rejects_short_hmac(monkeypatch):
    """Settings must raise if HMAC_SECRET_KEY decodes to fewer than 16 bytes."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", base64.b64encode(b"short").decode())

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="at least 16 bytes"):
        Settings.from_env()


def test_settings_requires_hmac_secret_key(monkeypatch):
    """Settings must raise if HMAC_SECRET_KEY is missing."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.delenv("HMAC_SECRET_KEY", raising=False)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="HMAC_SECRET_KEY"):
        Settings.from_env()


def test_settings_max_sequence_mint_count_has_default(monkeypatch):
    """max_sequence_mint_count must default to a large but finite cap so
    a buggy compute step cannot burn an unbounded slice of the
    sequence_idx space on a single POST."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", _TEST_SECRET_B64)
    monkeypatch.delenv("QIITA_MAX_SEQUENCE_MINT_COUNT", raising=False)

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert isinstance(settings.max_sequence_mint_count, int)
    assert settings.max_sequence_mint_count > 0


def test_settings_max_sequence_mint_count_from_env(monkeypatch):
    """QIITA_MAX_SEQUENCE_MINT_COUNT overrides the default."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", _TEST_SECRET_B64)
    monkeypatch.setenv("QIITA_MAX_SEQUENCE_MINT_COUNT", "12345")

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.max_sequence_mint_count == 12345


@pytest.mark.parametrize("bad_value", ["0", "-1", "-10000"])
def test_settings_rejects_nonpositive_max_sequence_mint_count(monkeypatch, bad_value):
    """A non-positive cap would make the route reject every mint silently;
    reject it at startup instead."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", _TEST_SECRET_B64)
    monkeypatch.setenv("QIITA_MAX_SEQUENCE_MINT_COUNT", bad_value)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="QIITA_MAX_SEQUENCE_MINT_COUNT"):
        Settings.from_env()


def test_settings_rejects_non_integer_max_sequence_mint_count(monkeypatch):
    """An un-parseable env var should name itself in the failure, not
    surface a bare int() ValueError with no context."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", _TEST_SECRET_B64)
    monkeypatch.setenv("QIITA_MAX_SEQUENCE_MINT_COUNT", "not-an-int")

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="QIITA_MAX_SEQUENCE_MINT_COUNT"):
        Settings.from_env()
