"""Tests for control plane Settings."""

import base64
import secrets

import pytest

# A valid base64-encoded 32-byte secret for tests.
_TEST_SECRET_B64 = base64.b64encode(secrets.token_bytes(32)).decode()


@pytest.fixture(autouse=True)
def _set_workspace_root(monkeypatch):
    """Default required env vars (PATH_SCRATCH, CONTACT_EMAIL) for every
    test. Tests that specifically exercise an env var's absence or invalid
    values can `delenv` / `setenv` to override after this fixture runs."""
    monkeypatch.setenv("PATH_SCRATCH", "/tmp/qiita-test-scratch-unused")
    monkeypatch.setenv("CONTACT_EMAIL", "qiita-test@example.org")


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


def test_settings_requires_path_scratch(monkeypatch):
    """Production boot must fail fast if PATH_SCRATCH is unset — the
    alternative is the first dispatched ticket failing at `mkdir` inside
    the runner (PATH_SCRATCH/ticket), or an `*_upload_idx` resolving
    against PATH_SCRATCH/staging, after the route has already returned
    202."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", _TEST_SECRET_B64)
    monkeypatch.delenv("PATH_SCRATCH", raising=False)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="PATH_SCRATCH"):
        Settings.from_env()


def test_settings_rejects_relative_path_scratch(monkeypatch):
    """A relative path resolves against the service's CWD (whatever
    systemd / uvicorn happened to start in), which is non-obvious surface
    for an operator. Force absolute."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", _TEST_SECRET_B64)
    monkeypatch.setenv("PATH_SCRATCH", "relative/path")

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="PATH_SCRATCH must be an absolute path"):
        Settings.from_env()


def test_settings_derives_ticket_and_staging_from_path_scratch(monkeypatch):
    """Happy path: an absolute PATH_SCRATCH derives the per-ticket
    workspace (`/ticket`) and upload-staging (`/staging`) subdirs."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", _TEST_SECRET_B64)
    monkeypatch.setenv("PATH_SCRATCH", "/var/lib/qiita/scratch")

    from pathlib import Path

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.path_scratch_ticket == Path("/var/lib/qiita/scratch/ticket")
    assert settings.path_scratch_staging == Path("/var/lib/qiita/scratch/staging")


def test_settings_requires_contact_email(monkeypatch):
    """Production boot must fail fast if CONTACT_EMAIL is unset — the
    landing page renders the value into both `mailto:` links, and a
    placeholder shipping into a public page is exactly the failure mode
    fail-fast is meant to catch."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", _TEST_SECRET_B64)
    monkeypatch.delenv("CONTACT_EMAIL", raising=False)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="CONTACT_EMAIL"):
        Settings.from_env()


@pytest.mark.parametrize(
    "bad_value",
    ["not-an-email", "@example.org", "user@", "user@localhost", "user@@example.org"],
)
def test_settings_rejects_malformed_contact_email(monkeypatch, bad_value):
    """Catch the obvious typo / placeholder shapes at boot rather than
    shipping a broken `mailto:` into the public page."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", _TEST_SECRET_B64)
    monkeypatch.setenv("CONTACT_EMAIL", bad_value)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="CONTACT_EMAIL"):
        Settings.from_env()


def test_settings_contact_email_set_from_env(monkeypatch):
    """Happy path: a well-formed CONTACT_EMAIL lands on the Settings object."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("HMAC_SECRET_KEY", _TEST_SECRET_B64)
    monkeypatch.setenv("CONTACT_EMAIL", "qiita.help@gmail.com")

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.contact_email == "qiita.help@gmail.com"
