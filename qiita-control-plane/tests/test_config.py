"""Tests for control plane Settings."""

import base64
import secrets

import pytest

# Valid base64-encoded 32-byte secrets for tests. Signing key and cookie key are
# DISTINCT so tests asserting the split can't pass by accident.
_TEST_SECRET_B64 = base64.b64encode(secrets.token_bytes(32)).decode()
_TEST_COOKIE_B64 = base64.b64encode(secrets.token_bytes(32)).decode()


@pytest.fixture(autouse=True)
def _set_workspace_root(monkeypatch):
    """Default required env vars for every test (PATH_SCRATCH, CONTACT_EMAIL,
    FLIGHT_TICKET_SIGNING_KEY, LOGIN_COOKIE_SECRET_KEY). Tests that specifically
    exercise an env var's absence or invalid value `delenv` / `setenv` to
    override after this fixture runs."""
    monkeypatch.setenv("PATH_SCRATCH", "/tmp/qiita-test-scratch-unused")
    monkeypatch.setenv("CONTACT_EMAIL", "qiita-test@example.org")
    monkeypatch.setenv("FLIGHT_TICKET_SIGNING_KEY", _TEST_SECRET_B64)
    monkeypatch.setenv("LOGIN_COOKIE_SECRET_KEY", _TEST_COOKIE_B64)


def test_settings_has_database_url(monkeypatch):
    """Settings must expose a database_url field read from DATABASE_URL env var."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.database_url == "postgresql://u:p@localhost:5432/db"


def test_settings_requires_database_url(monkeypatch):
    """Settings must raise if DATABASE_URL is missing."""
    monkeypatch.delenv("DATABASE_URL", raising=False)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        Settings.from_env()


def test_settings_flight_signing_key_is_bytes(monkeypatch):
    """Settings.flight_signing_key must be the decoded 32-byte Ed25519 seed."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    raw_bytes = secrets.token_bytes(32)
    monkeypatch.setenv("FLIGHT_TICKET_SIGNING_KEY", base64.b64encode(raw_bytes).decode())

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.flight_signing_key == raw_bytes
    assert isinstance(settings.flight_signing_key, bytes)


def test_settings_rejects_invalid_base64_flight_signing_key(monkeypatch):
    """Settings must raise if FLIGHT_TICKET_SIGNING_KEY is not valid base64."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("FLIGHT_TICKET_SIGNING_KEY", "not!!!valid%%%base64")

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="base64"):
        Settings.from_env()


def test_settings_rejects_wrong_length_flight_signing_key(monkeypatch):
    """Settings must raise if FLIGHT_TICKET_SIGNING_KEY is not exactly 32 bytes
    (an Ed25519 private seed)."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("FLIGHT_TICKET_SIGNING_KEY", base64.b64encode(b"short").decode())

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="32 bytes"):
        Settings.from_env()


def test_settings_requires_flight_signing_key(monkeypatch):
    """Settings must raise if FLIGHT_TICKET_SIGNING_KEY is missing."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.delenv("FLIGHT_TICKET_SIGNING_KEY", raising=False)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="FLIGHT_TICKET_SIGNING_KEY"):
        Settings.from_env()


def test_settings_login_cookie_key_is_distinct_bytes(monkeypatch):
    """login_cookie_secret_key is decoded independently of flight_signing_key —
    the split is the whole point (one leak can't forge both)."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    cookie_bytes = secrets.token_bytes(32)
    monkeypatch.setenv("LOGIN_COOKIE_SECRET_KEY", base64.b64encode(cookie_bytes).decode())

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.login_cookie_secret_key == cookie_bytes
    assert settings.login_cookie_secret_key != settings.flight_signing_key


def test_settings_requires_login_cookie_secret_key(monkeypatch):
    """Settings must raise if LOGIN_COOKIE_SECRET_KEY is missing."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.delenv("LOGIN_COOKIE_SECRET_KEY", raising=False)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="LOGIN_COOKIE_SECRET_KEY"):
        Settings.from_env()


def test_settings_rejects_invalid_base64_login_cookie(monkeypatch):
    """Settings must raise if LOGIN_COOKIE_SECRET_KEY is not valid base64."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("LOGIN_COOKIE_SECRET_KEY", "not!!!valid%%%base64")

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="base64"):
        Settings.from_env()


def test_settings_rejects_short_login_cookie(monkeypatch):
    """Settings must raise if LOGIN_COOKIE_SECRET_KEY decodes to < 16 bytes."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("LOGIN_COOKIE_SECRET_KEY", base64.b64encode(b"short").decode())

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="at least 16 bytes"):
        Settings.from_env()


def test_settings_max_sequence_mint_count_has_default(monkeypatch):
    """max_sequence_mint_count must default to a large but finite cap so
    a buggy compute step cannot burn an unbounded slice of the
    sequence_idx space on a single POST."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.delenv("QIITA_MAX_SEQUENCE_MINT_COUNT", raising=False)

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert isinstance(settings.max_sequence_mint_count, int)
    assert settings.max_sequence_mint_count > 0


def test_settings_max_sequence_mint_count_from_env(monkeypatch):
    """QIITA_MAX_SEQUENCE_MINT_COUNT overrides the default."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("QIITA_MAX_SEQUENCE_MINT_COUNT", "12345")

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.max_sequence_mint_count == 12345


@pytest.mark.parametrize("bad_value", ["0", "-1", "-10000"])
def test_settings_rejects_nonpositive_max_sequence_mint_count(monkeypatch, bad_value):
    """A non-positive cap would make the route reject every mint silently;
    reject it at startup instead."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("QIITA_MAX_SEQUENCE_MINT_COUNT", bad_value)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="QIITA_MAX_SEQUENCE_MINT_COUNT"):
        Settings.from_env()


@pytest.mark.parametrize(
    "var",
    [
        "AUTHROCKET_PAT_MAX_AUTH_AGE_SECONDS",
        "QIITA_TOKEN_DEFAULT_TTL_DAYS",
        "AUTH_HANDOFF_FRESHNESS_SECONDS",
        "CLI_LOGIN_CODE_TTL_SECONDS",
    ],
)
@pytest.mark.parametrize("bad_value", ["0", "-1", "notanint"])
def test_settings_rejects_bad_positive_auth_knob(monkeypatch, var, bad_value):
    """The strictly-positive auth knobs must fail loudly (naming the var) on a
    non-positive or non-int value rather than silently collapsing an auth
    window — previously they used a bare int() that accepted 0/negatives."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv(var, bad_value)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match=var):
        Settings.from_env()


def test_settings_jwt_leeway_allows_zero(monkeypatch):
    """AUTHROCKET_JWT_LEEWAY_SECONDS=0 is a valid strict setting (tolerate no
    clock skew) and must NOT be rejected — it routes through the non-negative
    parser, not the positive-only one."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("AUTHROCKET_JWT_LEEWAY_SECONDS", "0")

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.authrocket_jwt_leeway_seconds == 0


@pytest.mark.parametrize("bad_value", ["-1", "notanint"])
def test_settings_rejects_bad_jwt_leeway(monkeypatch, bad_value):
    """A negative or non-int leeway still fails loudly, naming the variable."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("AUTHROCKET_JWT_LEEWAY_SECONDS", bad_value)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="AUTHROCKET_JWT_LEEWAY_SECONDS"):
        Settings.from_env()


def test_settings_rejects_non_integer_max_sequence_mint_count(monkeypatch):
    """An un-parseable env var should name itself in the failure, not
    surface a bare int() ValueError with no context."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
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
    monkeypatch.delenv("PATH_SCRATCH", raising=False)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="PATH_SCRATCH"):
        Settings.from_env()


def test_settings_rejects_relative_path_scratch(monkeypatch):
    """A relative path resolves against the service's CWD (whatever
    systemd / uvicorn happened to start in), which is non-obvious surface
    for an operator. Force absolute."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("PATH_SCRATCH", "relative/path")

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="PATH_SCRATCH must be an absolute path"):
        Settings.from_env()


def test_settings_derives_ticket_and_staging_from_path_scratch(monkeypatch):
    """Happy path: an absolute PATH_SCRATCH derives the per-ticket
    workspace (`/ticket`) and upload-staging (`/staging`) subdirs."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
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
    monkeypatch.setenv("CONTACT_EMAIL", bad_value)

    from qiita_control_plane.config import Settings

    with pytest.raises(RuntimeError, match="CONTACT_EMAIL"):
        Settings.from_env()


def test_settings_contact_email_set_from_env(monkeypatch):
    """Happy path: a well-formed CONTACT_EMAIL lands on the Settings object."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("CONTACT_EMAIL", "qiita.help@gmail.com")

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.contact_email == "qiita.help@gmail.com"


def test_settings_build_sha_defaults_to_none(monkeypatch):
    """BUILD_SHA is optional — set only by the deploy scripts. A boot
    without it (dev / tests / first deploy) must leave build_sha None so
    the landing footer renders the version alone, never blocking boot."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.delenv("BUILD_SHA", raising=False)

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.build_sha is None


def test_settings_build_sha_set_from_env(monkeypatch):
    """When the deploy writes BUILD_SHA, it lands on the Settings object
    for the landing footer."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("BUILD_SHA", "a28c96e")

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.build_sha == "a28c96e"


def test_settings_build_sha_empty_is_none(monkeypatch):
    """An empty BUILD_SHA (activate.sh writes an empty build.env when the
    SHA is unavailable) must normalize to None, not the empty string —
    the footer keys off truthiness to decide whether to render the link."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("BUILD_SHA", "")

    from qiita_control_plane.config import Settings

    settings = Settings.from_env()
    assert settings.build_sha is None


def test_settings_default_adapter_reference_idx_unset_is_none(monkeypatch):
    """Optional setting: a deploy without QC leaves it unset → None."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.delenv("QIITA_DEFAULT_ADAPTER_REFERENCE_IDX", raising=False)

    from qiita_control_plane.config import Settings

    assert Settings.from_env().default_adapter_reference_idx is None


def test_settings_default_adapter_reference_idx_parsed(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv("QIITA_DEFAULT_ADAPTER_REFERENCE_IDX", "42")

    from qiita_control_plane.config import Settings

    assert Settings.from_env().default_adapter_reference_idx == 42


def test_settings_default_adapter_reference_idx_rejects_invalid(monkeypatch):
    """Present-but-invalid fails loudly (not silently treated as unset)."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")

    from qiita_control_plane.config import Settings

    for bad in ("0", "-1", "notanint"):
        monkeypatch.setenv("QIITA_DEFAULT_ADAPTER_REFERENCE_IDX", bad)
        with pytest.raises(RuntimeError, match="QIITA_DEFAULT_ADAPTER_REFERENCE_IDX"):
            Settings.from_env()
