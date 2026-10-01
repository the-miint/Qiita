"""Unit tests for `ena_submission.catalog`.

Network-free and DuckDB-free: `connect_with_miint_staged` is replaced with a
recording connection, and the module-level `_open_ena_submission_session` is
replaced by fully-qualified name.
"""

from pathlib import Path

import pytest

from qiita_control_plane import miint as miint_module
from qiita_control_plane.ena_submission import catalog as catalog_module
from qiita_control_plane.ena_submission.catalog import (
    EnaSubmissionCatalog,
    EnaSubmissionError,
)

_OPEN_SESSION = "qiita_control_plane.ena_submission.catalog._open_ena_submission_session"

_WEBIN_USER = "Webin-00000"
# miint's ENA endpoint is an environment selector, not a URL.
_ENDPOINT = "test"


class _RecordingConn:
    """Stands in for a DuckDB connection, recording every statement executed."""

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.closed = False

    def execute(self, sql: str) -> _RecordingConn:
        self.statements.append(sql)
        return self

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def password_file(tmp_path: Path) -> Path:
    """A readable Webin password file; its contents are miint's to consume."""
    path = tmp_path / "webin_password.txt"
    path.write_text("not-a-real-password\n")
    return path


def _valid_kwargs(password_file: Path, **overrides) -> dict:
    """A configuration that passes validation, with any field replaced."""
    return {
        "webin_user": _WEBIN_USER,
        "webin_password_file": password_file,
        "endpoint": _ENDPOINT,
        **overrides,
    }


def _open(monkeypatch, password_file: Path, *, dry_run: bool = False) -> _RecordingConn:
    """Open a session against a recording connection and return the recorder."""
    conn = _RecordingConn()
    monkeypatch.setattr(catalog_module, "connect_with_miint_staged", lambda: conn)
    catalog_module._open_ena_submission_session(**_valid_kwargs(password_file, dry_run=dry_run))
    return conn


def test__open_ena_submission_session_creates_secret(monkeypatch, password_file: Path):
    """Tests the case where a session is opened: the Webin secret is registered
    with the user, the password file, and the endpoint, as a TYPE ENA secret.

    The endpoint rides on the secret as well as the attach because the secret is
    the only side miint validates it on.
    """
    conn = _open(monkeypatch, password_file)
    secret = next(s for s in conn.statements if "CREATE SECRET" in s)
    assert catalog_module._SECRET_NAME in secret
    assert "TYPE ENA" in secret
    assert f"'{_WEBIN_USER}'" in secret
    assert f"'{password_file}'" in secret
    assert f"ENDPOINT '{_ENDPOINT}'" in secret


def test__open_ena_submission_session_attaches_catalog(monkeypatch, password_file: Path):
    """Tests the case where a session is opened: the catalog is attached at the
    endpoint, under the module's alias, bound to the registered secret."""
    conn = _open(monkeypatch, password_file)
    attach = next(s for s in conn.statements if s.startswith("ATTACH "))
    assert f"'{_ENDPOINT}'" in attach
    assert f"AS {catalog_module._CATALOG_ALIAS}" in attach
    assert "TYPE ENA" in attach
    assert catalog_module._SECRET_NAME in attach


@pytest.mark.parametrize(
    ("setting", "expected_value"),
    [
        ("threads", str(catalog_module._SESSION_THREADS)),
        ("memory_limit", catalog_module._SESSION_MEMORY_LIMIT),
    ],
)
def test__open_ena_submission_session_applies_resource_bounds(
    monkeypatch, password_file: Path, setting: str, expected_value: str
):
    """Tests the case where a session is opened: each resource bound is set to
    the module constant's value, and set before the catalog is attached."""
    conn = _open(monkeypatch, password_file)
    bound = next(s for s in conn.statements if s.startswith(f"SET {setting}"))
    assert expected_value in bound
    attach_at = next(i for i, s in enumerate(conn.statements) if s.startswith("ATTACH "))
    assert conn.statements.index(bound) < attach_at


@pytest.mark.parametrize("dry_run", [True, False])
def test__open_ena_submission_session_validate_only(
    monkeypatch, password_file: Path, dry_run: bool
):
    """Tests the case where dry-run is requested and the case where it is not:
    validate-only is set only under dry-run."""
    conn = _open(monkeypatch, password_file, dry_run=dry_run)
    emitted = any("miint_ena_validate_only" in s for s in conn.statements)
    assert emitted is dry_run


def test__open_ena_submission_session_emits_no_install(monkeypatch, password_file: Path):
    """Tests the case where a session is opened: no extension statement is
    issued, since the staged connect helper already loads miint and httpfs."""
    conn = _open(monkeypatch, password_file)
    for statement in conn.statements:
        assert "INSTALL" not in statement.upper(), statement
        assert "LOAD " not in statement.upper(), statement


@pytest.mark.parametrize(
    ("field", "overrides"),
    [
        ("webin_user", {"webin_user": ""}),
        ("webin_user", {"webin_user": "   "}),
        ("endpoint", {"endpoint": ""}),
        # ATTACH accepts any string, so a wrong endpoint has to fail here.
        ("endpoint", {"endpoint": "https://wwwdev.ebi.ac.uk/ena/submit/webin-v2/"}),
        ("endpoint", {"endpoint": "Production"}),
        ("password", {"webin_password_file": Path("/nonexistent/webin_password.txt")}),
    ],
)
def test_EnaSubmissionCatalog_invalid_configuration_raises(
    monkeypatch, password_file: Path, field: str, overrides: dict
):
    """Tests the case where a required parameter is blank or absent: construction
    fails loud naming the field, and no session is opened."""
    monkeypatch.setattr(_OPEN_SESSION, lambda **kwargs: pytest.fail("no session may be opened"))
    with pytest.raises(EnaSubmissionError, match=field):
        EnaSubmissionCatalog(**_valid_kwargs(password_file, **overrides))


@pytest.mark.parametrize("body_raises", [False, True])
def test_EnaSubmissionCatalog_detaches_and_closes(
    monkeypatch, password_file: Path, body_raises: bool
):
    """Tests the case where the block exits cleanly and the case where it exits
    on an exception: the catalog is detached and the connection closed either
    way, and an exception from the body is not swallowed."""
    conn = _RecordingConn()
    monkeypatch.setattr(_OPEN_SESSION, lambda **kwargs: conn)
    catalog = EnaSubmissionCatalog(**_valid_kwargs(password_file))

    if body_raises:
        with pytest.raises(ValueError, match="from the body"), catalog:
            raise ValueError("from the body")
    else:
        with catalog:
            pass

    assert any(s.startswith("DETACH ") for s in conn.statements)
    assert conn.closed is True


def test_EnaSubmissionCatalog_instances_are_independent(monkeypatch, password_file: Path):
    """Tests the case where two catalogs are open at once: each holds its own
    session, and closing one leaves the other's untouched."""
    first_conn, second_conn = _RecordingConn(), _RecordingConn()
    handed = iter((first_conn, second_conn))
    monkeypatch.setattr(_OPEN_SESSION, lambda **kwargs: next(handed))
    first = EnaSubmissionCatalog(**_valid_kwargs(password_file))
    second = EnaSubmissionCatalog(**_valid_kwargs(password_file))
    with first:
        with second:
            assert first._con is first_conn
            assert second._con is second_conn
        # The inner block's teardown must not have touched the outer session.
        assert second_conn.closed is True
        assert first_conn.closed is False
    assert first_conn.closed is True


def test_catalog_uses_staged_connect():
    """Tests the case where the module's connect helper is inspected: it is the
    LOAD-only staged form, with the INSTALL-based client helper absent."""
    assert catalog_module.connect_with_miint_staged is miint_module.connect_with_miint_staged
    assert not hasattr(catalog_module, "connect_with_miint")
