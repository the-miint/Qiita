"""Unit tests for the deploy-time miint staging gate (miint_staging).

The gate decides whether `redeploy.sh` can skip a FORCE INSTALL. The network
HEAD and the DuckDB platform probe are mocked here — these tests pin the
decision logic (when does it skip vs. re-stage), not the live mirror.
"""

from __future__ import annotations

import json
import urllib.error

import pytest

from qiita_compute_orchestrator import miint_staging

# A stable local identity for the running interpreter, so only the validator /
# marker bits vary between tests.
_DUCKDB_VERSION = "1.2.3"
_PLATFORM = "linux_amd64"
_REPO = "https://ftp.microbio.me/pub/miint"


@pytest.fixture(autouse=True)
def _stable_local_identity(monkeypatch, tmp_path):
    """Pin duckdb version / platform / repo and point the gate at a tmp
    extension dir so every test starts from a known, isolated state."""
    monkeypatch.setattr(miint_staging.duckdb, "__version__", _DUCKDB_VERSION)
    monkeypatch.setattr(miint_staging, "_duckdb_platform", lambda: _PLATFORM)
    monkeypatch.setattr(miint_staging, "miint_repo", lambda: _REPO)
    monkeypatch.setenv("MIINT_EXTENSION_DIRECTORY", str(tmp_path))
    return tmp_path


def _write_marker(tmp_path, **overrides) -> None:
    fp = {
        "duckdb_version": _DUCKDB_VERSION,
        "platform": _PLATFORM,
        "repo": _REPO,
        "etag": '"abc123"',
        "last_modified": "Wed, 18 Jun 2026 10:00:00 GMT",
    }
    fp.update(overrides)
    (tmp_path / miint_staging.MARKER_NAME).write_text(json.dumps(fp))


def _head_returns(etag='"abc123"', last_modified="Wed, 18 Jun 2026 10:00:00 GMT"):
    return lambda url: {"etag": etag, "last_modified": last_modified}


# --- staging_is_current ----------------------------------------------------


def test_no_extension_dir_is_not_current(monkeypatch):
    """No MIINT_EXTENSION_DIRECTORY (dev/test stage) → always stage."""
    monkeypatch.delenv("MIINT_EXTENSION_DIRECTORY", raising=False)
    assert miint_staging.staging_is_current() is False


def test_missing_marker_is_not_current(tmp_path):
    """Never staged here (no marker) → stage."""
    assert miint_staging.staging_is_current() is False


def test_matching_marker_and_unchanged_mirror_is_current(monkeypatch, tmp_path):
    _write_marker(tmp_path)
    monkeypatch.setattr(miint_staging, "_head_validators", _head_returns())
    assert miint_staging.staging_is_current() is True


def test_mirror_bump_changes_etag_is_not_current(monkeypatch, tmp_path):
    """Same DuckDB version, but the mirror published a new build (new ETag)."""
    _write_marker(tmp_path)
    monkeypatch.setattr(miint_staging, "_head_validators", _head_returns(etag='"NEW999"'))
    assert miint_staging.staging_is_current() is False


def _boom(url):  # pragma: no cover — used where the HEAD must not run
    raise AssertionError("HEAD should not run when the local triple already differs")


def test_duckdb_version_change_skips_network(monkeypatch, tmp_path):
    """A DuckDB-version change is detectable locally — no HEAD should fire."""
    _write_marker(tmp_path, duckdb_version="9.9.9")
    monkeypatch.setattr(miint_staging, "_head_validators", _boom)
    assert miint_staging.staging_is_current() is False


def test_repo_change_skips_network(monkeypatch, tmp_path):
    """A repo change (e.g. MIINT_EXTENSION_REPO override) is in the local triple
    too — detectable without a HEAD, like the DuckDB-version change."""
    _write_marker(tmp_path, repo="https://example.test/other-mirror")
    monkeypatch.setattr(miint_staging, "_head_validators", _boom)
    assert miint_staging.staging_is_current() is False


def test_network_failure_is_not_current(monkeypatch, tmp_path):
    """A HEAD that can't reach the mirror → re-stage (never skip on doubt)."""
    _write_marker(tmp_path)

    def _raise(url):
        raise urllib.error.URLError("mirror unreachable")

    monkeypatch.setattr(miint_staging, "_head_validators", _raise)
    assert miint_staging.staging_is_current() is False


def test_mirror_without_validators_is_not_current(monkeypatch, tmp_path):
    """A mirror that returns neither ETag nor Last-Modified gives us nothing to
    compare — we cannot prove currency, so re-stage."""
    _write_marker(tmp_path)
    monkeypatch.setattr(miint_staging, "_head_validators", _head_returns(etag="", last_modified=""))
    assert miint_staging.staging_is_current() is False


def test_last_modified_only_still_compares(monkeypatch, tmp_path):
    """ETag absent but Last-Modified present and unchanged → current."""
    _write_marker(tmp_path, etag="")
    monkeypatch.setattr(miint_staging, "_head_validators", _head_returns(etag=""))
    assert miint_staging.staging_is_current() is True


def test_corrupt_marker_is_not_current(tmp_path):
    (tmp_path / miint_staging.MARKER_NAME).write_text("{not valid json")
    assert miint_staging.staging_is_current() is False


# --- write_staging_marker --------------------------------------------------


def test_write_marker_records_fingerprint(monkeypatch, tmp_path):
    monkeypatch.setattr(miint_staging, "_head_validators", _head_returns())
    miint_staging.write_staging_marker()
    stored = json.loads((tmp_path / miint_staging.MARKER_NAME).read_text())
    assert stored == {
        "duckdb_version": _DUCKDB_VERSION,
        "platform": _PLATFORM,
        "repo": _REPO,
        "etag": '"abc123"',
        "last_modified": "Wed, 18 Jun 2026 10:00:00 GMT",
    }


def test_write_marker_noop_without_extension_dir(monkeypatch, tmp_path):
    monkeypatch.delenv("MIINT_EXTENSION_DIRECTORY", raising=False)
    # Must not raise, and must not write a marker anywhere under tmp_path.
    miint_staging.write_staging_marker()
    assert not (tmp_path / miint_staging.MARKER_NAME).exists()


def test_write_marker_survives_head_failure(monkeypatch, tmp_path):
    """Staging already succeeded by the time we write the marker; a HEAD failure
    must not raise (it only costs a re-stage next deploy)."""

    def _raise(url):
        raise urllib.error.URLError("mirror unreachable")

    monkeypatch.setattr(miint_staging, "_head_validators", _raise)
    miint_staging.write_staging_marker()  # no exception
    assert not (tmp_path / miint_staging.MARKER_NAME).exists()


# --- round trip ------------------------------------------------------------


def test_marker_then_check_is_current(monkeypatch, tmp_path):
    """The marker write_staging_marker() produces is exactly what
    staging_is_current() accepts when the mirror hasn't moved."""
    monkeypatch.setattr(miint_staging, "_head_validators", _head_returns())
    miint_staging.write_staging_marker()
    assert miint_staging.staging_is_current() is True
