"""Service-side miint connect contract.

The control plane loads miint in two places with opposite requirements: the
client CLI (`qiita reference load`) INSTALLs into its own cache, while the CP
service LOADs from the deploy-staged extension directory. Mixing them up is not
a style question — a service-side INSTALL resolves `$HOME/.duckdb/extensions`,
and the service account's home is `/dev/null`, so it dies with a DuckDB
`IO Error: Can't find the home directory` that names neither the variable to
set nor the service that needs it.

These tests pin that contract, including the `$HOME` behaviour itself
(CLAUDE.local.md: establish third-party behaviour by running it, then pin the
finding as a test).
"""

from __future__ import annotations

import duckdb
import pytest
from qiita_common.duckdb_miint import miint_connect_config

from qiita_control_plane.miint import connect_with_miint_staged


def test_staged_connect_requires_extension_directory(monkeypatch):
    """Unset MIINT_EXTENSION_DIRECTORY fails with an actionable message, not a
    DuckDB home-directory IOException."""
    monkeypatch.delenv("MIINT_EXTENSION_DIRECTORY", raising=False)
    with pytest.raises(RuntimeError, match="MIINT_EXTENSION_DIRECTORY is not set"):
        connect_with_miint_staged()


def test_staged_connect_rejects_non_directory(monkeypatch, tmp_path):
    """A path that is not a directory is caught here rather than surfacing as a
    confusing `extension not found` later."""
    not_a_dir = tmp_path / "regular-file"
    not_a_dir.write_text("")
    monkeypatch.setenv("MIINT_EXTENSION_DIRECTORY", str(not_a_dir))
    with pytest.raises(RuntimeError, match="is not a directory"):
        connect_with_miint_staged()


def test_connect_config_carries_extension_directory(monkeypatch, tmp_path):
    """The env var reaches DuckDB as `extension_directory` — the mechanism the
    staged path relies on to avoid resolving $HOME at all."""
    monkeypatch.setenv("MIINT_EXTENSION_DIRECTORY", str(tmp_path))
    assert miint_connect_config()["extension_directory"] == str(tmp_path)


def test_extension_resolution_ignores_home_when_directory_configured(monkeypatch, tmp_path):
    """REGRESSION PIN: with `extension_directory` configured, DuckDB resolves
    extensions under it and never touches `$HOME`.

    This is the exact production failure, reduced: the CP service account's home
    is `/dev/null`, so any extension resolution that falls back to
    `$HOME/.duckdb/extensions` raises `Can't find the home directory`. Pointing
    HOME at a non-directory reproduces that here without needing the real
    service account.

    Asserted through the LOAD error message, which names the directory DuckDB
    resolved — so the test is hermetic (no network, no staged extension) and
    still fails loudly if the config stops taking effect.
    """
    # Siblings, NOT nested — a fake_home under the extension directory would
    # make the "resolved elsewhere" assertions below trivially true.
    ext_dir = tmp_path / "extdir"
    ext_dir.mkdir()
    fake_home = tmp_path / "not-a-home"
    fake_home.write_text("")
    monkeypatch.setenv("HOME", str(fake_home))
    monkeypatch.setenv("MIINT_EXTENSION_DIRECTORY", str(ext_dir))

    con = duckdb.connect(":memory:", config=miint_connect_config())
    try:
        with pytest.raises(duckdb.IOException) as excinfo:
            con.execute("LOAD miint")
    finally:
        con.close()

    message = str(excinfo.value)
    # The resolved path is under the configured directory...
    assert str(ext_dir) in message
    # ...and the home-directory fallback was never reached.
    assert str(fake_home) not in message
    assert "Can't find the home directory" not in message

    # CONTROL — the same LOAD with the directory unset DOES fall back to $HOME,
    # which is what production hit. Without this branch the assertions above
    # could pass for reasons unrelated to the config taking effect.
    monkeypatch.delenv("MIINT_EXTENSION_DIRECTORY")
    con = duckdb.connect(":memory:", config=miint_connect_config())
    try:
        with pytest.raises(duckdb.IOException) as excinfo:
            con.execute("LOAD miint")
    finally:
        con.close()

    fallback = str(excinfo.value)
    assert str(fake_home) in fallback
    assert str(ext_dir) not in fallback


def test_read_ingest_uses_the_staged_helper():
    """The masked-read streamer must not import the client INSTALL helper.

    Guards the swap this module exists to prevent: `_stream_masked_reads_to_fastq`
    runs inside the CP service, where INSTALL cannot resolve a home directory.
    """
    from qiita_control_plane.runner import _read_ingest

    assert hasattr(_read_ingest, "connect_with_miint_staged")
    assert not hasattr(_read_ingest, "connect_with_miint")
