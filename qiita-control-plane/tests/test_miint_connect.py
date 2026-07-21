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

from qiita_control_plane import miint as miint_module
from qiita_control_plane.miint import connect_with_miint_staged


def test_staged_connect_requires_extension_directory(monkeypatch):
    """The CP wrapper reaches the shared requirement check and names ITSELF in
    the error (the check's own cases live in qiita-common's test_duckdb_miint)."""
    monkeypatch.delenv("MIINT_EXTENSION_DIRECTORY", raising=False)
    with pytest.raises(RuntimeError, match="control-plane service"):
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


def test_staged_connect_never_installs(monkeypatch, tmp_path):
    """THE contract: the staged helper LOADs and never INSTALLs.

    Without this, reintroducing `INSTALL` inside `connect_with_miint_staged()`
    — the exact production bug, in the function written to prevent it — passes
    every other test in this file. Asserted on the SQL actually executed, via a
    recording stand-in for the connection.
    """
    executed: list[str] = []

    class _RecordingConn:
        def execute(self, sql, *args, **kwargs):
            executed.append(sql)
            return self

        def close(self):
            pass

    monkeypatch.setenv("MIINT_EXTENSION_DIRECTORY", str(tmp_path))
    monkeypatch.setattr(
        miint_module.duckdb, "connect", lambda *a, **kw: _RecordingConn(), raising=True
    )

    conn = connect_with_miint_staged()
    conn.close()

    assert executed == ["LOAD miint;"]
    assert not any("INSTALL" in sql.upper() for sql in executed)


def test_read_ingest_uses_the_staged_helper():
    """The masked-read streamer must bind the STAGED helper, by identity.

    Guards the swap this module exists to prevent: `_stream_masked_reads_to_fastq`
    runs inside the CP service, where INSTALL cannot resolve a home directory.
    Identity rather than `hasattr` — an aliased
    `from ..miint import connect_with_miint as connect_with_miint_staged` would
    satisfy a name check while reintroducing the bug.
    """
    from qiita_control_plane.runner import _read_ingest

    assert _read_ingest.connect_with_miint_staged is miint_module.connect_with_miint_staged
    assert not hasattr(_read_ingest, "connect_with_miint")
