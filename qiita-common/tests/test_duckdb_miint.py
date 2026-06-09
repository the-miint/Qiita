"""Unit tests for the single-sourced miint install/connect helpers.

These pin the "one mirror version everywhere — no community/mirror patchwork"
contract (design note: docs/design/reference-load-resilience.md, F10): with no
``MIINT_EXTENSION_REPO`` override, installs FORCE-install from the team mirror
(never the community channel, which would let a host drift to a different
build), and connections always allow the mirror's unsigned extensions.
"""

from __future__ import annotations

from qiita_common.duckdb_miint import (
    MIINT_MIRROR_URL,
    miint_connect_config,
    miint_install_sql,
)


def test_install_sql_defaults_to_force_install_from_mirror(monkeypatch):
    """No override → FORCE INSTALL from the mirror, never the community channel.
    FORCE overwrites a stale cached extension dir (the compute-node root cause)."""
    monkeypatch.delenv("MIINT_EXTENSION_REPO", raising=False)
    sql = miint_install_sql()
    assert sql == f"FORCE INSTALL miint FROM '{MIINT_MIRROR_URL}';"
    assert "community" not in sql


def test_install_sql_honors_repo_override(monkeypatch):
    """MIINT_EXTENSION_REPO remains an override for local/dev builds."""
    monkeypatch.setenv("MIINT_EXTENSION_REPO", "/local/repo")
    assert miint_install_sql() == "FORCE INSTALL miint FROM '/local/repo';"


def test_connect_config_allows_unsigned_by_default(monkeypatch):
    """We always install from a mirror (team signing chain, not DuckDB's), so
    every miint connection must allow unsigned extensions — even with no env."""
    monkeypatch.delenv("MIINT_EXTENSION_REPO", raising=False)
    monkeypatch.delenv("MIINT_EXTENSION_DIRECTORY", raising=False)
    assert miint_connect_config().get("allow_unsigned_extensions") == "true"


def test_connect_config_sets_extension_directory_when_present(monkeypatch):
    monkeypatch.delenv("MIINT_EXTENSION_REPO", raising=False)
    monkeypatch.setenv("MIINT_EXTENSION_DIRECTORY", "/tmp/ext")
    config = miint_connect_config()
    assert config["extension_directory"] == "/tmp/ext"
    assert config["allow_unsigned_extensions"] == "true"
