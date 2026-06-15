"""Unit tests for the single-sourced miint install/connect helpers.

These pin the "one mirror version everywhere — no community/mirror patchwork"
contract: with no ``MIINT_EXTENSION_REPO`` override, installs come from the team
mirror (never the community channel, which would let a host drift to a different
build); connections always allow the mirror's unsigned extensions; and the
cluster runtime LOADs a pre-staged build rather than installing per job.
"""

from __future__ import annotations

from qiita_common.duckdb_miint import (
    MIINT_MIRROR_URL,
    miint_connect_config,
    miint_install_sql,
    miint_job_env,
    miint_load_sql,
)


def test_install_sql_defaults_to_plain_install_from_mirror(monkeypatch):
    """No override → plain INSTALL from the mirror, never the community channel.
    Plain (not FORCE) so a warm cache isn't re-downloaded — the client CLI fills
    its cache once; only deploy-time staging passes force=True."""
    monkeypatch.delenv("MIINT_EXTENSION_REPO", raising=False)
    sql = miint_install_sql()
    assert sql == f"INSTALL miint FROM '{MIINT_MIRROR_URL}';"
    assert "FORCE" not in sql
    assert "community" not in sql


def test_install_sql_force_for_deploy_staging(monkeypatch):
    """force=True (deploy-time staging only) refreshes the staged build to the
    mirror's current version."""
    monkeypatch.delenv("MIINT_EXTENSION_REPO", raising=False)
    assert miint_install_sql(force=True) == f"FORCE INSTALL miint FROM '{MIINT_MIRROR_URL}';"


def test_install_sql_honors_repo_override(monkeypatch):
    """MIINT_EXTENSION_REPO remains an override for local/dev builds."""
    monkeypatch.setenv("MIINT_EXTENSION_REPO", "/local/repo")
    assert miint_install_sql() == "INSTALL miint FROM '/local/repo';"
    assert miint_install_sql(force=True) == "FORCE INSTALL miint FROM '/local/repo';"


def test_load_sql_is_load_only():
    """Cluster runtime LOADs the pre-staged extension — no install verb."""
    assert miint_load_sql() == "LOAD miint;"


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


def test_job_env_propagates_extension_directory_when_set(monkeypatch):
    """A remote (SLURM) job carries MIINT_EXTENSION_DIRECTORY so it can LOAD the
    deploy-staged build. MIINT_EXTENSION_REPO is deliberately NOT propagated: the
    cluster path is LOAD-only, so the install repo is irrelevant on a node."""
    monkeypatch.setenv("MIINT_EXTENSION_DIRECTORY", "/scratch/derived/duckdb-ext")
    monkeypatch.setenv("MIINT_EXTENSION_REPO", "/local/repo")
    assert miint_job_env() == {"MIINT_EXTENSION_DIRECTORY": "/scratch/derived/duckdb-ext"}


def test_job_env_empty_when_extension_directory_unset(monkeypatch):
    """Unset → empty dict (dev/test runs that rely on the DuckDB default dir)."""
    monkeypatch.delenv("MIINT_EXTENSION_DIRECTORY", raising=False)
    assert miint_job_env() == {}
