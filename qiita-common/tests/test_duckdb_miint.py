"""Unit tests for the single-sourced miint install/connect helpers.

These pin the "one mirror version everywhere — no community/mirror patchwork"
contract: with no ``MIINT_EXTENSION_REPO`` override, installs come from the team
mirror (never the community channel, which would let a host drift to a different
build); connections always allow the mirror's unsigned extensions; and the
cluster runtime LOADs a pre-staged build rather than installing per job.
"""

from __future__ import annotations

import fnmatch
import gzip
import os
import tempfile

import pytest

from qiita_common.duckdb_miint import (
    MIINT_EXTENSION_DIRECTORY_VAR,
    MIINT_MIRROR_URL,
    MIINT_REQUIRED_JOB_VARS,
    is_empty_sequence_file,
    miint_connect_config,
    miint_install_sql,
    miint_job_env,
    miint_load_sql,
    require_staged_extension_directory,
    setup_miint_test_env,
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


def test_job_env_propagates_both_required_vars_when_set(monkeypatch):
    """A remote (SLURM) job carries MIINT_EXTENSION_DIRECTORY (to LOAD the
    deploy-staged build) AND MIINT_GPL_BOUNDARY_PATH (to reach the GPL-boundary
    host). MIINT_EXTENSION_REPO is deliberately NOT propagated: the cluster path
    is LOAD-only, so the install repo is irrelevant on a node."""
    monkeypatch.setenv("MIINT_EXTENSION_DIRECTORY", "/scratch/derived/duckdb-ext")
    monkeypatch.setenv("MIINT_GPL_BOUNDARY_PATH", "/scratch/derived/gpl-boundary")
    monkeypatch.setenv("MIINT_EXTENSION_REPO", "/local/repo")
    assert miint_job_env() == {
        "MIINT_EXTENSION_DIRECTORY": "/scratch/derived/duckdb-ext",
        "MIINT_GPL_BOUNDARY_PATH": "/scratch/derived/gpl-boundary",
    }


def test_job_env_raises_when_extension_directory_unset(monkeypatch):
    """miint is a CORE dependency: a missing extension dir must fail LOUD (not a
    silent empty dict), naming the missing var — a job submitted without it dies
    at `LOAD miint`."""
    monkeypatch.delenv("MIINT_EXTENSION_DIRECTORY", raising=False)
    monkeypatch.setenv("MIINT_GPL_BOUNDARY_PATH", "/scratch/derived/gpl-boundary")
    with pytest.raises(RuntimeError, match="MIINT_EXTENSION_DIRECTORY"):
        miint_job_env()


def test_job_env_raises_when_gpl_boundary_unset(monkeypatch):
    """miint is a CORE dependency: a missing GPL-boundary path must fail LOUD —
    it was the exact bug (bowtie2 shards died `gpl-boundary not installed` because
    the var never reached the job)."""
    monkeypatch.setenv("MIINT_EXTENSION_DIRECTORY", "/scratch/derived/duckdb-ext")
    monkeypatch.delenv("MIINT_GPL_BOUNDARY_PATH", raising=False)
    with pytest.raises(RuntimeError, match="MIINT_GPL_BOUNDARY_PATH"):
        miint_job_env()


def test_job_env_raises_lists_all_missing(monkeypatch):
    """Both unset → the error names both, so an operator fixes them in one pass."""
    monkeypatch.delenv("MIINT_EXTENSION_DIRECTORY", raising=False)
    monkeypatch.delenv("MIINT_GPL_BOUNDARY_PATH", raising=False)
    with pytest.raises(RuntimeError, match="MIINT_EXTENSION_DIRECTORY.*MIINT_GPL_BOUNDARY_PATH"):
        miint_job_env()


# --- require_staged_extension_directory ------------------------------------


def test_require_staged_extension_directory_returns_the_dir(monkeypatch, tmp_path):
    """The happy path returns the value, so a caller can use it directly."""
    monkeypatch.setenv("MIINT_EXTENSION_DIRECTORY", str(tmp_path))
    assert require_staged_extension_directory(service="test service") == str(tmp_path)


def test_require_staged_extension_directory_raises_when_unset(monkeypatch):
    """Unset is always a misconfiguration for a LOAD-only caller: it cannot fall
    back to INSTALL, which is what needs the writable $HOME in the first place.
    The message must name BOTH the var and the service, because DuckDB's own
    (`Can't find the home directory at '/dev/null'`) names neither."""
    monkeypatch.delenv("MIINT_EXTENSION_DIRECTORY", raising=False)
    with pytest.raises(RuntimeError) as excinfo:
        require_staged_extension_directory(service="control-plane service")
    assert "MIINT_EXTENSION_DIRECTORY" in str(excinfo.value)
    assert "control-plane service" in str(excinfo.value)


def test_require_staged_extension_directory_raises_on_non_directory(monkeypatch, tmp_path):
    """A path that is not a usable directory is caught here rather than surfacing
    later as a confusing `extension not found`."""
    not_a_dir = tmp_path / "regular-file"
    not_a_dir.write_text("")
    monkeypatch.setenv("MIINT_EXTENSION_DIRECTORY", str(not_a_dir))
    with pytest.raises(RuntimeError, match="not a readable directory"):
        require_staged_extension_directory(service="compute orchestrator")


def test_extension_directory_var_is_the_one_job_env_requires():
    """The named constant IS the job-env requirement, not a second spelling of
    it — the drift this constant exists to prevent."""
    assert MIINT_EXTENSION_DIRECTORY_VAR in MIINT_REQUIRED_JOB_VARS


# --- setup_miint_test_env ---------------------------------------------------


def _resolve_test_ext_dir(
    monkeypatch, tmp_path, worker: str | None, inherited: str | None = None
) -> str:
    """Run the harness helper with `PYTEST_XDIST_WORKER` set to `worker` and
    `MIINT_EXTENSION_DIRECTORY` pre-set to `inherited`, and return the directory
    it chose. `tempfile.gettempdir` is redirected so the call doesn't create
    directories in the real system temp."""
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    if inherited is None:
        monkeypatch.delenv(MIINT_EXTENSION_DIRECTORY_VAR, raising=False)
    else:
        monkeypatch.setenv(MIINT_EXTENSION_DIRECTORY_VAR, inherited)
    if worker is None:
        monkeypatch.delenv("PYTEST_XDIST_WORKER", raising=False)
    else:
        monkeypatch.setenv("PYTEST_XDIST_WORKER", worker)
    setup_miint_test_env("control-plane")
    return os.environ[MIINT_EXTENSION_DIRECTORY_VAR]


def test_setup_test_env_gives_each_xdist_worker_its_own_directory(monkeypatch, tmp_path):
    """Each xdist worker is a separate process and each one INSTALLs, downloading
    into its own `tmp-<uuid>` file and renaming that over `miint.duckdb_extension`
    and its `.info`. One directory across N workers is N concurrent writers to
    those two paths, so the directory carries the worker id."""
    gw0 = _resolve_test_ext_dir(monkeypatch, tmp_path, "gw0")
    gw1 = _resolve_test_ext_dir(monkeypatch, tmp_path, "gw1")
    assert gw0 != gw1
    assert os.path.isdir(gw0) and os.path.isdir(gw1)


def test_setup_test_env_keeps_the_plain_name_without_xdist(monkeypatch, tmp_path):
    """The suites that run single-process (qiita-common, the orchestrator, the
    integration tier) keep the directory they already cache in."""
    plain = _resolve_test_ext_dir(monkeypatch, tmp_path, None)
    assert os.path.basename(plain) == "qiita-control-plane-duckdb-ext"


def test_setup_test_env_replaces_the_directory_a_worker_inherits(monkeypatch, tmp_path):
    """A worker inherits its environment from the xdist controller, which ran this
    same helper first and exported the plain per-component directory. On `setdefault`
    alone every worker points back at that one shared path and the split is a no-op —
    the directories get created and nothing installs into them."""
    controller = _resolve_test_ext_dir(monkeypatch, tmp_path, None)
    worker = _resolve_test_ext_dir(monkeypatch, tmp_path, "gw0", inherited=controller)
    assert worker != controller
    assert os.path.basename(worker) == "qiita-control-plane-gw0-duckdb-ext"


def test_setup_test_env_leaves_an_externally_pinned_directory_alone(monkeypatch, tmp_path):
    """`setdefault`, not `set`: only the value this helper itself exports is replaced.
    A directory pinned from outside — a deploy-staged one, say — is a different string
    and reaches the suite as given, in a worker as anywhere else."""
    pinned = str(tmp_path / "staged-elsewhere")
    for worker in (None, "gw0"):
        assert _resolve_test_ext_dir(monkeypatch, tmp_path, worker, inherited=pinned) == pinned


def test_setup_test_env_directories_match_the_documented_clearing_glob(monkeypatch, tmp_path):
    """`setup_miint_test_env`'s docstring tells the reader to clear a stale build
    with `rm -rf "$TMPDIR"/qiita-*-duckdb-ext`. A name that escapes that glob
    leaves a cache nothing clears, and the symptom is a `Catalog Error` from a
    function the stale build predates."""
    for worker in (None, "gw0", "gw11"):
        chosen = _resolve_test_ext_dir(monkeypatch, tmp_path, worker)
        assert fnmatch.fnmatch(os.path.basename(chosen), "qiita-*-duckdb-ext")


@pytest.mark.parametrize(
    ("name", "payload", "empty"),
    [
        ("empty.fa", b"", True),
        ("full.fa", b">x\nACGT\n", False),
        ("empty.fa.gz", None, True),
        ("full.fa.gz", b">x\nACGT\n", False),
    ],
)
def test_is_empty_sequence_file(tmp_path, name, payload, empty):
    """The four cases the callers depend on, uncompressed and gzipped.

    `.gz` is the pair that matters: an empty gzip member still occupies its framing
    bytes on disk, so `st_size == 0` answers this question wrong in the direction
    that costs — `read_fastx` raises on the file and one such path aborts a whole
    multi-file scan.
    """
    path = tmp_path / name
    if name.endswith(".gz"):
        with gzip.open(path, "wb") as fh:
            fh.write(payload or b"")
    else:
        path.write_bytes(payload or b"")

    if name.endswith(".gz"):
        assert path.stat().st_size > 0, "an empty gzip member is still bytes on disk"

    assert is_empty_sequence_file(path) is empty
