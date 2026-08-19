"""Tests for qiita_compute_orchestrator.config.

Focus: the asymmetric token-resolution surface. `Settings.from_env()`
is called eagerly by the FastAPI lifespan (where `cp_to_co_token` is
required — it's the inbound `POST /step/*` bearer). It's *also*
called lazily by `get_settings()` from the SLURM-launcher / CLI path,
where `cp_to_co_token` is irrelevant because the launcher never serves
that route. The flag below threads that distinction.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from qiita_compute_orchestrator.config import (
    Settings,
    _resolve_slurm_settings,
    _resolve_token,
    _settings_ctx,
    get_settings,
    install_settings,
)


@pytest.fixture(autouse=True)
def _reset_settings_ctx():
    """Clear the install_settings ContextVar between tests so one test's
    install doesn't leak into the next test's no-install fallback path."""
    token = _settings_ctx.set(None)
    try:
        yield
    finally:
        _settings_ctx.reset(token)


def test_from_env_default_requires_cp_to_co_token(monkeypatch, tmp_path):
    """The orchestrator service path. With no readable token file and
    no env-var fallback, `from_env()` must raise — boot-time fail-fast."""
    # Remove both file and env-var sources for cp_to_co.
    monkeypatch.setenv("CP_TO_CO_TOKEN_PATH", str(tmp_path / "missing-cp-to-co.token"))
    monkeypatch.delenv("CP_TO_CO_TOKEN", raising=False)
    # Keep co_to_cp resolvable so this test isolates the cp_to_co path.
    monkeypatch.setenv("QIITA_ALLOW_TOKEN_ENV", "true")
    monkeypatch.setenv("CO_TO_CP_TOKEN", "test-co-to-cp-token")
    with pytest.raises(RuntimeError, match="CP↔CO"):
        Settings.from_env()


def test_from_env_skips_cp_to_co_token_when_not_required(monkeypatch, tmp_path):
    """The launcher / CLI path. Same missing-token surface, but
    `require_cp_to_co_token=False` returns Settings with an empty
    `cp_to_co_token` instead of raising. `co_to_cp_token` still
    resolves because the launcher does need it for outbound calls."""
    monkeypatch.setenv("CP_TO_CO_TOKEN_PATH", str(tmp_path / "missing-cp-to-co.token"))
    monkeypatch.delenv("CP_TO_CO_TOKEN", raising=False)
    monkeypatch.setenv("QIITA_ALLOW_TOKEN_ENV", "true")
    monkeypatch.setenv("CO_TO_CP_TOKEN", "test-co-to-cp-token")
    settings = Settings.from_env(require_cp_to_co_token=False)
    assert settings.cp_to_co_token == ""
    assert settings.co_to_cp_token == "test-co-to-cp-token"


def test_get_settings_no_install_fallback_skips_cp_to_co_token(monkeypatch, tmp_path):
    """End-to-end: with no `install_settings` and an unresolvable
    cp_to_co token, `get_settings()` must NOT raise — that's the
    SLURM-launcher path, where the no-install fallback is supposed to
    pass `require_cp_to_co_token=False` internally.

    This is the behavior that lets `SlurmBackend.submit_step` drop
    `CP_TO_CO_TOKEN` from the per-job env. If this test starts
    failing, the deploy host is back to needing the inbound bearer
    in every SLURM job's `scontrol show job` output."""
    monkeypatch.setenv("CP_TO_CO_TOKEN_PATH", str(tmp_path / "missing-cp-to-co.token"))
    monkeypatch.delenv("CP_TO_CO_TOKEN", raising=False)
    monkeypatch.setenv("QIITA_ALLOW_TOKEN_ENV", "true")
    monkeypatch.setenv("CO_TO_CP_TOKEN", "test-co-to-cp-token")
    settings = get_settings()
    assert settings.cp_to_co_token == ""
    assert settings.co_to_cp_token == "test-co-to-cp-token"


def test_get_settings_returns_installed_value(monkeypatch):
    """When `install_settings` was called (the orchestrator service
    lifespan path), `get_settings()` returns the cached value
    verbatim — no re-resolution from env."""
    sentinel = Settings(
        backend_type="local",
        path_scratch="/tmp/sentinel",
        path_derived="/tmp/sentinel-derived",
        cp_to_co_token="cached-cp-to-co",
        cp_url="https://sentinel.invalid",
        co_to_cp_token="cached-co-to-cp",
        slurm=None,
    )
    install_settings(sentinel)
    assert get_settings() is sentinel


def test_from_env_path_derived_explicit(monkeypatch):
    """PATH_DERIVED is the derived-artifact ROOT (NOT .../images). Resolved on
    every backend and leniently — no absolute/exists assertion — mirroring
    path_scratch, because native index builders (build_rype_index,
    build_minimap2_index) derive `{path_derived}/references/{idx}/...` under
    both LocalBackend and SLURM. Distinct from the strict slurm-only
    path_derived_images (which is PATH_DERIVED/images)."""
    monkeypatch.setenv("PATH_DERIVED", "/scratch/persistent")
    monkeypatch.setenv("QIITA_ALLOW_TOKEN_ENV", "true")
    monkeypatch.setenv("CO_TO_CP_TOKEN", "t")
    settings = Settings.from_env(require_cp_to_co_token=False)
    assert settings.path_derived == "/scratch/persistent"


def test_from_env_path_derived_dev_fallback(monkeypatch):
    """With no PATH_DERIVED, path_derived falls back under TMPDIR — the
    dev/local posture (no fail-fast)."""
    monkeypatch.delenv("PATH_DERIVED", raising=False)
    monkeypatch.setenv("TMPDIR", "/tmp/xyz")
    monkeypatch.setenv("QIITA_ALLOW_TOKEN_ENV", "true")
    monkeypatch.setenv("CO_TO_CP_TOKEN", "t")
    settings = Settings.from_env(require_cp_to_co_token=False)
    assert settings.path_derived == "/tmp/xyz/qiita/derived"


def test_from_env_data_plane_url_explicit(monkeypatch):
    """DATA_PLANE_URL is the gRPC origin native jobs DoGet reference chunks from.
    Resolved on every backend; when set, it is used verbatim."""
    monkeypatch.setenv("DATA_PLANE_URL", "grpc://qiita-miint.ucsd.edu:50051")
    monkeypatch.setenv("QIITA_ALLOW_TOKEN_ENV", "true")
    monkeypatch.setenv("CO_TO_CP_TOKEN", "t")
    settings = Settings.from_env(require_cp_to_co_token=False)
    assert settings.data_plane_url == "grpc://qiita-miint.ucsd.edu:50051"


def test_from_env_data_plane_url_dev_default(monkeypatch):
    """With no DATA_PLANE_URL, it falls back to the localhost default — NOT
    fail-fast (unlike the SLURM-only required vars), so a deploy that forgets it
    keeps the unit up rather than down."""
    monkeypatch.delenv("DATA_PLANE_URL", raising=False)
    monkeypatch.setenv("QIITA_ALLOW_TOKEN_ENV", "true")
    monkeypatch.setenv("CO_TO_CP_TOKEN", "t")
    settings = Settings.from_env(require_cp_to_co_token=False)
    assert settings.data_plane_url == "grpc://localhost:50051"


def test_resolve_co_to_cp_token_permission_denied_is_actionable(monkeypatch, tmp_path):
    """A present-but-unreadable CO→CP token file — the compute-readiness-as-the-
    wrong-user case (the token is 0400 qiita-orch, qiita-api can't read it) —
    must raise an *actionable* RuntimeError that names the file, the 0400
    qiita-orch ownership, and the correct `sudo -u qiita-orch` invocation, not
    fall through to the generic "no token available" message."""
    token_file = tmp_path / "co-to-cp.token"
    token_file.write_text("secret-pat\n")
    monkeypatch.setenv("CO_TO_CP_TOKEN_PATH", str(token_file))
    # No env-var fallback — the file IS present; it's just unreadable.
    monkeypatch.delenv("QIITA_ALLOW_TOKEN_ENV", raising=False)
    monkeypatch.delenv("CO_TO_CP_TOKEN", raising=False)

    real_read_text = Path.read_text

    def fake_read_text(self, *args, **kwargs):
        if self == token_file:
            raise PermissionError(13, "Permission denied")
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", fake_read_text)

    with pytest.raises(RuntimeError) as excinfo:
        Settings.from_env(require_cp_to_co_token=False)
    msg = str(excinfo.value)
    # Actionable content — not merely "a RuntimeError was raised".
    assert str(token_file) in msg
    assert "0400" in msg
    assert "qiita-orch" in msg
    assert "sudo -u qiita-orch" in msg
    # And NOT the misleading generic path.
    assert "no" not in msg.lower() or "token available" not in msg.lower()


def test_resolve_cp_to_co_token_permission_denied_does_not_misdirect_to_qiita_orch(
    monkeypatch, tmp_path
):
    """The CP↔CO bearer is the shared 0440 root:qiita-services token (read by
    both qiita-api and qiita-orch), so a PermissionError there is a perms
    misinstall — it must NOT tell the operator to "run as qiita-orch" (that's
    only right for the 0400 CO→CP token). Pins the per-kind perms guidance so a
    future edit can't re-hardcode the qiita-orch advice for both kinds."""
    token_file = tmp_path / "cp-to-co.token"
    token_file.write_text("shared-bearer\n")
    monkeypatch.setenv("CP_TO_CO_TOKEN_PATH", str(token_file))
    monkeypatch.delenv("QIITA_ALLOW_TOKEN_ENV", raising=False)
    monkeypatch.delenv("CP_TO_CO_TOKEN", raising=False)

    real_read_text = Path.read_text

    def fake_read_text(self, *args, **kwargs):
        if self == token_file:
            raise PermissionError(13, "Permission denied")
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", fake_read_text)

    with pytest.raises(RuntimeError) as excinfo:
        _resolve_token("cp_to_co")
    msg = str(excinfo.value)
    assert str(token_file) in msg
    # CP↔CO is the root:qiita-services group bearer, not the 0400 qiita-orch one.
    assert "0440" in msg
    assert "qiita-services" in msg
    assert "qiita-orch" not in msg
    assert "sudo -u qiita-orch" not in msg


def test_resolve_slurm_settings_requires_miint_extension_directory(monkeypatch):
    """miint is a CORE dependency: COMPUTE_BACKEND=slurm must fail at boot without
    MIINT_EXTENSION_DIRECTORY. The miint presence checks run BEFORE the SLURM vars,
    so no SLURM env is needed to reach the raise. (conftest sets both miint vars
    globally, so the test explicitly clears the one under test.)"""
    monkeypatch.delenv("MIINT_EXTENSION_DIRECTORY", raising=False)
    monkeypatch.setenv("MIINT_GPL_BOUNDARY_PATH", "/scratch/derived/gpl-boundary")
    with pytest.raises(RuntimeError, match="MIINT_EXTENSION_DIRECTORY"):
        _resolve_slurm_settings()


def test_resolve_slurm_settings_requires_miint_gpl_boundary_path(monkeypatch):
    """miint is a CORE dependency: COMPUTE_BACKEND=slurm must fail at boot without
    MIINT_GPL_BOUNDARY_PATH — the exact var whose absence stranded the bowtie2
    shard builds. Regression guard for the boot-time enforcement."""
    monkeypatch.setenv("MIINT_EXTENSION_DIRECTORY", "/scratch/derived/duckdb-ext")
    monkeypatch.delenv("MIINT_GPL_BOUNDARY_PATH", raising=False)
    with pytest.raises(RuntimeError, match="MIINT_GPL_BOUNDARY_PATH"):
        _resolve_slurm_settings()
