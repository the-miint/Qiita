"""What `redeploy.sh` step 6 imports to declare the operator CLI venv usable.

The venv holds both entrypoints — `qiita` (`cli.user`) and `qiita-admin`
(`cli.admin`) — and one stale `qiita_common` can break either. Importing one of
them is not a proxy for the other: the cases below measure that, so the probe
cannot quietly shrink back to a single entrypoint.

Companion to `qiita_compute_orchestrator.native_import_check`, which asks the same
question of the SLURM native venv. Both exist because a stale path dependency is
invisible at package granularity.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

_REDEPLOY = Path(__file__).resolve().parents[3] / "deploy" / "redeploy.sh"

# Admin-only at module level (`cli/admin/__init__.py`). `cli.user` reaches
# TERMINAL_WORK_TICKET_STATES only through `cli.reference_load`, which
# `cli/user/reference.py` imports inside a function, so it is not in the import
# closure either. Incident fixtures, not a contract: a rename here is a fixture
# update.
_DAMAGE = {
    "SYSTEM_PRINCIPAL_IDX": ("import qiita_common.auth_constants as m; del m.SYSTEM_PRINCIPAL_IDX"),
    "TERMINAL_WORK_TICKET_STATES": (
        "import qiita_common.models as m; del m.TERMINAL_WORK_TICKET_STATES"
    ),
}

_BOTH = "import qiita_control_plane.cli.user, qiita_control_plane.cli.admin"


def _import_after(damage: str, body: str) -> subprocess.CompletedProcess[str]:
    """Delete a name from an already-imported module, then run `body`.

    Deleting the attribute is how a subprocess reproduces "this symbol is not in
    the site-packages copy": a later `from module import NAME` looks it up on the
    module object and raises exactly as it would against stale sources.
    """
    return subprocess.run(
        [sys.executable, "-c", f"{damage}\n{body}"], capture_output=True, text=True
    )


@pytest.mark.parametrize("name", sorted(_DAMAGE))
def test_a_stale_symbol_fails_the_import_the_deploy_runs(name: str) -> None:
    result = _import_after(_DAMAGE[name], _BOTH)
    assert result.returncode != 0, result.stdout
    assert name in result.stderr, result.stderr


@pytest.mark.parametrize("name", sorted(_DAMAGE))
def test_importing_only_the_user_entrypoint_passes_on_that_damage(name: str) -> None:
    """The control the case above needs: `cli.user` alone is green on damage that
    breaks `qiita-admin`. Without this, the pair could be failing for an unrelated
    reason and nothing would say so."""
    assert _import_after(_DAMAGE[name], "import qiita_control_plane.cli.user").returncode == 0


def test_an_undamaged_venv_imports_both() -> None:
    """The other control — the check is not simply always red."""
    result = _import_after("", _BOTH)
    assert result.returncode == 0, result.stderr


def test_redeploy_verifies_both_entrypoints() -> None:
    """Pins the call site itself, since the cases above pass whether or not
    `redeploy.sh` actually runs that import."""
    assert _BOTH in _REDEPLOY.read_text()
