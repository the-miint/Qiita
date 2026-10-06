"""Guard: every DuckDB version pin in the repo names the same, exact release — the
one the Rust data plane's `duckdb` crate is built against.

A DuckDB bump has to land in lockstep across several spots; a past bump moved the
`duckdb` crate and the Python locks but left the `setup-libduckdb` action's
`version` default (and the deploy cache key) at the old 1.5.2, so CI would link a
mismatched libduckdb against the new crate. This test fails that drift loudly
instead of letting it ship.

Every pin is EXACT, never a floor. miint is built per DuckDB release — the mirror
publishes `v<duckdb-version>/<platform>/miint.duckdb_extension.gz`, and DuckDB
namespaces the staged extension directory the same way — so a resolve free to move
to a newer DuckDB lands on one with no miint build at all (DuckDB 1.5.6 was on PyPI
and crates.io while the mirror's newest build was 1.5.5). A lockfile protects only
the environments synced from it.

What it ties together, each against the crate:
- `qiita-data-plane/Cargo.toml` `[dependencies].duckdb` — the crate, which decides
  the embedded/linked DuckDB version. libduckdb-sys encodes the DuckDB version in
  the crate's middle field: `<major>.1<minor:02><patch:02>.<rev>`, so crate
  `1.10503.1` == DuckDB `1.5.3`.
- `.github/actions/setup-libduckdb/action.yml` `version` input default — the
  libduckdb tarball CI downloads for the dynamic-link build/test path. It is a
  *default*, so any workflow that doesn't pass `version:` silently inherits it —
  exactly why a stale value is dangerous. The action also derives its
  `~/.duckdb/extensions` cache key from that same input
  (`duckdb-ext-…-v${version}`), so guarding the default covers the extension
  cache too.
- `deploy/_common.sh` `QIITA_DUCKDB_VERSION` — the DuckDB CLI the lake scripts
  require. It opens the same catalog with the ducklake extension of its own
  version, and a newer one may want to migrate the catalog schema.
- Each Python component's `pyproject.toml` `duckdb` pin and the version its
  `uv.lock` resolves. The container defs' `python-duckdb` pins are tied to the
  orchestrator's lock by `test_container_duckdb_matches_the_orchestrator_lock`, so
  this closes that chain rather than repeating it.

When this fails, bump every spot to the same DuckDB version (and update
`_crate_to_duckdb_version` if libduckdb-sys ever changes its encoding).
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
CARGO_TOML = REPO_ROOT / "qiita-data-plane" / "Cargo.toml"
ACTION_YML = REPO_ROOT / ".github" / "actions" / "setup-libduckdb" / "action.yml"
DEPLOY_COMMON_SH = REPO_ROOT / "deploy" / "_common.sh"
PYTHON_COMPONENTS = (
    "qiita-common",
    "qiita-compute-orchestrator",
    "qiita-control-plane",
    "tests/integration",
)

# The `version:` input's `default:` inside setup-libduckdb/action.yml. Anchored on
# the 2-space-indented `version:` key, then its 4-space-indented `default:` — so a
# `default:` under a *different* input can't match.
_ACTION_VERSION_DEFAULT_RE = re.compile(
    r'^  version:\n(?:^ {4}.*\n)*?^ {4}default:\s*"([^"]+)"',
    re.MULTILINE,
)
_DEPLOY_VERSION_RE = re.compile(r'^QIITA_DUCKDB_VERSION="([^"]+)"$', re.MULTILINE)
# A `duckdb` requirement and its specifier; `duckdb-<suffix>` packages don't match.
_PY_DUCKDB_REQ_RE = re.compile(r"duckdb\s*([=<>!~].*)?")


def _data_plane_duckdb_crate_requirement() -> str:
    cargo = tomllib.loads(CARGO_TOML.read_text())
    dep = cargo["dependencies"]["duckdb"]
    return dep["version"] if isinstance(dep, dict) else dep


def _crate_to_duckdb_version(crate: str) -> str:
    """Map a libduckdb-sys crate version to the DuckDB version it links.

    Crate `1.10503.1` -> DuckDB `1.5.3`: the middle field is a literal `1`
    followed by 2-digit minor and 2-digit patch.
    """
    parts = crate.split(".")
    assert len(parts) == 3, f"unexpected duckdb crate version shape: {crate!r}"
    major, encoded = parts[0], parts[1]
    assert len(encoded) == 5 and encoded[0] == "1", (
        f"unexpected duckdb crate encoding {encoded!r} in {crate!r}; "
        "update _crate_to_duckdb_version if libduckdb-sys changed its scheme"
    )
    return f"{major}.{int(encoded[1:3])}.{int(encoded[3:5])}"


def _pinned_duckdb_version() -> str:
    """The DuckDB release the data-plane crate pins — what every other spot must name."""
    requirement = _data_plane_duckdb_crate_requirement()
    return _crate_to_duckdb_version(requirement.removeprefix("=").strip())


def test_data_plane_duckdb_crate_is_pinned_exactly() -> None:
    requirement = _data_plane_duckdb_crate_requirement()
    assert requirement.startswith("="), (
        f"qiita-data-plane/Cargo.toml pins duckdb as {requirement!r}, a caret range: "
        "`cargo update` may move it to a newer DuckDB with no miint build. Pin it "
        f'exactly, as "={requirement.lstrip("^~ ")}".'
    )


def test_libduckdb_action_default_matches_data_plane_crate() -> None:
    expected = _pinned_duckdb_version()

    m = _ACTION_VERSION_DEFAULT_RE.search(ACTION_YML.read_text())
    assert m, "could not find the `version` input default in setup-libduckdb/action.yml"
    action_default = m.group(1)

    assert action_default == expected, (
        f"setup-libduckdb action `version` default ({action_default!r}) does not match the "
        f"DuckDB version the data-plane crate links ({expected!r}). "
        "A DuckDB bump must update BOTH qiita-data-plane/Cargo.toml's `duckdb` crate and the "
        "action `version` default (which also keys the ~/.duckdb/extensions cache)."
    )


def test_deploy_duckdb_cli_version_matches_data_plane_crate() -> None:
    expected = _pinned_duckdb_version()

    m = _DEPLOY_VERSION_RE.search(DEPLOY_COMMON_SH.read_text())
    assert m, "could not find QIITA_DUCKDB_VERSION in deploy/_common.sh"

    assert m.group(1) == expected, (
        f"deploy/_common.sh QIITA_DUCKDB_VERSION ({m.group(1)!r}) does not match the DuckDB "
        f"version the data-plane crate links ({expected!r}); the lake scripts would tell "
        "the operator to install the wrong CLI."
    )


@pytest.mark.parametrize("component", PYTHON_COMPONENTS)
def test_python_duckdb_pin_is_exact_and_matches_data_plane_crate(component: str) -> None:
    expected = _pinned_duckdb_version()
    pyproject = tomllib.loads((REPO_ROOT / component / "pyproject.toml").read_text())

    specifiers = [
        m.group(1)
        for dep in pyproject["project"]["dependencies"]
        if (m := _PY_DUCKDB_REQ_RE.fullmatch(dep))
    ]

    assert specifiers == [f"=={expected}"], (
        f"{component}/pyproject.toml declares duckdb as {specifiers!r}; it must be exactly "
        f"['=={expected}'], the DuckDB version the data-plane crate links."
    )


@pytest.mark.parametrize("component", PYTHON_COMPONENTS)
def test_python_lock_resolves_data_plane_duckdb(component: str) -> None:
    expected = _pinned_duckdb_version()
    lock = tomllib.loads((REPO_ROOT / component / "uv.lock").read_text())

    resolved = [p["version"] for p in lock["package"] if p["name"] == "duckdb"]

    assert resolved == [expected], (
        f"{component}/uv.lock resolves duckdb {resolved!r}, not {expected!r} — the lock is "
        "stale against the pin. Re-lock with `uv lock --upgrade-package duckdb`."
    )
