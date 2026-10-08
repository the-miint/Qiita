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
  require; `qiita_resolve_duckdb_bin` refuses any other and says why.
- Every tracked Python project's `duckdb` requirements, in any dependency table,
  and the version its `uv.lock` resolves. The container defs' `python-duckdb` pins are tied to the
  orchestrator's lock by `test_container_duckdb_matches_the_orchestrator_lock`, so
  this closes that chain rather than repeating it.

When this fails, bump every spot to the same DuckDB version (and update
`_crate_to_duckdb_version` if libduckdb-sys ever changes its encoding).
"""

from __future__ import annotations

import re
import subprocess
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
CARGO_TOML = REPO_ROOT / "qiita-data-plane" / "Cargo.toml"
ACTION_YML = REPO_ROOT / ".github" / "actions" / "setup-libduckdb" / "action.yml"
DEPLOY_COMMON_SH = REPO_ROOT / "deploy" / "_common.sh"

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
# A Cargo version requirement: an optional operator, then the version.
_CARGO_REQ_RE = re.compile(r"\s*(>=|<=|=|\^|~|>|<)?\s*(\S+)\s*")


def _python_projects() -> list[str]:
    """Every tracked Python project's directory, found rather than listed, so a new
    one is held to the pin from the commit that adds it."""
    out = subprocess.run(
        ["git", "ls-files", "-z", "--", "*pyproject.toml"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return sorted(str(Path(path).parent) for path in out.split("\0") if path)


def _duckdb_specifiers(pyproject: dict) -> list[str | None]:
    """The specifier of every `duckdb` requirement in every table that can carry one
    (`None` for a bare `duckdb`)."""
    project = pyproject.get("project", {})
    tables = [
        project.get("dependencies", []),
        *project.get("optional-dependencies", {}).values(),
        *pyproject.get("dependency-groups", {}).values(),
        pyproject.get("tool", {}).get("uv", {}).get("dev-dependencies", []),
    ]
    return [
        m.group(1)
        for table in tables
        for requirement in table
        if isinstance(requirement, str) and (m := _PY_DUCKDB_REQ_RE.fullmatch(requirement))
    ]


def _split_crate_requirement(requirement: str) -> tuple[str, str]:
    """`"=1.10505.0"` -> `("=", "1.10505.0")`; the operator is `""` when absent."""
    m = _CARGO_REQ_RE.fullmatch(requirement)
    assert m, f"unparseable duckdb crate requirement: {requirement!r}"
    return m.group(1) or "", m.group(2)


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
    _, version = _split_crate_requirement(_data_plane_duckdb_crate_requirement())
    return _crate_to_duckdb_version(version)


def test_data_plane_duckdb_crate_is_pinned_exactly() -> None:
    requirement = _data_plane_duckdb_crate_requirement()
    operator, version = _split_crate_requirement(requirement)
    assert operator == "=", (
        f"qiita-data-plane/Cargo.toml pins duckdb as {requirement!r}, a range: "
        "`cargo update` may move it to a newer DuckDB with no miint build. Pin it "
        f'exactly, as "={version}".'
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


@pytest.mark.parametrize("component", _python_projects())
def test_python_duckdb_pin_is_exact_and_matches_data_plane_crate(component: str) -> None:
    expected = _pinned_duckdb_version()
    pyproject = tomllib.loads((REPO_ROOT / component / "pyproject.toml").read_text())

    specifiers = _duckdb_specifiers(pyproject)

    assert all(spec == f"=={expected}" for spec in specifiers), (
        f"{component}/pyproject.toml declares duckdb as {specifiers!r}; every duckdb "
        f"requirement must be exactly '=={expected}', the DuckDB version the data-plane "
        "crate links."
    )


@pytest.mark.parametrize("component", _python_projects())
def test_python_lock_resolves_data_plane_duckdb(component: str) -> None:
    expected = _pinned_duckdb_version()
    lock_path = REPO_ROOT / component / "uv.lock"
    if not lock_path.exists():
        pytest.skip(f"{component} has no uv.lock; its pyproject pin is the whole guard")
    lock = tomllib.loads(lock_path.read_text())
    declares = bool(
        _duckdb_specifiers(tomllib.loads((REPO_ROOT / component / "pyproject.toml").read_text()))
    )

    resolved = [p["version"] for p in lock["package"] if p["name"] == "duckdb"]

    assert resolved == [expected] or (resolved == [] and not declares), (
        f"{component}/uv.lock resolves duckdb {resolved!r}, not {expected!r} — the lock is "
        "stale against the pin. Re-lock with `uv lock --upgrade-package duckdb`."
    )


@pytest.mark.parametrize(
    ("requirement", "parts"),
    [
        ("=1.10505.0", ("=", "1.10505.0")),
        ("^1.10505.0", ("^", "1.10505.0")),
        ("1.10505.0", ("", "1.10505.0")),
        (">=1.10505.0", (">=", "1.10505.0")),
        ("~ 1.10505.0", ("~", "1.10505.0")),
    ],
)
def test_crate_requirement_splits_into_operator_and_version(
    requirement: str, parts: tuple[str, str]
) -> None:
    assert _split_crate_requirement(requirement) == parts


def test_duckdb_specifiers_come_from_every_dependency_table() -> None:
    pyproject = {
        "project": {
            "dependencies": ["duckdb==1.5.5", "duckdb-extensions>=1"],
            "optional-dependencies": {"cli": ["duckdb>=1.5"]},
        },
        "dependency-groups": {"dev": ["duckdb", {"include-group": "cli"}]},
        "tool": {"uv": {"dev-dependencies": ["duckdb~=1.5.5"]}},
    }
    assert sorted(_duckdb_specifiers(pyproject), key=str) == sorted(
        ["==1.5.5", ">=1.5", None, "~=1.5.5"], key=str
    )


def test_discovery_finds_every_known_python_project() -> None:
    """A discovery that silently found nothing would make the per-project checks vacuous."""
    assert {
        "qiita-common",
        "qiita-compute-orchestrator",
        "qiita-control-plane",
        "tests/integration",
    } <= set(_python_projects())
