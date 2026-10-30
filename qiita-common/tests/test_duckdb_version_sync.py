"""Guard: every DuckDB version pin in the repo names the same, exact release — the
one the Rust data plane's `duckdb` crate is built against.

A DuckDB bump has to land in lockstep across several spots; a past bump moved the
`duckdb` crate and the Python locks but left the `setup-libduckdb` action's
`version` default (and the deploy cache key) at the old 1.5.2, so CI would link a
mismatched libduckdb against the new crate. This test fails that drift loudly
instead of letting it ship.

Every pin is exact, never a floor. miint is built per DuckDB release, and both the
mirror and the staged extension directory are laid out by DuckDB version (see
`qiita_compute_orchestrator.miint_staging`), so a resolve free to move
to a newer DuckDB can land on one with no miint build at all. A lockfile protects
only the environments synced from it.

What it ties together, each against the crate:
- `qiita-data-plane/Cargo.toml` `[dependencies].duckdb` — the crate, which decides
  the embedded/linked DuckDB version. libduckdb-sys keeps its own major at `1` and
  encodes the DuckDB version in the middle field: `1.<major><minor:02><patch:02>.<rev>`,
  so crate `1.10503.1` == DuckDB `1.5.3` and `1.20000.1` == DuckDB `2.0.0`.
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
  and the version its `uv.lock` resolves. The container defs' `python-duckdb` pins
  are tied to the orchestrator's lock by `qiita-compute-orchestrator/tests/
  test_myloasm_split.py::test_container_duckdb_matches_the_orchestrator_lock`, so
  this closes that chain rather than repeating it.

When this fails, bump every spot to the same DuckDB version (and update
`_crate_to_duckdb_version` if libduckdb-sys ever changes its encoding).

It also holds the DuckLake catalog format every ATTACH names to one value: the data
plane's `DUCKLAKE_CATALOG_VERSION` (`qiita-data-plane/src/ducklake.rs` says why), the
lake scripts' `QIITA_DUCKLAKE_VERSION` (`deploy/_common.sh`) and the integration tests'
`DUCKLAKE_VERSION` (`tests/integration/_pg_env.py`) — and checks that no DuckLake
ATTACH leaves the format out, which on DuckDB 2.0 creates a 1.1 catalog.
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
DATA_PLANE_DUCKLAKE_RS = REPO_ROOT / "qiita-data-plane" / "src" / "ducklake.rs"
INTEGRATION_PG_ENV = REPO_ROOT / "tests" / "integration" / "_pg_env.py"

# The `version:` input's `default:` inside setup-libduckdb/action.yml. Anchored on
# the 2-space-indented `version:` key, then its 4-space-indented `default:` — so a
# `default:` under a *different* input can't match.
_ACTION_VERSION_DEFAULT_RE = re.compile(
    r'^  version:\n(?:^ {4}.*\n)*?^ {4}default:\s*"([^"]+)"',
    re.MULTILINE,
)
_DEPLOY_VERSION_RE = re.compile(r'^QIITA_DUCKDB_VERSION="([^"]+)"$', re.MULTILINE)
_RUST_DUCKLAKE_VERSION_RE = re.compile(
    r'^pub\(crate\) const DUCKLAKE_CATALOG_VERSION: &str = "([^"]+)";$', re.MULTILINE
)
_DEPLOY_DUCKLAKE_VERSION_RE = re.compile(r'^QIITA_DUCKLAKE_VERSION="([^"]+)"$', re.MULTILINE)
_PY_DUCKLAKE_VERSION_RE = re.compile(r'^DUCKLAKE_VERSION = "([^"]+)"$', re.MULTILINE)
# How far past an ATTACH's first line its option list may run: every site keeps the
# options on the ATTACH line or within the two lines after it.
_ATTACH_STATEMENT_LINES = 3
# A DuckLake ATTACH names its catalog by a quoted `ducklake:` path or by `TYPE
# ducklake`; searching for those, not for the ATTACH keyword, finds it whatever stands
# between ATTACH and the path.
_DUCKLAKE_ATTACH_PATTERNS = ("['\"]ducklake:", "type[[:space:]]+ducklake")
# The option and the start of its quoted value; a bare mention in a comment is not it.
_DUCKLAKE_VERSION_OPTION_RE = re.compile(r"(?i)\bDUCKLAKE_VERSION\s+['\"]")
# A `duckdb` requirement — any case, extras or not — and its specifier;
# `duckdb-<suffix>` packages don't match.
_PY_DUCKDB_REQ_RE = re.compile(r"(?i)duckdb\s*(?:\[[^\]]*\])?\s*([=<>!~].*)?")
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

    Crate `1.10503.1` -> DuckDB `1.5.3`, `1.20000.1` -> DuckDB `2.0.0`: the crate's
    own major is `1`, and its middle field is DuckDB's major followed by a 2-digit
    minor and a 2-digit patch.
    """
    parts = crate.split(".")
    assert len(parts) == 3 and parts[0] == "1", f"unexpected duckdb crate version: {crate!r}"
    encoded = parts[1]
    assert len(encoded) >= 5 and encoded.isdigit(), (
        f"unexpected duckdb crate encoding {encoded!r} in {crate!r}; "
        "update _crate_to_duckdb_version if libduckdb-sys changed its scheme"
    )
    return f"{int(encoded[:-4])}.{int(encoded[-4:-2])}.{int(encoded[-2:])}"


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
    assert lock_path.exists(), f"{component} has no uv.lock; CLAUDE.md requires it committed"
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
    ("crate", "duckdb_version"),
    [("1.10503.1", "1.5.3"), ("1.10505.0", "1.5.5"), ("1.20000.1", "2.0.0")],
)
def test_crate_version_decodes_to_the_duckdb_it_links(crate: str, duckdb_version: str) -> None:
    assert _crate_to_duckdb_version(crate) == duckdb_version


def _one(pattern: re.Pattern[str], path: Path) -> str:
    m = pattern.search(path.read_text())
    assert m, f"{path.relative_to(REPO_ROOT)} no longer declares {pattern.pattern!r}"
    return m.group(1)


def test_ducklake_catalog_version_is_the_same_everywhere() -> None:
    expected = _one(_RUST_DUCKLAKE_VERSION_RE, DATA_PLANE_DUCKLAKE_RS)
    assert _one(_DEPLOY_DUCKLAKE_VERSION_RE, DEPLOY_COMMON_SH) == expected, (
        "deploy/_common.sh QIITA_DUCKLAKE_VERSION differs from the data plane's "
        "DUCKLAKE_CATALOG_VERSION: the lake scripts would attach the catalog at another format."
    )
    assert _one(_PY_DUCKLAKE_VERSION_RE, INTEGRATION_PG_ENV) == expected, (
        "tests/integration/_pg_env.py DUCKLAKE_VERSION differs from the data plane's "
        "DUCKLAKE_CATALOG_VERSION: the integration tests would attach at another format."
    )


def _ducklake_attach_hits(*pathspec: str, cwd: Path = REPO_ROOT, no_index: bool = False):
    """`git grep -n` lines naming a DuckLake catalog, under `pathspec` in `cwd`."""
    patterns = [arg for pattern in _DUCKLAKE_ATTACH_PATTERNS for arg in ("-e", pattern)]
    return subprocess.run(
        ["git", "grep", *(["--no-index"] if no_index else []), "-n", "-i", "-E", *patterns]
        + ["--", *pathspec],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()


def test_every_ducklake_attach_names_the_catalog_version() -> None:
    """An ATTACH that leaves DUCKLAKE_VERSION out creates a catalog at the extension's
    default format, which on DuckDB 2.0 is 1.1, not the 1.0 production runs."""
    hits = _ducklake_attach_hits(
        "*.rs", "*.py", "*.sh", f":!{Path(__file__).relative_to(REPO_ROOT)}"
    )
    # The data plane, both lake scripts and both integration attaches, at least.
    assert len(hits) >= 5, f"found only {hits!r}; the search no longer sees the attach sites"
    missing = []
    for hit in hits:
        path, line, _ = hit.split(":", 2)
        lines = (REPO_ROOT / path).read_text().splitlines()
        start = int(line) - 1
        statement = "\n".join(lines[start : start + _ATTACH_STATEMENT_LINES])
        if not _DUCKLAKE_VERSION_OPTION_RE.search(statement):
            missing.append(f"{path}:{line}")
    assert not missing, f"DuckLake ATTACH without DUCKLAKE_VERSION at {missing}"


@pytest.mark.parametrize(
    "statement",
    [
        "ATTACH 'ducklake:postgres:x' AS l",
        'attach "ducklake:postgres:x" as l',
        "ATTACH OR REPLACE 'ducklake:x' AS l",
        "ATTACH IF NOT EXISTS 'ducklake:x' AS l",
        "ATTACH 'postgres:x' AS l (TYPE ducklake)",
        "ATTACH 'postgres:x' AS l (type  DuckLake)",
    ],
)
def test_the_attach_search_finds_a_ducklake_attach_written(statement: str, tmp_path: Path):
    (tmp_path / "site.py").write_text(f'SQL = """{statement}"""\n')
    assert _ducklake_attach_hits("site.py", cwd=tmp_path, no_index=True)


@pytest.mark.parametrize(
    ("options", "names_it"),
    [
        ("(DUCKLAKE_VERSION '1.0')", True),
        ('(ducklake_version "{v}")', True),
        ("(DATA_PATH 'x')  -- set DUCKLAKE_VERSION later", False),
        ("(DUCKLAKE_VERSION_FOO '1.0')", False),
    ],
)
def test_the_version_check_needs_the_option_and_its_value(options: str, names_it: bool):
    assert bool(_DUCKLAKE_VERSION_OPTION_RE.search(options)) is names_it


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
            "optional-dependencies": {"cli": ["duckdb>=1.5", "DuckDB[all]<2"]},
        },
        "dependency-groups": {"dev": ["duckdb", {"include-group": "cli"}]},
        "tool": {"uv": {"dev-dependencies": ["duckdb~=1.5.5"]}},
    }
    assert sorted(_duckdb_specifiers(pyproject), key=str) == sorted(
        ["==1.5.5", ">=1.5", "<2", None, "~=1.5.5"], key=str
    )


def test_discovery_finds_every_known_python_project() -> None:
    """A discovery that silently found nothing would make the per-project checks vacuous."""
    assert {
        "qiita-common",
        "qiita-compute-orchestrator",
        "qiita-control-plane",
        "tests/integration",
    } <= set(_python_projects())
