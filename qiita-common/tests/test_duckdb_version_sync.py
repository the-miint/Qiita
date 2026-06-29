"""Guard: the libduckdb the Rust data-plane CI downloads must match the DuckDB
version its `duckdb` crate is built against.

A DuckDB bump has to land in lockstep across several spots; a past bump moved the
`duckdb` crate and the Python locks but left the `setup-libduckdb` action's
`version` default (and the deploy cache key) at the old 1.5.2, so CI would link a
mismatched libduckdb against the new crate. This test fails that drift loudly
instead of letting it ship.

What it ties together:
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

When this fails, bump every spot to the same DuckDB version (and update
`_crate_to_duckdb_version` if libduckdb-sys ever changes its encoding).
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CARGO_TOML = REPO_ROOT / "qiita-data-plane" / "Cargo.toml"
ACTION_YML = REPO_ROOT / ".github" / "actions" / "setup-libduckdb" / "action.yml"

# The `version:` input's `default:` inside setup-libduckdb/action.yml. Anchored on
# the 2-space-indented `version:` key, then its 4-space-indented `default:` — so a
# `default:` under a *different* input can't match.
_ACTION_VERSION_DEFAULT_RE = re.compile(
    r'^  version:\n(?:^ {4}.*\n)*?^ {4}default:\s*"([^"]+)"',
    re.MULTILINE,
)


def _data_plane_duckdb_crate_version() -> str:
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


def test_libduckdb_action_default_matches_data_plane_crate() -> None:
    crate = _data_plane_duckdb_crate_version()
    expected = _crate_to_duckdb_version(crate)

    m = _ACTION_VERSION_DEFAULT_RE.search(ACTION_YML.read_text())
    assert m, "could not find the `version` input default in setup-libduckdb/action.yml"
    action_default = m.group(1)

    assert action_default == expected, (
        f"setup-libduckdb action `version` default ({action_default!r}) does not match the "
        f"DuckDB version the data-plane crate links ({expected!r}, from duckdb crate {crate!r}). "
        "A DuckDB bump must update BOTH qiita-data-plane/Cargo.toml's `duckdb` crate and the "
        "action `version` default (which also keys the ~/.duckdb/extensions cache)."
    )
