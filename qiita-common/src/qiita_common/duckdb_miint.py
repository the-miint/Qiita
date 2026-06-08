"""Shared helpers for components that load the miint DuckDB extension.

Pure Python — imports **no** duckdb, so qiita-common stays a lightweight
contract layer. It produces the connection-config dict, the INSTALL statement,
and the empty-input pre-check that both the orchestrator
(`qiita_compute_orchestrator.miint`, async) and the CLI
(`qiita_control_plane.miint`, sync) need, so the `MIINT_EXTENSION_REPO` /
`MIINT_EXTENSION_DIRECTORY` env contract is single-sourced rather than copied
into each.

`MIINT_EXTENSION_REPO` points installs at the team mirror; without it, install
pulls from the DuckDB community channel. The mirror implies
`allow_unsigned_extensions=true` (its signing chain is the team's own, not
DuckDB's). `MIINT_EXTENSION_DIRECTORY` isolates the install directory so a
mirror build doesn't clash with a community build in the shared default
`~/.duckdb/extensions` — DuckDB refuses a plain INSTALL when a cached
extension's origin differs.
"""

from __future__ import annotations

import gzip
import os
import tempfile
from pathlib import Path

MIINT_MIRROR_URL = "https://ftp.microbio.me/pub/miint"


def miint_connect_config() -> dict[str, str]:
    """DuckDB `connect()` config for loading miint: allow unsigned extensions
    when a mirror repo is set, and isolate the extension directory when one is
    configured. An empty dict means a plain `duckdb.connect(":memory:")`."""
    config: dict[str, str] = {}
    if os.environ.get("MIINT_EXTENSION_REPO") is not None:
        config["allow_unsigned_extensions"] = "true"
    ext_dir = os.environ.get("MIINT_EXTENSION_DIRECTORY")
    if ext_dir:
        config["extension_directory"] = ext_dir
    return config


def miint_install_sql() -> str:
    """The INSTALL statement for miint: FORCE INSTALL from the mirror when
    MIINT_EXTENSION_REPO is set, else INSTALL FROM the community channel."""
    repo = os.environ.get("MIINT_EXTENSION_REPO")
    return f"FORCE INSTALL miint FROM '{repo}';" if repo else "INSTALL miint FROM community;"


def is_empty_sequence_file(path: Path) -> bool:
    """True iff `path` decompresses to zero bytes — i.e., it holds no
    FASTQ/FASTA content. Callers pre-check with this before handing the path to
    miint's `read_fastx`, which throws `std::runtime_error("Empty file: " +
    path)` on zero-record inputs (see duckdb-miint/src/SequenceReader.cpp).
    Pre-checking routes empty inputs through an explicit code path instead of
    catching the exception and matching its wording — see #39 for the
    upstream-fix proposal that would let miint return a 0-row relation here.

    Why a decompressed-stream peek and not `os.path.getsize == 0`: the realistic
    empty case is a `.fastq.gz` from a sequencing run that produced no reads —
    still ~20 bytes of gzip framing on disk, but `gzip.open(...).read(1)`
    returns `b""`.

    Files with bytes but no parseable records (stray whitespace, comment lines)
    report False here and surface as a duckdb.Error from `read_fastx`
    downstream. That's a real data error and should fail loudly, not be
    silently treated as empty."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rb") as f:
        return f.read(1) == b""


def setup_miint_test_env(component: str) -> None:
    """Test-harness helper: point miint installs at the team mirror and a
    per-component private extension directory, via `setdefault` (a no-op when
    the env is already set). Call at conftest top. `component` names the
    private dir (`qiita-<component>-duckdb-ext` under the system temp), kept
    distinct per component so a mirror build in one suite doesn't collide with
    another's cached extension."""
    os.environ.setdefault("MIINT_EXTENSION_REPO", MIINT_MIRROR_URL)
    ext_dir = os.path.join(tempfile.gettempdir(), f"qiita-{component}-duckdb-ext")
    os.makedirs(ext_dir, exist_ok=True)
    os.environ.setdefault("MIINT_EXTENSION_DIRECTORY", ext_dir)
