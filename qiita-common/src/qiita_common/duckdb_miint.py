"""Shared helpers for components that load the miint DuckDB extension.

Pure Python — imports **no** duckdb, so qiita-common stays a lightweight
contract layer. It produces the connection-config dict, the INSTALL statement,
and the empty-input pre-check that both the orchestrator
(`qiita_compute_orchestrator.miint`, async) and the CLI
(`qiita_control_plane.miint`, sync) need, so the `MIINT_EXTENSION_REPO` /
`MIINT_EXTENSION_DIRECTORY` env contract is single-sourced rather than copied
into each.

miint always installs from the team mirror (default `MIINT_MIRROR_URL`;
`MIINT_EXTENSION_REPO` overrides for a local/dev build) so every Qiita
component runs the **same**, current build — the mirror is the single source of
truth for the miint version, and pulling everyone from it avoids the
community-vs-mirror patchwork where hosts drift to different builds.
Installing from the mirror implies
`allow_unsigned_extensions=true` (its signing chain is the team's own, not
DuckDB's). `MIINT_EXTENSION_DIRECTORY` isolates the install directory so a
mirror build doesn't clash with another origin's cached extension in the shared
default `~/.duckdb/extensions` — DuckDB refuses a plain INSTALL when a cached
extension's origin differs.
"""

from __future__ import annotations

import gzip
import os
import tempfile
from pathlib import Path

MIINT_MIRROR_URL = "https://ftp.microbio.me/pub/miint"


def _miint_repo() -> str:
    """The miint extension repo. Defaults to the team mirror so every Qiita
    component installs the SAME, current build — no community-vs-mirror
    patchwork where hosts drift to different builds. `MIINT_EXTENSION_REPO`
    overrides for a local/dev extension build."""
    return os.environ.get("MIINT_EXTENSION_REPO") or MIINT_MIRROR_URL


def miint_connect_config() -> dict[str, str]:
    """DuckDB `connect()` config for loading miint. miint always installs from
    a mirror (the team's signing chain, not DuckDB's), so unsigned extensions
    are always allowed; the extension directory is isolated when configured."""
    config: dict[str, str] = {"allow_unsigned_extensions": "true"}
    ext_dir = os.environ.get("MIINT_EXTENSION_DIRECTORY")
    if ext_dir:
        config["extension_directory"] = ext_dir
    return config


def miint_install_sql() -> str:
    """The INSTALL statement for miint: always FORCE INSTALL from the mirror
    (`MIINT_EXTENSION_REPO` override, else `MIINT_MIRROR_URL`). FORCE overwrites
    a stale cached extension so a compute node always runs the mirror's current
    build instead of whatever it happened to cache earlier."""
    return f"FORCE INSTALL miint FROM '{_miint_repo()}';"


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
