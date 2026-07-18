"""Shared helpers for components that load the miint DuckDB extension.

Pure Python — imports **no** duckdb, so qiita-common stays a lightweight
contract layer. It produces the connection-config dict, the INSTALL statement,
and the empty-input pre-check that both the orchestrator
(`qiita_compute_orchestrator.miint`, async) and the CLI
(`qiita_control_plane.miint`, sync) need, so the `MIINT_EXTENSION_REPO` /
`MIINT_EXTENSION_DIRECTORY` env contract is single-sourced rather than copied
into each.

miint installs from the team mirror (default `MIINT_MIRROR_URL`;
`MIINT_EXTENSION_REPO` overrides for a local/dev build) so every Qiita
component runs the **same** build — the mirror is the single source of truth for
the miint version, and pulling everyone from it avoids the community-vs-mirror
patchwork where hosts drift to different builds. Installing from the mirror
implies `allow_unsigned_extensions=true` (its signing chain is the team's own,
not DuckDB's). `MIINT_EXTENSION_DIRECTORY` selects the install directory; in
production it points at a shared directory that the deploy stages **once**
(`scripts/stage-miint-extension.sh`).

Install vs. load — the two halves of the contract:

- On the **cluster** (CO service, native SLURM jobs, the compute-readiness
  probe) miint is pre-staged into `MIINT_EXTENSION_DIRECTORY` at deploy, and
  runtime only ever `LOAD`s it (`miint_load_sql`). No compute node downloads the
  extension, needs the mirror reachable, or needs a writable `$HOME` — the
  per-job `FORCE INSTALL` that did all three was a footgun and is gone.
- The **client** `qiita reference load` CLI can't reach a deploy-staged dir
  (it runs from arbitrary hosts), so it `INSTALL`s into its own cache — but
  plain `INSTALL`, which is a no-op on a warm cache, not `FORCE`.

Cross-language note: the Rust data plane can't import this module, so it honors
the same env contract (`MIINT_EXTENSION_REPO` / `MIINT_EXTENSION_DIRECTORY`)
independently — keep the two sides in sync (see `qiita-data-plane/src/main.rs`).
"""

from __future__ import annotations

import gzip
import os
import tempfile
from pathlib import Path

MIINT_MIRROR_URL = "https://ftp.microbio.me/pub/miint"


def miint_repo() -> str:
    """The miint extension repo. Defaults to the team mirror so every Qiita
    component installs the SAME, current build — no community-vs-mirror
    patchwork where hosts drift to different builds. `MIINT_EXTENSION_REPO`
    overrides for a local/dev extension build.

    Public because the deploy-time staging gate
    (`qiita_compute_orchestrator.miint_staging`) builds the mirror URL it HEADs
    from this same value `miint_install_sql()` installs from."""
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


def miint_install_sql(*, force: bool = False) -> str:
    """The INSTALL statement for miint, from the mirror (`MIINT_EXTENSION_REPO`
    override, else `MIINT_MIRROR_URL`).

    Plain `INSTALL` by default — it is a no-op when the build is already present
    in the active `extension_directory`, so it never re-downloads on a warm
    cache. This is the form the client-side `qiita reference load` CLI uses (fill
    the local cache once, then reuse it).

    `force=True` is for **deploy-time staging only** (`stage_miint_extension`):
    it re-installs even when present, so a deploy refreshes the shared
    `extension_directory` to the mirror's current build. Cluster runtime never
    INSTALLs at all — it `LOAD`s from that pre-staged directory (`miint_load_sql`).
    """
    verb = "FORCE INSTALL" if force else "INSTALL"
    return f"{verb} miint FROM '{miint_repo()}';"


def miint_load_sql() -> str:
    """The LOAD statement for miint. Cluster runtime paths (CO service, native
    jobs, the compute-readiness probe) only LOAD: the extension is pre-staged
    into `MIINT_EXTENSION_DIRECTORY` at deploy, so no node downloads it, depends
    on mirror reachability, or needs a writable `$HOME`."""
    return "LOAD miint;"


# miint is a CORE, non-optional dependency (see CLAUDE.md "miint is a core
# dependency"). Every native SLURM job needs BOTH of these to function:
#   * MIINT_EXTENSION_DIRECTORY — the deploy-staged extension the job LOADs;
#   * MIINT_GPL_BOUNDARY_PATH   — the GPL-boundary host binary miint shells out
#                                 to (bowtie2 index/align, vsearch, MAFFT, …).
# The compute node receives ONLY what we explicitly forward (the slurmrestd
# `environment` is an allowlist, not an inherited copy — see SlurmBackend and
# payload.build_job_submit_payload), so an unforwarded var is simply absent at
# job runtime.
MIINT_REQUIRED_JOB_VARS = ("MIINT_EXTENSION_DIRECTORY", "MIINT_GPL_BOUNDARY_PATH")


def miint_job_env() -> dict[str, str]:
    """The miint env vars a remote (SLURM) job MUST carry to LOAD the
    deploy-staged extension AND reach the GPL-boundary host. Both the
    orchestrator's SlurmBackend (real jobs) and the compute-readiness probe
    inject exactly this into the job's slurmrestd `environment` — single-sourced
    here so the two can't drift.

    miint is a CORE dependency, not optional: this RAISES if either
    `MIINT_EXTENSION_DIRECTORY` or `MIINT_GPL_BOUNDARY_PATH` is unset. A silent
    empty dict was the bug — it let a job submit that then died at `LOAD miint`
    or the first GPL-boundary call (the bowtie2-shard `gpl-boundary not
    installed` incident), and it hid a broken boundary through a green deploy.
    Fail loud instead. `MIINT_EXTENSION_REPO` is deliberately NOT propagated: the
    cluster path is LOAD-only, so the install repo is irrelevant on a node.

    This is the CLUSTER/JOB path. The client-side `qiita reference load` CLI
    legitimately runs with these unset (it INSTALLs into its own cache) and uses
    `miint_connect_config()`, which stays optional — this requirement is scoped
    to job submission on purpose."""
    missing = [v for v in MIINT_REQUIRED_JOB_VARS if not os.environ.get(v)]
    if missing:
        raise RuntimeError(
            "miint is a core dependency; these required env var(s) are unset and "
            "must be set in compute-orchestrator.env for any SLURM job: "
            f"{', '.join(missing)}. See CLAUDE.md 'miint is a core dependency'."
        )
    return {v: os.environ[v] for v in MIINT_REQUIRED_JOB_VARS}


def is_empty_sequence_file(path: Path) -> bool:
    """True iff `path` decompresses to zero bytes — i.e., it holds no
    FASTQ/FASTA content. Callers pre-check with this before handing the path to
    miint's `read_fastx`, which throws `std::runtime_error("Empty file: " +
    path)` on zero-record inputs (see duckdb-miint/src/SequenceReader.cpp).
    Pre-checking routes empty inputs through an explicit code path instead of
    catching the exception and matching its wording. An upstream fix that let
    miint return a 0-row relation here would make this pre-check unnecessary.

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
    another's cached extension.

    The conftest fixtures INSTALL (not FORCE INSTALL) into that dir, so it caches
    across runs and NEVER refreshes. After the mirror rebuilds — e.g. a new miint
    function lands — a warm cache still holds the old build, and the symptom is a
    bare `Catalog Error: ... does not exist` from the new function, which points at
    the code rather than at the cache. Clear it:

        rm -rf "$TMPDIR"/qiita-*-duckdb-ext        # NOT /tmp on macOS

    CI is unaffected (it always starts cold), and the deploy is covered by
    compute-readiness's miint probes."""
    os.environ.setdefault("MIINT_EXTENSION_REPO", MIINT_MIRROR_URL)
    ext_dir = os.path.join(tempfile.gettempdir(), f"qiita-{component}-duckdb-ext")
    os.makedirs(ext_dir, exist_ok=True)
    os.environ.setdefault("MIINT_EXTENSION_DIRECTORY", ext_dir)
