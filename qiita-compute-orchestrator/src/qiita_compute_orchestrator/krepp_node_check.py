"""Are this node's copies of the active krepp builds the registered ones?

Run as a module by the compute-readiness probe, from the interpreter under test:

    <SLURM_NATIVE_PYTHON> -P -m qiita_compute_orchestrator.krepp_node_check

It asks the control plane for every reference's active build
(`GET /reference/krepp-build/active`) and runs the per-job check
(`krepp_build.verify_build`) on this node's copy of each, as a placement job would.
The exit code is the row's status, so the probe script can tell them apart:

- 0 `ok`: every active build's copy here passes;
- 3 `skip`: no reference has an active krepp build;
- 4 `warn`: a copy here is missing or does not pass. This is one node: a placement
  job that lands here fails naming it, and the cluster admins re-copy the folder
  and run its verify.sh (docs/runbooks/krepp-index.md). It does not fail the deploy;
- 1 `fail`: PATH_NODE_LOCAL is not in the job env, the control plane cannot be
  asked, or a build was written by a krepp other than the one the staged miint
  links, which fails every placement on every node.

One line on stdout says which build and why.
"""

from __future__ import annotations

import asyncio
import sys

import duckdb
import httpx
from qiita_common.api_paths import URL_REFERENCE_KREPP_BUILD_ACTIVE
from qiita_common.duckdb_miint import miint_connect_config, miint_load_sql
from qiita_common.krepp import KreppBuildRecord

from .config import get_settings
from .cp_client import make_cp_client
from .derived_store import node_relative_path
from .krepp_build import KreppBuildInvalid, linked_krepp_version, verify_build
from .native_import_check import MAX_DETAIL

EXIT_OK, EXIT_FAIL, EXIT_SKIP, EXIT_WARN = 0, 1, 3, 4


async def _active_builds() -> list[KreppBuildRecord]:
    async with make_cp_client() as http:
        resp = await http.get(URL_REFERENCE_KREPP_BUILD_ACTIVE)
        resp.raise_for_status()
        return [KreppBuildRecord.model_validate(b) for b in resp.json()]


def check(builds: list[KreppBuildRecord], node_root: str | None, linked: str) -> tuple[int, str]:
    """The row's exit code and its one-line detail."""
    if not builds:
        return EXIT_SKIP, "no active krepp build"
    if not node_root:
        return EXIT_FAIL, "PATH_NODE_LOCAL is not set in the job env"
    bad: list[str] = []
    for b in builds:
        try:
            verify_build(
                node_relative_path(node_root, b.fs_path),
                b.sha256sums_sha256,
                linked_krepp_version=linked,
            )
        except KreppBuildInvalid as exc:
            if exc.check == "krepp-version":
                return EXIT_FAIL, f"build {b.reference_index_idx}: {exc.detail}"
            bad.append(f"build {b.reference_index_idx} ({b.fs_path}): {exc.check}: {exc.detail}")
    if bad:
        return EXIT_WARN, "; ".join(bad)
    return EXIT_OK, f"{len(builds)} active build(s) pass"


def main() -> int:
    try:
        builds = asyncio.run(_active_builds())
        conn = duckdb.connect(":memory:", config=miint_connect_config())
        conn.execute(miint_load_sql())
        linked = linked_krepp_version(conn)
        code, detail = check(builds, get_settings().path_node_local, linked)
    except (httpx.HTTPError, RuntimeError, duckdb.Error) as exc:
        code, detail = EXIT_FAIL, f"{type(exc).__name__}: {exc}"
    print(" ".join(detail.split())[:MAX_DETAIL])
    return code


if __name__ == "__main__":
    sys.exit(main())
