"""Are this node's copies of the active krepp builds the registered ones?

Run as a module by the compute-readiness probe, from the interpreter under test:

    <SLURM_NATIVE_PYTHON> -P -m qiita_compute_orchestrator.krepp_node_check

It asks the control plane for every reference's active build
(`GET /reference/krepp-build/active`) and runs the per-job check
(`krepp_build.verify_build`) on this node's copy of each, as the activation job does.
The exit code is the row's status, so the probe script can tell them apart:

- 0 `ok`: every active build's copy here passes, and was written by the krepp the
  loaded miint links;
- 3 `skip`: no reference has an active krepp build;
- 4 `warn`: a copy here is missing or does not pass, or a build was written by a
  krepp other than the one the loaded miint links (or miint reports no
  `krepp-version` row to compare). A bad copy is one node: a krepp job that lands
  here fails, and the cluster admins re-copy the folder and run its verify.sh
  (docs/runbooks/krepp-index.md). A version difference only warns: nothing refuses a
  build for it, and activation checks a build by placing known tips with the krepp
  miint links. Neither fails the deploy;
- 1 `fail`: PATH_NODE_LOCAL is not in the job env, or the control plane cannot be
  asked.

One line on stdout says which build and why.
"""

from __future__ import annotations

import asyncio
import sys

import duckdb
import httpx
from qiita_common.api_paths import URL_REFERENCE_KREPP_BUILD_ACTIVE
from qiita_common.krepp import KreppBuildRecord

from .config import get_settings
from .cp_client import make_cp_client
from .derived_store import node_relative_path
from .krepp_build import KreppBuildInvalid, linked_krepp_version, verify_build
from .miint import open_miint_conn
from .native_import_check import MAX_DETAIL

EXIT_OK, EXIT_FAIL, EXIT_SKIP, EXIT_WARN = 0, 1, 3, 4


async def _active_builds() -> list[KreppBuildRecord]:
    async with make_cp_client() as http:
        resp = await http.get(URL_REFERENCE_KREPP_BUILD_ACTIVE)
        resp.raise_for_status()
        return [KreppBuildRecord.model_validate(b) for b in resp.json()]


def check(
    builds: list[KreppBuildRecord], node_root: str | None, linked: str | None
) -> tuple[int, str]:
    """The row's exit code and its one-line detail."""
    if not builds:
        return EXIT_SKIP, "no active krepp build"
    if not node_root:
        return EXIT_FAIL, "PATH_NODE_LOCAL is not set in the job env"
    bad: list[str] = []
    for b in builds:
        try:
            manifest = verify_build(node_relative_path(node_root, b.fs_path), b.sha256sums_sha256)
        except KreppBuildInvalid as exc:
            bad.append(f"build {b.reference_index_idx} ({b.fs_path}): {exc.check}: {exc.detail}")
            continue
        built = manifest["settings"]["krepp version"]
        if linked is None:
            bad.append(
                f"build {b.reference_index_idx}: written by krepp {built}; the loaded miint"
                " reports no krepp-version row to compare"
            )
        elif built != linked:
            bad.append(
                f"build {b.reference_index_idx}: written by krepp {built};"
                f" the loaded miint links {linked}"
            )
    if bad:
        return EXIT_WARN, "; ".join(bad)
    return EXIT_OK, f"{len(builds)} active build(s) pass"


def main() -> int:
    try:
        builds = asyncio.run(_active_builds())
        conn = open_miint_conn()
        try:
            linked = linked_krepp_version(conn)
        finally:
            conn.close()
        code, detail = check(builds, get_settings().path_node_local, linked)
    except (httpx.HTTPError, RuntimeError, duckdb.Error) as exc:
        code, detail = EXIT_FAIL, f"{type(exc).__name__}: {exc}"
    print(" ".join(detail.split())[:MAX_DETAIL])
    return code


if __name__ == "__main__":
    sys.exit(main())
