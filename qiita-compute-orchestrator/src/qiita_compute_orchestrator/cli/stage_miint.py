"""qiita-compute-orchestrator stage-miint — deploy-time miint staging.

Installs the miint DuckDB extension **once** into `MIINT_EXTENSION_DIRECTORY` so
every cluster runtime path (native SLURM jobs, the compute-readiness probe) can
`LOAD` it without downloading — no per-job egress, no compute-node mirror
dependency, no writable-`$HOME` requirement.

Run at deploy via `scripts/stage-miint-extension.sh`. Idempotent: it FORCE-
installs the mirror's current build, so re-run after a miint or DuckDB version
bump. The install/load SQL + connect config come from `qiita_common.duckdb_miint`
(single source); the work is `qiita_compute_orchestrator.miint.stage_miint_extension`.

A staging failure (unreachable mirror, unwritable dir, broken build) raises and
exits non-zero — fail loud at deploy, not at the first reference-load job.

`--check` does no staging — it reports whether the already-staged build matches
what the mirror serves now (`qiita_compute_orchestrator.miint_staging`), so the
deploy can skip a redundant FORCE INSTALL. Exit 0 = current, exit 1 = stage
needed (or any uncertainty — never skip on doubt).
"""

from __future__ import annotations

import argparse
import sys

from qiita_common.duckdb_miint import miint_install_sql

from ..miint import stage_miint_extension
from ..miint_staging import staging_is_current


def main() -> int:
    parser = argparse.ArgumentParser(prog="stage-miint", description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Report whether the staged build is already current instead of staging."
            " Exit 0 = current (deploy may skip); exit 1 = stage needed (not staged,"
            " DuckDB-version/platform changed, or the mirror published a new build)."
            " Any uncertainty (no marker, network failure) exits 1 — never skip on doubt."
        ),
    )
    args = parser.parse_args()

    if args.check:
        try:
            current = staging_is_current()
        except Exception as exc:  # noqa: BLE001 — any failure means re-stage, never skip
            print(f"miint staging check failed ({exc}); staging needed", file=sys.stderr)
            return 1
        print("miint staging up to date" if current else "miint staging needed")
        return 0 if current else 1

    ext_dir = stage_miint_extension()
    print(f"miint staged: {miint_install_sql(force=True)}")
    print(f"extension_directory: {ext_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
