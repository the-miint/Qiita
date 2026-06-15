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
"""

from __future__ import annotations

import sys

from qiita_common.duckdb_miint import miint_install_sql

from ..miint import stage_miint_extension


def main() -> int:
    ext_dir = stage_miint_extension()
    print(f"miint staged: {miint_install_sql(force=True)}")
    print(f"extension_directory: {ext_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
