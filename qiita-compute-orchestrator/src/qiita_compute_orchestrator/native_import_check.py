"""Does this venv carry the code native SLURM jobs are about to run?

Run as a module, from the interpreter under test:

    <SLURM_NATIVE_PYTHON> -P -m qiita_compute_orchestrator.native_import_check

Exit 0, `native-import: ok (N job modules)` on stdout. Exit 1, a single-line
`native-import: FAIL <type>: <message>` on stdout naming the module that could
not be imported and why.

Two callers, one implementation: `deploy/redeploy.sh` runs it on the HEAD node
after `uv sync`, and the `compute-readiness` probe job runs it on a COMPUTE node
(`cli/compute_readiness.py`). They ask the same question of two filesystems' view
of one venv, so they must not be able to disagree about what "imports cleanly"
means.

**Why the check is anchored on the job modules, not on qiita_common.** qiita-common
is a path dependency whose version string does not move between commits, so a
plain `uv sync` leaves stale sources in site-packages (CLAUDE.md, "Cross-package
staleness"). A stale copy is not visible at package granularity: `import
qiita_common` succeeds when a submodule is absent, and `import
qiita_common.api_paths` succeeds when only a NAME is absent. The second has no
granularity on the dependency side that would catch it — there is no import of
qiita_common that notices a missing constant. Importing the CONSUMER does: a job
module's top-level `from qiita_common.x import Y` fails on an absent module and an
absent name alike. Both shapes have reached production; `docs/runbooks/redeploy.md`
carries the incidents.

The walk is `jobs.scan_native_jobs`, the orchestrator's own boot scan, called
rather than re-implemented — a second walk here would be a second answer to "which
modules are native jobs", and the one that decides at RUN time is that one. It
skips leading-underscore helpers (`jobs/_feature_load.py` and friends), which are
covered transitively by the job modules that import them. It also validates each
module's `Inputs`/`execute` contract, which is not scope creep: `main.py` runs the
same scan at boot, so a malformed job already stops the service — this makes the
native venv fail at the same point rather than later.

Output goes to STDOUT on both paths, one line, so the compute-node caller can
capture it into its `err=` field the way every miint probe does. `scan_native_jobs`
raises a multi-line message; collapsing it is what keeps the probe log's
one-check-per-line contract (`_parse_probe_log`).
"""

# Matches the miint probes' cap in `cli/compute_readiness.py`: enough to name the
# module and the error, short enough not to swamp the probe log.
_MAX_DETAIL = 500


def main() -> int:
    """Import what native jobs import; return 0 on success, 1 on failure.

    `qiita_common` and `config` are named explicitly rather than left to the job
    closure. They are almost certainly in it — every native job reaches
    `get_settings` — but a future jobs tree that stopped importing one should not
    quietly shrink what a deploy checks.
    """
    try:
        import qiita_common  # noqa: F401

        from qiita_compute_orchestrator import config  # noqa: F401
        from qiita_compute_orchestrator.jobs import scan_native_jobs

        modules = scan_native_jobs()
    except Exception as exc:
        # Widened deliberately, matching `scan_native_jobs`' own import catch: a
        # stale venv surfaces as ImportError, but a half-written one can raise
        # anything at import time, and every one of them means the same thing here.
        # chr(10)/chr(13) rather than backslash escapes so the collapse is legible.
        detail = (f"{type(exc).__name__}: {exc}").replace(chr(10), " ").replace(chr(13), " ")
        print(f"native-import: FAIL {detail[:_MAX_DETAIL]}")
        return 1
    print(f"native-import: ok ({len(modules)} job modules)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
