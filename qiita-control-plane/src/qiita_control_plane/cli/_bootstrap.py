"""Import-clean console-script shim for `qiita` / `qiita-admin`.

THIS MODULE MUST NOT IMPORT qiita_common — not at top level, not
transitively. Only stdlib. That is the entire point of the module.

Why this exists (the cross-package staleness trap; see CLAUDE.md
"Cross-package staleness"): `qiita-common` is a path dependency of
qiita-control-plane. After a `git pull` that changes `qiita-common`
WITHOUT bumping its version string, a plain `uv sync` skips reinstalling
the path-dep, leaving stale `qiita_common` sources in the venv's
site-packages. The real CLI modules (`cli.user`, `cli.admin`) import
`qiita_common` at module top level, so launching `qiita`/`qiita-admin`
then raises an `ImportError` (e.g. "cannot import name 'X' from
'qiita_common.api_paths'") AT IMPORT TIME — before any `main()` is
reachable, so a try/except inside `main` cannot catch it.

The fix: point the console-script entry points at this shim instead.
This module stays importable when `qiita_common` is stale (it imports no
qiita_common), so it can import the real CLI module LAZILY inside a
guarded function and translate the staleness ImportError into a clean,
actionable operator message naming the exact `uv sync` fix — rather than
dumping a raw traceback. A genuine / unrelated ImportError is re-raised
untouched, so real bugs are never masked.
"""

import sys

# The fix command we tell the operator to run. The cross-package
# staleness trap is defeated by reinstalling the path-dep explicitly.
_FIX_CMD = "uv sync --reinstall-package qiita-common"


def _is_qiita_common_staleness(exc: ImportError) -> bool:
    """Is this ImportError the qiita_common path-dep being stale?

    Conservative on purpose: when unsure, return False so the raw error
    still surfaces (never translate an unrelated import failure into the
    qiita_common hint). ModuleNotFoundError is an ImportError subclass and
    is handled the same way (a removed module reads as staleness too).
    """
    name = getattr(exc, "name", "") or ""
    if name == "qiita_common" or name.startswith("qiita_common."):
        return True
    return "qiita_common" in str(exc)


def _print_staleness_hint(exc: ImportError) -> None:
    """Print a clean, actionable operator message to stderr (no traceback)."""
    print(
        "ERROR: the installed 'qiita_common' is out of date with this checkout.\n"
        "\n"
        "This is the cross-package staleness trap: a plain 'uv sync' skips\n"
        "reinstalling the 'qiita-common' path-dependency when its version string\n"
        "is unchanged, leaving stale sources in this venv's site-packages.\n"
        "\n"
        "Fix it from the qiita-control-plane project directory you launched this\n"
        f"CLI from:\n"
        f"    {_FIX_CMD}\n"
        "\n"
        f"Original import error: {exc}",
        file=sys.stderr,
    )


def _run(import_main):
    """Shared body: lazily import the real `main`, guard the import only.

    `import_main` is a zero-arg callable that performs the lazy import and
    returns the real `main`. The import is inside the try (that is the
    staleness-prone step); the `main()` CALL is OUTSIDE it, so runtime /
    lazy ImportErrors raised from within command handlers are NOT
    translated — only the import of `main` itself is guarded.
    """
    try:
        main = import_main()
    except ImportError as exc:
        if _is_qiita_common_staleness(exc):
            _print_staleness_hint(exc)
            raise SystemExit(1) from exc
        raise
    return main()


def qiita():
    """Entry point for the `qiita` console script."""

    def _import_main():
        from qiita_control_plane.cli.user import main

        return main

    return _run(_import_main)


def qiita_admin():
    """Entry point for the `qiita-admin` console script."""

    def _import_main():
        from qiita_control_plane.cli.admin import main

        return main

    return _run(_import_main)
