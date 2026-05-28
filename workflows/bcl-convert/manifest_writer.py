"""Walk a step's output directory and emit the manifest the data-plane
verifier expects.

The manifest carries two top-level keys:

  * ``files``: a list of every file under the output root (recursive),
    each entry ``{path, size_bytes}`` where ``path`` is relative to the
    output root and uses forward slashes. ``manifest.json`` itself is
    excluded so the manifest is self-describing without referencing
    itself (mirrors the native-step launcher's behaviour at
    qiita_compute_orchestrator/jobs/__main__.py:83-89).
  * ``outputs``: ``{name: relative_path}`` keyed on the YAML's
    ``outputs:`` declaration; the verifier resolves the value to either
    a file in ``files`` or a directory whose contents are.

Idempotent on re-run: an existing ``manifest.json`` is silently
overwritten with the fresh walk.
"""

import json
import sys
from pathlib import Path

_MANIFEST_NAME = "manifest.json"


def _parse_output_pairs(argv: list[str]) -> dict[str, str]:
    """argv is ``[<output_root>, '<name1>=<relpath1>', '<name2>=<relpath2>', ...]``.

    Each pair declares one entry in the manifest's ``outputs`` map. The
    name comes from the YAML's ``outputs:`` list; the relative path is
    resolved against the output root.
    """
    if len(argv) < 2:
        raise SystemExit(
            "manifest_writer.py: usage: <output_root> <name>=<relpath> [...]"
            f" — got {len(argv) - 1} args"
        )
    pairs: dict[str, str] = {}
    for raw in argv[2:]:
        if "=" not in raw:
            raise SystemExit(
                f"manifest_writer.py: '{raw}' is not '<name>=<relpath>'"
                " — every output arg must be NAME=PATH"
            )
        name, _, relpath = raw.partition("=")
        if not name or not relpath:
            raise SystemExit(f"manifest_writer.py: empty name or relpath in '{raw}'")
        if name in pairs:
            raise SystemExit(f"manifest_writer.py: duplicate output name '{name}'")
        pairs[name] = relpath
    return pairs


def _walk_files(output_root: Path) -> list[dict[str, object]]:
    """Enumerate every regular file under ``output_root`` recursively,
    skipping ``manifest.json`` itself.

    Entries are sorted by relative path so a re-run on identical content
    produces an identical manifest. Symlinks are followed only if they
    point inside the output tree — the verifier rejects symlinks that
    escape so resolving them here would just shift the error from
    verification to a confusing manifest mismatch.
    """
    files: list[dict[str, object]] = []
    for path in sorted(output_root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(output_root).as_posix()
        if rel == _MANIFEST_NAME:
            continue
        files.append({"path": rel, "size_bytes": path.stat().st_size})
    return files


def main(argv: list[str]) -> int:
    output_root = Path(argv[1]).resolve()
    if not output_root.is_dir():
        raise SystemExit(
            f"manifest_writer.py: output root {output_root} is not a directory"
        )

    outputs = _parse_output_pairs(argv)
    files = _walk_files(output_root)

    manifest = {"files": files, "outputs": outputs}
    manifest_path = output_root / _MANIFEST_NAME
    # `sort_keys` + explicit indent keep on-disk diffs minimal across
    # re-runs that walk the same content; the verifier doesn't care about
    # whitespace but a human reading a failed-run dir does.
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
