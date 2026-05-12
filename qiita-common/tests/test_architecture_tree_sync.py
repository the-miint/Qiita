"""Assert every path the directory-tree code block in docs/architecture.md
lists actually exists on disk.

Asymmetric (curated) check: paths in the doc must exist; files on disk
that aren't in the doc are fine (the tree is a high-level map, not an
exhaustive listing). Catches the failure mode where someone deletes or
renames a file/directory without updating the doc.

When this test fails, either update the tree in docs/architecture.md to
match disk, or restore the missing path.

Parser notes:
- The tree is rendered with box-drawing characters (├ └ │). Indent is
  counted in 4-character steps from the start of the line.
- Lines whose name is `...` (e.g. `└── ...`) are skipped as intentional
  truncation markers.
- The root marker line `qiita/` (no box-drawing char) is implicit and
  not checked — it's the repo root.
- Inline `# comment` annotations after the name are stripped.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
ARCH_DOC = REPO_ROOT / "docs" / "architecture.md"

# A tree line looks like: `<prefix>├── <name>[ # comment]` or `<prefix>└── <name>...`.
# Prefix is composed of `│   ` or `    ` segments (4 chars each).
_TREE_LINE_RE = re.compile(r"^(.*?)([├└])── (.+?)\s*(?:#.*)?$")

# The tree code block: find a fenced ``` block that starts with `qiita/`.
_TREE_BLOCK_RE = re.compile(r"```\n(qiita/\n.*?)\n```", re.DOTALL)


def _parse_tree(text: str) -> list[str]:
    """Parse the tree text into a flat list of repo-relative paths."""
    paths: list[str] = []
    stack: list[str] = []
    for line in text.splitlines():
        m = _TREE_LINE_RE.match(line)
        if not m:
            continue
        prefix, _connector, name = m.group(1), m.group(2), m.group(3).strip()
        name = name.rstrip("/")
        if name in ("...", ""):
            continue
        depth = len(prefix) // 4
        stack = stack[:depth]
        stack.append(name)
        paths.append("/".join(stack))
    return paths


def test_architecture_tree_paths_exist() -> None:
    text = ARCH_DOC.read_text()
    m = _TREE_BLOCK_RE.search(text)
    assert m, "no fenced tree block starting with 'qiita/' found in docs/architecture.md"

    paths = _parse_tree(m.group(1))
    assert paths, "tree parsed to zero paths — parser is probably broken"

    missing = sorted(p for p in paths if not (REPO_ROOT / p).exists())
    assert not missing, (
        "docs/architecture.md tree references paths that don't exist:\n  "
        + "\n  ".join(missing)
        + "\n\nFix: remove the stale entries from the tree, or restore "
        "the missing files/directories."
    )
