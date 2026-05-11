"""Assert the curated `makefile fenced block in docs/architecture.md
mirrors the public-API targets in the repo-root Makefile.

Catches recipe-body and dependency-list drift on every target the doc
chooses to show. Allows the Makefile to declare additional internal
targets (e.g. $(DBMATE_BIN), $(GRPCURL_BIN), dev-setup) that the doc
does not embed.

When this test fails, copy the affected recipes from Makefile into the
```makefile block in docs/architecture.md.

Parser limitations (extend if the Makefile starts using these):
- Multi-target lines `a b: deps` are silently dropped (the regex
  requires the name to be followed directly by `:`).
- Double-colon rules `target:: deps` parse but would overwrite any
  single-colon rule of the same name in the dict.
- Pattern rules `%.o: %.c` are correctly skipped (leading `%`).
- Duplicate target names within either file overwrite via dict
  semantics — fine for the curated subset; would mask drift if anyone
  ever adds an intentional override.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
MAKEFILE = REPO_ROOT / "Makefile"
ARCH_DOC = REPO_ROOT / "docs" / "architecture.md"

# Match `<target>: <deps...>` lines. Excludes `.PHONY:`, `$(VAR):`, and
# variable assignment lines like `FOO := bar`. The negative lookahead on
# `=` rules out `name := value` (assignment) without rejecting `name:`
# (target with no deps).
_TARGET_RE = re.compile(r"^([a-zA-Z][a-zA-Z0-9_\-]*)\s*:(?!=).*$", re.MULTILINE)
_DOC_FENCE_RE = re.compile(r"```makefile\n(.*?)```", re.DOTALL)


def _parse_targets(text: str) -> dict[str, str]:
    """Return {target_name: full block} where each block contains the
    `name: deps` header plus all subsequent tab-indented and blank lines,
    trimmed of trailing blank lines."""
    blocks: dict[str, str] = {}
    lines = text.splitlines(keepends=True)
    i = 0
    while i < len(lines):
        m = _TARGET_RE.match(lines[i])
        if not m:
            i += 1
            continue
        name = m.group(1)
        start = i
        i += 1
        while i < len(lines) and (lines[i].startswith("\t") or lines[i].strip() == ""):
            i += 1
        end = i
        while end > start + 1 and lines[end - 1].strip() == "":
            end -= 1
        blocks[name] = "".join(lines[start:end])
    return blocks


def _extract_doc_block(text: str) -> str:
    m = _DOC_FENCE_RE.search(text)
    assert m, "no ```makefile fenced block found in docs/architecture.md"
    return m.group(1)


def test_doc_makefile_block_matches_repo_makefile() -> None:
    makefile_blocks = _parse_targets(MAKEFILE.read_text())
    doc_blocks = _parse_targets(_extract_doc_block(ARCH_DOC.read_text()))

    missing = sorted(name for name in doc_blocks if name not in makefile_blocks)
    assert not missing, (
        "docs/architecture.md references Makefile targets that don't exist "
        f"in Makefile: {missing}. Either remove them from the doc or add "
        "them to the Makefile."
    )

    mismatched = {
        name: (doc_blocks[name], makefile_blocks[name])
        for name in doc_blocks
        if doc_blocks[name] != makefile_blocks[name]
    }
    if mismatched:
        parts = ["docs/architecture.md recipes drifted from Makefile:\n"]
        for name, (doc, mk) in sorted(mismatched.items()):
            parts.append(f"\n--- doc: {name} ---\n{doc}")
            parts.append(f"--- Makefile: {name} ---\n{mk}")
        parts.append(
            "\n\nFix: copy the affected recipes from Makefile into the "
            "```makefile block in docs/architecture.md. The fenced block "
            "is a curated subset of the public-API targets; internal "
            "helpers ($(DBMATE_BIN), $(GRPCURL_BIN), dev-setup) live in "
            "Makefile only."
        )
        raise AssertionError("".join(parts))
