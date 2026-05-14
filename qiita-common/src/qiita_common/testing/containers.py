"""Stable container-image test constants.

These mirror the container values declared in
`workflows/reference-add/1.0.0.yaml`. Tests across all three Python
packages reference them by name so a rename of the canonical container
(or a version bump) only needs to update this file.

Production code never reads these — the actual container values flow
in from the YAML at sync time and live on `qiita.action.steps`. These
constants exist only so test fixtures and assertions can declare a
known runtime without inlining a magic string in every site.
"""

from __future__ import annotations

REFERENCE_HASH_CONTAINER: str = "qiita/reference-hash:1.0.0"
REFERENCE_LOAD_CONTAINER: str = "qiita/reference-load:1.0.0"
