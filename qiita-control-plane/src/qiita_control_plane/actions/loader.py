"""Walk a workflows directory and parse each YAML with a top-level
`action_id` key into an ActionDefinition.

Every YAML found is loaded, but only those with an `action_id` top-level
key are treated as action definitions. Other YAML files in the tree
(container build manifests, smoke-test workflows, scaffolding) are
silently skipped — the loader does not attempt to validate them as
action definitions. Operators can therefore put unrelated YAML alongside
action YAMLs without surprising the sync pass.

A duplicate `(action_id, version)` across two files is a hard error: that's
either a copy-paste bug or two operators racing edits, both of which should
fail the deploy and prompt human review.
"""

from pathlib import Path

import yaml
from qiita_common.actions import ActionDefinition


class DuplicateActionError(ValueError):
    """Two YAML files declare the same (action_id, version)."""


def _version_sort_key(version: str) -> tuple[tuple[tuple[int, int, str], ...], str]:
    """Order a version string by its dotted components, numbers as numbers.

    `sync_actions` walks this list in order, re-enabling each version it syncs and
    auto-deprecating every OTHER version of that action_id, so the last one wins and
    the comparison here decides which version of an action_id a deploy leaves
    submittable — not just what order rows are written in.

    A plain string compare puts `"1.10.0"` before `"1.9.0"`, which would leave the
    newer version disabled and refuse every submission against it while the catalog
    still listed both. So each dotted component compares as a number when it is one.

    `ActionDefinition.version` is a free-form `str` (`min_length`/`max_length` only,
    no format check), so this stays total rather than raising on a component it
    cannot parse. A non-numeric component sorts BEFORE every numeric one in the same
    position, which puts the unparseable version where being wrong is cheapest: last
    is what stays enabled, so a stray `"latest"` or a mistyped version gets disabled
    rather than winning the deploy and disabling the real release. It also matches
    semver for the shape that matters here, where a prerelease precedes its release:
    `"1.0.0-rc1"` < `"1.0.0"` < `"1.0.1"` < `"1.9.0"` < `"1.10.0"`. It is NOT a semver
    comparator in general — a non-numeric component sorts ahead of every numeric one
    in its position, so `"1.0.10-rc1"` < `"1.0.9"`, which semver reverses. The test
    below refuses any such version outright, so the divergence has no live case.

    Nothing under `workflows/` uses a non-numeric component today, and
    `test_the_version_sync_leaves_enabled_is_the_highest_of_each_action` refuses one
    outright — a pure-unit test, so such a version fails `make test` long before a
    deploy. That guard is repo-scoped rather than function-scoped (`load_actions`
    takes any directory), which is why the ordering here is defined for it anyway.

    The trailing raw string keeps the order STRICT: `"1.0.0"` and `"01.0.0"` produce
    identical component tuples, and two entries comparing equal would leave which
    one survives the sync resting on sort stability.
    """
    return (
        tuple(
            (1, int(part), "") if part.isdecimal() else (0, 0, part) for part in version.split(".")
        ),
        version,
    )


def load_actions(workflows_dir: Path) -> list[ActionDefinition]:
    """Load every action YAML under `workflows_dir`.

    Returns the list sorted by (action_id, version), the version ordered
    NUMERICALLY per `_version_sort_key`. Deterministic, so the upsert order is
    stable across runs (helps with integration-test diffs and audit log
    readability), and newest-last, which is what `sync_actions` reconciles
    `enabled` against. Raises ValidationError on any malformed action YAML and
    DuplicateActionError on a (action_id, version) collision.
    """
    if not workflows_dir.is_dir():
        raise FileNotFoundError(f"workflows directory not found: {workflows_dir}")

    by_key: dict[tuple[str, str], tuple[Path, ActionDefinition]] = {}
    # rglob result order is filesystem-dependent — sort for determinism so
    # the duplicate-detection error message points to a stable "first seen"
    # path across runs.
    for path in sorted(workflows_dir.rglob("*.yaml")):
        with path.open("r") as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict) or "action_id" not in data:
            continue
        action = ActionDefinition.model_validate(data)
        key = (action.action_id, action.version)
        if key in by_key:
            existing_path, _ = by_key[key]
            raise DuplicateActionError(
                f"duplicate action ({action.action_id}, {action.version}) "
                f"declared in both {existing_path} and {path}"
            )
        by_key[key] = (path, action)

    return [
        action
        for _key, (_path, action) in sorted(
            by_key.items(), key=lambda item: (item[0][0], _version_sort_key(item[0][1]))
        )
    ]
