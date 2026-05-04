"""Walk a workflows directory and parse each YAML with a top-level
`action_id` key into an ActionDefinition.

Every YAML found is loaded, but only those with an `action_id` top-level
key are treated as B7 action definitions. Other YAML files in the tree
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


def load_actions(workflows_dir: Path) -> list[ActionDefinition]:
    """Load every B7 action YAML under `workflows_dir`.

    Returns the list sorted deterministically by (action_id, version) so the
    upsert order is stable across runs (helps with integration-test diffs
    and audit log readability). Raises ValidationError on any malformed
    action YAML and DuplicateActionError on a (action_id, version) collision.
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

    return [action for _key, (_path, action) in sorted(by_key.items())]
