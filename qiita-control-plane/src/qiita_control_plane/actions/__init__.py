"""Action-registry sync — YAML-source-of-truth, DB-operational-state.

`loader` walks the workflows directory and parses every YAML with a top-level
`action_id` key into an `ActionDefinition`. `sync` upserts those into the
qiita.action table, only writing YAML-authoritative columns; DB-authoritative
columns (enabled, first_seen_at, disabled_*, updated_at) are never touched.

Both are imported by the qiita-admin CLI's `actions sync` subcommand and by
any future control-plane bootstrap path that needs to refresh the registry
without operator intervention (e.g. on integration-test setup).
"""

from qiita_control_plane.actions.loader import (
    DuplicateActionError,
    load_actions,
)
from qiita_control_plane.actions.sync import sync_actions

__all__ = ["DuplicateActionError", "load_actions", "sync_actions"]
