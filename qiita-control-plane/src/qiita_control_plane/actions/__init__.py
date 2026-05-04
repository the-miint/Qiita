"""Action registry and primitive library.

Two halves:

- `loader` + `sync`: walk the workflows directory, validate each
  ActionDefinition YAML, and upsert YAML-authoritative columns into
  qiita.action (DB-authoritative columns like `enabled` / `disabled_*`
  are preserved across syncs).

- `library`: named callable primitives (`mint-features`,
  `write-membership`, `register-files`) composed by workflow `action:`
  entries. Both this library and the corresponding REST routes call the
  same underlying functions, so HTTP and workflow-runner invocations
  share one implementation.

Imported by the qiita-admin CLI's `actions sync` subcommand and by any
future control-plane bootstrap path that needs to refresh the registry
without operator intervention (e.g. on integration-test setup), and by
the route handlers in qiita_control_plane.routes.{feature,reference}.
"""

from qiita_control_plane.actions.library import LIBRARY
from qiita_control_plane.actions.loader import (
    DuplicateActionError,
    load_actions,
)
from qiita_control_plane.actions.sync import sync_actions

__all__ = ["LIBRARY", "DuplicateActionError", "load_actions", "sync_actions"]
