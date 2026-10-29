"""qiita — end-user CLI for the Qiita control plane.

Scope: credentialed HTTP calls against a running deployment.

Placement rule (qiita vs qiita-admin) — the deciding test is how a
command reaches the system and whether the auth model can gate it:

  qiita        — credentialed API calls over HTTP+PAT. The server's
                 role/scope guards decide what's allowed, so the binary
                 is NOT the security boundary; the server is. A command
                 only a system_admin can use still belongs here if it's a
                 normal authenticated API call (the server 403s everyone
                 else).
  qiita-admin  — operator-on-the-host actions that run *outside* the
                 API/auth model: direct Postgres writes (gated by
                 DATABASE_URL) or host/cluster operations. They exist for
                 moments the auth system can't help — no admin exists yet,
                 the API is down, or you're recovering state.

The entry point is `main`. `_parser.py` builds the user-facing argparse
surface, and each subcommand's handler lives in the submodule for its
area. PAT file I/O, the LoginRocket loopback flow, the
authenticated HTTP call helper, and the generic token-read + invoke +
JSON-print runner live in `cli._common`.

Authentication: HTTP subcommands read the PAT from QIITA_TOKEN env or
from ~/.qiita/token (mode 0600).
"""

from .. import _common
from ._helpers import _build_body, _lane_arg
from ._parser import _build_parser
from .auth import _patch_user_me
from .biosample import _post_biosample
from .pacbio import (
    _PacbioPreflightRow,
    _read_pacbio_preflight_rows,
    _resolve_sample_bams,
)
from .pool import (
    _BCL_CONVERT_ACTION_ID,
    _BCL_CONVERT_ACTION_VERSION,
    _READ_MASK_ACTION_VERSION,
    _assert_host_reference_ready,
    _assert_resolved_references_ready,
    _PreflightRow,
    _read_preflight_rows,
    _resolved_decisions,
)
from .reference import _run_reference_load, _serializable
from .sequencing import (
    _post_preflight_update_lane,
    _post_sequenced_pool,
    _post_sequenced_sample,
    _post_sequencing_run,
)
from .study import _post_study
from .ticket import (
    _get_work_ticket,
    _get_work_ticket_step_logs,
    _list_work_tickets,
    _post_work_ticket,
    _render_ticket_logs,
    _run_work_ticket,
)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _common.validate_base_url(args, parser)
    return args.handler(args, parser)


__all__ = [
    "_BCL_CONVERT_ACTION_ID",
    "_BCL_CONVERT_ACTION_VERSION",
    "_PacbioPreflightRow",
    "_PreflightRow",
    "_READ_MASK_ACTION_VERSION",
    "_assert_host_reference_ready",
    "_assert_resolved_references_ready",
    "_resolved_decisions",
    "_build_body",
    "_get_work_ticket",
    "_get_work_ticket_step_logs",
    "_lane_arg",
    "_list_work_tickets",
    "_patch_user_me",
    "_post_biosample",
    "_post_preflight_update_lane",
    "_post_sequenced_pool",
    "_post_sequenced_sample",
    "_post_sequencing_run",
    "_post_study",
    "_post_work_ticket",
    "_read_pacbio_preflight_rows",
    "_read_preflight_rows",
    "_render_ticket_logs",
    "_resolve_sample_bams",
    "_run_reference_load",
    "_run_work_ticket",
    "_serializable",
    "main",
]
