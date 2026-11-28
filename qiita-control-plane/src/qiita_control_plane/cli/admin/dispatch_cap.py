"""qiita-admin CLI — the control plane's process-wide dispatch cap.

Thin client over `GET`/`PATCH /work-ticket/dispatch-cap`, same shape as `fanout`: it
checks the value against the allowed set so a typo costs no round trip, and lets
the server own the rest (the ceiling, `max_settable`, and the counts). Gated server-side
on the `work_ticket:cancel` scope (system_admin).

Renders the full JSON to stdout and a one-line summary to stderr.
"""

import argparse
import json
import sys

from qiita_common.api_paths import PATH_WORK_TICKET_DISPATCH_CAP, PATH_WORK_TICKET_PREFIX
from qiita_common.models import DISPATCH_MAX_INFLIGHT_CHOICES

from .. import _common

_PATH = f"{PATH_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_DISPATCH_CAP}"


def _summarise(status: dict) -> str:
    cap = f"cap {status.get('max_inflight')}"
    if status.get("override") is not None:
        cap += f" (override; DISPATCH_MAX_INFLIGHT={status.get('default')} after a restart)"
    else:
        cap += " (DISPATCH_MAX_INFLIGHT)"
    return (
        f"dispatch {cap}: {status.get('running')} running, {status.get('waiting')} waiting;"
        f" settable up to {status.get('max_settable')} without a restart"
    )


def _render(body: dict | list) -> None:
    print(json.dumps(body, indent=2))
    if isinstance(body, dict):
        print(_summarise(body), file=sys.stderr)


def _handle_dispatch_cap(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    if args.set is None and not args.reset:
        return _common.run_http_subcommand(
            lambda token: _common.call("GET", args.base_url, token, _PATH), render=_render
        )
    # The route requires the field, so clearing is an explicit null.
    body = {"max_inflight": None if args.reset else args.set}
    return _common.run_http_subcommand(
        lambda token: _common.call("PATCH", args.base_url, token, _PATH, json=body),
        render=_render,
    )


def add_dispatch_cap_parser(subparsers) -> None:
    """Wire `qiita-admin dispatch-cap [--set N | --reset]`."""
    p = subparsers.add_parser(
        "dispatch-cap",
        help="show, set or reset the control plane's process-wide dispatch cap",
    )
    what = p.add_mutually_exclusive_group()
    what.add_argument(
        "--set",
        type=int,
        choices=DISPATCH_MAX_INFLIGHT_CHOICES,
        default=None,
        metavar="N",
        help=(
            f"set the runtime override, one of {list(DISPATCH_MAX_INFLIGHT_CHOICES)};"
            " lost on a control-plane restart"
        ),
    )
    what.add_argument(
        "--reset",
        action="store_true",
        help="drop the override, reverting to DISPATCH_MAX_INFLIGHT",
    )
    p.set_defaults(handler=_handle_dispatch_cap)
