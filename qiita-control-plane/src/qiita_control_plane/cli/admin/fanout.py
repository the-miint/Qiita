"""qiita-admin CLI — fan-out throttle control (calls the CP fan-out routes).

Thin client, same shape as `ticket_cancel`: it validates only what argparse can
(kind, the cap's range) and lets the server own everything else — which cohorts
exist, whether one is fail-stopped, how many slots are free. Gated server-side on
the `work_ticket:cancel` scope (system_admin).

Renders the full JSON to stdout and a human summary to stderr. The summary always
states WHY a pump released nothing, because a bare `released 0` is exactly what
made a frozen alignment fan-out take an afternoon to diagnose.
"""

import argparse
import json
import sys

from qiita_common.api_paths import (
    PATH_WORK_TICKET_FANOUT,
    PATH_WORK_TICKET_FANOUT_COHORT,
    PATH_WORK_TICKET_FANOUT_COHORT_PUMP,
    PATH_WORK_TICKET_PREFIX,
)
from qiita_common.models import MAX_FANOUT_OVERRIDE, FanoutCohortKind

from .. import _common

_LIST_PATH = f"{PATH_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_FANOUT}"


def _cohort_path(kind: str, key: int) -> str:
    return f"{PATH_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_FANOUT_COHORT}".format(kind=kind, key=key)


def _pump_path(kind: str, key: int) -> str:
    return f"{PATH_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_FANOUT_COHORT_PUMP}".format(
        kind=kind, key=key
    )


def _summarise(status: dict) -> str:
    """One line per cohort: identity, counts, effective cap, and the frozen flag."""
    cap = f"cap {status.get('max_inflight')}"
    if status.get("override") is not None:
        cap += " (override)"
    line = (
        f"  {status.get('kind')}/{status.get('key')}: "
        f"{status.get('held')} held, {status.get('running')} running, "
        f"{status.get('failed')} failed of {status.get('total')} — {cap}"
    )
    if status.get("fail_stopped"):
        line += "  [FAIL-STOPPED: redrive the failed child(ren), nothing will release]"
    return line


def _render_list(body: dict | list) -> None:
    print(json.dumps(body, indent=2))
    if not isinstance(body, dict):
        return
    cohorts = body.get("cohorts", [])
    if not cohorts:
        print("no active fan-out cohorts.", file=sys.stderr)
        return
    print(f"{len(cohorts)} active fan-out cohort(s):", file=sys.stderr)
    for status in cohorts:
        print(_summarise(status), file=sys.stderr)


def _render_pump(body: dict | list) -> None:
    print(json.dumps(body, indent=2))
    if not isinstance(body, dict):
        return
    released = body.get("released") or []
    status = body.get("status") or {}
    print(f"released {len(released)} ticket(s): {released}", file=sys.stderr)
    print(_summarise(status), file=sys.stderr)
    if not released and not status.get("fail_stopped"):
        # The other reason for a zero, spelled out so it isn't read as a failure.
        print(
            "  (nothing released: no free slots, or nothing held)",
            file=sys.stderr,
        )


def _handle_fanout_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda token: _common.call("GET", args.base_url, token, _LIST_PATH),
        render=_render_list,
    )


def _handle_fanout_set(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    # Exactly-one of --max-inflight/--clear is enforced by the parser's required
    # mutually-exclusive group. Explicit null clears it; the route requires the
    # field, so an omitted key is not the same thing as a null.
    body = {"max_inflight": None if args.clear else args.max_inflight}
    return _common.run_http_subcommand(
        lambda token: _common.call(
            "PATCH", args.base_url, token, _cohort_path(args.kind, args.key), json=body
        ),
        render=_render_pump,
    )


def _handle_fanout_pump(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda token: _common.call("POST", args.base_url, token, _pump_path(args.kind, args.key)),
        render=_render_pump,
    )


def _cap(raw: str) -> int:
    """Bound the cap client-side too, so a typo costs no round trip. The ceiling is
    the server's constant, not a second copy of the number."""
    value = int(raw)
    if not 1 <= value <= MAX_FANOUT_OVERRIDE:
        raise argparse.ArgumentTypeError(f"must be between 1 and {MAX_FANOUT_OVERRIDE}")
    return value


def add_fanout_parser(subparsers) -> None:
    """Wire `qiita-admin fanout {list,set,pump}`."""
    p = subparsers.add_parser(
        "fanout",
        help="inspect and retune the fan-out dispatch throttle",
    )
    sub = p.add_subparsers(dest="fanout_cmd", required=True)

    p_list = sub.add_parser("list", help="every cohort with held or in-flight children")
    p_list.set_defaults(handler=_handle_fanout_list)

    kinds = [k.value for k in FanoutCohortKind]
    for name, handler, help_text in (
        ("set", _handle_fanout_set, "set or clear a cohort's in-flight cap, then pump it"),
        ("pump", _handle_fanout_pump, "re-trigger a cohort's pump without changing its cap"),
    ):
        sp = sub.add_parser(name, help=help_text)
        sp.add_argument("kind", choices=kinds, help="cohort kind")
        sp.add_argument("key", type=int, help="reference_idx / mask_idx / alignment_idx")
        if name == "set":
            what = sp.add_mutually_exclusive_group(required=True)
            what.add_argument(
                "--max-inflight",
                type=_cap,
                default=None,
                help=f"new cap for this cohort (1..{MAX_FANOUT_OVERRIDE})",
            )
            what.add_argument(
                "--clear", action="store_true", help="drop the override, reverting to the default"
            )
        sp.set_defaults(handler=handler)
