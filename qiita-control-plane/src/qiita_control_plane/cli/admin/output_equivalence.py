"""qiita-admin CLI — output equivalences between two definitions of one kind (calls the
/admin/output-equivalence routes).

Thin client, same shape as `alignment`: argparse checks only what it can, and the
server owns every refusal (cross-scope output equivalences, contradictions, a downstream
decision relying on the output equivalence). Gated server-side on the kind's lifecycle
scope (system_admin).

Renders the full JSON to stdout and a one-line summary to stderr.
"""

import argparse
import json
import sys

from qiita_common.api_paths import (
    PATH_ADMIN_OUTPUT_EQUIVALENCE,
    PATH_ADMIN_OUTPUT_EQUIVALENCE_BY_IDX,
    PATH_ADMIN_OUTPUT_EQUIVALENCE_DECISION,
    PATH_ADMIN_OUTPUT_EQUIVALENCE_REVOKE,
    PATH_ADMIN_PREFIX,
)
from qiita_common.models import (
    JOINING_VERDICTS,
    NON_JOINING_VERDICTS,
    DefinitionKind,
    OutputEquivalenceVerdict,
)

from .. import _common

_DECIDABLE_VERDICTS = sorted(v.value for v in JOINING_VERDICTS | NON_JOINING_VERDICTS)


def _path(template: str, **fields: int) -> str:
    return f"{PATH_ADMIN_PREFIX}{template}".format(**fields)


def _equivalence_line(body: dict) -> str:
    line = (
        f"output equivalence {body.get('output_equivalence_idx')} ({body.get('definition_kind')}"
        f" {body.get('low_definition_idx')} ~ {body.get('high_definition_idx')}):"
        f" {body.get('verdict')}"
    )
    if body.get("detached_at"):
        line += " [detached]"
    if body.get("derived_only"):
        line += " [derived_only]"
    return line


def _render_equivalence(body: dict | list) -> None:
    print(json.dumps(body, indent=2))
    if isinstance(body, dict):
        print(_equivalence_line(body), file=sys.stderr)


def _render_list(body: dict | list) -> None:
    print(json.dumps(body, indent=2))
    if not isinstance(body, dict):
        return
    for equivalence in body.get("output_equivalences", []):
        print(_equivalence_line(equivalence), file=sys.stderr)
    tail = f"{body.get('count')} output equivalence(s)"
    cursor = body.get("next_after_output_equivalence_idx")
    if cursor is not None:
        tail += f" (more: --after {cursor}, or --all)"
    print(tail, file=sys.stderr)


def _list_equivalences(base_url: str, token: str, args: argparse.Namespace) -> dict:
    """GET /admin/output-equivalence, following `next_after_output_equivalence_idx` across
    pages with --all. One page otherwise, cursor included."""
    params: dict = {"limit": args.limit}
    if args.verdict is not None:
        params["verdict"] = args.verdict
    if args.kind is not None:
        params["definition_kind"] = args.kind
    if args.after is not None:
        params["after_output_equivalence_idx"] = args.after
    equivalences: list = []
    while True:
        page = _common.call(
            "GET", base_url, token, _path(PATH_ADMIN_OUTPUT_EQUIVALENCE), params=params
        )
        equivalences.extend(page["output_equivalences"])
        cursor = page["next_after_output_equivalence_idx"]
        if not args.all_pages or cursor is None:
            return {
                "output_equivalences": equivalences,
                "count": len(equivalences),
                "next_after_output_equivalence_idx": cursor,
            }
        params["after_output_equivalence_idx"] = cursor


def _handle_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda token: _list_equivalences(args.base_url, token, args), render=_render_list
    )


def _handle_show(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda token: _common.call(
            "GET",
            args.base_url,
            token,
            _path(
                PATH_ADMIN_OUTPUT_EQUIVALENCE_BY_IDX,
                output_equivalence_idx=args.output_equivalence_idx,
            ),
        ),
        render=_render_equivalence,
    )


def _handle_open(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda token: _common.call(
            "POST",
            args.base_url,
            token,
            _path(PATH_ADMIN_OUTPUT_EQUIVALENCE),
            json={"definition_kind": args.kind, "definition_idx": args.definition_idx},
        ),
        render=_render_equivalence,
    )


def _handle_decide(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda token: _common.call(
            "POST",
            args.base_url,
            token,
            _path(
                PATH_ADMIN_OUTPUT_EQUIVALENCE_DECISION,
                output_equivalence_idx=args.output_equivalence_idx,
            ),
            json={"verdict": args.verdict, "reason": args.reason},
        ),
        render=_render_equivalence,
    )


def _handle_revoke(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda token: _common.call(
            "POST",
            args.base_url,
            token,
            _path(
                PATH_ADMIN_OUTPUT_EQUIVALENCE_REVOKE,
                output_equivalence_idx=args.output_equivalence_idx,
            ),
            json={"reason": args.reason},
        ),
        render=_render_equivalence,
    )


def add_output_equivalence_parser(subparsers) -> None:
    """Wire `qiita-admin output-equivalence {list,show,open,decide,revoke}`."""
    p = subparsers.add_parser(
        "output-equivalence",
        help="verdicts on whether two masks, alignments or processings give equivalent"
        " outputs (system_admin)",
    )
    sub = p.add_subparsers(dest="output_equivalence_cmd", required=True)
    kinds = [k.value for k in DefinitionKind]

    sp = sub.add_parser("list", help="list output equivalences, newest first, one page at a time")
    sp.add_argument("--verdict", choices=[v.value for v in OutputEquivalenceVerdict], default=None)
    sp.add_argument("--kind", choices=kinds, default=None)
    sp.add_argument("--limit", type=int, default=100, help="page size (max 1000)")
    sp.add_argument(
        "--after",
        type=int,
        default=None,
        help="list the next page: the output equivalences older than this one (a page's"
        " next_after_output_equivalence_idx)",
    )
    sp.add_argument("--all", dest="all_pages", action="store_true", help="fetch every page")
    sp.set_defaults(handler=_handle_list)

    sp = sub.add_parser("show", help="show one output equivalence and its history")
    sp.add_argument("output_equivalence_idx", type=int)
    sp.set_defaults(handler=_handle_show)

    sp = sub.add_parser(
        "open",
        help="open a pending output equivalence of two definitions of one kind (in either order)",
    )
    sp.add_argument("kind", choices=kinds)
    sp.add_argument("definition_idx", type=int, nargs=2)
    sp.set_defaults(handler=_handle_open)

    sp = sub.add_parser(
        "decide",
        help="decide an output equivalence: same or similar join the group, different or"
        " not_benchmarked keep the two apart",
    )
    sp.add_argument("output_equivalence_idx", type=int)
    sp.add_argument("--verdict", required=True, choices=_DECIDABLE_VERDICTS)
    sp.add_argument("--reason", required=True, help="why; recorded with the decision")
    sp.set_defaults(handler=_handle_decide)

    sp = sub.add_parser("revoke", help="return a decided output equivalence to pending")
    sp.add_argument("output_equivalence_idx", type=int)
    sp.add_argument(
        "--reason", required=True, help="why; recorded in the output equivalence's history"
    )
    sp.set_defaults(handler=_handle_revoke)
