"""qiita-admin CLI — pair decisions between two definitions of one kind (calls the
/admin/definition-pair routes).

Thin client, same shape as `alignment`: argparse checks only what it can, and the
server owns every refusal (cross-scope pairs, contradictions, a downstream decision
relying on the pair). Gated server-side on the kind's lifecycle scope (system_admin).

Renders the full JSON to stdout and a one-line summary to stderr.
"""

import argparse
import json
import sys

from qiita_common.api_paths import (
    PATH_ADMIN_DEFINITION_PAIR,
    PATH_ADMIN_DEFINITION_PAIR_BY_IDX,
    PATH_ADMIN_DEFINITION_PAIR_DECISION,
    PATH_ADMIN_DEFINITION_PAIR_REVOKE,
    PATH_ADMIN_PREFIX,
)
from qiita_common.models import DefinitionKind, DefinitionPairVerdict

from .. import _common

_DECIDABLE_VERDICTS = [
    DefinitionPairVerdict.SIMILAR.value,
    DefinitionPairVerdict.NOT_SIMILAR.value,
]


def _path(template: str, **fields: int) -> str:
    return f"{PATH_ADMIN_PREFIX}{template}".format(**fields)


def _pair_line(body: dict) -> str:
    line = (
        f"pair {body.get('definition_pair_idx')} ({body.get('definition_kind')}"
        f" {body.get('low_definition_idx')} ~ {body.get('high_definition_idx')}):"
        f" {body.get('verdict')}"
    )
    if body.get("detached_at"):
        line += " [detached]"
    if body.get("derived_only"):
        line += " [derived_only]"
    return line


def _render_pair(body: dict | list) -> None:
    print(json.dumps(body, indent=2))
    if isinstance(body, dict):
        print(_pair_line(body), file=sys.stderr)


def _render_list(body: dict | list) -> None:
    print(json.dumps(body, indent=2))
    if not isinstance(body, dict):
        return
    for pair in body.get("pairs", []):
        print(_pair_line(pair), file=sys.stderr)
    tail = f"{body.get('count')} pair(s)"
    if body.get("truncated"):
        tail += " (truncated; narrow with --verdict or --kind)"
    print(tail, file=sys.stderr)


def _handle_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    params = {}
    if args.verdict is not None:
        params["verdict"] = args.verdict
    if args.kind is not None:
        params["definition_kind"] = args.kind
    return _common.run_http_subcommand(
        lambda token: _common.call(
            "GET", args.base_url, token, _path(PATH_ADMIN_DEFINITION_PAIR), params=params or None
        ),
        render=_render_list,
    )


def _handle_show(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda token: _common.call(
            "GET",
            args.base_url,
            token,
            _path(PATH_ADMIN_DEFINITION_PAIR_BY_IDX, definition_pair_idx=args.definition_pair_idx),
        ),
        render=_render_pair,
    )


def _handle_open(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda token: _common.call(
            "POST",
            args.base_url,
            token,
            _path(PATH_ADMIN_DEFINITION_PAIR),
            json={"definition_kind": args.kind, "definition_idx": args.definition_idx},
        ),
        render=_render_pair,
    )


def _handle_decide(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda token: _common.call(
            "POST",
            args.base_url,
            token,
            _path(
                PATH_ADMIN_DEFINITION_PAIR_DECISION, definition_pair_idx=args.definition_pair_idx
            ),
            json={"verdict": args.verdict, "reason": args.reason},
        ),
        render=_render_pair,
    )


def _handle_revoke(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda token: _common.call(
            "POST",
            args.base_url,
            token,
            _path(PATH_ADMIN_DEFINITION_PAIR_REVOKE, definition_pair_idx=args.definition_pair_idx),
            json={"reason": args.reason},
        ),
        render=_render_pair,
    )


def add_pair_parser(subparsers) -> None:
    """Wire `qiita-admin pair {list,show,open,decide,revoke}`."""
    p = subparsers.add_parser(
        "pair", help="pair decisions between two masks, alignments or processings (system_admin)"
    )
    sub = p.add_subparsers(dest="pair_cmd", required=True)
    kinds = [k.value for k in DefinitionKind]

    sp = sub.add_parser("list", help="list pairs, oldest first")
    sp.add_argument("--verdict", choices=[v.value for v in DefinitionPairVerdict], default=None)
    sp.add_argument("--kind", choices=kinds, default=None)
    sp.set_defaults(handler=_handle_list)

    sp = sub.add_parser("show", help="show one pair and its history")
    sp.add_argument("definition_pair_idx", type=int)
    sp.set_defaults(handler=_handle_show)

    sp = sub.add_parser(
        "open", help="open a pending pair of two definitions of one kind (in either order)"
    )
    sp.add_argument("kind", choices=kinds)
    sp.add_argument("definition_idx", type=int, nargs=2)
    sp.set_defaults(handler=_handle_open)

    sp = sub.add_parser("decide", help="decide a pair similar or not_similar")
    sp.add_argument("definition_pair_idx", type=int)
    sp.add_argument("--verdict", required=True, choices=_DECIDABLE_VERDICTS)
    sp.add_argument("--reason", required=True, help="why; recorded with the decision")
    sp.set_defaults(handler=_handle_decide)

    sp = sub.add_parser("revoke", help="return a decided pair to pending")
    sp.add_argument("definition_pair_idx", type=int)
    sp.add_argument("--reason", required=True, help="why; recorded in the pair's history")
    sp.set_defaults(handler=_handle_revoke)
