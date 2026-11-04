"""qiita-admin CLI — alignment lifecycle (calls the /alignment-definition routes).

Thin client, same shape as `fanout`: argparse checks only what it can, and the
server owns every refusal (unfinished tickets, a running block, an unknown
alignment). Gated server-side on the `alignment_definition:lifecycle` scope
(system_admin).

Renders the full JSON to stdout and a one-line summary to stderr.
"""

import argparse
import json
import sys

from qiita_common.api_paths import (
    PATH_ALIGNMENT_DEFINITION_ABANDON_PLAN,
    PATH_ALIGNMENT_DEFINITION_PREFIX,
    PATH_ALIGNMENT_DEFINITION_SAMPLE_STATUS,
    PATH_ALIGNMENT_DEFINITION_STATUS,
)
from qiita_common.models import AlignmentDefinitionStatus

from .. import _common


def _path(template: str, alignment_idx: int) -> str:
    return f"{PATH_ALIGNMENT_DEFINITION_PREFIX}{template}".format(alignment_idx=alignment_idx)


def _render_status(body: dict | list) -> None:
    print(json.dumps(body, indent=2))
    if isinstance(body, dict):
        line = f"alignment {body.get('alignment_idx')}: {body.get('status')}"
        if body.get("deprecation_reason"):
            line += f" ({body['deprecation_reason']})"
        print(line, file=sys.stderr)


def _render_sample_status(body: dict | list) -> None:
    print(json.dumps(body, indent=2))
    if not isinstance(body, dict):
        return
    print(
        f"alignment {body.get('alignment_idx')} -> {body.get('state')}:"
        f" {len(body.get('updated', []))} updated,"
        f" {len(body.get('unchanged', []))} unchanged",
        file=sys.stderr,
    )
    # The two buckets that mean the request did not do what it named.
    for key, meaning in (
        ("not_found", "no gate under this alignment"),
        ("skipped_pending", "still pending; abandon-plan clears these instead"),
    ):
        if body.get(key):
            print(f"  {key} ({meaning}): {body[key]}", file=sys.stderr)


def _render_abandon(body: dict | list) -> None:
    print(json.dumps(body, indent=2))
    if not isinstance(body, dict):
        return
    print(
        f"alignment {body.get('alignment_idx')}:"
        f" {len(body.get('abandoned_block_idx', []))} block(s) abandoned,"
        f" {len(body.get('cancelled_work_ticket_idx', []))} held ticket(s) cancelled,"
        f" {len(body.get('cleared_prep_sample_idx', []))} prep_sample(s) cleared,"
        f" {body.get('rows_deleted')} row(s) purged",
        file=sys.stderr,
    )
    if body.get("cleared_prep_sample_idx"):
        print("  re-plan them with only_missing=true", file=sys.stderr)


def _patch_status(args: argparse.Namespace, body: dict) -> int:
    return _common.run_http_subcommand(
        lambda token: _common.call(
            "PATCH",
            args.base_url,
            token,
            _path(PATH_ALIGNMENT_DEFINITION_STATUS, args.alignment_idx),
            json=body,
        ),
        render=_render_status,
    )


def _handle_deprecate(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    body = {"status": AlignmentDefinitionStatus.DEPRECATED.value, "reason": args.reason}
    if args.superseded_by is not None:
        body["superseded_by"] = args.superseded_by
    return _patch_status(args, body)


def _handle_activate(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _patch_status(args, {"status": AlignmentDefinitionStatus.ACTIVE.value})


def _patch_sample_status(args: argparse.Namespace, body: dict) -> int:
    return _common.run_http_subcommand(
        lambda token: _common.call(
            "PATCH",
            args.base_url,
            token,
            _path(PATH_ALIGNMENT_DEFINITION_SAMPLE_STATUS, args.alignment_idx),
            json={"prep_sample_idx": args.prep_sample_idx, **body},
        ),
        render=_render_sample_status,
    )


def _handle_invalidate(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _patch_sample_status(args, {"state": "invalidated", "reason": args.reason})


def _handle_restore(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _patch_sample_status(args, {"state": "completed"})


def _handle_abandon_plan(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda token: _common.call(
            "POST",
            args.base_url,
            token,
            _path(PATH_ALIGNMENT_DEFINITION_ABANDON_PLAN, args.alignment_idx),
        ),
        render=_render_abandon,
    )


def add_alignment_parser(subparsers) -> None:
    """Wire `qiita-admin alignment {deprecate,activate,invalidate,restore,abandon-plan}`."""
    p = subparsers.add_parser("alignment", help="alignment lifecycle (system_admin)")
    sub = p.add_subparsers(dest="alignment_cmd", required=True)

    def _with_idx(name: str, help_text: str, handler) -> argparse.ArgumentParser:
        sp = sub.add_parser(name, help=help_text)
        sp.add_argument("alignment_idx", type=int)
        sp.set_defaults(handler=handler)
        return sp

    sp = _with_idx(
        "deprecate",
        "void an alignment config; refused while any of its work tickets is unfinished",
        _handle_deprecate,
    )
    sp.add_argument("--reason", required=True, help="why the config is void")
    sp.add_argument(
        "--superseded-by", type=int, default=None, help="alignment_idx that replaces it"
    )
    _with_idx("activate", "return a deprecated alignment to active", _handle_activate)

    sp = _with_idx("invalidate", "withdraw completed prep_sample runs", _handle_invalidate)
    sp.add_argument("--prep-sample-idx", type=int, nargs="+", required=True)
    sp.add_argument("--reason", required=True, help="why the runs are withdrawn")
    sp = _with_idx("restore", "restore invalidated prep_sample runs", _handle_restore)
    sp.add_argument("--prep-sample-idx", type=int, nargs="+", required=True)

    _with_idx(
        "abandon-plan",
        "give up on the plan's unfinished blocks so their prep_samples can be re-planned",
        _handle_abandon_plan,
    )
