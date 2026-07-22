"""qiita-admin CLI — reference exclusion blocklist curation subcommands.

`reference exclusion add` / `remove` drive the system_admin POST/DELETE
`/reference/exclusion` routes (`reference:exclusion:write`); `reference exclusion
list` reads `GET /reference/{idx}/exclusion` (`reference:read`). Everything goes
through the routes (not a direct DB write) so the scope gate AND the data-plane
re-sync fire exactly as they do for any API caller — the CLI is not the security
boundary (see the qiita vs qiita-admin placement note in this package's __init__).
"""

import argparse
import json
import sys

from qiita_common.api_paths import (
    PATH_REFERENCE_EXCLUSION,
    PATH_REFERENCE_EXCLUSION_BY_IDX,
    PATH_REFERENCE_PREFIX,
)

from .. import _common

# The post-API_PREFIX path segments (`_common.call` prepends API_PREFIX).
_EXCLUSION_PATH = f"{PATH_REFERENCE_PREFIX}{PATH_REFERENCE_EXCLUSION}"


def _add_exclusion_via_route(base_url: str, token: str, *, genome_idx, feature_idx, reason) -> dict:
    """POST /api/v1/reference/exclusion as the PAT. Exactly one of genome_idx /
    feature_idx is set by the caller (argparse's mutually-exclusive required
    group guarantees it)."""
    body: dict = {"reason": reason}
    if genome_idx is not None:
        body["genome_idx"] = genome_idx
    if feature_idx is not None:
        body["feature_idx"] = feature_idx
    return _common.call("POST", base_url, token, _EXCLUSION_PATH, json=body)


def _remove_exclusion_via_route(base_url: str, token: str, *, genome_idx, feature_idx) -> dict:
    """DELETE /api/v1/reference/exclusion?<target> as the PAT."""
    params: dict = {}
    if genome_idx is not None:
        params["genome_idx"] = genome_idx
    if feature_idx is not None:
        params["feature_idx"] = feature_idx
    return _common.call("DELETE", base_url, token, _EXCLUSION_PATH, params=params)


def _list_exclusions_via_route(base_url: str, token: str, reference_idx: int) -> list:
    """GET /api/v1/reference/{reference_idx}/exclusion as the PAT."""
    scoped = PATH_REFERENCE_EXCLUSION_BY_IDX.format(reference_idx=reference_idx)
    path = f"{PATH_REFERENCE_PREFIX}{scoped}"
    return _common.call("GET", base_url, token, path)


def _target_str(body: dict) -> str:
    if body.get("target_kind") == "genome":
        return f"genome_idx={body.get('genome_idx')}"
    return f"feature_idx={body.get('feature_idx')}"


def _handle_exclusion_add(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    def _render(body: dict | list) -> None:
        print(json.dumps(body, indent=2))
        if isinstance(body, dict):
            verb = "blocked" if body.get("changed") else "already blocked (no change)"
            print(
                f"{verb}: {_target_str(body)}; lake mirror now holds"
                f" {body.get('synced_feature_count')} excluded feature(s).",
                file=sys.stderr,
            )

    return _common.run_http_subcommand(
        lambda t: _add_exclusion_via_route(
            args.base_url,
            t,
            genome_idx=args.genome_idx,
            feature_idx=args.feature_idx,
            reason=args.reason,
        ),
        render=_render,
    )


def _handle_exclusion_remove(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    def _render(body: dict | list) -> None:
        print(json.dumps(body, indent=2))
        if isinstance(body, dict):
            verb = "unblocked" if body.get("changed") else "was not blocked (no change)"
            print(
                f"{verb}: {_target_str(body)}; lake mirror now holds"
                f" {body.get('synced_feature_count')} excluded feature(s).",
                file=sys.stderr,
            )

    return _common.run_http_subcommand(
        lambda t: _remove_exclusion_via_route(
            args.base_url,
            t,
            genome_idx=args.genome_idx,
            feature_idx=args.feature_idx,
        ),
        render=_render,
    )


def _handle_exclusion_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    def _render(body: dict | list) -> None:
        print(json.dumps(body, indent=2))
        if isinstance(body, list):
            print(
                f"{len(body)} blocked feature(s) in reference {args.reference_idx}.",
                file=sys.stderr,
            )

    return _common.run_http_subcommand(
        lambda t: _list_exclusions_via_route(args.base_url, t, args.reference_idx),
        render=_render,
    )


def add_reference_exclusion_subparser(reference_sub) -> None:
    """Register `exclusion add/remove/list` under an existing `reference`
    subparser group. Kept here (not in cli.admin.__init__) so the whole
    reference-exclusion CLI surface lives in one module."""
    p_excl = reference_sub.add_parser(
        "exclusion", help="Curate the global reference exclusion blocklist"
    )
    p_excl_sub = p_excl.add_subparsers(dest="exclusion_cmd", required=True)

    p_add = p_excl_sub.add_parser(
        "add",
        help=(
            "Block a bad genome/feature via POST /reference/exclusion"
            " (system_admin, reference:exclusion:write). Writes the Postgres"
            " blocklist row and re-syncs the data-plane anti-join mirror."
        ),
    )
    add_target = p_add.add_mutually_exclusive_group(required=True)
    add_target.add_argument("--genome-idx", type=int, dest="genome_idx", default=None)
    add_target.add_argument("--feature-idx", type=int, dest="feature_idx", default=None)
    p_add.add_argument("--reason", required=True, help="Free-text curator note (required).")
    p_add.set_defaults(handler=_handle_exclusion_add)

    p_remove = p_excl_sub.add_parser(
        "remove",
        help=(
            "Unblock a genome/feature via DELETE /reference/exclusion"
            " (system_admin, reference:exclusion:write). Re-syncs the mirror so"
            " the entity is surfaced again."
        ),
    )
    remove_target = p_remove.add_mutually_exclusive_group(required=True)
    remove_target.add_argument("--genome-idx", type=int, dest="genome_idx", default=None)
    remove_target.add_argument("--feature-idx", type=int, dest="feature_idx", default=None)
    p_remove.set_defaults(handler=_handle_exclusion_remove)

    p_list = p_excl_sub.add_parser(
        "list",
        help=(
            "List what the blocklist filters from one reference via"
            " GET /reference/{idx}/exclusion (reference:read): blocked features"
            " with reason + external ids (genome source/source_id, accession)."
        ),
    )
    p_list.add_argument("--reference-idx", type=int, dest="reference_idx", required=True)
    p_list.set_defaults(handler=_handle_exclusion_list)
