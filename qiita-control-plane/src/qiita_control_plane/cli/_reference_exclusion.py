"""Reference-exclusion blocklist CLI, shared between `qiita-admin` and `qiita`.

`add` / `remove` drive the system_admin POST/DELETE `/reference/exclusion` routes
(`reference:exclusion:write`) and live under `qiita-admin reference exclusion`;
`list` reads `GET /reference/{idx}/exclusion` (`reference:read`) and lives under
`qiita reference exclusion` — a normal user needs to see what the blocklist
filtered out of their feature table, and by the placement rule (see
`cli/admin/__init__.py`) a plain authenticated read belongs in `qiita`.

Everything goes through the routes (not a direct DB write) so the scope gate AND
the data-plane re-sync fire exactly as for any API caller — the CLI is not the
security boundary. This module is shared (not under `cli/admin`) so the `qiita`
CLI can register `list` without depending on the admin package.
"""

import argparse
import json
import sys
from collections.abc import Callable

from qiita_common.api_paths import (
    PATH_REFERENCE_EXCLUSION,
    PATH_REFERENCE_EXCLUSION_BY_IDX,
    PATH_REFERENCE_EXCLUSION_SYNC,
    PATH_REFERENCE_PREFIX,
)

from . import _common

# The post-API_PREFIX path segments (`_common.call` prepends API_PREFIX).
_EXCLUSION_PATH = f"{PATH_REFERENCE_PREFIX}{PATH_REFERENCE_EXCLUSION}"
_EXCLUSION_SYNC_PATH = f"{PATH_REFERENCE_PREFIX}{PATH_REFERENCE_EXCLUSION_SYNC}"


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


def _sync_exclusion_via_route(base_url: str, token: str) -> dict:
    """POST /api/v1/reference/exclusion/sync as the PAT — force-resync the mirror,
    no body, no Postgres change."""
    return _common.call("POST", base_url, token, _EXCLUSION_SYNC_PATH)


def _target_str(body: dict) -> str:
    if body.get("target_kind") == "genome":
        return f"genome_idx={body.get('genome_idx')}"
    return f"feature_idx={body.get('feature_idx')}"


def _handle_exclusion_mutation(
    args: argparse.Namespace,
    *,
    call: Callable[[str], dict | list],
    verbs: tuple[str, str],
) -> int:
    """Shared body of `add` / `remove`: run the route call and render the JSON
    plus a one-line human summary. The two differ only by which route helper
    `call` invokes and the `(changed, unchanged)` verb pair — so an improvement to
    the summary (e.g. printing `reason`) is made once, not twice."""
    changed_verb, unchanged_verb = verbs

    def _render(body: dict | list) -> None:
        print(json.dumps(body, indent=2))
        if isinstance(body, dict):
            verb = changed_verb if body.get("changed") else unchanged_verb
            print(
                f"{verb}: {_target_str(body)}; lake mirror now holds"
                f" {body.get('synced_feature_count')} excluded feature(s).",
                file=sys.stderr,
            )

    return _common.run_http_subcommand(call, render=_render)


def _handle_exclusion_add(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _handle_exclusion_mutation(
        args,
        call=lambda t: _add_exclusion_via_route(
            args.base_url,
            t,
            genome_idx=args.genome_idx,
            feature_idx=args.feature_idx,
            reason=args.reason,
        ),
        verbs=("blocked", "already blocked (no change)"),
    )


def _handle_exclusion_remove(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _handle_exclusion_mutation(
        args,
        call=lambda t: _remove_exclusion_via_route(
            args.base_url,
            t,
            genome_idx=args.genome_idx,
            feature_idx=args.feature_idx,
        ),
        verbs=("unblocked", "was not blocked (no change)"),
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


def _handle_exclusion_sync(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    def _render(body: dict | list) -> None:
        print(json.dumps(body, indent=2))
        if isinstance(body, dict):
            print(
                f"mirror re-synced: lake mirror now holds"
                f" {body.get('synced_feature_count')} excluded feature(s).",
                file=sys.stderr,
            )

    return _common.run_http_subcommand(
        lambda t: _sync_exclusion_via_route(args.base_url, t),
        render=_render,
    )


def _add_target_group(sub_parser: argparse.ArgumentParser) -> None:
    """The mutually-exclusive, required `--genome-idx | --feature-idx` group
    shared by `add` and `remove`."""
    target = sub_parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--genome-idx", type=int, dest="genome_idx", default=None)
    target.add_argument("--feature-idx", type=int, dest="feature_idx", default=None)


def _add_list_parser(exclusion_sub) -> None:
    """Register `exclusion list` (the `reference:read` query) — used by BOTH the
    `qiita` reference group (its home) and, for operator discoverability, kept
    off the `qiita-admin` group (admins run the `qiita` command with the same
    PAT)."""
    p_list = exclusion_sub.add_parser(
        "list",
        help=(
            "List what the blocklist filters from one reference via"
            " GET /reference/{idx}/exclusion (reference:read): blocked features"
            " with reason + external ids (genome source/source_id, accession)."
        ),
    )
    p_list.add_argument("--reference-idx", type=int, dest="reference_idx", required=True)
    p_list.set_defaults(handler=_handle_exclusion_list)


def add_admin_exclusion_subparsers(reference_sub) -> None:
    """Register `exclusion add / remove` under `qiita-admin`'s `reference` group
    (the write surface, system_admin `reference:exclusion:write`). `list` lives in
    the `qiita` CLI (see add_user_exclusion_subparsers)."""
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
    _add_target_group(p_add)
    p_add.add_argument("--reason", required=True, help="Free-text curator note (required).")
    p_add.set_defaults(handler=_handle_exclusion_add)

    p_remove = p_excl_sub.add_parser(
        "remove",
        help=(
            "Unblock a genome/feature via DELETE /reference/exclusion"
            " (system_admin, reference:exclusion:write). Soft-deletes the block"
            " (kept as a record) and re-syncs the mirror so the entity is surfaced"
            " again."
        ),
    )
    _add_target_group(p_remove)
    p_remove.set_defaults(handler=_handle_exclusion_remove)

    p_sync = p_excl_sub.add_parser(
        "sync",
        help=(
            "Force-resync the data-plane exclusion mirror from the current Postgres"
            " blocklist via POST /reference/exclusion/sync (system_admin,"
            " reference:exclusion:write). Makes no blocklist change — operator"
            " recovery when the mirror drifts (a failed sync, a rebuilt DuckLake"
            " catalog, or a fresh data plane)."
        ),
    )
    p_sync.set_defaults(handler=_handle_exclusion_sync)


def add_user_exclusion_subparsers(reference_sub) -> None:
    """Register `exclusion list` under the `qiita` `reference` group (the
    `reference:read` query surface any user can run)."""
    p_excl = reference_sub.add_parser(
        "exclusion", help="Inspect what the global reference blocklist filters"
    )
    p_excl_sub = p_excl.add_subparsers(dest="exclusion_cmd", required=True)
    _add_list_parser(p_excl_sub)
