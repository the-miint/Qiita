"""`qiita-admin krepp-index remove`: mark a krepp build removed.

Goes through `POST /reference/{reference_idx}/index/{reference_index_idx}/remove`
(`reference:delete`, system_admin) so the route's refusals apply: the active build,
and a build a running ticket is fixed to, are not removed. Registering and activating
a build are workflows (`reference-index-register`, `reference-index-activate`);
docs/runbooks/krepp-index.md has the whole procedure.
"""

import argparse

from qiita_common.api_paths import PATH_REFERENCE_INDEX_REMOVE, PATH_REFERENCE_PREFIX

from .. import _common


def _remove_via_route(
    base_url: str, token: str, reference_idx: int, reference_index_idx: int
) -> dict:
    path = PATH_REFERENCE_PREFIX + PATH_REFERENCE_INDEX_REMOVE.format(
        reference_idx=reference_idx, reference_index_idx=reference_index_idx
    )
    return _common.call("POST", base_url, token, path)


def _handle_remove(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda token: _remove_via_route(
            args.base_url, token, args.reference_idx, args.reference_index_idx
        )
    )


def add_krepp_index_subparsers(sub) -> None:
    p = sub.add_parser("krepp-index", help="krepp placement index builds")
    p_sub = p.add_subparsers(dest="krepp_index_cmd", required=True)
    p_remove = p_sub.add_parser(
        "remove",
        help=(
            "Mark a registered or retired krepp build removed (system_admin,"
            " reference:delete). Refused for the active build and while a ticket fixed"
            " to the build runs. Delete its files afterwards, per"
            " docs/runbooks/krepp-index.md."
        ),
    )
    p_remove.add_argument("--reference-idx", type=int, required=True)
    p_remove.add_argument(
        "--reference-index-idx",
        type=int,
        required=True,
        help="the build's reference_index_idx (`qiita reference list --index-type krepp`)",
    )
    p_remove.set_defaults(handler=_handle_remove)
