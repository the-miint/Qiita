"""qiita-admin CLI — the audited edit of a definition's locked settings (calls
PATCH /admin/definition-settings/{kind}/{idx}).

Thin client: the server owns every refusal (`qiita_control_plane.definition_settings`).
system_admin with the kind's lifecycle scope. Renders the recorded edit as JSON to
stdout and a one-line summary to stderr.
"""

import argparse
import json
import sys
from pathlib import Path

from qiita_common.api_paths import PATH_ADMIN_DEFINITION_SETTINGS, PATH_ADMIN_PREFIX
from qiita_common.models import DefinitionKind

from .. import _common


def _render(body: dict | list) -> None:
    print(json.dumps(body, indent=2))
    if isinstance(body, dict):
        line = (
            f"{body.get('definition_kind')} {body.get('definition_idx')}: settings edited"
            f" (edit {body.get('definition_settings_edit_idx')})"
        )
        if body.get("repaired_output_equivalence_idx"):
            line += f"; output equivalences recomputed: {body['repaired_output_equivalence_idx']}"
        print(line, file=sys.stderr)


def _read_params(path: Path) -> dict:
    params = json.loads(path.read_text())
    if not isinstance(params, dict):
        raise ValueError(f"{path} holds a JSON {type(params).__name__}, not an object")
    return params


def _handle_edit_settings(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    try:
        params = _read_params(args.params_file)
    except (OSError, ValueError) as exc:
        parser.error(f"--params-file: {exc}")
    body: dict = {"params": params, "reason": args.reason}
    for field in ("workflow", "version"):
        if getattr(args, field) is not None:
            body[field] = getattr(args, field)
    path = f"{PATH_ADMIN_PREFIX}{PATH_ADMIN_DEFINITION_SETTINGS}".format(
        definition_kind=args.kind, definition_idx=args.definition_idx
    )
    return _common.run_http_subcommand(
        lambda token: _common.call("PATCH", args.base_url, token, path, json=body),
        render=_render,
    )


def add_definition_parser(subparsers) -> None:
    """Wire `qiita-admin definition edit-settings`."""
    p = subparsers.add_parser(
        "definition", help="a mask's, alignment's or processing's settings (system_admin)"
    )
    sub = p.add_subparsers(dest="definition_cmd", required=True)
    sp = sub.add_parser(
        "edit-settings",
        help="replace a definition's params (locked after its mint); the edit is recorded"
        " with the old and new values and the reason",
    )
    sp.add_argument("kind", choices=[k.value for k in DefinitionKind])
    sp.add_argument("definition_idx", type=int)
    sp.add_argument(
        "--params-file",
        type=Path,
        required=True,
        help="JSON file holding the new params, whole; params_hash is recomputed from it",
    )
    sp.add_argument(
        "--workflow", default=None, help="a mask's filter_workflow or a processing's workflow"
    )
    sp.add_argument(
        "--version", default=None, help="a mask's filter_version or a processing's version"
    )
    sp.add_argument("--reason", required=True, help="why; recorded with the edit")
    sp.set_defaults(handler=_handle_edit_settings)
