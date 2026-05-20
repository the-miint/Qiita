"""qiita — end-user CLI for the Qiita control plane.

Scope: actions a regular user performs against a running deployment.
The parallel operator/admin CLI is `qiita-admin`; that one stays
scoped to principal/role/token management and is deliberately separate.

This module owns the user-facing argparse surface and its subcommand
handlers. PAT file I/O, the LoginRocket loopback flow, the
authenticated HTTP call helper, and the generic token-read + invoke +
JSON-print runner live in `cli._common`.

Authentication: HTTP subcommands read the PAT from QIITA_TOKEN env or
from ~/.qiita/token (mode 0600).
"""

import argparse
import sys

from pydantic import BaseModel, ValidationError
from qiita_common.models import BiosampleImportRequest, StudyCreate, Tier, UserUpdate

from . import _common

# ---------------------------------------------------------------------------
# HTTP helpers (call sites for individual endpoints)
# ---------------------------------------------------------------------------


def _patch_user_me(base_url: str, token: str, updates: dict) -> dict:
    """PATCH /api/v1/user/me with the (already-pruned) updates dict.

    Only the fields the caller actually set are sent; unset ones stay
    absent so the server's `exclude_unset` SET-clause builder never
    UPDATEs a field the user didn't ask about. With an empty dict, the
    route round-trips the current profile — handy as a side-effect
    "show" but argparse requires at least one --flag here so empty
    bodies don't slip through silently.
    """
    return _common.call("PATCH", base_url, token, "/user/me", json=updates)


def _post_study(base_url: str, token: str, body: dict) -> dict:
    """POST /api/v1/study with the (already-pruned) body. Owner defaults to
    the caller server-side; the CLI does not surface --owner-idx because
    naming a different owner requires wet_lab_admin+ (lab-tech-on-behalf),
    out of scope for the regular-user CLI."""
    return _common.call("POST", base_url, token, "/study", json=body)


def _post_biosample(base_url: str, token: str, study_idx: int, body: dict) -> dict:
    """POST /api/v1/study/{study_idx}/biosample with the (already-pruned) body.

    The route currently requires owner_idx in the body explicitly; the CLI
    handler resolves it via whoami when --owner-idx is omitted so the
    caller can run `qiita biosample create` for themselves without first
    chasing their own principal_idx.
    """
    return _common.call("POST", base_url, token, f"/study/{study_idx}/biosample", json=body)


# ---------------------------------------------------------------------------
# argparse entry point
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qiita", description="Qiita end-user CLI")
    _common.add_base_url_arg(parser)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_login = sub.add_parser(
        "login",
        help="AuthRocket LoginRocket Web flow with localhost loopback",
    )
    _common.add_token_file_arg(p_login)
    p_login.set_defaults(handler=_handle_login)

    p_whoami = sub.add_parser("whoami", help="Print the authenticated principal")
    p_whoami.set_defaults(handler=_handle_whoami)

    p_profile = sub.add_parser("profile", help="User profile operations")
    p_profile_sub = p_profile.add_subparsers(dest="profile_cmd", required=True)
    p_profile_set = p_profile_sub.add_parser(
        "set",
        help="Update affiliation / address / phone / orcid / mail prefs (PATCH /user/me)",
    )
    # All optional; argparse default None lets main() prune unset fields out
    # of the JSON body, matching the server's exclude_unset semantics.
    p_profile_set.add_argument("--affiliation")
    p_profile_set.add_argument("--address")
    p_profile_set.add_argument("--phone")
    p_profile_set.add_argument(
        "--orcid",
        help="ORCID iD (format NNNN-NNNN-NNNN-NNNX); server-side regex enforces shape",
    )
    p_profile_set.add_argument(
        "--receive-processing-emails",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Opt in (--receive-processing-emails) or out"
            " (--no-receive-processing-emails); omit to leave the current"
            " value unchanged"
        ),
    )
    p_profile_set.set_defaults(handler=_handle_profile_set)

    p_study = sub.add_parser("study", help="Study operations")
    p_study_sub = p_study.add_subparsers(dest="study_cmd", required=True)
    p_study_create = p_study_sub.add_parser(
        "create",
        help="Create a study owned by the calling principal (POST /study)",
    )
    p_study_create.add_argument("--title", required=True)
    p_study_create.add_argument("--alias")
    p_study_create.add_argument("--description")
    p_study_create.add_argument("--abstract")
    p_study_create.add_argument("--funding")
    p_study_create.add_argument("--ebi-study-accession")
    p_study_create.add_argument("--notes")
    p_study_create.add_argument(
        "--principal-investigator-idx",
        type=int,
        help="principal_idx of the PI; must already exist as a user-kind principal",
    )
    p_study_create.add_argument(
        "--default-tier",
        choices=tuple(t.value for t in Tier),
        help="Default study_access tier; server defaults to 'member' when unset",
    )
    p_study_create.set_defaults(handler=_handle_study_create)

    p_biosample = sub.add_parser("biosample", help="Biosample operations")
    p_biosample_sub = p_biosample.add_subparsers(dest="biosample_cmd", required=True)
    p_biosample_create = p_biosample_sub.add_parser(
        "create",
        help="Create a biosample on a study (POST /study/{S}/biosample)",
    )
    p_biosample_create.add_argument("--study-idx", type=int, required=True)
    p_biosample_create.add_argument(
        "--owner-idx",
        type=int,
        help="principal_idx of the biosample's owner; defaults to the caller (resolved via whoami)",
    )
    p_biosample_create.add_argument(
        "--owner-biosample-id-field-name",
        required=True,
        help="display_name of the study's local field that carries the owner-biosample-id",
    )
    p_biosample_create.add_argument(
        "--owner-biosample-id-value",
        required=True,
        help="the owner-biosample-id value to record on this biosample",
    )
    p_biosample_create.add_argument(
        "--metadata",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help=(
            "Metadata entry; repeat for multiple. KEY is a biosample_global_field"
            " display_name; the route parses VALUE into the field's data type."
        ),
    )
    p_biosample_create.add_argument("--metadata-checklist-idx", type=int)
    p_biosample_create.add_argument(
        "--biosample-accession",
        help="External biosample accession (e.g. NCBI), if known at create time",
    )
    # --ena-sample-accession is deliberately NOT exposed: it's the
    # publication-lock signal set by the submission subsystem, not a
    # caller-supplied field.
    p_biosample_create.set_defaults(handler=_handle_biosample_create)

    return parser


# ---------------------------------------------------------------------------
# Subcommand handlers (registered via parser.set_defaults(handler=...))
# ---------------------------------------------------------------------------


def _handle_login(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.do_login(
        base_url=args.base_url,
        token_file=args.token_file,
        cli_command="qiita login",
    )


def _handle_whoami(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(lambda t: _common.whoami(args.base_url, t))


def _handle_profile_set(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    updates = _build_body(UserUpdate, args, parser)
    if not updates:
        parser.error(
            "qiita profile set requires at least one of"
            " --affiliation / --address / --phone / --orcid /"
            " --receive-processing-emails / --no-receive-processing-emails"
        )
    return _common.run_http_subcommand(lambda t: _patch_user_me(args.base_url, t, updates))


def _handle_study_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    body = _build_body(StudyCreate, args, parser)
    return _common.run_http_subcommand(lambda t: _post_study(args.base_url, t, body))


def _handle_biosample_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Mint a biosample on a study, auto-defaulting owner_idx to the caller.

    The whoami lookup that resolves a missing --owner-idx shares the
    token-read path with the POST itself, so both calls happen inside the
    same `run_http_subcommand` invocation. Body construction runs after
    that resolution so BiosampleImportRequest sees a populated owner_idx.
    """
    args.metadata = _common.parse_kv_pairs(args.metadata, parser, flag="--metadata")

    def _run(token: str) -> dict:
        if args.owner_idx is None:
            args.owner_idx = _common.whoami(args.base_url, token)["principal_idx"]
        body = _build_body(BiosampleImportRequest, args, parser)
        return _post_biosample(args.base_url, token, args.study_idx, body)

    return _common.run_http_subcommand(_run)


def _build_body(
    model_cls: type[BaseModel],
    args: argparse.Namespace,
    parser: argparse.ArgumentParser,
) -> dict:
    """Construct `model_cls` from the parsed-args fields that match its
    model_fields, then return the exclude_unset JSON dump.

    Filters None out of the namespace before construction so the only
    fields Pydantic treats as "set" are the ones the caller actually
    passed (matches the server's exclude_unset semantics on the PATCH
    side; honest with the schema on the POST side). Argparse's dest
    names line up with the Pydantic field names (snake_case from
    hyphenated flags), so the filter is a single comprehension.

    On ValidationError (e.g. a too-long --title, malformed --orcid),
    flattens the errors into a single stderr line and exits 2 via
    parser.error — same code path as argparse's own validation
    failures, so callers don't see a Python traceback for invalid
    input.
    """
    fields = {
        name: getattr(args, name)
        for name in model_cls.model_fields
        if getattr(args, name, None) is not None
    }
    try:
        return model_cls(**fields).model_dump(exclude_unset=True, mode="json")
    except ValidationError as exc:
        msgs = "; ".join(f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors())
        parser.error(f"invalid {model_cls.__name__}: {msgs}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _common.validate_base_url(args, parser)
    return args.handler(args, parser)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
