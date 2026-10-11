"""qiita user CLI — study subcommands: create, list, and access list/grant/set-tier/revoke."""

import argparse

from qiita_common.api_paths import (
    PATH_STUDY_ACCESS,
    PATH_STUDY_ACCESS_BY_PRINCIPAL,
    PATH_STUDY_PREFIX,
)
from qiita_common.models import (
    StudyAccessGrant,
    StudyAccessTierUpdate,
    StudyCreate,
)

from .. import _common
from ._helpers import _build_body


def _post_study(base_url: str, token: str, body: dict) -> dict:
    """POST /api/v1/study with the (already-pruned) body. Owner defaults to
    the caller server-side; the CLI does not surface --owner-idx because
    naming a different owner requires wet_lab_admin+ (lab-tech-on-behalf),
    out of scope for the regular-user CLI."""
    return _common.call("POST", base_url, token, "/study", json=body)


def _handle_study_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Mint a study owned by the caller. --extra-metadata is parsed from
    JSON before Pydantic validation so a malformed paste surfaces as a
    clean argparse exit 2."""
    args.extra_metadata = _common.parse_json_arg(
        args.extra_metadata, parser, flag="--extra-metadata"
    )
    body = _build_body(StudyCreate, args, parser)
    return _common.run_http_subcommand(lambda t: _post_study(args.base_url, t, body))


_ACCESS_PATH = f"{PATH_STUDY_PREFIX}{PATH_STUDY_ACCESS}"
_ACCESS_BY_PRINCIPAL_PATH = f"{PATH_STUDY_PREFIX}{PATH_STUDY_ACCESS_BY_PRINCIPAL}"


def _list_studies(base_url: str, token: str, args: argparse.Namespace) -> dict:
    """GET /study, following `next_after_study_idx` across pages with --all.
    One page otherwise, cursor included so a caller can ask for the next.

    Defaults to the caller's own and shared studies (min_tier=viewer): on a
    deploy with many public studies, those otherwise bury the handful a
    person actually works on. --min-tier public lists everything readable."""
    params: dict = {"limit": args.limit, "min_tier": args.min_tier}
    if args.query:
        params["q"] = args.query
    if args.after_study_idx is not None:
        params["after_study_idx"] = args.after_study_idx
    studies: list = []
    while True:
        page = _common.call("GET", base_url, token, PATH_STUDY_PREFIX, params=params)
        studies.extend(page["studies"])
        cursor = page["next_after_study_idx"]
        if not args.all_pages or cursor is None:
            return {"studies": studies, "next_after_study_idx": cursor}
        params["after_study_idx"] = cursor


def _handle_study_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(lambda t: _list_studies(args.base_url, t, args))


def _handle_study_access_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """GET /study/{S}/access — every access row on the study."""
    path = _ACCESS_PATH.format(study_idx=args.study_idx)
    return _common.run_http_subcommand(lambda t: _common.call("GET", args.base_url, t, path))


def _handle_study_access_grant(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """POST /study/{S}/access — grant a tier to the account using --email."""
    body = _build_body(StudyAccessGrant, args, parser)
    path = _ACCESS_PATH.format(study_idx=args.study_idx)
    return _common.run_http_subcommand(
        lambda t: _common.call("POST", args.base_url, t, path, json=body)
    )


def _handle_study_access_set_tier(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """PATCH /study/{S}/access/{P} — change one grantee's tier."""
    body = _build_body(StudyAccessTierUpdate, args, parser)
    path = _ACCESS_BY_PRINCIPAL_PATH.format(
        study_idx=args.study_idx, principal_idx=args.principal_idx
    )
    return _common.run_http_subcommand(
        lambda t: _common.call("PATCH", args.base_url, t, path, json=body)
    )


def _handle_study_access_revoke(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """DELETE /study/{S}/access/{P} — remove one grantee's row; prints it."""
    path = _ACCESS_BY_PRINCIPAL_PATH.format(
        study_idx=args.study_idx, principal_idx=args.principal_idx
    )
    return _common.run_http_subcommand(lambda t: _common.call("DELETE", args.base_url, t, path))
