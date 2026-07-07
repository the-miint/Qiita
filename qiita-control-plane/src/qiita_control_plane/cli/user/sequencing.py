"""qiita user CLI — sequencing-run / sequenced-pool / sequenced-sample / prep subcommands.

Split out of the former single-file ``cli.user`` module; behavior unchanged.
"""

import argparse
import base64
from pathlib import Path

from pydantic import ValidationError
from qiita_common.api_paths import (
    PATH_PREP_PROTOCOL_PREFIX,
    PATH_PREP_SAMPLE_PREFIX,
    PATH_PREP_SAMPLE_RETIRED,
    PATH_SEQUENCING_RUN_LOOKUP_BY_INSTRUMENT_RUN_ID,
    PATH_SEQUENCING_RUN_PREFIX,
)
from qiita_common.models import (
    SequencedPoolCreateRequest,
    SequencedPoolPreflightUpdateLaneRequest,
    SequencedSampleCreateRequest,
    SequencingRunCreateRequest,
)

from .. import _common
from ._helpers import _build_body


def _post_sequencing_run(base_url: str, token: str, body: dict) -> dict:
    """POST /api/v1/sequencing-run with the (already-pruned) body.

    Instrument-level resource: not study-scoped, has no owner field. The
    creator is recorded server-side as created_by_idx.
    """
    return _common.call("POST", base_url, token, "/sequencing-run", json=body)


def _post_sequenced_pool(base_url: str, token: str, run_idx: int, body: dict) -> dict:
    """POST /api/v1/sequencing-run/{run_idx}/sequenced-pool with the
    (already-pruned) body. The run-preflight pair (blob + filename) is
    co-populated; the route's Pydantic validator returns 422 on a
    half-populated pair, but the CLI guards earlier so the user sees an
    argparse-style error instead of a server round-trip.
    """
    return _common.call(
        "POST", base_url, token, f"/sequencing-run/{run_idx}/sequenced-pool", json=body
    )


def _post_preflight_update_lane(
    base_url: str, token: str, run_idx: int, pool_idx: int, body: dict
) -> dict:
    """POST .../sequenced-pool/{pool_idx}/preflight/update-lane — bulk lane
    reassignment on the pool's run-preflight SQLite blob. wet_lab_admin+; the
    server runs run_preflight.update_lane and refuses (409) once the run has been
    processed."""
    return _common.call(
        "POST",
        base_url,
        token,
        f"/sequencing-run/{run_idx}/sequenced-pool/{pool_idx}/preflight/update-lane",
        json=body,
    )


def _post_sequenced_sample(
    base_url: str, token: str, run_idx: int, pool_idx: int, body: dict
) -> dict:
    """POST /api/v1/sequencing-run/{R}/sequenced-pool/{P}/sequenced-sample
    composer. Atomically mints the prep_sample + sequenced_sample subtype +
    prep_sample_to_study links (primary + each secondary) + metadata rows.
    """
    return _common.call(
        "POST",
        base_url,
        token,
        f"/sequencing-run/{run_idx}/sequenced-pool/{pool_idx}/sequenced-sample",
        json=body,
    )


def _handle_sequencing_run_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Mint a sequencing-run row. --extra-metadata is parsed from JSON
    before Pydantic validation so a malformed paste surfaces as a clean
    argparse exit 2."""
    args.extra_metadata = _common.parse_json_arg(
        args.extra_metadata, parser, flag="--extra-metadata"
    )
    body = _build_body(SequencingRunCreateRequest, args, parser)
    return _common.run_http_subcommand(lambda t: _post_sequencing_run(args.base_url, t, body))


def _handle_sequencing_run_lookup(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Resolve instrument_run_id(s) to sequencing_run idx via the bulk lookup.

    Prints the {resolved, missing} mapping.
    """
    body = {"instrument_run_ids": args.instrument_run_ids}
    path = f"{PATH_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCING_RUN_LOOKUP_BY_INSTRUMENT_RUN_ID}"
    return _common.run_http_subcommand(
        lambda t: _common.call("POST", args.base_url, t, path, json=body)
    )


def _handle_sequenced_pool_create(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Mint a sequenced-pool under a sequencing-run.

    The run-preflight is an optional co-populated pair. When
    --run-preflight-blob is supplied: read the file, refuse-on-empty,
    and default --run-preflight-filename to the file's basename when
    the user didn't pass one. When --run-preflight-filename is supplied
    without --run-preflight-blob: refuse before the server round-trip.
    Otherwise both flags are absent and the pool is created with no
    preflight, which is the optional-pair case the schema permits.
    """
    if args.run_preflight_blob is not None:
        blob_path: Path = args.run_preflight_blob
        if not blob_path.is_file():
            parser.error(f"--run-preflight-blob {blob_path} is not a regular file")
        blob_bytes = blob_path.read_bytes()
        if not blob_bytes:
            parser.error(f"--run-preflight-blob {blob_path} is empty")
        # Pydantic's Base64Bytes interprets input as an *already* base64-
        # encoded string and decodes it. We base64-encode here so the model
        # round-trips back to the same raw bytes; mode="json" then re-encodes
        # to the canonical base64 string for the wire.
        args.run_preflight_blob = base64.b64encode(blob_bytes).decode("ascii")
        if args.run_preflight_filename is None:
            args.run_preflight_filename = blob_path.name
    elif args.run_preflight_filename is not None:
        parser.error(
            "--run-preflight-filename supplied without --run-preflight-blob;"
            " the preflight pair must be both or neither"
        )

    args.extra_metadata = _common.parse_json_arg(
        args.extra_metadata, parser, flag="--extra-metadata"
    )
    body = _build_body(SequencedPoolCreateRequest, args, parser)
    return _common.run_http_subcommand(
        lambda t: _post_sequenced_pool(args.base_url, t, args.run_idx, body)
    )


def _handle_run_preflight_update_lane(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> int:
    """Bulk-reassign the lane on a pool's run-preflight blob via the server.

    The mutation runs server-side — the control plane loads the blob, applies
    run_preflight.update_lane, and writes it back — so this handler only validates
    the request locally (via the shared model, catching a degenerate from==to pair
    before the round-trip) and forwards it. The body is built directly rather than
    via _build_body because a NULL lane is a real value to send, and _build_body
    drops None fields."""
    try:
        req = SequencedPoolPreflightUpdateLaneRequest(
            platform=args.platform,
            from_lane=args.from_lane,
            to_lane=args.to_lane,
            reason=args.reason,
        )
    except ValidationError as exc:
        msgs = "; ".join(f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors())
        parser.error(f"invalid {SequencedPoolPreflightUpdateLaneRequest.__name__}: {msgs}")
    body = req.model_dump(mode="json")
    return _common.run_http_subcommand(
        lambda t: _post_preflight_update_lane(
            args.base_url, t, args.sequencing_run_idx, args.sequenced_pool_idx, body
        )
    )


def _handle_sequenced_sample_create(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> int:
    """Mint a sequenced_sample composer atomically with its study links and
    metadata. --owner-idx auto-defaults to the caller via whoami when omitted;
    --metadata KEY=VALUE entries collect via the shared parse_kv_pairs helper.
    """
    args.metadata = _common.parse_kv_pairs(args.metadata, parser, flag="--metadata")

    def _run(token: str) -> dict:
        _common.resolve_owner_idx(args, args.base_url, token)
        body = _build_body(SequencedSampleCreateRequest, args, parser)
        return _post_sequenced_sample(args.base_url, token, args.run_idx, args.pool_idx, body)

    return _common.run_http_subcommand(_run)


def _handle_prep_protocol_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """List prep protocols (GET /prep-protocol) for `--prep-protocol-idx`.
    Retired protocols are excluded unless --all is given."""
    params = {"include_retired": "true"} if args.include_retired else None
    return _common.run_http_subcommand(
        lambda t: _common.call("GET", args.base_url, t, PATH_PREP_PROTOCOL_PREFIX, params=params)
    )


def _handle_prep_sample_retire(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Retire or un-retire a prep-sample (PATCH /prep-sample/{idx}/retired). The
    `retire` subcommand sets retired=true (dropping an empty/failed-yield well
    out of a pool's active set); `un-retire` sets retired=false (recovering a
    misclassified one). The route returns 204, so on success we print a short
    confirmation rather than a JSON body."""

    def _do(token: str) -> dict:
        path = PATH_PREP_SAMPLE_PREFIX + PATH_PREP_SAMPLE_RETIRED.format(
            prep_sample_idx=args.prep_sample_idx
        )
        body = {"retired": args.retired, "reason": args.reason}
        _common._request("PATCH", args.base_url, token, path, json=body)
        return {
            "prep_sample_idx": args.prep_sample_idx,
            "retired": args.retired,
        }

    return _common.run_http_subcommand(_do)
