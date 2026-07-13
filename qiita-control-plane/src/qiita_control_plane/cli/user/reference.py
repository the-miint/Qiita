"""qiita user CLI — reference list / load subcommands.

Split out of the former single-file ``cli.user`` module; behavior unchanged.
"""

import argparse
import asyncio
import json
import sys
from typing import Any

from qiita_common.api_paths import (
    PATH_REFERENCE_INDEX,
    PATH_REFERENCE_PREFIX,
)
from qiita_common.models import (
    ReferenceStatus,
    WorkTicketState,
)

from .. import _common


def _handle_reference_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """List references with their built index types, filtered by --host /
    --active / --index-type — discover the idx for --host-rype-reference-idx etc.

    Index types come from a separate endpoint (GET /reference/{idx}/index), so
    each listed reference costs one extra GET — fine for a human discovery
    command. With --index-type, references lacking that built index are dropped,
    so the result is exactly the set submit-host-filter-pool's readiness gate
    (`_assert_host_reference_ready`) accepts.

    That `/index` endpoint requires the `reference:read` scope (it exposes
    `fs_path`), so — unlike `prep-protocol list` — this command needs a token
    carrying that scope; a token without it fails on the first per-row call."""
    params: dict[str, str] = {}
    if args.host:
        params["is_host"] = "true"
    if args.active:
        params["status"] = ReferenceStatus.ACTIVE.value

    def _fetch(token: str) -> list:
        references = _common.call(
            "GET", args.base_url, token, PATH_REFERENCE_PREFIX, params=params or None
        )
        enriched: list = []
        for reference in references:
            idx = reference["reference_idx"]
            indexes = _common.call(
                "GET",
                args.base_url,
                token,
                f"{PATH_REFERENCE_PREFIX}{PATH_REFERENCE_INDEX.format(reference_idx=idx)}",
            )
            index_types = sorted({row["index_type"] for row in indexes})
            if args.index_type and args.index_type not in index_types:
                continue
            enriched.append({**reference, "index_types": index_types})
        return enriched

    return _common.run_http_subcommand(_fetch)


async def _run_reference_load(
    *,
    base_url: str,
    token: str,
    data_plane_url: str | None,
    args: argparse.Namespace,
) -> dict:
    """Construct real httpx + (for remote ingest) pyarrow.flight clients and
    drive `do_reference_load`. Lives next to the CLI handler so the handler
    stays a thin argparse → entry-point shim; the entry point itself
    (in cli.reference_load) takes injected clients so tests bypass this
    function entirely.

    Under `--local` no bytes cross the wire, so no Flight client is built and
    `--data-plane-url` is not needed; the by-path manifest + companions ride in
    action_context. The remote path requires `--data-plane-url`."""
    import httpx as _httpx

    from ..reference_load import do_reference_load

    # Shared keyword args for both ingest modes.
    common_kwargs: dict[str, Any] = dict(
        token=token,
        local=args.local,
        fasta_path=args.fasta,
        fasta_manifest_path=args.fasta_manifest,
        name=args.name,
        version=args.version,
        kind=args.kind,
        host=args.host,
        shard_index=args.shard_index,
        reference_idx=args.reference_idx,
        taxonomy_path=args.taxonomy,
        tree_path=args.tree,
        jplace_path=args.jplace,
        genome_map_path=args.genome_map,
        build_rype=not args.no_rype_index,
        build_minimap2=not args.no_minimap2_index,
        build_bowtie2=not args.no_bowtie2_index,
        rype_w=args.rype_w,
        minimap2_preset=args.minimap2_preset,
        watch=not args.no_watch,
        poll_interval_seconds=args.poll_interval,
        timeout_seconds=args.timeout,
        mem_gb=args.mem_gb,
    )

    if args.local:
        async with _httpx.AsyncClient(
            base_url=base_url, timeout=_common.CLI_HTTP_TIMEOUT_SECONDS
        ) as http:
            return await do_reference_load(http=http, flight_client=None, **common_kwargs)

    if not data_plane_url:
        raise ValueError("--data-plane-url is required for remote ingest (or use --local)")

    import pyarrow.flight as flight

    flight_client = flight.FlightClient(data_plane_url)
    try:
        async with _httpx.AsyncClient(
            base_url=base_url, timeout=_common.CLI_HTTP_TIMEOUT_SECONDS
        ) as http:
            return await do_reference_load(http=http, flight_client=flight_client, **common_kwargs)
    finally:
        flight_client.close()


def _handle_reference_load(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Entry point for `qiita reference load`. Reads the PAT, builds
    a real httpx + flight client, and calls `do_reference_load`. Maps
    every known failure shape to exit 1 with a one-line stderr message —
    no silent retry, no buried traceback. Terminal work_ticket=failed
    also exits 1 so callers wrapping this in a Makefile / CI step get
    the build break."""
    import httpx as _httpx
    import pyarrow.flight as _flight

    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    try:
        result = asyncio.run(
            _run_reference_load(
                base_url=args.base_url,
                token=token,
                data_plane_url=args.data_plane_url,
                args=args,
            )
        )
    except _httpx.HTTPStatusError as exc:
        print(
            f"http error {exc.response.status_code}: {exc.response.text}",
            file=sys.stderr,
        )
        return 1
    except _flight.FlightError as exc:
        # Catch the gRPC-level error explicitly so the operator sees a
        # formatted error line instead of a raw traceback. FlightError is
        # NOT a RuntimeError subclass, so the catch-all below would miss
        # it. Common shapes: network refused, expired ticket, DP
        # rejected the stream mid-write.
        print(f"flight error: {exc}", file=sys.stderr)
        return 1
    except (RuntimeError, ValueError, TimeoutError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    # Under --watch, anything but a COMPLETED ticket surfaces as exit 1, not exit
    # 0 — a CI step wrapping this CLI must distinguish a successful reference
    # build from one that didn't produce a reference. The JSON body still goes to
    # stdout so the caller can see the failure_reason.
    #
    # Tested against COMPLETED rather than against a list of bad states: `no_data`
    # is a terminal outcome that builds nothing, and a positive list of failures
    # would let it — and every future state — exit 0 as a success.
    #
    # `state` is absent under --no-watch (we returned before the ticket reached an
    # outcome), and that is not a failure: the caller polls it themselves.
    work_ticket = result.get("work_ticket") or {}
    final_state = work_ticket.get("state")
    print(json.dumps(_serializable(result), indent=2))
    if final_state is not None and final_state != WorkTicketState.COMPLETED.value:
        return 1
    return 0


def _serializable(obj):
    """Recursively replace Pydantic / Path values with their JSON form so
    `json.dumps` succeeds on the result dict (which carries upload-idx
    metadata + the final work_ticket body)."""
    from pathlib import Path as _Path

    if isinstance(obj, dict):
        return {k: _serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serializable(v) for v in obj]
    if isinstance(obj, _Path):
        return str(obj)
    return obj
