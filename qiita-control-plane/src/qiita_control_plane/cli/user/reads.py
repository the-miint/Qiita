"""`qiita submit-reads` — load one sample's reads from the local machine.

The route a regular user takes. Naming a host path in `action_context` is
wet_lab_admin-or-higher, and a `user`'s reads are on their own machine, which
the cluster cannot see either way. So this gesture:

  1. validates the local file(s) — present, non-empty, and a format this
     system loads;
  2. streams each one to the data plane over Flight DoPut, byte-exact,
     recording the basename on the upload row;
  3. submits the ingest work_ticket naming the upload handles.

For a FASTQ, the recorded basename is what the submit gate applies the
filename-prefix rule to (it must be the prep_sample's `sequenced_pool_item_id`
followed by `_` or `.`), so a mismatch comes back as a 422 from step 3 — after
the upload, since the rule needs the prep_sample the ticket names. **The rule
does not apply to a BAM**: `FASTQ_PATH_CONTEXT_KEYS` names only the two fastq
keys, and a PacBio demux BAM is named for its movie and barcode
(`{movie}.hifi_reads.{barcode}.bam`), which carries no pool item id — the
barcode is how that file is bound to a sample, upstream in
`submit-pacbio-ingest`.

Format decides the action: FASTQ (one or two files) → `fastq-to-parquet`, BAM →
`bam-to-parquet`. Both accept either an `*_upload_idx` handle or a host path,
and the runner resolves the handle to the same step input, so an uploaded
sample and a path-named one run the identical workflow.

Nothing is decompressed on the way out. `read_fastx` reads gzip directly, and
the step names the stitched file from the bytes' own magic number.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from qiita_common.api_paths import URL_WORK_TICKET_PREFIX
from qiita_common.models import ScopeTargetKind, WorkTicketCreateRequest, WorkTicketState

from . import _common

# Pinned here rather than taken from a flag: this gesture knows which workflow
# loads reads, and an operator who needs a different one has `qiita ticket
# submit`. Bumping a workflow's version means bumping it here.
_FASTQ_ACTION_ID = "fastq-to-parquet"
_FASTQ_ACTION_VERSION = "1.3.0"
_BAM_ACTION_ID = "bam-to-parquet"
_BAM_ACTION_VERSION = "1.0.0"

# Suffixes that decide which loader a file goes to. Checked against the
# basename with any `.gz` stripped first, so `a.fastq.gz` and `a.fastq` land
# the same way.
_FASTQ_SUFFIXES = (".fastq", ".fq")
_BAM_SUFFIXES = (".bam",)


def _classify(path: Path) -> str:
    """`"fastq"` or `"bam"` from the basename, or raise ValueError.

    Keyed on the name because that is what the submitter chose and what the
    error can name back at them; the step re-derives format from the bytes.
    """
    name = path.name
    if name.endswith(".gz"):
        name = name[: -len(".gz")]
    lowered = name.lower()
    if lowered.endswith(_FASTQ_SUFFIXES):
        return "fastq"
    if lowered.endswith(_BAM_SUFFIXES):
        return "bam"
    raise ValueError(
        f"{path}: cannot tell the format from the filename — expected one of "
        f"{', '.join(_FASTQ_SUFFIXES + _BAM_SUFFIXES)} (optionally .gz)"
    )


def _check_local_file(path: Path, *, flag: str) -> None:
    """Refuse a file this machine cannot read, before anything is uploaded.

    Unlike the host-path route, this check is meaningful: the file is on the
    machine running the CLI, so what it sees is what will be sent.
    """
    if not path.is_file():
        raise ValueError(f"{flag} {path} is not a regular file")
    if path.stat().st_size == 0:
        raise ValueError(f"{flag} {path} is empty")


async def do_submit_reads(
    *,
    http: Any,
    flight_client: Any,
    token: str,
    prep_sample_idx: int,
    forward: Path,
    reverse: Path | None,
    watch: bool,
    poll_interval_seconds: float,
    timeout_seconds: float,
) -> dict:
    """Upload the reads and submit the ingest ticket. Injected clients so tests
    drive this without a live control plane, matching `do_reference_load`."""
    from ..reference_load import upload_file, watch_work_ticket

    kind = _classify(forward)
    if reverse is not None:
        if kind != "fastq":
            raise ValueError("--reverse-fastq is only meaningful with --fastq")
        if _classify(reverse) != "fastq":
            raise ValueError(f"--reverse-fastq {reverse} is not a FASTQ")

    uploads: dict[str, Any] = {}
    context: dict[str, Any] = {}

    forward_result = await upload_file(
        http=http,
        token=token,
        flight_client=flight_client,
        file_path=forward,
        role="reads",
    )
    uploads[forward.name] = forward_result
    if kind == "fastq":
        context["fastq_upload_idx"] = forward_result.upload_idx
        action_id, action_version = _FASTQ_ACTION_ID, _FASTQ_ACTION_VERSION
    else:
        context["bam_upload_idx"] = forward_result.upload_idx
        # The loader supports only unaligned basecaller uBAMs; declare it the
        # same way the PacBio fan-out does.
        context["expect_unaligned"] = True
        action_id, action_version = _BAM_ACTION_ID, _BAM_ACTION_VERSION

    if reverse is not None:
        reverse_result = await upload_file(
            http=http,
            token=token,
            flight_client=flight_client,
            file_path=reverse,
            role="reads",
        )
        uploads[reverse.name] = reverse_result
        context["reverse_fastq_upload_idx"] = reverse_result.upload_idx

    ticket_body = WorkTicketCreateRequest(
        action_id=action_id,
        action_version=action_version,
        scope_target={
            "kind": ScopeTargetKind.PREP_SAMPLE.value,
            "prep_sample_idx": prep_sample_idx,
        },
        action_context=context,
    ).model_dump(exclude_unset=True, mode="json")

    resp = await http.post(
        URL_WORK_TICKET_PREFIX,
        headers={"Authorization": f"Bearer {token}"},
        json=ticket_body,
    )
    if resp.status_code != 202:
        # The two the submitter can act on: 403 means this account may not name
        # a host path (it did not — worth surfacing verbatim rather than
        # translating), 422 carries the schema / filename-prefix detail.
        raise RuntimeError(
            f"POST {URL_WORK_TICKET_PREFIX} expected 202, got {resp.status_code}: {resp.text}"
        )
    ticket = resp.json()

    result: dict[str, Any] = {
        "action_id": action_id,
        "action_version": action_version,
        "prep_sample_idx": prep_sample_idx,
        "uploads": {
            name: {
                "upload_idx": r.upload_idx,
                "sha256": r.sha256,
                "bytes_received": r.bytes_received,
            }
            for name, r in uploads.items()
        },
        "work_ticket": ticket,
    }
    if watch:
        result["work_ticket"] = await watch_work_ticket(
            http,
            token,
            ticket["work_ticket_idx"],
            poll_interval_seconds=poll_interval_seconds,
            timeout_seconds=timeout_seconds,
        )
    return result


async def _run_submit_reads(*, base_url: str, token: str, args: argparse.Namespace) -> dict:
    """Build the real httpx + Flight clients and drive `do_submit_reads`. The
    handler stays a thin argparse shim; the entry point above takes injected
    clients so tests bypass this."""
    import httpx as _httpx
    import pyarrow.flight as flight

    flight_client = flight.FlightClient(args.data_plane_url)
    try:
        async with _httpx.AsyncClient(
            base_url=base_url, timeout=_common.CLI_HTTP_TIMEOUT_SECONDS
        ) as http:
            return await do_submit_reads(
                http=http,
                flight_client=flight_client,
                token=token,
                prep_sample_idx=args.prep_sample_idx,
                forward=args.fastq or args.bam,
                reverse=args.reverse_fastq,
                watch=not args.no_watch,
                poll_interval_seconds=args.poll_interval_seconds,
                timeout_seconds=args.timeout_seconds,
            )
    finally:
        flight_client.close()


def _handle_submit_reads(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Entry point for `qiita submit-reads`. Local validation runs before the
    PAT is read so a bad filename costs no network call; every known failure
    shape becomes exit 1 with a one-line stderr message."""
    import httpx as _httpx
    import pyarrow.flight as _flight

    if not args.data_plane_url:
        parser.error("--data-plane-url is required (the reads are streamed to the data plane)")

    forward = args.fastq or args.bam
    try:
        _check_local_file(forward, flag="--fastq" if args.fastq else "--bam")
        if args.reverse_fastq is not None:
            if args.bam is not None:
                parser.error("--reverse-fastq is only meaningful with --fastq")
            _check_local_file(args.reverse_fastq, flag="--reverse-fastq")
        _classify(forward)
    except ValueError as exc:
        parser.error(str(exc))

    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    try:
        result = asyncio.run(_run_submit_reads(base_url=args.base_url, token=token, args=args))
    except _httpx.HTTPStatusError as exc:
        print(f"http error {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
        return 1
    except _flight.FlightError as exc:
        # FlightError is not a RuntimeError subclass, so the catch-all below
        # would miss it and the operator would get a raw traceback.
        print(f"flight error: {exc}", file=sys.stderr)
        return 1
    except (RuntimeError, ValueError, TimeoutError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2, default=str))
    # Under --watch anything but COMPLETED exits 1, so a script wrapping this
    # distinguishes "reads loaded" from "ticket finished without loading any".
    state = (result.get("work_ticket") or {}).get("state")
    if not args.no_watch and state != WorkTicketState.COMPLETED.value:
        return 1
    return 0
