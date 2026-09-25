"""`qiita submit-ena-import` / `qiita ena-import-status` — batch ENA study import.

`submit-ena-import` POSTs a list of INSDC STUDY accessions (positional, or one
per line via `--from-file`) and by default watches the batch to terminal;
`ena-import-status` reads a batch's current per-item status. Both require
wet_lab_admin or system_admin (the route's own gate).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

from qiita_common.api_paths import (
    PATH_ENA_IMPORT_BATCH_BY_IDX,
    PATH_ENA_IMPORT_BATCH_PREFIX,
    URL_ENA_IMPORT_BATCH_BY_IDX,
    URL_ENA_IMPORT_BATCH_PREFIX,
)
from qiita_common.ena_accession import InvalidEnaAccessionError, validate_study_accession
from qiita_common.models.ena_import import BatchItemState

from .. import _common

_TERMINAL_STATES = (BatchItemState.DONE, BatchItemState.FAILED)


def _read_accessions_from_file(path: Path) -> list[str]:
    try:
        lines = path.read_text().splitlines()
    except OSError as exc:
        raise ValueError(f"--from-file {path}: {exc}") from exc
    stripped = (line.strip() for line in lines)
    accessions = [line for line in stripped if line and not line.startswith("#")]
    if not accessions:
        raise ValueError(f"--from-file {path} contains no accessions")
    return accessions


def _resolve_accessions(args: argparse.Namespace, parser: argparse.ArgumentParser) -> list[str]:
    if args.from_file is not None:
        if args.accessions:
            parser.error("accessions and --from-file are mutually exclusive")
        try:
            return _read_accessions_from_file(args.from_file)
        except ValueError as exc:
            parser.error(str(exc))
    if not args.accessions:
        parser.error("provide at least one accession, positionally or via --from-file")
    return args.accessions


def _announce(batch_idx: int) -> None:
    print(
        f"ena_import_batch {batch_idx} submitted; poll with `qiita ena-import-status {batch_idx}`",
        file=sys.stderr,
    )


def _report_state_change(batch_idx: int, item: dict) -> None:
    accession, state = item["ena_study_accession"], item["state"]
    detail = ""
    if state == BatchItemState.FAILED and item.get("failure_reason"):
        detail = f" ({item['failure_reason']})"
    elif item.get("download_work_ticket_idxs"):
        detail = f" tickets={item['download_work_ticket_idxs']}"
    print(f"ena_import_batch {batch_idx}: {accession} -> {state}{detail}", file=sys.stderr)


async def _watch_ena_import_batch(
    http: Any,
    token: str,
    batch_idx: int,
    *,
    poll_interval_seconds: float,
    timeout_seconds: float,
) -> dict:
    """Poll until every item is `done` or `failed`. Raises TimeoutError after
    `timeout_seconds`; returns the final status body otherwise."""
    url = URL_ENA_IMPORT_BATCH_BY_IDX.format(ena_import_batch_idx=batch_idx)
    deadline = time.monotonic() + timeout_seconds
    last_states: dict[str, str] = {}
    while True:
        resp = await http.get(url, headers={"Authorization": f"Bearer {token}"})
        resp.raise_for_status()
        body = resp.json()
        items = body["items"]
        for item in items:
            accession = item["ena_study_accession"]
            if last_states.get(accession) != item["state"]:
                last_states[accession] = item["state"]
                _report_state_change(batch_idx, item)
        if all(item["state"] in _TERMINAL_STATES for item in items):
            return body
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"ena_import_batch {batch_idx} did not reach a terminal state"
                f" within {timeout_seconds:.0f}s"
            )
        await asyncio.sleep(poll_interval_seconds)


async def do_submit_ena_import(
    *,
    http: Any,
    token: str,
    accessions: list[str],
    watch: bool,
    poll_interval_seconds: float,
    timeout_seconds: float,
) -> dict:
    """POST the batch and, by default, watch it to terminal. Injected client
    so tests drive this without a live control plane, matching
    `do_submit_reads`."""
    resp = await http.post(
        URL_ENA_IMPORT_BATCH_PREFIX,
        headers={"Authorization": f"Bearer {token}"},
        json={"accessions": accessions},
    )
    if resp.status_code != 202:
        raise RuntimeError(
            f"POST {URL_ENA_IMPORT_BATCH_PREFIX} expected 202, got {resp.status_code}: {resp.text}"
        )
    body = resp.json()
    _announce(body["ena_import_batch_idx"])
    if not watch:
        return body
    return await _watch_ena_import_batch(
        http,
        token,
        body["ena_import_batch_idx"],
        poll_interval_seconds=poll_interval_seconds,
        timeout_seconds=timeout_seconds,
    )


async def _run_submit_ena_import(
    *,
    base_url: str,
    token: str,
    accessions: list[str],
    watch: bool,
    poll_interval_seconds: float,
    timeout_seconds: float,
) -> dict:
    import httpx as _httpx

    async with _httpx.AsyncClient(
        base_url=base_url, timeout=_common.CLI_HTTP_TIMEOUT_SECONDS
    ) as http:
        return await do_submit_ena_import(
            http=http,
            token=token,
            accessions=accessions,
            watch=watch,
            poll_interval_seconds=poll_interval_seconds,
            timeout_seconds=timeout_seconds,
        )


def _handle_submit_ena_import(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    import httpx as _httpx

    accessions = _resolve_accessions(args, parser)
    for accession in accessions:
        try:
            validate_study_accession(accession)
        except InvalidEnaAccessionError as exc:
            parser.error(str(exc))

    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    try:
        result = asyncio.run(
            _run_submit_ena_import(
                base_url=args.base_url,
                token=token,
                accessions=accessions,
                watch=not args.no_watch,
                poll_interval_seconds=args.poll_interval_seconds,
                timeout_seconds=args.timeout_seconds,
            )
        )
    except _httpx.HTTPStatusError as exc:
        print(f"http error {exc.response.status_code}: {exc.response.text}", file=sys.stderr)
        return 1
    except _httpx.RequestError as exc:
        print(f"error: could not reach the control plane: {exc!r}", file=sys.stderr)
        return 1
    except (RuntimeError, ValueError, TimeoutError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(result, indent=2, default=str))
    watching_failed = not args.no_watch and any(
        item["state"] == BatchItemState.FAILED for item in result["items"]
    )
    return 1 if watching_failed else 0


def _get_ena_import_batch(base_url: str, token: str, ena_import_batch_idx: int) -> dict:
    """GET /api/v1/ena-import-batch/{idx}. Auth: wet_lab_admin or system_admin."""
    by_idx = PATH_ENA_IMPORT_BATCH_BY_IDX.format(ena_import_batch_idx=ena_import_batch_idx)
    path = f"{PATH_ENA_IMPORT_BATCH_PREFIX}{by_idx}"
    return _common.call("GET", base_url, token, path)


def _handle_ena_import_status(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Read a batch's current status. Exits 0 even with a failed item — the
    read succeeded; the caller inspects each item's `state`."""
    return _common.run_http_subcommand(
        lambda t: _get_ena_import_batch(args.base_url, t, args.ena_import_batch_idx)
    )
