"""qiita-admin CLI — work-ticket backfill-mask-idx subcommand.

Split out of the former single-file ``cli.admin`` module; behavior unchanged.
"""

import argparse
import asyncio
import base64
import os
import sys
import tempfile
from pathlib import Path

import asyncpg

from qiita_control_plane.runner import backfill_work_ticket_mask_idx

from ._helpers import _DB_CONNECT_TIMEOUT_SECONDS

# ---------------------------------------------------------------------------
# work-ticket backfill-mask-idx — one-time idempotent mask_idx backfill
# ---------------------------------------------------------------------------


def _decode_hmac_secret() -> bytes:
    """Decode HMAC_SECRET_KEY (base64) the same way Settings.from_env does — the
    backfill re-materializes the canonical adapter set via a signed Flight ticket,
    so it needs the same signing key the CP boots with. Mirror from_env's
    >=16-byte floor so a too-short key is rejected here too (it would otherwise
    sign tickets the data plane refuses)."""
    raw = os.environ.get("HMAC_SECRET_KEY")
    if not raw:
        raise RuntimeError("HMAC_SECRET_KEY not set")
    try:
        secret = base64.b64decode(raw)
    except Exception as exc:  # noqa: BLE001 — surface the decode reason
        raise RuntimeError("HMAC_SECRET_KEY must be valid base64") from exc
    if len(secret) < 16:
        raise RuntimeError("HMAC_SECRET_KEY must decode to at least 16 bytes")
    return secret


def _parse_optional_adapter_ref() -> int | None:
    """Read QIITA_DEFAULT_ADAPTER_REFERENCE_IDX (the canonical adapter set the
    mask hash covers). Optional: a deploy without it minted maskless configs, and
    the backfill then derives params with adapter_set_hash=None."""
    raw = os.environ.get("QIITA_DEFAULT_ADAPTER_REFERENCE_IDX")
    if raw is None or raw == "":
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(
            f"QIITA_DEFAULT_ADAPTER_REFERENCE_IDX must be an integer, got {raw!r}"
        ) from exc
    if value <= 0:
        raise RuntimeError(f"QIITA_DEFAULT_ADAPTER_REFERENCE_IDX must be positive, got {value}")
    return value


async def _backfill_mask_idx(database_url: str, *, apply: bool) -> dict:
    """Acquire a pool, re-derive each eligible ticket's mask params, look it up,
    and (when apply) populate work_ticket.mask_idx. The adapter set is
    re-materialized into a throwaway temp workspace (only its bytes are hashed)."""
    data_plane_url = os.environ.get("DATA_PLANE_URL", "grpc://localhost:50051")
    default_adapter_reference_idx = _parse_optional_adapter_ref()
    # The HMAC key only signs the adapter-fetch Flight ticket; a maskless deploy
    # (no adapter reference configured) never re-materializes adapters, so require
    # the key only when it would actually be used.
    hmac_secret = _decode_hmac_secret() if default_adapter_reference_idx is not None else b""
    try:
        pool = await asyncpg.create_pool(
            database_url, timeout=_DB_CONNECT_TIMEOUT_SECONDS, min_size=1, max_size=4
        )
    except Exception as exc:  # noqa: BLE001 — show full reason, including OS errors
        raise RuntimeError(
            f"could not connect to DATABASE_URL: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        with tempfile.TemporaryDirectory(prefix="qiita-backfill-mask-") as tmp:
            return await backfill_work_ticket_mask_idx(
                pool,
                workspace=Path(tmp),
                default_adapter_reference_idx=default_adapter_reference_idx,
                data_plane_url=data_plane_url,
                hmac_secret=hmac_secret,
                apply=apply,
            )
    finally:
        await pool.close()


def _handle_work_ticket_backfill_mask_idx(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("error: DATABASE_URL not set", file=sys.stderr)
        return 2
    try:
        report = asyncio.run(_backfill_mask_idx(database_url, apply=args.apply))
    except (RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    mode = "APPLIED" if report["applied"] else "DRY-RUN (no writes; pass --apply to commit)"
    counted = report["counted"]
    populated = report["populated"]
    skipped_no_mask = len(report["skipped_no_mask"])
    print(f"backfill-mask-idx [{mode}]")
    print(f"  counted (mask_idx IS NULL, mask-bearing actions): {counted}")
    print(f"  populated:           {populated}")
    print(f"  skipped (no matching mask): {skipped_no_mask}")
    print(f"  skipped (not prep_sample):  {len(report['skipped_not_prep_sample'])}")
    if report["skipped_no_mask"]:
        print(f"  skipped-no-mask ticket idxs: {report['skipped_no_mask']}")
    if report["skipped_not_prep_sample"]:
        print(f"  skipped-not-prep-sample ticket idxs: {report['skipped_not_prep_sample']}")
    # The backfill matches a ticket only when its re-derived mask params hash to an
    # already-minted mask. A serialization / config / adapter-writer drift between
    # this run and the original mint would make EVERY real ticket miss the lookup
    # and land in skipped_no_mask instead of populated — a silent no-op that looks
    # like success. Before trusting an --apply, verify populated > 0 and that
    # skipped_no_mask is the small residue you expect (tickets that genuinely
    # failed before minting), not the bulk of the candidates.
    if counted > 0 and populated == 0:
        print(
            "  WARNING: candidate tickets exist but NONE matched a mask — this"
            " likely indicates a hash-repro drift (serialization / adapter writer /"
            " config), not 'nothing to do'. Do NOT --apply until resolved."
        )
    elif not report["applied"]:
        print(
            "  Before running --apply: confirm populated > 0 and skipped_no_mask is"
            " expected-small; an unexpected all-skipped result means a hash-repro"
            " drift, not real work to skip."
        )
    return 0
