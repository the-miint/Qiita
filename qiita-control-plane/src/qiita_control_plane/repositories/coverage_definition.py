"""Mint / look up the CP-side identity of a coverage measurement.

`coverage_idx` tags every row of the data plane's `qiita_lake.coverage` feature table.
Same params-hash identity as `mask_idx` and `alignment_idx`, deduplicated fleet-wide on
the canonical-JSON SHA-256 of the config, so the same measurement always resolves to one
idx and re-running is idempotent rather than double-counting.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg
from qiita_common.hashing import canonical_params_hash

_RETURNING = "coverage_idx, params_hash, params, created_by_idx, created_at"


def build_coverage_params(
    *,
    reference_idx: int,
    aligner: str,
    preset: str,
    min_identity: float,
    min_aligned_fraction: float,
    depth_mode: str,
    mask_idx: int | None,
) -> dict[str, Any]:
    """The canonical config blob a `coverage_idx` is the identity OF.

    The single source of truth for the hash shape. Every key here changes the NUMBER in
    the feature table:

    * `reference_idx` — different reference, different features.
    * `aligner` / `preset` — how reads were placed on the parent.
    * `min_identity` / `min_aligned_fraction` — the measurement gate: which reads
      contribute bases at all.
    * `depth_mode` — whether a deleted reference position inside the feature counts as
      covered. It measurably moves the number.
    * `mask_idx` — which reads were measured.

    A knob that moves the number and is NOT here is the failure this identity exists to
    prevent: the job would compute differently while the idx stayed the same, so new rows
    would land under a coverage_idx whose stored params describe the OLD measurement.
    """
    return {
        "reference_idx": reference_idx,
        "aligner": aligner,
        "preset": preset,
        "min_identity": min_identity,
        "min_aligned_fraction": min_aligned_fraction,
        "depth_mode": depth_mode,
        "mask_idx": mask_idx,
    }


async def mint_coverage_definition(
    conn: asyncpg.Connection | asyncpg.Pool,
    params: dict[str, Any],
    principal_idx: int,
) -> asyncpg.Record:
    """Mint-or-get the `coverage_idx` for `params`. Race-safe (the plpgsql function does
    ON CONFLICT DO NOTHING + re-select), so two concurrent tickets for the same config
    converge on one idx rather than minting two."""
    return await conn.fetchrow(
        f"SELECT {_RETURNING} FROM qiita.mint_coverage_definition($1, $2::jsonb, $3)",
        canonical_params_hash(params),
        json.dumps(params, sort_keys=True, separators=(",", ":")),
        principal_idx,
    )


async def lookup_coverage_idx_by_params(
    conn: asyncpg.Connection | asyncpg.Pool,
    params: dict[str, Any],
) -> int | None:
    """The `coverage_idx` for `params`, or None if never minted. A pure lookup — a
    consumer that wants to READ a measurement must never mint one as a side effect."""
    return await conn.fetchval(
        "SELECT coverage_idx FROM qiita.coverage_definition WHERE params_hash = $1",
        canonical_params_hash(params),
    )


async def fetch_coverage_definition_by_idx(
    conn: asyncpg.Connection | asyncpg.Pool,
    coverage_idx: int,
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        f"SELECT {_RETURNING} FROM qiita.coverage_definition WHERE coverage_idx = $1",
        coverage_idx,
    )
