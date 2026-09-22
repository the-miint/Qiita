"""Re-key mask_definition rows onto the current adapter-identity derivation.

`params.resolved_qc.adapter_set_hash` used to be the SHA-256 of the serialized
adapter Parquet. The pyarrow writer stamps its version into the file footer, so
that digest was a function of the writer version as well as the sequences, and a
re-derivation under a different pyarrow resolved to a different mask. It is now
the SHA-256 of the reference's sorted `qiita.feature.sequence_hash` values
(`repositories.reference_membership.reference_sequence_set_hash`), which the mint stamps as
`adapter_hash_scheme = 'sequence_hash_v1'`.

`qiita.mint_mask_definition` converts a row the moment something mints its config
again (it falls back to the legacy hash and re-keys in place). This backfill
converts the rest — a config nothing re-submits would otherwise stay on the old
derivation, and the contract phase that deletes the fallback reads the column
being free of NULLs as its go-ahead.

**Attribution without the bytes.** A stored row records the adapter *hash*, not
the reference it came from, so a row cannot be re-keyed unless we know which
adapter set produced it. Recomputing the legacy digest to check would need the
adapter Parquet, i.e. a data-plane DoGet — the dependency this migration exists
to remove. Instead the rows are grouped by their stored hash: when they all carry
ONE value, that value is the single canonical adapter set (the deploy has one
`QIITA_DEFAULT_ADAPTER_REFERENCE_IDX`) and the mapping is settled. When there is
more than one, the extra values are REPORTED and nothing is written — attributing
them needs the bytes, and per-protocol adapter sets are the case that produces
them.

Contract per this package: dry-run by default, idempotent (a stamped row is out
of the query), and residue is reported rather than guessed at.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import asyncpg
from qiita_common.hashing import canonical_params_hash

from ..repositories.mask_definition import (
    ADAPTER_HASH_SCHEME_SEQUENCE_HASH,
    ADAPTER_SET_HASH_JSON_PATH,
    ADAPTER_SET_HASH_KEY,
    RESOLVED_QC_KEY,
)
from ..repositories.reference_membership import reference_sequence_set_hash

# Rows still on the legacy derivation AND carrying an adapter set. A NULL
# adapter_set_hash means the config uses no adapter set (PacBio, or a deploy with
# no configured reference); the two derivations agree on None there, so those
# rows are not part of this migration and keep a NULL scheme.
_LEGACY_ROWS_SQL = (
    f"SELECT mask_idx, params, params_hash,"
    f"       params->{ADAPTER_SET_HASH_JSON_PATH} AS adapter_set_hash"
    "  FROM qiita.mask_definition"
    " WHERE adapter_hash_scheme IS NULL"
    f"   AND params->{ADAPTER_SET_HASH_JSON_PATH} IS NOT NULL"
    "   AND ($1::bigint[] IS NULL OR mask_idx = ANY($1::bigint[]))"
    " ORDER BY mask_idx"
)


@dataclass(frozen=True)
class RekeyRow:
    """One row the apply step would touch: its mask_idx, the params_hash it held
    when the plan read it, and the one it lands on once its adapter_set_hash is
    the current one. The two are equal for a `stamp_only` row."""

    mask_idx: int
    planned_params_hash: bytes
    rekeyed_params: dict
    rekeyed_params_hash: bytes


@dataclass(frozen=True)
class RekeyPlan:
    """What the backfill would write, what would collide, and what it cannot
    attribute.

    `stamp_only` already store the current hash and need only the scheme stamp.
    Pre-column code cannot produce one — it stored the byte digest by
    construction; what does is the mint's fast path, which returns an existing
    row without stamping it, and a migrate:down/up round trip, which drops the
    column and its values. `convertible`
    carry the one legacy hash and need params and params_hash rewritten too.
    `collided` land on a params_hash another row already holds, or share one with
    a sibling in this plan — either way two mask_idx values would describe one
    config. `unattributable` maps every other stored hash to the mask_idx values
    carrying it.

    Only `stamp_only` and `convertible` are written; the other two are the report.
    """

    reference_idx: int
    current_hash: str
    stamp_only: list[RekeyRow] = field(default_factory=list)
    convertible: list[RekeyRow] = field(default_factory=list)
    collided: list[int] = field(default_factory=list)
    unattributable: dict[str, list[int]] = field(default_factory=dict)

    def writable(self) -> list[RekeyRow]:
        """Every row the apply step writes, in mask_idx order."""
        return sorted([*self.stamp_only, *self.convertible], key=lambda r: r.mask_idx)

    def blocked(self) -> bool:
        """True when the report carries something an operator must decide before
        writing. `apply_rekey` refuses in that state rather than converting the
        rows it happens to be sure about."""
        return bool(self.unattributable or self.collided)


def _decode_params(params) -> dict:
    """asyncpg returns jsonb as str unless a codec is registered; decode
    explicitly so this does not depend on the connection's codec set."""
    return json.loads(params) if isinstance(params, str) else params


async def plan_rekey(
    pool: asyncpg.Pool,
    *,
    reference_idx: int,
    mask_idxs: list[int] | None = None,
    attribute_all: bool = False,
) -> RekeyPlan:
    """Compute the re-key plan for `reference_idx`, writing nothing.

    Every value the apply step writes is computed here, including the collision
    check — so the dry-run report is the report of exactly what a subsequent
    `--execute` writes.

    `mask_idxs` restricts which rows the plan considers. On its own it changes
    nothing about attribution: the rows still have to carry one distinct hash to
    be convertible.

    `attribute_all` is the operator asserting that every row in scope was minted
    from `reference_idx`, so they all convert however many distinct hashes they
    carry. That is the way out of an unattributable report — the grouping
    heuristic cannot settle a deploy that has held more than one adapter set, and
    without an explicit assertion the residue would never be convertible and the
    contract phase never reachable. It requires `mask_idxs`: asserting over
    "whatever is in the table" is not a decision anyone can check.

    Raises when the reference names no sequences: there is no identity to re-key
    onto, and a digest over no input would be one value shared by every
    memberless reference.
    """
    if attribute_all and mask_idxs is None:
        raise RuntimeError("attribute_all needs an explicit mask_idxs list to attribute")
    current_hash = await reference_sequence_set_hash(pool, reference_idx)
    if current_hash is None:
        raise RuntimeError(
            f"adapter reference {reference_idx} has no rows in "
            "qiita.reference_membership, so it names no adapter sequences to "
            "derive an identity from"
        )

    stamp_only_rows: list[asyncpg.Record] = []
    by_hash: dict[str, list[asyncpg.Record]] = {}
    for row in await pool.fetch(_LEGACY_ROWS_SQL, mask_idxs):
        if row["adapter_set_hash"] == current_hash:
            stamp_only_rows.append(row)
        else:
            by_hash.setdefault(row["adapter_set_hash"], []).append(row)

    if attribute_all:
        # The operator named these rows as this reference's, so every one
        # converts and nothing is left unattributed.
        convertible_rows = [r for rows in by_hash.values() for r in rows]
        by_hash = {}
    else:
        # A single distinct stored hash is the one canonical adapter set; more
        # than one cannot be told apart without the adapter bytes, so none
        # convert.
        convertible_rows = by_hash.popitem()[1] if len(by_hash) == 1 else []

    stamp_only = [_rekey_row(r, current_hash) for r in stamp_only_rows]
    convertible = [_rekey_row(r, current_hash) for r in convertible_rows]

    # Anything that would raise 23505 at write time, resolved here so the dry run
    # reports it. Two sources, and the second is not a DB question:
    #
    #   * the target hash already belongs to another row (the config was minted
    #     under the current derivation too), and
    #   * two rows IN THIS PLAN target the same hash. That is the pyarrow-bump
    #     shape: one config minted either side of a writer bump carries two byte
    #     digests, and unifying the adapter hash merges them. Only reachable via
    #     `attribute_all`, since otherwise two stored hashes are unattributable.
    #
    # Both mean two mask_idx values describe one config, which is a decision about
    # which survives (and what repoints onto it), not something to write.
    planned = [*stamp_only, *convertible]
    taken = await _params_hashes_in_use(pool, [r.rekeyed_params_hash for r in planned])
    by_target: dict[bytes, list[int]] = {}
    for r in planned:
        by_target.setdefault(r.rekeyed_params_hash, []).append(r.mask_idx)
    collided = sorted(
        r.mask_idx
        for r in planned
        if taken.get(r.rekeyed_params_hash, r.mask_idx) != r.mask_idx
        or len(by_target[r.rekeyed_params_hash]) > 1
    )
    collided_set = set(collided)

    return RekeyPlan(
        reference_idx=reference_idx,
        current_hash=current_hash,
        stamp_only=[r for r in stamp_only if r.mask_idx not in collided_set],
        convertible=[r for r in convertible if r.mask_idx not in collided_set],
        collided=collided,
        unattributable={h: [r["mask_idx"] for r in rows] for h, rows in by_hash.items()},
    )


def _rekey_row(row: asyncpg.Record, current_hash: str) -> RekeyRow:
    """The row as it would be after conversion. A `stamp_only` row already stores
    `current_hash`, so its params and params_hash come out unchanged."""
    params = _decode_params(row["params"])
    params[RESOLVED_QC_KEY][ADAPTER_SET_HASH_KEY] = current_hash
    return RekeyRow(
        mask_idx=row["mask_idx"],
        planned_params_hash=bytes(row["params_hash"]),
        rekeyed_params=params,
        rekeyed_params_hash=canonical_params_hash(params),
    )


async def _params_hashes_in_use(pool: asyncpg.Pool, params_hashes: list[bytes]) -> dict[bytes, int]:
    """params_hash → the mask_idx currently holding it, for the given hashes."""
    if not params_hashes:
        return {}
    rows = await pool.fetch(
        "SELECT mask_idx, params_hash FROM qiita.mask_definition"
        " WHERE params_hash = ANY($1::bytea[])",
        params_hashes,
    )
    return {bytes(r["params_hash"]): r["mask_idx"] for r in rows}


async def apply_rekey(pool: asyncpg.Pool, plan: RekeyPlan) -> int:
    """Apply `plan`'s writable rows; returns how many were written.

    Refuses a plan carrying residue (`RekeyPlan.blocked`): an unattributable hash
    means the single-canonical-adapter-set premise does not hold on this deploy,
    which also puts the `convertible` attribution in doubt, and a collision needs
    a decision about which mask_idx survives. Both are operator calls, so nothing
    is written until they are resolved.

    Each row is written in its own transaction with the row locked, so the mint's
    own in-place re-key cannot land between the plan's read and this write. Two
    things are skipped rather than crashing, both reachable while the CP serves
    traffic: a row deleted since the plan (`qiita-admin mask delete`), and one
    whose params_hash has moved since the plan read it — the mint re-keyed it
    first, so the plan's rewritten params describe a stale blob.

    Returns the count of rows actually written, which is why a skip is not a
    silent no-op: the caller reports it against `len(plan.writable())`.
    """
    if plan.blocked():
        raise RuntimeError(
            "refusing to write: the plan carries "
            f"{sum(map(len, plan.unattributable.values()))} unattributable row(s) and "
            f"{len(plan.collided)} collision(s); resolve them before re-running"
        )
    written = 0
    async with pool.acquire() as conn:
        for row in plan.writable():
            async with conn.transaction():
                locked = await conn.fetchrow(
                    "SELECT params_hash FROM qiita.mask_definition WHERE mask_idx = $1 FOR UPDATE",
                    row.mask_idx,
                )
                if locked is None:
                    continue
                await conn.execute(
                    "UPDATE qiita.mask_definition"
                    "   SET params = $1::jsonb, params_hash = $2, adapter_hash_scheme = $3"
                    " WHERE mask_idx = $4",
                    json.dumps(row.rekeyed_params),
                    row.rekeyed_params_hash,
                    ADAPTER_HASH_SCHEME_SEQUENCE_HASH,
                    row.mask_idx,
                )
            written += 1
    return written
