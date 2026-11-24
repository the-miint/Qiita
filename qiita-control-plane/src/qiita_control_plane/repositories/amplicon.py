"""The amplicon completion gate, `qiita.amplicon_sample`.

One row per (processing_idx, prep_sample): 'pending' for the pool's population
when the runner mints the run identity, then 'completed' or 'no_data' when the
run's terminal `finalize-amplicon-sample` action closes it. Written a pool at a
time because an amplicon ticket covers a whole sequenced pool. How many ASVs a
sample carries is a count over the lake's `amplicon_membership`, not kept here.
"""

from __future__ import annotations

import asyncpg
from qiita_common.models import AmpliconSampleState

from . import gate_state_literal, require_transaction

AMPLICON_SAMPLE_PENDING = gate_state_literal("pending", AmpliconSampleState)
AMPLICON_SAMPLE_COMPLETED = gate_state_literal("completed", AmpliconSampleState)
AMPLICON_SAMPLE_NO_DATA = gate_state_literal("no_data", AmpliconSampleState)


class AmpliconSampleNotCompleteError(RuntimeError):
    """prep_samples whose `amplicon_sample` gate is not 'completed', ascending."""

    def __init__(self, processing_idx: int, not_complete: list[int]) -> None:
        self.processing_idx = processing_idx
        self.not_complete = not_complete
        super().__init__(
            f"{len(not_complete)} prep_sample(s) are not completed under processing"
            f" {processing_idx}: {not_complete}"
        )


async def assert_pool_samples_completed(
    conn: asyncpg.Connection, *, processing_idx: int, prep_sample_idxs: list[int]
) -> None:
    """Raise `AmpliconSampleNotCompleteError` unless every `prep_sample_idxs` row is
    'completed' under this run. Holds the processing row `FOR SHARE` so the gate
    cannot move under a mint the caller runs next in the same transaction — the
    mask kind guards its mint the same way (`mint_exported_mask_identifiers`)."""
    require_transaction(conn)
    cohort = sorted(set(prep_sample_idxs))
    await conn.execute(
        "SELECT 1 FROM qiita.processing WHERE processing_idx = $1 FOR SHARE", processing_idx
    )
    completed = {
        r["prep_sample_idx"]
        for r in await conn.fetch(
            "SELECT prep_sample_idx FROM qiita.amplicon_sample"
            " WHERE processing_idx = $1 AND prep_sample_idx = ANY($2::bigint[]) AND state = $3",
            processing_idx,
            cohort,
            AMPLICON_SAMPLE_COMPLETED,
        )
    }
    not_complete = [idx for idx in cohort if idx not in completed]
    if not_complete:
        raise AmpliconSampleNotCompleteError(processing_idx, not_complete)


async def create_amplicon_samples_pending(
    conn: asyncpg.Connection, *, processing_idx: int, prep_sample_idxs: list[int]
) -> None:
    """Materialize the run's gate rows at 'pending' for `prep_sample_idxs`.

    Idempotent, and asymmetric on re-entry: a row a previous run of the same
    identity left 'no_data' reopens to 'pending' (this run has not finished), while
    one it left 'completed' stays — that run's ASVs are already in the lake, and a
    later run counting none for the sample must not remove them."""
    require_transaction(conn)
    await conn.execute(
        "INSERT INTO qiita.amplicon_sample (processing_idx, prep_sample_idx, state)"
        " SELECT $1, unnest($2::bigint[]), $3"
        " ON CONFLICT (processing_idx, prep_sample_idx) DO UPDATE SET state = $3"
        "   WHERE qiita.amplicon_sample.state = $4",
        processing_idx,
        prep_sample_idxs,
        AMPLICON_SAMPLE_PENDING,
        AMPLICON_SAMPLE_NO_DATA,
    )


async def close_amplicon_samples(
    conn: asyncpg.Connection,
    *,
    processing_idx: int,
    prep_sample_idxs: list[int],
    completed_prep_sample_idxs: list[int],
) -> tuple[int, int]:
    """Close one ticket's share of the run's gate: 'completed' for every sample in
    `completed_prep_sample_idxs` (the ones denoise counted ASVs for), 'no_data' for
    every other still-'pending' row among `prep_sample_idxs`, the ticket's pool
    population. Returns (completed, no_data) written.

    Scoped to the pool because the run identity is the denoise knobs, not the
    pool: two pools denoised alike share a processing_idx, and closing every
    pending row of it would mark the other pool's samples 'no_data' while its
    ticket is still running.

    An upsert for the counted samples, so a sample with no pending row (the run
    reached it some other way) is still recorded."""
    require_transaction(conn)
    completed = await conn.fetchval(
        "WITH written AS ("
        "  INSERT INTO qiita.amplicon_sample (processing_idx, prep_sample_idx, state)"
        "  SELECT $1, unnest($2::bigint[]), $3"
        "  ON CONFLICT (processing_idx, prep_sample_idx) DO UPDATE SET state = $3"
        "  RETURNING 1"
        ") SELECT count(*) FROM written",
        processing_idx,
        completed_prep_sample_idxs,
        AMPLICON_SAMPLE_COMPLETED,
    )
    no_data = await conn.fetchval(
        "WITH written AS ("
        "  UPDATE qiita.amplicon_sample SET state = $3"
        "   WHERE processing_idx = $1 AND state = $2"
        "     AND prep_sample_idx = ANY($5::bigint[])"
        "     AND prep_sample_idx <> ALL($4::bigint[])"
        "  RETURNING 1"
        ") SELECT count(*) FROM written",
        processing_idx,
        AMPLICON_SAMPLE_PENDING,
        AMPLICON_SAMPLE_NO_DATA,
        completed_prep_sample_idxs,
        prep_sample_idxs,
    )
    return completed, no_data
