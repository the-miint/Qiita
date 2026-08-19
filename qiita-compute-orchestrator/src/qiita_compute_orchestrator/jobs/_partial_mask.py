"""Shared guard for the read-mask chain's long-read steps.

The read-mask chain threads a partial mask (`(sequence_idx, reason, left_trim1,
right_trim1, left_trim2, right_trim2)`) through its pre-`host_filter` steps:
`syndna -> lima -> qc`. Each step optionally consumes the prior step's mask,
re-classifies only its still-`pass` rows, and carries every non-`pass` row verbatim.

**What is deliberately NOT guarded here.** The mask's shape — one row per read, and
trims that fit inside the read — is established BY CONSTRUCTION by the only two
things that produce it, so re-checking it at every consumer would be guarding our
own code against itself:

  * `syndna` emits `reads LEFT JOIN hits` with a DISTINCT hit set: exactly one row
    per read, trims literally `0`.
  * `lima_mask` emits miint's `infer_trim`, which returns one row per ORIGINAL read
    and fails loud unless the clipped read is a contiguous substring of it — so the
    trims cannot exceed the read.

Those invariants are pinned at the producers (`test_syndna.py`,
`test_lima_chain_smoke.py`), which is where they are actually established. A runtime
re-check at each consumer buys nothing a unit test does not already buy, and it
implies a distrust of our own prior step that the code does not otherwise have.

What DOES stay is the one condition our construction does not establish: the gate
combination is CLIENT-supplied (`action_context`), so a caller can ask for the
long-read chain over a read set its seams cannot serve. That is a real boundary.

Note `assert_single_end` is called UNCONDITIONALLY by `syndna`, `lima_export` and `qc`
— not only when a mask is bound. The whole long-read chain is single-end-only (PacBio
HiFi), so the check is about the CHAIN, not about the mask.
"""

from __future__ import annotations

from pathlib import Path

import duckdb


def assert_single_end(
    conn: duckdb.DuckDBPyConnection, reads_relation: str, field: str, source: Path | str
) -> None:
    """Reject a paired-end read set on a long-read-chain step.

    NOT defensive programming against our own steps — this guards a CLIENT-supplied
    combination. `syndna_enabled` / `lima_enabled` arrive in `action_context`, and
    nothing cross-validates them against the pool's platform, so a submission can turn
    the long-read chain on over a paired-end (Illumina) read set.

    The chain is single-end-only throughout: lima is a HiFi tool, and the incoming-mask
    seams fold trims back into `sequence1`/`qual1` only — PE would need per-mate
    `in_left2`/`in_right2` math that nothing produces today, so those rows would take
    the PE seam and their incoming trims would be SILENTLY DROPPED. `syndna` calls this
    FIRST (it is the chain's first step), so a bad submission dies before a full
    alignment pass rather than after it.

    Fail loudly at the boundary instead of shipping an untested path.

    `reads_relation` is the caller's already-bound reads relation (see
    `read_source.bind_step_reads`), not a filesystem path — so this check reads
    the SAME rows the job will process regardless of whether they came from a
    staged Parquet or a data-plane stream. `source` is used only to name the
    offending input in the error message.
    """
    (pe_rows,) = conn.execute(
        f"SELECT count(*) FROM {reads_relation} WHERE sequence2 IS NOT NULL"
    ).fetchone()
    if pe_rows:
        raise ValueError(
            f"{field} is bound ({source}) but reads contain {pe_rows} paired-end "
            "row(s); an incoming mask is single-end only (long reads)"
        )
