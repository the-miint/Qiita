# Design draft: full reads + downstream mask (replacing destructive host filtering)

> Status: **draft for discussion**. Not yet accepted. No code written.
> Author: (you) · Date: 2026-06-23 · Revised after adversarial review.

## 0. Key decisions (all resolved)

Adversarial review against the live code found two assumptions that didn't hold
and one unverified pillar. The clear-cut bugs are fixed inline (PE trim §5.1,
qual-slice/length invariants §4, `_r1r2` counts §5, the missed
`_DOGET_ALLOWED_TABLES` and the `register_files` no-whitelist correction §4, the
"no live consumer" reality §7). The three judgement calls are now decided:

- **D1 — mask identity. RESOLVED: narrow `mask_idx`.** Multiple masks per sample
  must coexist (e.g. human-filter vXXX and vYYY queryable side by side over the
  same reads). `processing_idx`/`processed_prep_sample_idx` **do not exist**
  (documented intent only; `routes/sequencing_run.py` says so verbatim), so we
  introduce a small CP-minted **`mask_idx`** identifying a *filtering config*
  (filter workflow + version + host reference(s) + QC params), deduped on a
  `SHA-256(params)` hash in a new `qiita.mask_definition` table. Same config →
  same `mask_idx` fleet-wide, so "sample S under vXXX" is well-defined. The join
  itself needs no new id: `sequence_idx` is already globally unique
  (`mint_sequence_range` draws one `sequence_idx_seq`, disjoint ranges per
  sample). `mask_idx` is purely the *which-mask* discriminator. (If the full
  `processing_idx` hierarchy is built later, a `mask_definition` row maps onto
  it.) Threaded through §3–§6.
- **D2 — DuckLake view support. RESOLVED: works (spiked).** Verified on DuckDB
  **1.5.4** — the exact version the data plane links (`duckdb` crate
  `1.10504.0`, `Cargo.lock`). A `CREATE VIEW` *inside* the attached DuckLake
  catalog (a) succeeds, (b) **persists across re-attach** (catalog-stored, not
  session-only), and (c) the §4 view returned the correct trimmed read
  (`substr` + list-slice math) with the `host_*` row excluded by
  `WHERE reason='pass'`. So §4 stands as written — a persistent catalog view; no
  fallback to DP-built SQL needed. (The in-memory-session view also works, so a
  future DuckDB regression still has a path.) **One residual:** the spike used a
  *file-backed* DuckLake catalog; production attaches a **Postgres** catalog
  (`ducklake.rs:44`). View-definition persistence is the catalog's job, so re-run
  the same spike against a Postgres-backed catalog in PR 2 to confirm
  catalog-stored persistence there (the in-memory-session fallback is
  backend-independent regardless, so the design is safe either way).
- **D3 — raw read access surface. RESOLVED: out of Flight entirely.**
  `read`/`read_mask` are in **neither** `ALLOWED_TABLES` nor `_DOGET_ALLOWED_TABLES`;
  `read_masked` is the only Flight-reachable read surface. Human reads are
  therefore unreachable *by construction*, not by a scope check — no admin-audit
  Flight scope. Admin inspection of raw/filtered reads is via direct DB tooling
  on the host. (If that ever proves too restrictive, revisit — but the default is
  the structural guarantee.)

## 1. Summary

Today the read pipeline **physically drops** reads at the `qc` and `host_filter`
steps: each step anti-joins the unwanted `sequence_idx`es and rewrites a smaller
Parquet (`qc_reads.parquet`, then `filtered_reads.parquet`). Those files are
workspace-only and never registered into DuckLake.

This proposal inverts that:

1. **Load the full reads once** into a permanent DuckLake `read` table — no
   physical filtering, ever.
2. Produce a **`read_mask`** table (one logical mask per *masking processing*)
   that records, per read, whether it survives and how it should be trimmed.
3. **Encapsulate mask application in the data plane.** Consumers never join
   `read` to `read_mask` themselves — they ask the data plane (DoGet) for the
   reads of a `(prep_sample_idx, mask_idx)` and receive only valid, trimmed
   reads. The join + trim + privacy exclusion live in exactly one place.

Two facts from the discussion shape the design:

- **No existing read data to preserve.** We can drop whatever is in
  staging/DuckLake and define the final schema directly — a clean cutover, no
  expand/contract, no backfill.
- **Read access is restricted to admins / non-loggable service principals.**
  Retaining human reads in the lake is acceptable *because* the DoGet boundary
  excludes them by construction (see §4), not merely by policy.

## 2. Goals / non-goals

**Goals**
- Store reads once; never re-run bcl-convert/fastq when only the *filter* changes
  (a new host reference → a new mask, same `read` table).
- A single masking boundary so downstream logic (denoise, future alignment) is
  unchanged in behavior — it just receives valid, trimmed reads.
- Human/host reads physically present but unreachable through the normal read
  path.

**Non-goals**
- Migrating existing read data (there is none worth keeping).
- Changing the identifier-minting model. `sequence_idx` stays the CP-minted,
  contiguous-per-`prep_sample`, sorted BIGINT join key it is today.
- Re-deriving Python enums from Postgres or vice-versa (parity rules stand).

## 3. Data model

### 3.1 Reconciling the proposed mask columns

The sketch was:

```
processing_idx::bigint, sequenced_sample_idx::bigint, sequence_index::bigint,
valid::bool, left_trim::uint, right_trim::uint, ...
```

Three corrections before it becomes real:

| Proposed | Use instead | Why |
|---|---|---|
| `sequence_index` | `sequence_idx` | `sequence_index` already exists as miint's **transient, 1-based, per-file** counter (never persisted). The persisted global join key is `sequence_idx`. Reusing the old name resurrects exactly the confusion the codebase comments warn against. |
| `sequenced_sample_idx` | `prep_sample_idx` (scope key) | Reads and `qiita.sequence_range` are scoped by `prep_sample_idx`; `sequenced_sample` is a subtype with its own `idx`. Join on `prep_sample_idx`. `sequenced_sample_idx` is **not** carried in the lake tables (the final §3.2 schemas drop it) — it's derivable via `sequenced_sample.prep_sample_idx`. |
| `valid::bool` | `reason` enum (+ derived `valid`) | A bool can't distinguish "human host hit" (must *never* be exposed) from "too short" (a tunable QC call). The reason drives the privacy exclusion and makes per-stage read counts derivable. `valid` = `reason = 'pass'`. |
| single `left_trim`/`right_trim` | per-mate `left_trim1/right_trim1/left_trim2/right_trim2` | R1 and R2 share one `sequence_idx`; one trim pair can't express paired-end. |

### 3.2 DuckLake tables (in `qiita-data-plane/src/ducklake.rs::ensure_*`)

DuckLake has no PK/UNIQUE/FK — integrity is enforced upstream (CP dedup, CO
verify), same as the reference tables.

```sql
-- Full reads, written ONCE per sequenced sample. Independent of any mask.
-- Keyed by the identifiers that EXIST and are minted today: prep_sample_idx +
-- the globally-unique sequence_idx. (study/prep/sample_idx may be carried as
-- denormalized pruning columns IF those idx's exist; do NOT add the
-- not-yet-implemented processing_idx/processed_prep_sample_idx — D1/§0.)
CREATE TABLE IF NOT EXISTS qiita_lake.read (
    prep_sample_idx            BIGINT NOT NULL,
    sequence_idx               BIGINT NOT NULL,     -- globally unique join key
    read_id                    VARCHAR NOT NULL,
    sequence1                  VARCHAR NOT NULL,
    qual1                      UTINYINT[],          -- NULL for FASTA
    sequence2                  VARCHAR,             -- NULL when single-end
    qual2                      UTINYINT[]
);
-- Parquet sort: prep_sample_idx, sequence_idx (sequence_idx last for row-group
-- pruning). Files mode 440 before register.

-- One row per (mask, read). `mask_idx` is the filtering-config discriminator
-- (CP-minted in qiita.mask_definition, dedup over {filter workflow, version,
-- host references, QC params}) — the same config yields the same mask_idx
-- fleet-wide. Multiple mask_idx values coexist over the same `read` rows
-- (human-filter vXXX vs vYYY). D1/§0.
CREATE TABLE IF NOT EXISTS qiita_lake.read_mask (
    mask_idx                   BIGINT NOT NULL,     -- which filtering config
    prep_sample_idx            BIGINT NOT NULL,     -- pruning / scope
    sequence_idx               BIGINT NOT NULL,     -- joins read.sequence_idx
    reason                     VARCHAR NOT NULL,    -- ReadMaskReason (§3.3)
    left_trim1                 UINTEGER NOT NULL DEFAULT 0,
    right_trim1                UINTEGER NOT NULL DEFAULT 0,  -- SE: trimmed_5p/3p
    left_trim2                 UINTEGER,            -- PE: structurally 0 (§5.1)
    right_trim2                UINTEGER             -- PE: trimmed2_3p; NULL if SE
);
-- Sort: mask_idx, prep_sample_idx, sequence_idx (sequence_idx last for pruning).
```

> Naming: singular `read` / `read_mask` follows the table-naming rule. `read`
> is not a DuckDB reserved word, but confirm quoting in `build_query` before
> committing; `sequence_read` / `sequence_mask` are fallbacks if it bites.

### 3.3 `ReadMaskReason` — a Python StrEnum, **not** a Postgres ENUM

```python
class ReadMaskReason(StrEnum):
    PASS = "pass"                  # survives; apply trims
    QC_TOO_SHORT = "qc_too_short"  # filter_read fail_reason 'length'
    QC_TOO_LONG = "qc_too_long"    # filter_read fail_reason 'too_long' (max_length)
    QC_LOW_QUALITY = "qc_low_quality"  # fail_reason 'quality'
    QC_TOO_MANY_N = "qc_too_many_n"    # fail_reason 'n_base'
    HOST_RYPE = "host_rype"        # privacy-sensitive: human/host hit
    HOST_MINIMAP2 = "host_minimap2"
```

This value set lives only in `qiita-common` (used by the orchestrator job and
referenced by the DoGet view) and as a DuckDB-side `CHECK`/value convention. It
is **deliberately not** a Postgres `CREATE TYPE`, so per the Enum-parity carve-out
in `CLAUDE.md` it gets **no `ENUM_PAIRS` entry** — a `StrEnum` backed by a
non-Postgres column is a valid, in-scope-of-nothing choice.

**Reason precedence (privacy-critical):** when a read both fails QC and hits the
host filter, `reason` records the **host** hit (`host_*`), so it can never leak
through a "QC-only" code path. Trims are still recorded (from QC) so an admin
reading raw `read` can reconstruct, but the masked path drops it.

## 4. The masking + access boundary (encapsulation)

A DuckLake **view** does the join, trim, and privacy exclusion once:

```sql
CREATE VIEW qiita_lake.read_masked AS
SELECT
    m.mask_idx,
    m.prep_sample_idx,
    r.sequence_idx,
    r.read_id,
    -- substr: 1-based start, 3rd arg is a LENGTH; clamps to '' if <= 0.
    substr(r.sequence1, m.left_trim1 + 1,
           length(r.sequence1) - m.left_trim1 - m.right_trim1)        AS sequence1,
    -- list slice: 1-based, inclusive both ends. Guard qual1 for FASTA (NULL)
    -- symmetrically with qual2; invariant len(sequence1)=len(qual1) (§4 note).
    CASE WHEN r.qual1 IS NULL THEN NULL ELSE
      r.qual1[m.left_trim1 + 1 : len(r.qual1) - m.right_trim1] END    AS qual1,
    CASE WHEN r.sequence2 IS NULL THEN NULL ELSE
      substr(r.sequence2, m.left_trim2 + 1,
             length(r.sequence2) - m.left_trim2 - m.right_trim2) END  AS sequence2,
    CASE WHEN r.qual2 IS NULL THEN NULL ELSE
      r.qual2[m.left_trim2 + 1 : len(r.qual2) - m.right_trim2] END    AS qual2
FROM qiita_lake.read r
JOIN qiita_lake.read_mask m
  ON  r.prep_sample_idx = m.prep_sample_idx
  AND r.sequence_idx    = m.sequence_idx
WHERE m.reason = 'pass';        -- host/human + QC-failed never appear here
```

**Trim invariants (must hold, asserted at mask-emit + tested):**
- `len(sequence1) == len(qual1)` (and `…2`) for every non-FASTA read — the
  `substr` *length* arg and the list-slice *end index* are computed from
  different columns, so they only agree when the two lengths match. Assert this
  when emitting the mask, not at query time.
- A read whose post-trim length `< min_length` must be `reason = 'qc_too_short'`,
  **never** `pass`. `filter_read` runs on the *trimmed* sequence (§5.1), so this
  holds by construction — but it is load-bearing for both correctness and the
  privacy story, so it gets an explicit test (the trim-length invariant above).

Consumers DoGet `read_masked` filtered by `mask_idx` (which filtering config —
vXXX vs vYYY) and `prep_sample_idx` (which samples). They never see `read`,
`read_mask`, the join, or a single human base. *Human* reads are excluded *by
construction* regardless of caller — that is what makes retention acceptable
under the admin-only access model.

**Mandatory-filter invariant (don't skip — review caught this).** Privacy from
*human* reads is structural (the view's `WHERE reason='pass'`), but **scoping** is
not: `build_query` returns `SELECT * FROM qiita_lake.read_masked` for an *empty*
filter (`flight_service.rs:878-881`) — i.e. an unfiltered ticket would dump every
sample's pass reads across every mask, fleet-wide. So the new read-pull route
**MUST inject a non-empty `prep_sample_idx` and a `mask_idx`** into every signed
ticket, and **reject a ticket with an empty filter at sign time**. Add a CP test
for it; optionally a DP-side guard that refuses an unfiltered `read_masked` query.
The DP enforces no mandatory-filter rule today, so this lives in the route.

### Data-plane changes (`qiita-data-plane/src/flight_service.rs`)
- `ALLOWED_TABLES` (`flight_service.rs:83`): add `"read_masked"` (the normal
  path). **Leave `read`/`read_mask` out entirely** so raw reads are unreachable
  via Flight — this, not a scope check, is what makes the privacy claim airtight
  (see D3, §0). If admin raw access is ever needed, do it via direct DB
  tooling, not a new Flight allowlist entry.
- `ALLOWED_FILTER_COLUMNS` (`flight_service.rs:94`, currently only
  `feature_idx, reference_idx, node_index`): add `"mask_idx"` and
  `"prep_sample_idx"`. Both are BIGINT, so the existing integer-only `IN (...)`
  builder in `build_query` (`flight_service.rs:875`) handles them with no new
  machinery — a plain view, no membership-join special case.
- **CP-side allowlist (missed in earlier draft):** the control plane keeps its
  *own* copy, `_DOGET_ALLOWED_TABLES` (`routes/reference.py:309`, "Must match
  the data plane's ALLOWED_TABLES"). `read_masked` must be added there too, and
  the filter columns to whatever gate the CP route applies — otherwise the CP
  rejects the ticket before the DP sees it.
- **`register_files` needs no table-whitelist change** — it registers whatever
  table the signed payload names (`flight_service.rs:594-651`, no whitelist).
  The real requirement is that `ducklake.rs::ensure_*` (run at startup) CREATE
  the `read`/`read_mask` tables; registration then uses the existing
  `ducklake_add_data_files` path unchanged.

## 5. Pipeline changes (orchestrator jobs)

```
bcl-convert  ──►  fastq_to_parquet  ──►  [register read]        (full reads, once)
                                          │
                  ┌───────────────────────┘
   masking workflow (one mask_idx = host refs + QC params; vXXX, vYYY, … coexist)
                  qc ──► host_filter ──► [register read_mask @ mask_idx]
```

- **`fastq_to_parquet`** (`jobs/fastq_to_parquet.py`): unchanged in spirit —
  still mints the `sequence_idx` range and writes all reads. New: emit the
  `prep_sample_idx` column and register the result as the `read` table (today
  it's workspace-only). This is the one genuinely net-new data-plane capability
  (registering per-sample reads, not just reference data).
- **`qc`** (`jobs/qc.py`): stop the anti-join/rewrite. Keep calling
  `trim_adapters[_pe]` / `trim_polyg` (they already return `trimmed_5p/3p`
  counts) and `filter_read` (already returns `passed` + `fail_reason`). Emit a
  partial mask: `(sequence_idx, reason, left_trim1, right_trim1, left_trim2,
  right_trim2)`. No `qc_reads.parquet`.
- **`host_filter`** (`jobs/host_filter.py`): stop the anti-join. Run
  `rype_classify` then `align_minimap2` on survivors as today, but instead of
  dropping, mark hit `sequence_idx`es with `reason = host_rype | host_minimap2`,
  merging with the QC mask under the precedence in §3.3. Output the final
  `read_mask.parquet`, then register.
- **Read counts** (`raw/biological/quality_filtered_read_count_r1r2` on
  `sequenced_sample`): now derivable from `read_mask` — but the persisted columns
  are **`_r1r2`** (both mates; a PE pair counts as 2), while the mask has **one
  row per pair** (`read_count.py:55-79` counts `count(*) + count(sequence2)`). So
  derive each bucket as `COUNT(*) + COUNT(sequence2-present)`, **not** a bare
  `COUNT(*)`, or PE numbers silently halve: `raw` = all rows, `biological` = rows
  not `qc_*`, `quality_filtered` = `pass` rows. The existing `persist-read-metrics`
  action reads JSON sidecars from the steps (`workflows/.../1.2.0.yaml`), so
  re-sourcing it from the mask is a real rewrite of that primitive, not a config
  tweak.

**Host classification runs only on QC-pass reads** (as it effectively does
today, where `host_filter` consumes `qc`'s survivors). A QC-failed read is
already `reason != 'pass'` and so already excluded from `read_masked` — no need
to spend rype/minimap2 on it, and `host_*` only ever overrides `pass`. This
keeps precedence trivial: host wins, but only over `pass`. Corollary: a
QC-failed read that *is* human stays `qc_*` (never reclassified `host_*`); it's
still excluded from `read_masked`, so privacy holds, but per-reason **host
counts under-count** human hits. Do **not** "fix" this by running host-classify
on QC-failed reads — that would put human reads back on the classification path.

### 5.1 Concrete mask-emit SQL

**Paired-end trim caveat (corrected after review):** the symmetric four-column
`left/right_trim{1,2}` model only fully applies to single-end. SE `trim_adapters`
returns both `trimmed_5p` and `trimmed_3p`. But PE `trim_adapters_pe` returns
`STRUCT(sequence1, quality1, sequence2, quality2, overlap_len, adapter_trimmed,
trimmed1_3p, trimmed2_3p)` — **3′-only, no 5′ output** (`docs/duckdb-miint.md`),
and `trim_polyg` trims only the 3′ end (its struct exposes `trimmed_5p` but it's
always 0). So for PE, `left_trim1`/`left_trim2` are
**structurally 0** (`right_trim1 = trimmed1_3p`, `right_trim2 = trimmed2_3p`).
Keep the four columns for a uniform schema, but document that PE never populates
the left pair. (If a future 5′ PE step appears, the column is already there.)

**`qc` — single-end** (PE mirrors per mate via `trim_adapters_pe`, mapping
`trimmed1_3p`/`trimmed2_3p` to `right_trim1`/`right_trim2` and leaving the
`left_trim*` at 0):

```sql
COPY (
  WITH adapter AS (   -- trim_adapters → STRUCT(sequence, qual, trimmed_5p, trimmed_3p)
    SELECT sequence_idx,
           trim_adapters(sequence1, qual1, $adapters) AS a
    FROM read_input
  ),
  polyg AS (          -- 3'-only polyG on the adapter-trimmed seq (2-color instruments)
    SELECT sequence_idx, a,
           trim_polyg(a.sequence, a.qual) AS g   -- STRUCT(sequence, qual, trimmed_3p)
    FROM adapter
  ),
  filtered AS (       -- filter_read → STRUCT(passed, fail_reason, length, n_bases, ...)
    SELECT sequence_idx, a, g,
           filter_read(g.sequence, g.qual, 100, 0, 15, 40, 5, 0) AS f
    FROM polyg
  )
  SELECT
    sequence_idx,
    CASE
      WHEN f.passed                       THEN 'pass'
      WHEN f.fail_reason = 'length'       THEN 'qc_too_short'
      WHEN f.fail_reason = 'too_long'     THEN 'qc_too_long'
      WHEN f.fail_reason = 'n_base'       THEN 'qc_too_many_n'
      ELSE 'qc_low_quality'  -- fail_reason = 'quality'
    END                                   AS reason,
    a.trimmed_5p::UINTEGER                AS left_trim1,
    (a.trimmed_3p + g.trimmed_3p)::UINTEGER AS right_trim1,
    NULL::UINTEGER                        AS left_trim2,
    NULL::UINTEGER                        AS right_trim2
  FROM filtered
  ORDER BY sequence_idx
) TO 'qc_mask.parquet' (FORMAT parquet, ...);
```

`left/right_trim` are the **cumulative** bases removed from each end (adapter +
polyG), recorded even when the read fails — so an admin reading raw `read` can
still reconstruct, while `read_masked` drops the row.

**`host_filter` — merge host hits into the QC mask** (rype/minimap2 hit sets are
the same accumulator tables built today, but populated from the QC-pass subset):

```sql
COPY (
  SELECT
    q.sequence_idx,
    CASE
      WHEN mm.sequence_idx IS NOT NULL THEN 'host_minimap2'
      WHEN ry.sequence_idx IS NOT NULL THEN 'host_rype'
      ELSE q.reason                       -- 'pass' or an unchanged qc_* reason
    END AS reason,
    q.left_trim1, q.right_trim1, q.left_trim2, q.right_trim2
  FROM qc_mask q
  LEFT JOIN host_filter_rype_hits     ry USING (sequence_idx)
  LEFT JOIN host_filter_minimap2_hits mm USING (sequence_idx)
  ORDER BY q.sequence_idx
) TO 'read_mask.parquet' (FORMAT parquet, ...);
```

The `mask_idx` and `prep_sample_idx` columns are constants for the run, added at
register time, keeping the in-job SQL focused on `sequence_idx`.

## 6. Control plane / qiita-common
- `qiita-common`: add `ReadMaskReason` (§3.3). The read-pull path is a **new
  REST route + ticket-signing call site** — *not* an "adjust a helper." The only
  DoGet-issuing route today is `POST PATH_REFERENCE_DOGET` (`routes/reference.py`),
  which hardcodes `filter={"reference_idx": …}`, requires `_scope = TICKET_DOGET`,
  and gates on an `active` `qiita.reference` row — it cannot sign a read-masked
  ticket. So this needs: a new path + request/response models, a new (or reused)
  ticket scope, the signing call, `_DOGET_ALLOWED_TABLES` entry (§4), and the
  `api_paths.py` `PATH_*`/`URL_*` pair + parity-test registration. This is real
  scope, sized below as its own PR.
- **`mask_idx` minting (new CP machinery, D1/§0 resolved).** A mask's identity is
  its filtering config. Mint it like `mint_sequence_range` does for
  `sequence_idx` — a new table + dedup function:

  ```sql
  CREATE TABLE qiita.mask_definition (
      mask_idx        BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
      params_hash     BYTEA   NOT NULL UNIQUE,   -- SHA-256(canonical config JSON)
      filter_workflow TEXT    NOT NULL,
      filter_version  TEXT    NOT NULL,
      params          JSONB   NOT NULL,          -- host refs, QC settings, ...
      created_by_idx  BIGINT  NOT NULL,
      created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  ```
  `mint_mask_definition(params) → mask_idx` upserts on `params_hash` (same config
  → same `mask_idx` fleet-wide; mirrors the `processing_idx` dedup idea but
  scoped only to masks). Note this is the *same params-hash* discipline as
  `processing_idx` canonical-JSON hashing — when the full processing hierarchy is
  built later, `mask_definition` folds into it cleanly. (`mask_idx` is **minted
  once per config, not per sample** — the same vXXX `mask_idx` tags the
  `read_mask` rows of every sample filtered under vXXX.)

## 7. Cutover (no migration)
Because nothing in the lake is worth keeping:
1. Drop existing read/filtered Parquet from staging; drop any registered read
   data. (Reference tables untouched.)
2. Ship the new `read` / `read_mask` / `read_masked` definitions in their final
   shape — no expand/contract window.
3. Flip `qc`/`host_filter` to mask-emit, and **stand up** the `read_masked`
   consumer path.

> Reality check (from review): there is **no live consumer of
> `filtered_reads.parquet` today** — `amplicon/workflow.yaml` is a container
> workflow with no `reads`/`filtered_reads` binding, and nothing reads that file
> (it's "forwarded for a future consumer"). So step 3 is *building* the masked
> read path, not migrating an existing one, and "retire `filtered_reads.parquet`"
> just deletes an unconsumed output (grep clean before deleting). This makes the
> cutover lower-risk but means the consumer side is greenfield work, not a swap.

## 8. PR plan

> The earlier "2 PRs, each green" plan **did not survive review** (§0): PR 2's
> identifiers/route didn't exist. D1 (`mask_idx`) and D2 (DuckLake views) are now
> resolved, so the plan below is 4 PRs (+1 optional consumer). The only residual
> verification is the `UTINYINT[]`-over-DoGet round-trip, folded into PR 2's
> tests rather than a standalone spike.

**PR 1 — `mask_idx` + read-pull route (CP, the §0 prerequisite).** The
`qiita.mask_definition` table + `mint_mask_definition` dedup (§6), and the new
DoGet route + scope + `api_paths.py` triple + `_DOGET_ALLOWED_TABLES`. No DuckLake
data yet — route can return empty until PR 3 produces rows. Trips
`deploy-note-check` (new scope in `auth/scopes.py`) → `DEPLOY_CHECKLIST` fold;
the `mask_definition` migration runs via the normal dbmate flow.

**PR 2 — data-plane tables + view (Rust, additive).** `read`/`read_mask` in
`ensure_*`, the persistent `read_masked` catalog view (§4, proven in D2),
`ALLOWED_TABLES` + `ALLOWED_FILTER_COLUMNS`. Integration tests insert fixture
`read`/`read_mask` rows (no producer yet) and assert the view's trim math +
`pass`-only exclusion, **plus the `UTINYINT[]`-over-DoGet round-trip** (incl. the
empty-result `RecordBatch::new_empty` branch) — the one residual unknown.

**PR 3 — the producer cutover (orchestrator, atomic).** `fastq_to_parquet`
registers full `read` (identifier cols, sort order, mode 440); `qc`/`host_filter`
emit `read_mask` (§5.1) instead of dropping; `ReadMaskReason` in `qiita-common`;
`persist-read-metrics` re-sourced from the mask with the `_r1r2` count fix (§5);
delete `filtered_reads.parquet`/`qc_reads.parquet`. Workflow YAML changes reach
`qiita.action` via `qiita-admin actions sync` → add a `verify-deploy` check; fold
into `DEPLOY_CHECKLIST`.

**PR 4 (optional) — first masked-read consumer.** Wire the actual downstream
(denoise/alignment) to the new route once one exists. Greenfield (no consumer
today, §7), so it can land later without blocking the cutover.

Per-PR `CHANGELOG.md` is mandatory (`changelog-check` fires on every PR).
Ordering: 1 → 2 → 3 → (4); all land in one deploy.

## 9. Open questions (lower-stakes; the blocking ones are §0)
1. **`read` table name** — `read` is not a DuckDB reserved word and is always
   schema-qualified (`qiita_lake.read`), so `build_query` is fine; `sequence_read`
   remains a safer fallback to avoid reader confusion with `read_parquet`/
   `read_fastx`. Low stakes.
2. **Mask as one processing vs two steps** — `qc` + `host_filter` as two steps of
   one masking workflow emitting one `read_mask` (recommended), vs two masks
   merged at query time (more flexible, more join cost).
3. **Storage** — full reads incl. host are ~3–5× the filtered volume; confirm
   the lake budget and whether host-flagged rows should live in a separate
   Parquet partition for cheap exclusion/pruning.

*(Resolved by the inline fixes: the qual-slice/`substr` consistency + the
zero-length→`qc_too_short` invariant are now stated in §4; raw-access is D3 in
§0.)*

## 10. ENA / external accessions

ENA (and other external) accessions are **experiment/sample-level metadata, not
per-read attributes**, and they already live as Postgres columns. They **stay
exactly where they are** — none move into the DuckLake `read`/`read_mask` tables:

| ENA object | Column (Postgres) |
|---|---|
| study | `study.ena_study_accession` |
| sample | `biosample.ena_sample_accession` |
| **experiment** | **`sequenced_sample.ena_experiment_accession`** |
| run | `sequenced_sample.ena_run_accession` |

So the ENA **experiment_id** is `sequenced_sample.ena_experiment_accession`,
unchanged. `sequenced_sample` carries `prep_sample_idx`, which is the join key
into the new `read`/`read_mask` tables — so "this experiment's reads" is a direct
`read`/`read_masked` lookup by that `prep_sample_idx`.

**What the mask model changes is *which reads ENA submission exports*, and it
makes that safer:**
- A public ENA raw-read submission **must be human-depleted**. Today you'd ship
  the physically host-filtered `filtered_reads.parquet`. In the new model the ENA
  exporter pulls **`read_masked` at the host-filtered `mask_idx`** for the
  experiment's `prep_sample_idx` — which, by construction (D3 + `WHERE
  reason='pass'`), contains **no human reads**. The same privacy boundary that
  gates internal access now also guarantees nothing human-derived leaks into a
  public archive. Net win.
- **New (because masks can coexist, D1):** an accession must be tied to the exact
  bytes submitted. Since vXXX/vYYY produce different read sets, record **which
  `mask_idx` was submitted** alongside the accession — add
  `sequenced_sample.ena_submitted_mask_idx BIGINT` (FK-ish to `mask_definition`).
  Then `(ena_experiment_accession, ena_submitted_mask_idx)` is a reproducible
  pointer to "what we sent ENA," and a re-export uses the same mask. Without it,
  changing the default mask later would silently desync the archive from the lake.

**publication_lock interaction (corrected after review).** `publication_lock`
does **not** trigger off the ENA accession columns — it keys off
`prep_sample_to_study.is_published` (`20260520000000_publication_lock.sql`:
"ENA accessions do NOT set is_published … wires no trigger from them"). So the
accession columns themselves don't interact with the lock. But the **converse is
a real constraint**: `sequenced_sample_publication_lock` rejects *all* UPDATEs to
a `sequenced_sample` once its parent prep is published (P0001). Since
`ena_submitted_mask_idx` (and `ena_experiment_accession`/`ena_run_accession`) are
UPDATEs on `sequenced_sample`, they must be written **before** the prep is
published, or the ENA-export design must record them on a path the lock exempts.
Settle this when specing the ENA exporter.

## 11. Impact on open issues

This change reshapes several in-flight issues. Address them *through* this design
rather than against the soon-to-be-deleted destructive path:

- **#146 (end-to-end prep orchestration)** — this design **is** the new shape of
  that pipeline. The "fan out `fastq-to-parquet/1.2.0` per sample → `qc` →
  `host_filter` (destructive)" becomes "register `read` once → emit `read_mask`."
  #146's steps 2–4 change accordingly: minting/linking `sequenced_sample` → reads
  is now read-table registration; per-stage read counts come from the mask (§5,
  `_r1r2` fix), not step sidecars. **#146 should be re-specced around the mask
  model** before its glue is built — otherwise it orchestrates a pipeline we're
  removing.
- **#164 (empty FASTQ wells / no `no_data` outcome)** — the mask model makes the
  target state cleaner but does **not** "just enable" it; the mechanism in the
  earlier draft was wrong. Empty input still short-circuits **before** any DuckDB
  work (`fastq_to_parquet.py:201-204`), and `mint_sequence_range` *raises* on
  `count <= 0` (`20260514000000_sequence_range.sql`), so "register zero reads" is
  not a thing — the mint contract assumes ≥1 read. A `no_data` outcome is genuine
  **outcome-plumbing work**: a new terminal `no_data` state (no `read`/`read_mask`
  rows written, no range minted) threaded through the dispatcher → runner →
  `work_ticket`, *distinct* from `permanent` failure. A `no_data` sample leaves
  all three `*_read_count_r1r2` NULL (the monotonic CHECK is vacuous on NULL).
  Worth doing alongside PR 3 (same files), but scope it as outcome plumbing, not
  a registration. (#164's other asks — a `no_data` bucket in
  `PoolCompletionStatus` so `complete` can fire, and a `prep_sample` retire
  route/CLI — are orthogonal completion/disposition surfaces, still needed.)
- **#28 (read-only access to the data lake)** — largely answered/bounded here.
  #28's worry is ungated DuckLake bypass ("internal jobs only, never external
  clients"). D3 + the mask-aware DoGet route define exactly that gate:
  `read_masked` is the only Flight-reachable read surface, raw `read`/`read_mask`
  are out of Flight entirely, and human reads are unreachable by construction.
  So "read access to the lake" = `read_masked` via signed DoGet, never a raw
  catalog bypass for external callers — which is the policy #28 was reaching for.
- **#131 (converge `qc`/`host_filter` COPY path literals on
  `validate_parquet_path`)** — the exact `COPY`/`CREATE VIEW` literals it targets
  in `qc.py`/`host_filter.py` are **rewritten** by PR 3 (mask-emit instead of
  `filtered_reads.parquet`). **Fold #131 into PR 3**: route the new mask-emit
  paths through `validate_parquet_path` so the cleanup lands on the new code, not
  on code we're deleting.

Touched but not reshaped (apply opportunistically during PR 3's job rewrite):
**#96** (standardize native DuckDB jobs: tmp-dir cleanup, `validate_parquet_path`,
`index_type` Literal), **#121** (job consistency), **#40** (`sequence_range`
follow-ups — still minted; `fastq_to_parquet` now also registers the `read`
table), **#38** (DuckDB resource caps into native jobs).
