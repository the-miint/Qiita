# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

### 2. One-time host setup

- **Re-run `assembly_coverage` for any `long-read-assembly` ticket that completed it before
  this deploy** (#422, closes #374). `assembly_coverage` now writes its coverage BAM
  coordinate sorted and `binning.sh` no longer sorts what it stages, so a BAM
  written by the old code reaches a post-deploy `binning` unsorted and
  dies in `jgi_summarize_bam_contig_depths` ("the bam file is not sorted!"). A plain `/run`
  does not fix it: resume fast-forwards a step already `completed` in
  `qiita.work_ticket_step` and rebuilds its outputs from the workspace rather than
  re-running it, so the same BAM comes back.

  Listed states are those a ticket can still reach `binning` from: `failed` and `cancelled`
  are both redrivable in place via `/run` (`_RUN_REDRIVE_STATES`), so both stay in scope;
  only `completed` and `no_data` are excluded. Tickets that already cleared `binning` on
  the old image are excluded too — `binning` is step 5 of 11, so a ticket now in `checkm`
  is not at risk and must not be restarted (its `assemble` cost 192–384 GB).
  ```bash
  sudo -u qiita-api bash -c 'set -a; . /etc/qiita/control-plane.env; set +a
  psql "$DATABASE_URL" -Atc "SELECT DISTINCT wt.work_ticket_idx, wt.state FROM qiita.work_ticket wt JOIN qiita.work_ticket_step s USING (work_ticket_idx) WHERE wt.action_id='\''long-read-assembly'\'' AND s.step_name='\''assembly_coverage'\'' AND s.state='\''completed'\'' AND wt.state NOT IN ('\''completed'\'', '\''no_data'\'') AND NOT EXISTS (SELECT 1 FROM qiita.work_ticket_step b WHERE b.work_ticket_idx=wt.work_ticket_idx AND b.step_name='\''binning'\'' AND b.state='\''completed'\'');"'
  ```
  For each row, drop just that step's progress row and redrive — the runner then advances
  to a fresh attempt dir and re-runs `assembly_coverage`, keeping the completed `assemble`.
  Cancel first if the ticket is still in flight (`/run` applies to `failed` / `cancelled` /
  `pending`, not to `processing`):
  ```bash
  qiita-admin ticket cancel <idx>   # only if still in flight
  sudo -u qiita-api bash -c 'set -a; . /etc/qiita/control-plane.env; set +a
  psql "$DATABASE_URL" -c "DELETE FROM qiita.work_ticket_step WHERE work_ticket_idx=<idx> AND step_name='\''assembly_coverage'\'';"'
  qiita ticket run <idx>
  ```
  The reverse pairing needs no action: an older binning image re-sorting an already-sorted
  BAM only costs the sort.

### 3. Migrations

- `make migrate` applies `20260802000000_work_ticket_escalated_resource_floor.sql`, no
  out-of-band setup: an additive `ALTER TABLE qiita.work_ticket ADD COLUMN
  escalated_resource_floor JSONB`, plus a shape CHECK (`NULL` or a JSON object) and the
  column's `COMMENT ON`. Nullable, backfill-free — every existing row reads NULL, which
  the runner treats as "nothing escalated yet". Carries the per-step memory/walltime floor
  the OOM/TIMEOUT retry ladder climbs to, so a CP restart or a `/run` redrive continues
  the ladder instead of restarting at the YAML baseline. (#415)

### 4. Deploy

_None yet._

### 5. Verify

- (#420, closes #411) Confirm the six re-sized actions carry their raised ceilings.
  `actions sync` runs inside `activate.sh`, so this only checks it took — if a row still
  reads its old value, an OOM or TIMEOUT on that workflow's heaviest step keeps failing
  permanently on attempt 0 (a baseline at its ceiling has no rung to climb to) and the
  change is a silent no-op:

  ```bash
  psql "$DATABASE_URL" -tAc "SELECT action_id, version, mem_ceiling_gb, walltime_ceiling
    FROM qiita.action
    WHERE (action_id, version) IN
      (('bcl-convert','1.0.0'),('fastq-to-parquet','1.1.0'),('fastq-to-parquet','1.2.0'),
       ('fastq-to-parquet','1.3.0'),('read-mask','1.0.0'),('read-mask-block','1.0.0'))
    ORDER BY action_id, version"
  ```

  Expect (was → is): `bcl-convert|1.0.0|500|1 day` (was `480|12:00:00`),
  `fastq-to-parquet|1.1.0|32|08:00:00` and `|1.2.0|32|08:00:00` (both were `16|04:00:00`),
  `fastq-to-parquet|1.3.0|64|08:00:00`, `read-mask|1.0.0|64|08:00:00` and
  `read-mask-block|1.0.0|64|08:00:00` (all three were `32|08:00:00`).

- The rebuilt `long-read-assembly` binning image stages the coverage BAM instead of
  sorting it (#422, closes #374). Anchored at start-of-line so a comment mentioning
  the command cannot satisfy it:
  ```bash
  cd /tmp && sudo -u qiita-orch apptainer exec --no-home \
    "${PATH_DERIVED}/images/long-read-assembly-binning-1.0.0.sif" \
    bash -c 'grep -q "^cp .*COVERAGE_BAM" /opt/qiita/binning.sh' \
    && echo BINNING_STAGE_OK
  ```

- Re-run the bucket-2 query (#422). The CP keeps serving through buckets 2→4, so a
  ticket can complete `assembly_coverage` on the old code after that check and
  before the restart. Expect zero rows; anything listed landed in that
  window and takes the same fix.

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- After this deploy, a ticket whose step OOM-kills or times out keeps its escalated size
  across a CP restart and a `/run` redrive — the previous behaviour re-burned one failing
  attempt per affected step climbing back (observed on `long-read-assembly` 6978 / 6980 /
  6989: `assemble` back at 192 GB after redriving from 384 GB). To put a redriven ticket
  back on its YAML baseline (e.g. after correcting an oversized input), NULL the column
  first: `UPDATE qiita.work_ticket SET escalated_resource_floor = NULL WHERE
  work_ticket_idx = <idx>;` — SQL `NULL`, not `'null'::jsonb`, which the CHECK rejects
  precisely because the runner would read it as "nothing escalated yet". (#415)
- (#420, closes #411) **Six workflows' `action_ceiling` was raised so an OOM/TIMEOUT can
  actually retry; no step baseline changed, so nothing schedules differently.**
  `bcl-convert/1.0.0`, `fastq-to-parquet/1.1.0`, `1.2.0`, `1.3.0`, `read-mask/1.0.0` and
  `read-mask-block/1.0.0` each had a ceiling equal to their heaviest step's baseline, which
  makes escalation clamp on the first failure and fail the ticket permanently at
  `retry_count=0` — the shape behind the `long-read-assembly` incident (#393). Only a
  *failing* step now climbs; a healthy ticket requests exactly what it did before. Reaches
  `qiita.action` via `qiita-admin actions sync` inside `activate.sh` — no host action, just
  the bucket-5 checks that it took.
- (#422, closes #374) **The `long-read-assembly` binning SIF auto-rebuilds on this
  deploy** (`activate.sh` → `build-sifs.sh`; its entrypoint and def are build
  inputs) to pick up staging the coverage BAM rather than running
  `samtools sort` over it — `assembly_coverage` now writes it coordinate sorted. Each
  binning step drops a sort of the whole coverage BAM (19 s wall and 11.1 GiB peak RSS at
  16 cpu, measured in this image on a 2.0 GB BAM); the staged copy itself remains.
  No host action for the rebuild — but the pairing with BAMs written before the deploy is
  not free: see the bucket-2 drain step.

- (#420, closes #411) **The admin `resource_override` envelope widens with these ceilings**
  (`POST /work-ticket` 422s when `resource_override.mem_gb` exceeds `mem_ceiling_gb`). A
  per-ticket nudge that used to be rejected at 17 GB on `fastq-to-parquet/1.1.0` is now
  accepted up to 32. Nothing to do — noted so the wider envelope isn't a surprise.

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
