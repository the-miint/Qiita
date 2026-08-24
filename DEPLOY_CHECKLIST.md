# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

**Control plane.** `PATH_INGEST_ROOTS` bounds which host paths a submitted `action_context`
may name (`bcl_input_dir`, `bam_path`, `fastq_path`, the `local-*-reference-add` `*_path`
set). Required — `from_env()` refuses to boot without it, deliberately: an unset value would
mean every absolute path the orchestrator can open is nameable through the API. Colon-separated
absolute dirs; `/` is refused.

```bash
sudo bash -c 'grep -q "^PATH_INGEST_ROOTS=" /etc/qiita/control-plane.env || echo "PATH_INGEST_ROOTS=/sequencing" >> /etc/qiita/control-plane.env'   # (#feat/ingest-path-roots-and-upload)
```

Set it to the mount(s) sequencing data actually lives on. **No group grant for `qiita-api` is
needed:** the gate is written for the account split (CP runs as `qiita-api`, steps as
`qiita-job`, different groups) — it treats a permission error as "cannot tell" and admits, so a
run folder only `qiita-job` can read still submits. Add a second root by extending the value
with `:` rather than adding a line. (#feat/ingest-path-roots-and-upload)

### 2. One-time host setup

_None yet._

### 3. Migrations

Plain `make migrate` — no out-of-band setup.

- `20260824000000_upload_source_filename.sql` — adds nullable `qiita.upload.source_filename`
  (the client's basename, so the fastq filename-prefix rule applies to an upload-fed
  submission). Existing rows keep NULL, which the gate skips.
  (#feat/ingest-path-roots-and-upload)

### 4. Deploy

_None yet._

### 5. Verify

Beyond `sudo make verify-deploy QIITA_HOSTNAME=<fqdn>`:

- **The ingest-root gate is live and bounded.** A path outside the roots must be refused at
  submit rather than accepted and failed inside a job. As a wet-lab admin or system admin,
  with a PAT (see [`user-cli-quickstart.md`](docs/runbooks/user-cli-quickstart.md)):
  ```bash
  qiita ticket submit --base-url https://<fqdn> \
    --action-id fastq-to-parquet --action-version 1.3.0 \
    --prep-sample-idx <idx> --context-json '{"fastq_path": "/tmp/not-a-root_R1.fastq"}'
  ```
  Expect exit 1 and a 422 whose detail carries `outside every configured ingest root` and an
  `ingest_roots` list matching what bucket 1 set. A 500, or a 202, means the var is wrong.
  (#feat/ingest-path-roots-and-upload)
- **The widened read-ingest schemas synced.** `qiita-admin actions list` must still show
  `fastq-to-parquet 1.3.0` and `bam-to-parquet 1.0.0` enabled — the change is a
  `context_schema` widening in place, not a version bump, so no new version appears and none
  should have been auto-deprecated. (#feat/ingest-path-roots-and-upload)

### 6. After the deploy verifies green

_None yet._


### Notes (no host action)

- **A `user` can no longer name a host path in `action_context`; it is now wet_lab_admin+.**
  A `fastq_path` / `bam_path` submission from a `user` role returns 403 naming the
  `*_upload_idx` handle to use instead. They load reads with the new `qiita submit-reads`,
  which streams the file to the data plane and submits against the upload — the same workflow
  either way, since the runner resolves the handle to the same step input. Existing
  wet-lab-admin and system-admin path submissions are unaffected apart from the root bound.
  (#feat/ingest-path-roots-and-upload)
- **`qiita submit-reads` needs `--data-plane-url`**, like `qiita reference load` — the reads go
  over Flight. From off the host that is the public TLS edge
  (`grpc+tls://<fqdn>:443`). (#feat/ingest-path-roots-and-upload)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
