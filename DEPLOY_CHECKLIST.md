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

Set it to the mount(s) sequencing data actually lives on. **The gate itself needs no grant for
`qiita-api`:** it is written for the account split (CP runs as `qiita-api`, steps as
`qiita-job`, different groups) — it treats a permission error as "cannot tell" and admits, so a
run folder only `qiita-job` can read still submits. Reading a run folder through
`POST /run-folder/inspect` does need one; that is bucket 2. Add a second root by extending the
value with `:` rather than adding a line. (#feat/ingest-path-roots-and-upload)

### 2. One-time host setup

**`qiita-api` needs read+traverse on the PacBio run tree**, or `POST /run-folder/inspect`
answers 403 for those folders and `submit-pacbio-ingest` still has to run from a node that
mounts `/sequencing`. Illumina needs nothing — `/sequencing/igm_runs/**` is world-readable the
whole way down, measured. `/sequencing/gcore_runs/**` is group `kl-seq-rw`, which `qiita-job`
is in and `qiita-api` is not.

Granted as an ACL on directories, not group membership: the route lists directories and opens
no BAM (`index_run_bams` globs `*/hifi_reads/*.bam` for filenames), and `gcore_runs` is
`drwxrwsr-x root kl-seq-rw` — adding `qiita-api` to that group would also give the public API's
service account write on the raw drop directory.

```bash
ROOT=/sequencing/gcore_runs
# Access entry + default entry on every existing directory; new project / run / well /
# hifi_reads dirs inherit both from their parent as they are created.
sudo find "$ROOT" -type d -exec setfacl -m u:qiita-api:rx,d:u:qiita-api:rx {} +
sudo -u qiita-api ls -d "$ROOT"/*/*/*/hifi_reads | head -3   # must list, not EACCES
```

If `setfacl` reports `Operation not supported`, the mount has no ACL support: fall back to
`sudo usermod -aG kl-seq-rw qiita-api`, which also grants group write on `gcore_runs`, and note
that a supplementary group is read at process start — the bucket-4 restart is what picks it up.
(#feat/ingest-path-roots-and-upload)

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
- **A submit can now run off the cluster — both platforms.** `POST /run-folder/inspect`
  reads the run folder as `qiita-api`, a NARROWER account than the `qiita-job` that runs
  the jobs: it reaches the IGM folders through their world bits, and the gcore folders
  only through the ACL bucket 2 grants. Confirm one real folder of each kind:
  ```bash
  curl -sS -X POST https://<fqdn>/api/v1/run-folder/inspect \
    -H "Authorization: Bearer $QIITA_TOKEN" -H 'Content-Type: application/json' \
    -d '{"path": "/sequencing/igm_runs/<a-real-run>", "platform": "illumina"}'
  curl -sS -X POST https://<fqdn>/api/v1/run-folder/inspect \
    -H "Authorization: Bearer $QIITA_TOKEN" -H 'Content-Type: application/json' \
    -d '{"path": "/sequencing/gcore_runs/<project>/<a-real-run>", "platform": "pacbio_smrt"}'
  ```
  Illumina: 200 with `illumina.instrument_run_id` / `instrument_model` — measured working
  for `/sequencing/igm_runs/*`, no grant involved. PacBio: 200 with a non-empty
  `pacbio.hifi_bam_by_barcode`. A **403** there means bucket 2's ACL did not take (it was
  measured failing before that step); a 200 with an empty index means the ACL took and the
  folder holds no demuxed BAMs. (#feat/ingest-path-roots-and-upload)
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
- **`submit-bcl-convert` / `submit-pacbio-ingest` no longer have to run on a machine that
  mounts the cluster.** Both used to read the run folder locally — RunInfo.xml for
  Illumina, the `*/hifi_reads/*.bam` glob for PacBio — which is why the runbook told
  operators which machine to type on. Both reads moved to `POST /run-folder/inspect`. The
  path still has to be the one the CLUSTER sees; what changed is that it is now checked
  and read where it exists, so a wrong path fails at the terminal instead of inside a job.
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
