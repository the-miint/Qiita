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
# (#484)  Derives the second root from PATH_SCRATCH already in the file, and
# refuses rather than guess if it is unset. Quoted heredoc: nothing below is
# expanded on the way in.
sudo bash -s <<'SH'
F=/etc/qiita/control-plane.env
[ -f "$F" ] || { echo "no such file: $F" >&2; exit 1; }
grep -q "^PATH_INGEST_ROOTS=" "$F" && exit 0
s=$(grep "^PATH_SCRATCH=" "$F" | tail -1 | cut -d= -f2-)
s=${s%\"}; s=${s#\"}; s=${s%\'}; s=${s#\'}; s=${s%/}   # EnvironmentFile allows either quote
: "${s:?PATH_SCRATCH is not set in control-plane.env; set it first}"
[ -n "$(tail -c1 "$F")" ] && echo >> "$F"                # the file may not end in a newline
echo "PATH_INGEST_ROOTS=/sequencing:$s/references/staging" >> "$F"
SH
```

Set it to every mount a submitted path may live under, not the sequencing mount alone.
Reference sources are the second one: `qiita reference load --local` names its manifest and
companions as raw `*_path` keys, and they are staged at
`${PATH_SCRATCH}/references/staging/{name}/{version}/`
([`reference-data-staging.md`](docs/reference-data-staging.md)) — a scratch mount, not a
sequencing one. Omit it and every `local-reference-add` / `local-host-reference-add` submit
422s. The roots bound the manifest path itself, not the FASTA paths listed inside it —
`stage_local_fasta._read_manifest` requires each entry be absolute and existing, and checks
nothing else — so this fence narrows what a submit can name, not what a reference can read.
The command above reads `PATH_SCRATCH` back out of the same env file and refuses to write
anything if it is unset — otherwise it would append a plausible-looking `/references/staging`
that `_parse_ingest_roots` accepts (it checks absolute and not-`/`, never existence), leaving a
control plane that boots clean and refuses every reference. Check the line it appends against
where reference sources are actually staged on this host. **The gate itself
needs no grant for
`qiita-api`:** it is written for the account split (CP runs as `qiita-api`, steps as
`qiita-job`, different groups) — it treats a permission error as "cannot tell" and admits, so a
run folder only `qiita-job` can read still submits. Reading a run folder through
`POST /run-folder/inspect` does need one; that is bucket 2. Add a second root by extending the
value with `:` rather than adding a line. (#484)

### 2. One-time host setup

**`qiita-api` needs read on the PacBio run tree**, or `POST /run-folder/inspect` answers 403 for
those folders and `submit-pacbio-ingest` still has to run from a node that mounts `/sequencing`.
Illumina needs nothing — `/sequencing/igm_runs/**` is readable as `qiita-api` today, measured.

`/sequencing/gcore_runs` is a separate NFS mount from `qs-kl.sdsc.edu:/knightlab/stor-31/...`,
mounted `vers=3,noacl,sec=sys`. Three consequences, all measured on the host:

- **ACLs are not available.** `setfacl` on it returns `Operation not supported` — the mount
  carries `noacl` and NFSv3 has no POSIX-ACL sideband. The client-side mode bits `getfacl` and
  `namei` print (`drwxrwsr-x root kl-seq-rw`, `other::r-x`) are what the server reports, not what
  it enforces: `qiita-api` is refused despite `other::r-x`.
- **Group membership does work.** `sec=sys` means the server trusts the client's uid/gid list,
  and `qiita-job` (in `kl-seq-rw`) lists the tree today — which is how PacBio ingest reads it now.
- **`kl-seq-rw` carries group write** on `gcore_runs`. There is no read-only alternative on this
  export, so the grant below gives the public API's service account write on the raw instrument
  drop. The control plane never writes there — the route lists directories and opens no BAM
  (`index_run_bams` globs `*/hifi_reads/*.bam` for filenames) — but the capability is real.

```bash
sudo usermod -aG kl-seq-rw qiita-api   # (#484)
```

Deploy host only — the control plane is a systemd unit there and `qiita-api` reads no run folder
anywhere else. Account group membership is not uniform across this cluster (`qiita-orch` is in
`qiita-pipeline` on the deploy host and not on a worker, measured), so do not assume this or any
other grant propagated.

A supplementary group is read at process start, so the **bucket-4 restart** is what picks this
up; verifying before it will still fail. After the restart:

```bash
ROOT=/sequencing/gcore_runs
# Runs sit at two depths under $ROOT: most directly (`r84137_.../<well>/hifi_reads`), some under
# a project dir (`Knightlab/<run>/<well>/hifi_reads`). Check both or a pass on one layout hides
# a failure on the other.
sudo -u qiita-api ls -d "$ROOT"/*/*/hifi_reads "$ROOT"/*/*/*/hifi_reads 2>&1 | head -5
```

Read the output rather than counting lines — `2>&1` keeps the reason, which is the whole test:

- **Paths** — the grant landed.
- **`Permission denied`** — it did not. Check `id qiita-api` lists `kl-seq-rw`, then that the
  unit has actually restarted since the `usermod`.
- **`No such file or directory` on a literal `*` path** — that glob matched nothing, which is
  expected for whichever of the two layouts this deploy does not use. Only worrying if BOTH say it.

The globs are expanded by your own account, not `qiita-api`, so an unapplied grant shows as
denials rather than as silence. (#484)

### 3. Migrations

Plain `make migrate` — no out-of-band setup.

- `20260824000000_upload_source_filename.sql` — adds nullable `qiita.upload.source_filename`
  (the client's basename, so the fastq filename-prefix rule applies to an upload-fed
  submission). Existing rows keep NULL, which the gate skips.
  (#484)

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
  Then confirm the roots are not too *narrow*, which fails the other way — a legitimate
  submit refused: `ingest_roots` in that same body must contain the reference staging root,
  or `qiita reference load --local` 422s for every reference. (#484)
- **A submit can now run off the cluster — both platforms.** `POST /run-folder/inspect`
  reads the run folder as `qiita-api`, a NARROWER account than the `qiita-job` that runs
  the jobs: it reaches the IGM folders through their world bits, and the gcore folders
  only through the group membership bucket 2 grants. Confirm one real folder of each kind:
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
  `pacbio.hifi_bam_by_barcode`. A **403** means the grant did not take, and its `reason` says
  whether the denial is at the run folder (or a parent) or at a directory below it — a
  partial grant is refused rather than reported as an empty index.
  (#484)
- **The widened read-ingest schemas synced.** `qiita-admin actions list` must still show
  `fastq-to-parquet 1.3.0` and `bam-to-parquet 1.0.0` enabled — the change is a
  `context_schema` widening in place, not a version bump, so no new version appears and none
  should have been auto-deprecated. (#484)

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- **A `user` role gains `ticket:doput` on the next deploy.** No host action — role
  ceilings are code, applied by the restart. **Existing user PATs do not gain it**: token
  auth resolves to `frozen mint-time scopes & current role ceiling` (`auth/principal.py`),
  and widening the ceiling cannot add what the token never carried — a user wanting the
  new capability mints a fresh token.
  The scope buys a staging slot the caller owns; `reference:write` stays admin-only, so
  this does not let a user load a reference database. (#484)

- **A `user` can no longer name a host path in `action_context`; it is now wet_lab_admin+.**
  A `fastq_path` / `bam_path` submission from a `user` role returns 403 naming the
  `*_upload_idx` handle to use instead. They load reads with the new `qiita submit-reads`,
  which streams the file to the data plane and submits against the upload — the same workflow
  either way, since the runner resolves the handle to the same step input. Existing
  wet-lab-admin and system-admin path submissions are unaffected apart from the root bound.
  (#484)
- **`submit-bcl-convert` / `submit-pacbio-ingest` no longer have to run on a machine that
  mounts the cluster.** Both used to read the run folder locally — RunInfo.xml for
  Illumina, the `*/hifi_reads/*.bam` glob for PacBio — which is why the runbook told
  operators which machine to type on. Both reads moved to `POST /run-folder/inspect`. The
  path still has to be the one the CLUSTER sees; what changed is that it is now checked
  and read where it exists, so a wrong path fails at the terminal instead of inside a job.
  (#484)
- **`qiita submit-reads` needs `--data-plane-url`**, like `qiita reference load` — the reads go
  over Flight. From off the host that is the public TLS edge
  (`grpc+tls://<fqdn>:443`). (#484)

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
