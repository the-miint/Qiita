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

_None yet._

### 3. Migrations

_None yet._

### 4. Deploy

_None yet._

### 5. Verify

- The rebuilt `long-read-assembly` assemble image carries myloasm at the pinned 0.6.0 and the splitter it runs (#259). The version is the contract: the circular/linear split reads a myloasm FASTA **header string** probed against 0.6.0, so a drifted solve would classify every genome as linear rather than error. Expect `ASSEMBLE_MYLOASM_OK`.
  ```bash
  cd /tmp && sudo -u qiita-orch apptainer exec --no-home \
    "${PATH_DERIVED}/images/long-read-assembly-assemble-1.0.0.sif" \
    bash -c "micromamba run -n assemble myloasm --version | grep -Fxq 'myloasm 0.6.0' \
             && test -s /opt/qiita/myloasm_split.awk" \
    && echo ASSEMBLE_MYLOASM_OK
  ```

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- The `long-read-assembly` assemble SIF auto-rebuilds on this deploy to add myloasm 0.6.0 alongside hifiasm_meta; its solve is bigger than before, so this image's build takes longer than a routine no-op verify (#259).
- `long-read-assembly` 1.0.0 accepts `assembler: myloasm` from this deploy on — previously it exited 64 mid-step. The **default is unchanged** (`hifiasm_meta`), so no existing ticket changes behaviour; picking myloasm is an assay decision made per action context (#259).

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
