# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (most are `from_env()` fail-fast; a missing one keeps the unit down)

- **CP `MIINT_EXTENSION_DIRECTORY`** — the CP runner now LOADs miint in-process to stream masked reads (the `long-read-assembly` input). Copied from the CO's env so the two stay byte-identical; the directory only needs to be **readable** by `qiita-api` (LOAD writes nothing). Not fail-fast, unlike most of this bucket: the CP boots and serves every other route, and only `long-read-assembly` tickets fail — at submission, with a message naming this var. Bucket 5's `cp-miint` check is what catches a missed step. `(#350)`
  ```bash
  sudo bash -c 'grep -q "^MIINT_EXTENSION_DIRECTORY=" /etc/qiita/control-plane.env && exit 0
  line=$(grep -h "^MIINT_EXTENSION_DIRECTORY=" /etc/qiita/compute-orchestrator.env) \
    || { echo "MIINT_EXTENSION_DIRECTORY missing from compute-orchestrator.env — set it there first" >&2; exit 1; }
  printf "%s\n" "$line" >> /etc/qiita/control-plane.env
  echo "installed: $line"'
  ```

### 2. One-time host setup

_None yet._

### 3. Migrations

_None yet._

### 4. Deploy

_None yet._

### 5. Verify

- **`cp-miint`** — new `make verify-deploy` check (no separate command): asserts the control plane can LOAD miint, the masked-read streaming path `long-read-assembly` depends on. A red row here means bucket 1 was missed. `(#350)`

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

_None yet._

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
