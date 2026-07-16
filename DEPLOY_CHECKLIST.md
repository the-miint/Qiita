# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

_None yet._

### 2. One-time host setup

_None yet._

### 3. Migrations

_None yet._

### 4. Deploy

_None yet._

### 5. Verify

- **`read-mask` re-synced with the `lima` entrypoint fix.** The `lima` container
  step now carries `entrypoint: /opt/qiita/lima.sh` (it was missing, so PacBio masks
  died at the lima step with an apptainer arg error). The YAML is re-synced by
  `qiita-admin actions sync` inside `activate.sh`, not migrated. Confirm `read-mask`
  is present at 1.0.0 in the `qiita.action` list `make verify-deploy` prints. No SIF
  rebuild is needed — only the step's entrypoint changed, not lima.def/lima.sh. (#311)

### 6. After the deploy verifies green

_None yet._

### Notes (no host action)

- **PacBio read-mask fetch UNAVAILABLE is now retriable (#311).** A transient
  data-plane gRPC UNAVAILABLE during read materialization is recorded retriable
  (`DATA_PLANE_TRANSIENT`) rather than permanently failing the ticket. Code-only —
  rides the standard CP redeploy, no host action.

---

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
