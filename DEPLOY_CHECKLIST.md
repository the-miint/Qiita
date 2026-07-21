# Deploy checklist

Operator-facing deploy instructions — **not** a "what changed" log (that's [`CHANGELOG.md`](CHANGELOG.md); the git log is the authoritative record). `## Pending deploy` is the single consolidated checklist for the next deploy; past deploys are archived one file each under [`docs/deploy-archive/`](docs/deploy-archive/).

- **Deploying?** Follow [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) — it is the source of truth for the procedure (bucket order, `[admin]`/`[operator]` labels, the migration guard, archiving).
- **Adding to a PR?** Fold your operator steps into the `## Pending deploy` buckets with `/deploy-note`; don't add a standalone entry. The authoring rules are in CLAUDE.md ("Operator-facing changes").

Substitute your host's FQDN for the `qiita-miint.ucsd.edu` examples and `<scratch>` for the scratch root chosen at first deploy.

---

## Pending deploy

Everything merged but not yet deployed, folded in by each PR as it merges. Run buckets 1→6 in order; buckets 1–3 must precede the bucket-4 restart, and bucket 6 (irreversible cleanup — anything that burns the rollback path) must not run until bucket 5 is green. Each step carries its source `(#N)` tag.

### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast; a missing one keeps the unit down)

- **CP `MIINT_EXTENSION_DIRECTORY`** — the CP runner now LOADs miint in-process to stream masked reads (the `long-read-assembly` input). Copied from the CO's env so the two are byte-identical; the directory only needs to be **readable** by `qiita-api` (LOAD writes nothing). Unlike the rest of this bucket it does **not** keep the unit down — the CP boots fine and only `long-read-assembly` tickets fail (at submission, with a message naming this var). `(#fix/cp-miint-extension-directory)`
  ```bash
  sudo bash -c 'grep -q "^MIINT_EXTENSION_DIRECTORY=" /etc/qiita/control-plane.env \
    || grep -h "^MIINT_EXTENSION_DIRECTORY=" /etc/qiita/compute-orchestrator.env \
       >> /etc/qiita/control-plane.env'
  ```

### 2. One-time host setup

_None yet._

### 3. Migrations

_None yet._

### 4. Deploy

_None yet._

### 5. Verify

- **CP can LOAD miint** — the masked-read streaming path `long-read-assembly` depends on. `cd /tmp` first: `qiita-api`'s home is `/dev/null`, so running from an operator home dir fails on cwd. `(#fix/cp-miint-extension-directory)`
  ```bash
  cd /tmp && sudo -u qiita-api env $(grep -h '^MIINT_EXTENSION_DIRECTORY=' /etc/qiita/control-plane.env) \
    /home/qiita/qiita-miint/qiita-control-plane/.venv/bin/python -c \
    'from qiita_control_plane.miint import connect_with_miint_staged; connect_with_miint_staged().close(); print("miint LOAD ok")'
  ```

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
