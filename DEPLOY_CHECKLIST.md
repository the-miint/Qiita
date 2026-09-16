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

_None yet._

### 6. After the deploy verifies green

- **[operator] Mint `edge_id` on references loaded before this deploy.** Unless its Newick carried jplace `{N}` decorations, such a reference's `reference_phylogeny` rows carry `edge_id` NULL on every node (the loader mints it only from this deploy on), which a phylogenetic-placement index cannot join back to. One call per reference, `reference:write` (wet_lab_admin or system_admin); idempotent, so re-running is a no-op and a reference whose tree already carries numbering is left alone:
  ```bash
  curl -sS -X POST -H "Authorization: Bearer $QIITA_TOKEN" \
    "https://qiita-miint.ucsd.edu/api/v1/reference/<reference_idx>/phylogeny/mint-edge-id"
  ```
  Expect `{"reference_idx": N, "phylogeny_rows": <tree size>, "already_numbered_rows": 0, "minted_rows": <tree size>}`. A `200` with `minted_rows: 0` and `already_numbered_rows == phylogeny_rows` means that tree already had its numbering — nothing to do. A `404` is an unknown `reference_idx`. A `409` means the reference exists but has no phylogeny rows, or its tree is partly numbered; both need a look before anything is written. A `502` is either the data plane being unreachable or a reply whose counts do not add up — read the detail: it says whether re-issuing is safe, and for a partially written tree it is not. Run it for **both** references currently in the lake — `18` (Web of Life 3, 392,123 rows) and `16` (452,189 rows); both carry `edge_id` NULL on every row, and a tree left unnumbered fails at placement time rather than at load. References loaded after this PR are numbered by `reference_load` itself and need no call. (#581)

### Notes (no host action)

_None yet._

## Deployed history

Past deploys live one file each in [`docs/deploy-archive/`](docs/deploy-archive/) — newest
first in its [index](docs/deploy-archive/README.md). `/deploy-archive` writes the next one
there when a deploy closes out.

(This heading has no content under it by design, and is not dead weight: it terminates the
`sed` range that prints `## Pending deploy` for the operator and for `/deploy-note`. See
`test_deployed_history_heading_pins_the_live_section_boundary`.)
