# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Python version

This repo targets **Python 3.14**. Run tooling via `uv run` — a stray pre-3.14 `python3` misparses `except A, B:` ([PEP 758](https://peps.python.org/pep-0758/)), which is valid here. Don't "fix" it to `except (A, B):`.

## Common commands

```bash
# First-time setup (run once after cloning)
make install-hooks   # installs pre-commit hooks via uv tool

# Check tool prerequisites; prints install commands only for what's missing
make dev-setup

# Build all components (release data-plane binary uses bundled DuckDB)
make build

# Test
make test                          # pure-unit tests (all components, no infrastructure required)
make test-control-plane-with-db    # full control-plane suite incl. DB-bound (-m db) tests; brings up Postgres + applies dbmate migrations
make test-integration              # cross-component tests; requires Docker (or QIITA_USE_HOST_POSTGRES=1 with libpq env vars to use a host postgres); runs Python + Rust integration suites against postgres on :5433; excludes -m system
make test-system                   # real GG2 backbone data; slow (~10 min); needs localdocs/scratch/
make test-workflows                # requires apptainer (Linux-only — macOS skips gracefully); CI runs this on ubuntu only

# Lint
make lint

# Database migrations (auto-installs dbmate)
make migrate

# Deploy (prints systemd + nginx instructions; does not sudo)
make deploy
make verify-health         # auto-installs grpcurl; localhost-only health curls

# Established-host deploy tooling (run on the deploy host)
sudo make redeploy QIITA_HOSTNAME=<fqdn> # admin (root): all-in-one guided redeploy — sudo -u's into operator/qiita-api/qiita-orch per step (pull→preflight→migrate gate→deploy→stage→verify)
sudo make preflight                      # read-only config/secret consistency + non-secret fingerprints (or plain `make preflight` as the ACL-granted operator — token fingerprints degrade to n/a)
sudo make verify-deploy QIITA_HOSTNAME=<fqdn>  # generic post-deploy checks (health, actions, compute-readiness) with correct run-as each

# Clean component build artifacts (.venv, target/, caches)
make clean
```

**Running a single test:**
```bash
# Python
cd qiita-control-plane && uv run pytest tests/test_smoke.py::test_health

# Rust — DUCKDB_DOWNLOAD_LIB=1 dynamically links a prebuilt libduckdb from
# target/duckdb-download instead of rebuilding the bundled DuckDB from source.
# Without it, every invocation can spend many minutes compiling DuckDB.
cd qiita-data-plane && DUCKDB_DOWNLOAD_LIB=1 cargo test config_with_valid_env
```

**Linting a single component:**
```bash
cd qiita-common && uv run ruff check . && uv run ruff format --check .
cd qiita-data-plane && DUCKDB_DOWNLOAD_LIB=1 cargo clippy -- -D warnings && cargo fmt --check
```

**Cross-package staleness — handled by `make build` / `make test*`.** When `qiita-common`, `qiita-control-plane`, or `qiita-compute-orchestrator` change, plain `uv sync` in a dependent skips the rebuild because the version string is unchanged, leaving stale sources in `.venv/.../site-packages/<pkg>/` and producing confusing `ImportError`s for newly-added symbols or `TypeError: __init__() got an unexpected keyword argument` for newly-added fields. The `build-*` and `test-*` Makefile targets pass `--reinstall-package` to force a rebuild of the affected path deps in every consuming venv (the three project venvs plus `tests/integration/.venv`).

If you bypass `make` and run `uv` directly after a cross-package change, replicate the flag yourself, e.g.:
```bash
cd qiita-control-plane && uv sync --reinstall-package qiita-common
```

## Development ethos

**Fail fast, fail early, fail loudly.** Validate inputs at every boundary. Return structured errors with enough context to diagnose without a debugger. Prefer raising/panicking over silently returning defaults for unexpected states. Silent failures are bugs.

**No issue/PR numbers in code.** Don't tag comments, docstrings, or string literals with this repo's GitHub issue/PR numbers (`(#142)`, `see #131`, `tracked in #40`, etc.) — keep the explanatory comment, drop the number. Provenance belongs in git history, `CHANGELOG.md`, and the PR, not the source. Two carve-outs: references to an **external** tracker are fine when qualified (`DuckDB #23229`), and `CHANGELOG.md` / `DEPLOY_CHECKLIST.md` are the provenance logs where `(#N)` PR tags belong.

**Use of opaque identifiers outside of Qiita requires explicit approval; stable and publicly sharable IDs are prioritized** (e.g., biosample, `exported_identifier`, etc). Our minted `*_idx` values — `prep_sample_idx`, `sequenced_pool_idx`, `sequencing_run_idx`, `alignment_idx`, `study_idx` — are internal: they mean nothing to anyone outside this system, they expose our structure, and they are a handle we do not promise to keep. This governs everything that crosses the boundary, not just API bodies: a response a user publishes beside a result, an exported filename, a column in a feature table or BIOM file, a label composed for a manuscript. Echoing back an identifier the caller themselves sent is not a leak; composing one into something they will publish is. When a public handle is needed and no accession exists, mint one (`qiita.exported_identifier`) rather than reaching for an `_idx`.

**State a rationale once, at the site that acts on it.** A measured number, a security argument, or a design trade-off belongs at the one place that *enforces* it — the constant, the guard, the signing boundary — and nowhere else. Every other site that touches the same subject either points at that one ("see `auth/tickets.py`") or says nothing. The test: **would this comment have to change if the underlying fact changed?** If yes, it is a second copy, not context — replace it with a pointer. Repetition is not merely wasteful: each copy drifts on its own schedule, and a reader who finds two versions of a claim has no way to tell which one is current. If a fact really is needed at N sites, that is a signal it wants to be a named constant or a helper whose docstring is the single copy. A worked example: `cigar` being ~96% of an alignment row was, at one point, asserted in nine files.

**No development-plan vocabulary in code.** "M0", "M3", "Phase 4", "item 6", "the milestone" — these name a planning document that is not in the repo and will not outlive the branch, so they say nothing to whoever reads the line next. This covers comments, docstrings, test names, and `CHANGELOG.md` prose alike. Write the substance instead: not "M0 measured the break-even at ~4 Gbit/s", just "the measured break-even is ~4 Gbit/s". The `(#N)` carve-out above does not extend to these — a PR number resolves forever, a milestone label does not.

**Don't whole-file-read the big files.** Several source and test modules here run past 3,000 lines (the control-plane CLI/runner test modules, `qiita-data-plane/src/flight_service.rs`). Reading one whole costs more context than every instruction file in this repo combined, so locate first (grep / LSP / a targeted `Read` offset+limit) and read only the span you need. `CHANGELOG.md` is long for the same reason: its header says where to add an entry, so you never need to read the body.

## miint is a core dependency

The duckdb-miint extension is the **foundation** of the compute/data system, not optional — every bioinformatics primitive (read_fastx, hashing/chunking, minimap2/bowtie2, host-filter, phylogeny, feature tables) runs through it. On the cluster it is **fail-loud, never fail-soft**: `config._resolve_slurm_settings()` keeps the CO **down** at boot, and `miint_job_env()` **raises** (never a silent empty dict), unless BOTH required job vars are set — `MIINT_EXTENSION_DIRECTORY` (the staged extension) and `MIINT_GPL_BOUNDARY_PATH` (the GPL-boundary binary for bowtie2/vsearch/MAFFT/SortMeRNA). The boundary can't ride `$HOME` — native jobs get an ephemeral per-ticket HOME — so it must be **forwarded into the job** (the slurmrestd env is an allowlist), and the compute-readiness `miint-gpl-boundary` probe fails the *deploy* if it's unreachable. Carve-out: the client `qiita reference load` CLI runs with these unset (installs into its own cache) via `miint_connect_config()`.

**A miint surprise gets an issue, in the PR that lands the workaround.** When miint's behaviour doesn't match what we expected — an undocumented or outright false contract, a missing capability we have to route around, a bug — file it upstream at [the-miint/duckdb-miint](https://github.com/the-miint/duckdb-miint/issues) **in the same PR**, not "later". Then name it by qualified number at every site that carries the workaround: the code comment, the [`docs/duckdb-miint.md`](docs/duckdb-miint.md) entry, and the `CHANGELOG.md` line (`duckdb-miint#173` — an external tracker, so the "No issue/PR numbers in code" rule above permits it). If the workaround is code we intend to delete once upstream fixes it, also open a Qiita issue for the removal with its **exit criteria**, and add a row to [Open upstream gaps](docs/duckdb-miint.md#open-upstream-gaps) — that table is the standing list of what we carry for miint's sake, and the row is what makes the cleanup scheduled rather than remembered. A workaround with no issue behind it is the defect: it quietly becomes permanent, and the next reader can't tell whether it is still needed. The reverse holds too — a claim about miint that a probe disproves is worth an issue even when we don't need a fix, because the next person will otherwise re-derive it from the same wrong docs.

The **control plane** also LOADs miint in-process (the masked-read streamer that feeds `long-read-assembly`), so `MIINT_EXTENSION_DIRECTORY` must be set in `control-plane.env` too, byte-identical to the CO's — `make preflight` compares them and `make verify-deploy`'s `cp-miint` check LOADs it. It is deliberately **not** fail-fast at CP boot (the CP serves every other route without it; only assembly tickets fail, naming the var via `require_staged_extension_directory()`), because taking the whole REST API down for one workflow's input binding is the wrong trade. Service-side connects are **LOAD-only, never INSTALL**: `qiita-api`'s home is `/dev/null`, so an INSTALL resolves `$HOME/.duckdb/extensions` and dies with `Can't find the home directory`.

## Workflow runtimes

A step in a workflow YAML must declare **exactly one** of `container:` or `module:`. The `module:` form (a native step) runs in the orchestrator's Python environment under SLURM and may only use dependencies that already ship in `qiita-compute-orchestrator`'s `pyproject.toml`; anything heavier (bioinformatics deps, system packages) belongs in a container.

Native job modules export two required symbols — `class Inputs(BaseModel)` (typed input contract) and `async def execute(inputs, workspace)` (the work) — plus an optional `def plan(inputs) -> JobPlan` for submit-time resource sizing, which the control plane reads once per native step before submit. A single framework dispatcher handles import, validation, and error classification; both the local backend and the SLURM launcher route through it. [`docs/writing-a-job.md`](docs/writing-a-job.md) carries the full contract table.

The wire validator enforces shape only (exactly-one). The module-prefix invariant (`qiita_compute_orchestrator.jobs.`) is enforced separately at sync, submit, boot scan, and dispatcher — [`docs/architecture.md`](docs/architecture.md) carries the per-site breakdown.

Container steps declare a bare SIF filename; a workflow opts into the one generic
builder with a per-workflow spec. The resolution rules, the two spec forms, the
two-gate idempotency check, and why the deploy rebuilds SIFs automatically are in
[`docs/container-images.md`](docs/container-images.md) — read it before editing a
`.def`, an `entrypoint.sh`, or `workflows/_shared/manifest_writer.py`.

## Naming conventions

**DB tables, REST resource segments, scope strings, OpenAPI tags, and the source files that own them are always singular**, never plural — `reference` not `references`, `auth_event` not `auth_events`, `/user` not `/users`, `reference:read` not `references:read`, `routes/reference.py` not `routes/references.py`, `tests/test_user.py` not `tests/test_users.py`. This applies to junction tables (`user_identity`, not `user_identities`); use `_to_` for many-to-many junctions when both sides need to be named (e.g. `biosample_to_study`). Column names follow the same rule unless the column genuinely holds a list/array.

**Carve-outs:** verb / action path segments stay plural where natural (`/admin/principal/{idx}/revoke-all-tokens` — `revoke-all-tokens` is a verb, not a resource). On-disk directory names (`/scratch/persistent-local/references/`, `references/incoming/`) are not REST resources and are not constrained by this rule. `/user/me` reads awkwardly but is the correct form — the alternative is a permanent carve-out for `/me`-suffixed paths.

Fixed in #11 after the initial schema mixed both forms.

## REST path constants

REST paths live exclusively in `qiita-common/src/qiita_common/api_paths.py`. Never hardcode `"/api/v1/..."` literals in routes, tests, or clients — import the constants instead.

Two flavours per route:

- `PATH_*` — sub-path used by FastAPI `@router.<verb>(...)` decorators and the matching `prefix=` declaration.
- `URL_*` — full path under `API_PREFIX` for tests and clients, with `{placeholder}` segments where parameterized.

When you add or rename a route, define both flavours and register the triple in the parity test in `qiita-common/tests/test_api_paths.py`. A missing triple fails the test; a `URL_*` constant left out of the registration list also fails. Routers sharing a prefix (`/study` is reused by biosample and sequenced-sample; `/sequencing-run` by sequenced-sample) declare `prefix=PATH_STUDY_PREFIX` etc. so a prefix rename moves every router at once.

## Database migrations

The qiita-miint deploy is live; every migration currently in `qiita-control-plane/db/migrations/` (`YYYYMMDDHHMMSS_<name>.sql`, starting with `20260501000000_schema.sql`) has been applied to its Postgres. **Never edit an already-applied migration** — `dbmate` tracks applied versions in `schema_migrations` and won't re-run an edited file, so the live DB silently drifts from the source.

Every schema change is a **new migration file** (`YYYYMMDDHHMMSS_<name>.sql`, with `migrate:up` and `migrate:down` blocks). Common shapes:
- Add a column / index / constraint: a single `ALTER TABLE` migration.
- Add a Postgres ENUM value: `ALTER TYPE ... ADD VALUE`, with the Python `StrEnum` twin updated in the same PR (see Enum parity below).
- Rename / drop / type-change: expand-then-contract across two migrations (and usually two PRs) so a rolling deploy doesn't 500.

Before merging: `make test-control-plane-with-db` runs `dbmate up` against a fresh DB and must pass — that's the only safety net before the migration touches production. After merging: the operator runs `make migrate` against the live DB on the next deploy.

## Enum parity (Python ↔ Postgres)

Many closed value sets are **deliberately duplicated**: once as a Python `StrEnum` in
`qiita-common` (so Pydantic models type-check at import time, with no DB connection) and once
as a Postgres `CREATE TYPE ... AS ENUM` (so the database itself rejects bad values). This
duplication is a chosen compromise — the DB is *not* the single source of truth — so do **not**
derive one side from the other.

**Only value sets that are `CREATE TYPE ... AS ENUM` are in scope.** A `StrEnum`/`Literal`
backed by a `TEXT`/`CHECK` column is a valid, deliberate choice: `auth_event.event_type`,
`reference.status`, `reference.kind`, and `upload.status` are intentionally plain `TEXT`. A new
`StrEnum` with no Postgres ENUM is *not* a defect — do not flag it in review.

When you add, rename, or remove a value in an enum that *does* have a `CREATE TYPE` twin:

1. **Change both sides in the same PR.** Postgres ENUM changes go in a **new migration**
   (`ALTER TYPE ... ADD VALUE`) — editing an already-applied `CREATE TYPE` does not reach
   databases that already ran it.
2. **Keep the two-way comment.** The Python enum's docstring names its Postgres twin and vice
   versa.
3. **Register the pair in `ENUM_PAIRS`** (`qiita-control-plane/tests/test_enum_parity.py`).
   `test_enum_parity` fails on drift; `test_all_postgres_enums_are_covered` fails on an
   unregistered Postgres ENUM. Both run under `make test-control-plane-with-db`.

A PR that changes one side without the other, the two-way comment, and the `ENUM_PAIRS` entry is
incomplete.

## Operator-facing changes (DEPLOY_CHECKLIST.md)

`DEPLOY_CHECKLIST.md` is the operator's deploy checklist — **not** a per-PR change log (that's
`CHANGELOG.md`). It is one *living* `## Pending deploy` section: a consolidated, deduplicated,
ordered checklist of everything merged but not yet deployed. See the file's own preamble for the
bucket layout, and [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md) for the
deploy/archive lifecycle.

**A PR with operator-impacting changes folds its steps into `## Pending deploy` in the same PR**,
never as a standalone heading. Run `/deploy-note` on the branch. Fold whenever the PR introduces
a new required env var, a new shared directory / service-account grant / vendored SIF, a
migration needing out-of-band setup, a new or changed `workflows/` entry, or a soft API-contract
change downstream clients should know about.

**Merge, don't append.** Add your line alongside the *existing* lines for that file, never a
parallel block. The `deploy-note-check` CI job fails a PR that changes an operator-impacting
surface (a `.env.*.example` var, `db/migrations/`, `workflows/`, `auth/scopes.py`) without
touching `DEPLOY_CHECKLIST.md`; the `no-deploy-note` label opts out.

A PR that only changes Python/Rust code, tests, docs, or migrations the dbmate flow handles
autonomously needs **no** fold. When in doubt: "if the operator follows `redeploy.md` without
reading this PR, does the deploy succeed and behave as intended?" If no, fold.

## Per-PR changelog (CHANGELOG.md)

`CHANGELOG.md` at the repo root is the "what changed" log — the human-readable counterpart to the git history, distinct from the operator deploy checklist (`DEPLOY_CHECKLIST.md`, above). It follows [Keep a Changelog](https://keepachangelog.com/); since the project doesn't cut versioned releases yet, every entry lands under `## [Unreleased]` in an `Added` / `Changed` / `Fixed` / `Removed` bucket, tagged with its `(#N)` PR ref.

**Every PR adds an entry** under `## [Unreleased]`. The `changelog-check` CI job (`.github/workflows/ci.yml`) fails any PR whose diff doesn't touch `CHANGELOG.md`; a PR that genuinely warrants no entry (a typo fix, a CI-only tweak) carries the `no-changelog` label to opt out. This is a deliberately blanket gate — unlike `deploy-note-check`, it fires on *every* PR, not just operator-impacting ones. A change can warrant a `CHANGELOG.md` entry, a `DEPLOY_CHECKLIST.md` fold, or both; keep the two files from drifting into each other (the changelog says *what changed*, the checklist says *what the operator must do*).

**Never create a new bucket heading** — duplicate headings are how the file previously grew to 5,745 lines with eleven of them. Add your bullet under the existing `### Added` / `### Changed` / `### Fixed` / `### Removed`. Entries predating the last rotation live in [`docs/changelog-archive/`](docs/changelog-archive/).

## Deployments

We deploy **many PRs at once**: a sequence of PRs to `main`, then a single manual deploy. We do
not cut releases. Migrations are applied **out-of-band, before the restart** (`make migrate` is
a separate operator step — `activate.sh` asserts every shipped migration is recorded and aborts
before any restart if one is missing). New required env vars are written **before** the restart,
or `from_env()` fail-fast keeps the unit down.

The full invariant list, the operator helper scripts (`redeploy`, `preflight`, `verify-deploy`),
and what each one must not weaken are in [`docs/deployments.md`](docs/deployments.md). The
standing procedure is [`docs/runbooks/redeploy.md`](docs/runbooks/redeploy.md).

## Architecture

`docs/architecture.md` is the system reference — the diagram, the component map and ports, the
identifier hierarchy, the data-plane Flight design, the compute-orchestrator pattern, and the
workflow runner all live there, under **Cross-cutting structure**. Read the section you need
rather than the whole file; it runs past 1,500 lines.

| Looking for | Read |
|---|---|
| system diagram, components, ports, identifier hierarchy, Flight ops, orchestrator, runner | [`docs/architecture.md`](docs/architecture.md) |
| how a `container:` filename resolves to a SIF, the generic builder, spec forms, idempotency | [`docs/container-images.md`](docs/container-images.md) |
| deploy invariants and the operator helper scripts | [`docs/deployments.md`](docs/deployments.md) |
| the three test tiers and what infrastructure each needs | [`docs/testing.md`](docs/testing.md) |
| how reference databases are ingested | [`docs/reference-data-staging.md`](docs/reference-data-staging.md) |
| the auth surface — principals, OIDC, scopes, admin endpoints | [`docs/auth.md`](docs/auth.md) |
| the duckdb-miint SQL extension | [`docs/duckdb-miint.md`](docs/duckdb-miint.md) |
| writing a native job module | [`docs/writing-a-job.md`](docs/writing-a-job.md) |
| operational runbooks | [`docs/runbooks/`](docs/runbooks/) |
| changelog entries predating the last rotation | [`docs/changelog-archive/`](docs/changelog-archive/) |

### qiita-common as a path dependency

```toml
# in qiita-control-plane/pyproject.toml and qiita-compute-orchestrator/pyproject.toml
qiita-common = { path = "../qiita-common" }
```

This is the contract layer between the two Python services. Pydantic models for work ticket states and API schemas live here. Changes here affect both dependents — re-run `uv sync` in each.

### Lock files

Both `uv.lock` (Python) and `Cargo.lock` (Rust) are committed. Do not add them to `.gitignore`.

### `.claude/settings.json` is presentation-only

The checked-in `.claude/settings.json` sets `PYTEST_ADDOPTS`, `CARGO_TERM_QUIET`, and `UV_NO_PROGRESS` so that a *green* test run doesn't spend an agent's context restating itself (a passing `cargo test` printed one `... ok` line per test; a passing `pytest` printed a header and a warnings block). Every failure is still reported, with its file, line, and assertion — `--tb=short` shortens each frame to one line, it does not drop frames or hide failures.

**Only presentation belongs there.** These vars apply to agent-run commands and *not* to a human's shell or to CI, so anything that changes test **selection, ordering, or exit status** — `-x`, `--tb=no`, `-p no:randomly`, `-k` — would silently give agents different semantics than the run that gates the merge. Those belong in the `Makefile`, where CI sees them too.

### Test layout and tiers

Three tiers by the infrastructure each needs: `make test` (pure-unit, no infrastructure),
`make test-control-plane-with-db` (adds the `db`-marked tests), `make test-integration`
(cross-component, Docker). Details, markers, the Postgres harness, and the DuckLake catalog
reset are in [`docs/testing.md`](docs/testing.md).
