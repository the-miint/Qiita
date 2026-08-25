# Container image tier

How a container step's `container:` filename resolves to a SIF, the one generic
builder, the per-workflow spec forms, and the two-gate idempotency check.
Referenced from [`CLAUDE.md`](../CLAUDE.md).

Container steps declare a bare SIF filename in `container:` (e.g. `bcl-convert-4.5.4.sif`). The orchestrator joins this against `Settings.path_derived_images` (derived as `PATH_DERIVED/images`; `PATH_DERIVED` env var required when `COMPUTE_BACKEND=slurm`) to resolve the absolute SIF path. Registry-URL forms with `://` pass through; anything else with a path separator → `CONTRACT_VIOLATION`.

**One generic builder, declarative per-workflow spec.** `scripts/build-sif.sh <workflow>` is the *only* SIF build script. A container workflow opts in by adding a spec (`SIF_FILENAME`, optional `SOURCES`, `VERIFY_CMD`, `VERIFY_MATCH`, optional `AUTO_BUILD`) — never a per-workflow build script, and never stage build inputs into the checkout. The spec takes one of two forms, and `deploy/build-sifs.sh` globs both:

- **Single image** — `workflows/<workflow>/sif-build.env`, alongside `Apptainer.def` / `entrypoint.sh` (e.g. `bcl-convert`).
- **Multiple images** — `workflows/<workflow>/sif-build.d/<image>.env`, one spec per image, each with its own `<image>.def` / `<image>.sh`, for a workflow whose steps need different containers (e.g. `read-mask`'s `lima`; `long-read-assembly`'s `assemble` / `binning` / `bin_refine` / `checkm`).

The builder stages into a temp build root **owned by the invoking user** and only ever *reads* the checkout, so a locked-down service account (e.g. `qiita-orch`) can build without write access to the qiita-owned `/home/qiita` checkout. A CI guard (`qiita-compute-orchestrator/tests/test_sif_build_spec.py`, under `make test`) forbids `scripts/build-*-sif.sh`, requires each spec to be complete, and asserts `SIF_FILENAME` matches the workflow YAML's `container:` value.

**Idempotency is two-gate.** `build-sif.sh` skips a rebuild only when the existing SIF satisfies the spec's `VERIFY_MATCH` (the vendored binary's version) *and* a content hash of the in-repo build inputs (the `Apptainer.def`, `entrypoint.sh`, and `workflows/_shared/manifest_writer.py`) matches the stamp from the last build (`<sif>.buildhash`, computed by `qiita_sif_build_inputs_hash` in `deploy/_common.sh`). The hash deliberately **excludes** the vendored `SOURCES`, so re-vendoring a licensed RPM (4.5.4-1 → 4.5.4-2) doesn't force a rebuild. `FORCE=1` is an emergency override that skips both gates.

**The deploy builds SIFs automatically.** `activate.sh` runs `deploy/build-sifs.sh`, which iterates every spec of either form above and invokes `build-sif.sh` for each, before the service restarts (see the SIF invariant under "Deployments"). So a routine deploy picks up an edited def/entrypoint/manifest with no manual step. A spec may opt out with `AUTO_BUILD=0` (then build it by hand); names starting with `_` (e.g. `_sif-build-smoke`) are never auto-built.

After editing a workflow YAML or its container artifacts (the spec, its `.def` / `.sh`, or the shared `workflows/_shared/manifest_writer.py`), the SIF rebuild happens **automatically at the next deploy** (the content hash detects the change) — **don't write a manual "rebuild the SIF" deploy step**, and `/deploy-note` won't either. These steps run on the **Linux deploy host** — they need `apptainer` and `systemd` — so they don't apply on a macOS dev box (mirrors `make test-workflows`, which skips gracefully off Linux). On macOS, edit the artifacts and run the unit tests.

To build out-of-band (e.g. to verify a def change on the host before a full deploy), **run it as root** — `apptainer build` bind-mounts the invoking account's home, and the `qiita-orch` service account's home is `/dev/null` (build fails to mount it), whereas root's `/root` is real. This matches the deploy's auto-build, which runs as root and then chowns the SIF to `qiita-orch`:

```bash
# Build one SIF directly, as root (idempotent — two-gate skip above; FORCE=1 to override).
sudo env PATH_DERIVED=<derived> bash scripts/build-sif.sh <workflow>
sudo chown qiita-orch:qiita-orch <derived>/images/<sif> <derived>/images/<sif>.buildhash
make deploy   # the deploy also (re)builds any changed SIF via build-sifs.sh
sudo systemctl restart qiita-control-plane qiita-compute-orchestrator
make verify-health
```

Container input bind mounts are computed by `SlurmBackend._resolve_input_binds` (file → parent dir, directory → itself, deduped by resolved path). This means a step's YAML-declared `inputs:` paths must be absolute when they originate from `action_context` and must be visible from the compute node — bind mounts only expose host paths, they do not copy.
