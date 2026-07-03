# Writing a native job

A **native job** is one workflow `step:` that runs in the compute
orchestrator's own Python environment (under SLURM in production, in-process
under `LocalBackend` in dev/test) — the `module:` form of a step, as opposed to
the `container:` form. This is the guide to adding one.

If your step needs bioinformatics tooling or system packages that don't already
ship in `qiita-compute-orchestrator`'s `pyproject.toml`, it belongs in a
**container**, not a native job — see [`architecture.md`](architecture.md) and
the "Workflow runtimes" section of [`../CLAUDE.md`](../CLAUDE.md). Native jobs
may only import dependencies already in the orchestrator's environment.

## The contract: three symbols

A native job is a Python module under
`qiita_compute_orchestrator/jobs/` that exports:

| Symbol | Required | Shape | Role |
|---|---|---|---|
| `Inputs` | ✅ | `class Inputs(BaseModel)` | Typed input contract (the "bind") |
| `execute` | ✅ | `async def execute(inputs, workspace) -> dict[str, Path]` | The work |
| `plan` | ⬜ | `def plan(inputs) -> JobPlan` | Optional submit-time resource sizing |

A single framework dispatcher (`run_native_job`) handles import, validation, and
error classification; both `LocalBackend` and the SLURM launcher route through
it, so a job behaves identically regardless of backend. `plan()` is dispatched
separately (`run_native_job_plan`) at submit time.

**Every non-dunder file under `jobs/` must be a valid native job.** The boot
scan (`scan_native_jobs`) validates the whole tree at startup and refuses to
start the orchestrator if any module is malformed. Shared helpers therefore live
in a **sibling** module *outside* `jobs/` (e.g. `read_count.py`,
`job_resource_plan.py`), never inside it.

The module path must start with `qiita_compute_orchestrator.jobs.` — enforced at
sync, submit, boot scan, and dispatch.

### Minimal skeleton

```python
"""Native job: <one-line what it does>."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel


class Inputs(BaseModel):
    """Typed input contract for <job>."""

    some_input: Path              # a declared workflow input (a shared-FS path)
    some_scalar: int              # a scalar build param (see `params:` below)
    prep_sample_idx: int          # framework-injected scope scalar (see below)
    work_ticket_idx: int          # framework-injected, always present


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Do the work; return a name -> path map matching the YAML `outputs:`."""
    out = workspace / "result.parquet"
    # ... produce out ...
    return {"result": out}
```

## `Inputs` — the input contract

`Inputs` is a Pydantic `BaseModel`. The dispatcher validates the raw wire inputs
against it before calling `execute()`; a validation failure is classified
`BAD_INPUT` (permanent — the same inputs fail the same way on retry).

Three kinds of field arrive in `Inputs`:

1. **Declared inputs / optional inputs** — the YAML step's `inputs:` /
   `optional_inputs:` names, bound to absolute shared-filesystem paths. Declare
   them as `Path`. Optional ones are omitted from the map when absent, so give
   them a default (`x: Path | None = None`).
2. **Scalar build params** — the YAML step's `params:` (an
   `action_context_key -> Inputs field` map). These are NOT paths; they arrive
   as strings and Pydantic re-coerces to the declared type (`"35"` → `int`).
   Use `params:` for scalars precisely *because* they can't ride `inputs:`
   (which is path-typed and, for container steps, bind-mounted).
3. **Framework-injected scope scalars** — the work ticket's `scope_target`
   idx scalars, merged in by `flatten_native_inputs` per the ticket's scope
   kind, plus the always-present `work_ticket_idx`:

   | scope_target kind | injected fields |
   |---|---|
   | `reference` | `reference_idx` |
   | `study_prep` | `study_idx`, `prep_idx` |
   | `prep_sample` | `prep_sample_idx` |
   | `sequenced_pool` | `sequencing_run_idx`, `sequenced_pool_idx` |

   Declare exactly the ones your job needs. These names are **reserved** — a
   `inputs:`/`params:` field colliding with one is a `CONTRACT_VIOLATION`.

## `execute` — the work

`async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]`

- `workspace` is a per-attempt scratch directory the job writes into.
- Return a `name -> Path` map whose keys **exactly match** the YAML step's
  `outputs:` names (a mismatch is a workflow-authoring error surfaced as a
  `KeyError`).
- Result Parquet written for DuckLake registration must be mode `0o440` and
  carry the identifier columns in the canonical sort order (see
  [`architecture.md`](architecture.md) — the data-plane result-file contract).

### Error classification

Raise the right thing; the dispatcher maps it:

| Raised in `execute()` | Classified as | Retry? |
|---|---|---|
| `StepNoData` | `NO_DATA` (terminal, not a failure) | — |
| `FileNotFoundError`, `ValueError` | `BAD_INPUT` (permanent) | no |
| `NotImplementedError` | `UNKNOWN_PERMANENT` | no |
| anything else | propagates (logged with traceback) | — |

`StepNoData` is for a legitimate empty input (e.g. an empty FASTQ well) — a
terminal *no-data* outcome, distinct from a failure. Follow the repo ethos:
**fail fast, fail loud** — validate at boundaries and raise with context rather
than silently returning a default.

## `plan` — optional resource sizing

`def plan(inputs: Inputs) -> JobPlan` (sync — it runs at **submit time** in the
orchestrator process, never on a compute node; an `async` plan is rejected by
the boot scan). It lets a job size its SLURM allocation from its actual inputs
instead of a static YAML guess.

```python
from ..job_resource_plan import count_read_pairs, linear_walltime
from . import JobPlan, JobResourcePlan

def plan(inputs: Inputs) -> JobPlan:
    read_pairs = count_read_pairs(inputs.reads)          # Parquet footer, no scan
    return JobPlan(resources=JobResourcePlan(
        walltime=linear_walltime(read_pairs, base_seconds=300, seconds_per_million_pairs=30),
    ))
```

Rules and semantics:

- **Advisory, never correctness.** The control plane treats the hint as an
  optimization: any failure (unreadable input, unreachable orchestrator, a
  buggy `plan()`) degrades to the YAML baseline. Never make `execute()` depend
  on `plan()` having run.
- **Down-size only (today).** Each axis the hint sets (`cpu`, `mem_gb`,
  `walltime`) only ever *lowers* the step below its YAML baseline; a hint above
  the baseline is a no-op. Up-sizing remains the job of the OOM/TIMEOUT
  escalation floors. (The composition applies the hint *before* those raise-only
  floors, so a retry always restores at least the baseline.)
- **Size the axis that actually varies with input.** For a **streaming** job
  (per-row transform + a spill-to-disk sort) peak memory is roughly *flat* in
  row count — the axis that scales is **walltime**. Sizing memory linearly in
  reads would be wrong. There is deliberately no `gpu` axis (GPU need is
  algorithm-, not input-, determined).
- **Keep it cheap.** `plan()` runs inline at submit time — a Parquet
  footer/metadata read, not a data scan.

## Declaring resources in the workflow YAML

Every `step:` declares `baseline_resources`. Two populations (exactly one):

```yaml
# Flat — the common case.
baseline_resources:
  cpu: 4
  mem_gb: 12
  walltime: PT4H          # ISO-8601 duration
  # gpu: 0                # optional, defaults to 0
```

```yaml
# Lookup — pick a profile from an upstream step's output file contents
# (used by bcl-convert, keyed on instrument model).
baseline_resources:
  from_step_output: instrument_model
  profiles:
    "Illumina NovaSeq 6000": { cpu: 16, mem_gb: 480, walltime: PT6H }
    "Illumina iSeq 100":     { cpu: 16, mem_gb: 16,  walltime: PT3H }
```

Resolution order (all in the runner, before submit): flat/lookup baseline →
`plan()` down-size → raise-only escalation floor (OOM/TIMEOUT retries) → clamp
to the action ceiling. A resolved value over the ceiling is a `CONTRACT_VIOLATION`.

## Wiring the step into a workflow

```yaml
steps:
  - step: qc
    step_type: singleton
    module: qiita_compute_orchestrator.jobs.qc      # native — exactly one of module:/container:
    inputs: [reads, adapter_parquet]
    params: { instrument_model: instrument_model }  # action_context_key: Inputs field
    outputs: [qc_mask]
    baseline_resources: { cpu: 4, mem_gb: 12, walltime: PT4H }
```

New/changed `workflows/` files reach `qiita.action` via `qiita-admin actions
sync` at deploy — they are *synced, not migrated*.

## Testing

- **Boot scan** (`scan_native_jobs`) validates the whole `jobs/` tree — a
  malformed module fails `make test` via `tests/test_jobs_discovery.py`.
- **Dispatcher** branches are covered per-error-kind in
  `tests/test_run_native_job.py` (stubs injected into `sys.modules`).
- **Per-job** logic gets its own `tests/jobs/test_<job>.py`; the shared
  `write_reads` / `write_reads_q` fixtures (`tests/jobs/conftest.py`) own the
  reads.parquet schema so it lives in one place. Miint-dependent assertions go
  in a `_smoke` variant.
- **`plan()`** is a pure sync function — test it directly (construct `Inputs`,
  call `plan()`, assert the `JobResourcePlan`), and test the down-size
  composition in the runner's `tests/test_runner_baseline.py`.

All of the above run in the pure-unit tier (`make test`) — no infrastructure.
