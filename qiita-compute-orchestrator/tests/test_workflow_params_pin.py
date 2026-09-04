"""Pin every workflow `params:` mapping against the real native-job `Inputs`.

A `params:` entry on a native step maps an action_context key to a field on the
job's Pydantic `Inputs` model (the runner merges it un-coerced; the model
re-coerces). The `Inputs` models do NOT set `extra="forbid"`, so a mistyped
field name would be silently ignored — the parameter would vanish and the
builder would quietly use its default. This test runs in the orchestrator tier
(the only one that can import the job modules) and walks every on-disk workflow
YAML, asserting each `params` VALUE names a real field on the target module's
`Inputs`. It is the fail-loud guard for that fail-quiet seam; the control-plane
loader test pins the YAML `params` VALUES, so the two ends are locked together.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import get_args

import pytest
import yaml
from qiita_common.actions import NATIVE_MODULE_PREFIX, ActionDefinition

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOWS_DIR = _REPO_ROOT / "workflows"


def _native_steps_with_params() -> list[tuple[str, str, dict[str, str]]]:
    """Yield (yaml_path, module, params) for every native step declaring a
    non-empty `params:` across all workflow YAMLs."""
    found: list[tuple[str, str, dict[str, str]]] = []
    for yaml_path in sorted(_WORKFLOWS_DIR.glob("*/*.yaml")):
        data = yaml.safe_load(yaml_path.read_text())
        for entry in data.get("steps", []):
            module = entry.get("module")
            params = entry.get("params")
            if module and params:
                found.append((str(yaml_path), module, params))
    return found


def _baseline_cpu_every_version(workflow: str, step: str) -> list[tuple[str, int]]:
    """`(where, baseline cpu)` for `step` in EVERY on-disk version of
    `workflows/<workflow>/`, where `where` is a repo-relative label for the
    assertion message.

    Globbed rather than naming one filename: a pin that reads `1.0.0.yaml` stops
    covering the workflow the moment a second version lands beside it, and the
    version it keeps covering is the one nobody runs any more. `fastq-to-parquet`
    already carries four versions; `long-read-assembly` is the one whose second
    version is the one that runs.

    A version that does not declare `step` at all contributes nothing rather than
    failing — dropping a step between versions is a legitimate shape change, and
    there is no cpu to pin on a step that is not there. Every version dropping it
    is the different case, and this function raises on it below, so no workflow
    can lose its pin entirely by having its step renamed out from under one. Two
    steps sharing a name — which would make the pin silently cover whichever came
    first — is rejected by `ActionDefinition` itself ("duplicate step name(s)"),
    so nothing is re-checked here.

    Both `baseline_resources` populations resolve through
    `ActionDefinition._labelled_baselines()` rather than being re-read out of the
    raw YAML dict. That method already decides what each population means — a flat
    one contributes a single pair under the step's name, a `profiles:` lookup one
    pair per profile labelled ``name[profile]`` — and it is the model the runner
    dispatches on. A second copy of that rule here would be free to drift from it,
    and reading `["cpu"]` directly (which is what this did) raises a bare
    `KeyError` on a lookup, naming neither the workflow nor the step.
    """
    yaml_paths = sorted(_WORKFLOWS_DIR.glob(f"{workflow}/*.yaml"))
    assert yaml_paths, f"no workflow YAML under workflows/{workflow}/"
    found: list[tuple[str, int]] = []
    for yaml_path in yaml_paths:
        action = ActionDefinition.model_validate(yaml.safe_load(yaml_path.read_text()))
        pairs = [
            (label, flat)
            for label, flat in action._labelled_baselines()
            if label == step or label.startswith(f"{step}[")
        ]
        where = str(yaml_path.relative_to(_REPO_ROOT))
        found.extend((f"{where} {label}", flat.cpu) for label, flat in pairs)
    assert found, (
        f"no version under workflows/{workflow}/ declares a {step} step, so this pin covers nothing"
    )
    return found


def test_baseline_cpu_helper_reads_both_resource_populations():
    """`_baseline_cpu_every_version` covers a `profiles:` lookup, not just a flat
    `cpu:`.

    The pins below all happen to sit on flat steps today, so the lookup branch has
    no other caller and would rot unnoticed. `long-read-assembly`'s `assemble`
    carries one of each across its versions — 1.0.0 flat, 1.0.1 a per-assembler
    lookup — so both branches are read out of real workflow files rather than a
    fixture. Reading `["cpu"]` off the lookup would raise a bare `KeyError` naming
    neither workflow nor step.

    Asserted on the SHAPE of the result, not on what the profile keys say: the keys
    are the upstream output's contents, and pinning their spelling here would make
    this fail for an unrelated reason the day that format changes.
    """
    entries = _baseline_cpu_every_version("long-read-assembly", "assemble")
    flat = [(where, cpu) for where, cpu in entries if "[" not in where]
    profiles = [(where, cpu) for where, cpu in entries if "[" in where]

    assert flat, f"expected 1.0.0's flat assemble to contribute one entry, got {entries}"
    assert len(profiles) >= 2, (
        f"expected 1.0.1's assemble lookup to contribute one entry per profile, got {entries}"
    )
    # A label locates the file, and the per-profile labels are distinct, so a
    # failing pin names which version and which profile it fired on.
    assert all(where.startswith("workflows/long-read-assembly/") for where, _ in entries)
    assert len({where for where, _ in entries}) == len(entries)
    assert all(isinstance(cpu, int) for _, cpu in entries)


def test_workflows_dir_present():
    """Guard against a wrong _REPO_ROOT (the glob would silently match nothing
    and make the param pin below vacuously pass)."""
    assert _WORKFLOWS_DIR.is_dir(), f"expected workflows/ at {_WORKFLOWS_DIR}"


def test_at_least_one_params_workflow_exists():
    """host-reference-add / local-host-reference-add introduced `params:`; if
    this list goes empty the pin below stops protecting anything."""
    assert _native_steps_with_params(), "expected at least one native step with params:"


@pytest.mark.parametrize(
    "yaml_path,module,params",
    _native_steps_with_params(),
    ids=lambda v: v if isinstance(v, str) else "",
)
def test_params_values_are_real_inputs_fields(yaml_path: str, module: str, params: dict[str, str]):
    """Every `params` value (the target Inputs field) must exist on the native
    job's `Inputs` model — otherwise the merged scalar is silently dropped."""
    assert module.startswith(NATIVE_MODULE_PREFIX), module
    mod = importlib.import_module(module)
    fields = set(mod.Inputs.model_fields)
    for ctx_key, field_name in params.items():
        assert field_name in fields, (
            f"{yaml_path}: params[{ctx_key!r}] -> {field_name!r} is not a field on"
            f" {module}.Inputs (have: {sorted(fields)})"
        )


def test_align_cpu_pins_duckdb_threads():
    """`align`'s `baseline_resources.cpu` must equal `align_sharded._DUCKDB_THREADS`.

    For the sharded align the cpu allocation is spent by the ALIGNER's cross-shard
    concurrency, not by DuckDB's own operators: miint derives `max_active_shards` from
    DuckDB's thread pool and ignores its own `threads` argument in sharded mode. But
    nothing derives that thread count from the cgroup — it is a module literal — so
    the two numbers are a hand-maintained pair, and drifting them silently either
    oversubscribes N concurrent shards onto fewer cores or leaves cores idle. Neither
    fails; the job just runs at the wrong size, which is exactly the class of defect
    the sizing work was about.

    Scoped to this step rather than asserted repo-wide: most native jobs pick threads
    for DuckDB's per-thread operator memory (sort / HASH_AGG state) and legitimately
    differ from their `cpu:` — `hash_sequences` (cpu 4 / threads 8) among them.
    Generalizing this pin would fail those for no reason.

    `align_denovo` and `assembly_coverage` carry the same pin below for a different
    mechanism — both make one non-sharded `align_minimap2` call, whose parallelism is
    the DuckDB pool directly rather than a shard count. Three pins side by side rather
    than one repo-wide rule, because the reason differs per job and the jobs that
    legitimately differ must keep differing.
    """
    mod = importlib.import_module("qiita_compute_orchestrator.jobs.align_sharded")
    for where, cpu in _baseline_cpu_every_version("align", "align_sharded"):
        assert cpu == mod._DUCKDB_THREADS, (
            f"{where} baseline cpu={cpu} but "
            f"align_sharded._DUCKDB_THREADS={mod._DUCKDB_THREADS}; these size the same"
            " thing (miint's concurrent-shard count) and must be changed together"
        )


def test_align_denovo_cpu_pins_duckdb_threads():
    """`align-denovo`'s baseline `cpu:` must equal `align_denovo._DUCKDB_THREADS`.

    Same pin as `align`'s above, different mechanism. There the DuckDB pool is miint's
    concurrent-SHARD count; here the job makes one non-sharded `align_minimap2` call,
    and that call draws its parallelism from the pool directly. So a `cpu:` above the
    literal allocates cores nothing uses, and one below it oversubscribes the aligner
    onto fewer. The thread-scaling measurement behind that is stated once, on the
    step's `baseline_resources` in `workflows/align-denovo/1.0.0.yaml`.
    """
    mod = importlib.import_module("qiita_compute_orchestrator.jobs.align_denovo")
    for where, cpu in _baseline_cpu_every_version("align-denovo", "align_denovo"):
        assert cpu == mod._DUCKDB_THREADS, (
            f"{where} baseline cpu={cpu} but "
            f"align_denovo._DUCKDB_THREADS={mod._DUCKDB_THREADS}; the aligner's"
            " parallelism IS that pool, so these must be changed together"
        )


def test_assembly_coverage_cpu_pins_duckdb_threads():
    """`long-read-assembly`'s `assembly_coverage` step: baseline `cpu:` must equal
    `assembly_coverage._DUCKDB_THREADS`, for the reason stated on `align_denovo`'s pin.

    This step made the same non-sharded `align_minimap2` call at threads 8 against
    `cpu: 16`, reserving cores it could not use. The gap was closed by lowering the
    request, not by raising the pool: on this step the thread count is also a memory
    multiplier for the unspillable extension side, which `assembly_coverage`'s own
    `_DUCKDB_THREADS` comment sizes against the measured MaxRSS. Pinned here so the two
    cannot drift apart again — the drift is invisible at runtime, since nothing fails.

    Read `1.0.0.yaml` by name until the 1.0.1 residue pass landed beside it, which
    left the version that actually runs unpinned; `_baseline_cpu_every_version`
    covers each of them.
    """
    mod = importlib.import_module("qiita_compute_orchestrator.jobs.assembly_coverage")
    versions = _baseline_cpu_every_version("long-read-assembly", "assembly_coverage")
    for where, cpu in versions:
        assert cpu == mod._DUCKDB_THREADS, (
            f"{where} assembly_coverage baseline cpu={cpu}"
            f" but assembly_coverage._DUCKDB_THREADS={mod._DUCKDB_THREADS}; the"
            " aligner's parallelism IS that pool, so these must be changed together"
        )


def test_build_shard_index_cpu_is_below_duckdb_threads():
    """`build-shard-index`'s `build_minimap2_index` step: baseline `cpu:` is 1, below
    `build_minimap2_index._DUCKDB_THREADS`, where the three pins above assert equality.

    Those pins exist because a job drawing its parallelism from DuckDB's pool wastes
    any core the pool cannot use. This step is the opposite case: it runs blocked on
    the data-plane chunk stream rather than computing, so the serial work one core must
    absorb is small. The cores were the waste, so `cpu` dropped to the measured 1 while
    the pool stayed at 4. The step's `baseline_resources` comment in the workflow YAML
    states that measurement.

    Pinned for the same reason the others are: nothing fails at runtime when these
    drift, so a later edit raising `cpu` to match the pool would silently undo a
    measured change. It asserts the measured value, not merely the inequality, so an
    intermediate `cpu: 2` does not pass unnoticed; it also asserts the value stays
    under the pool, so lowering `_DUCKDB_THREADS` to 1 fires it too. If it fires, the
    question is whether the measurement still holds.
    """
    mod = importlib.import_module("qiita_compute_orchestrator.jobs.build_minimap2_index")
    for where, cpu in _baseline_cpu_every_version("build-shard-index", "build_minimap2_index"):
        assert cpu == 1 and cpu < mod._DUCKDB_THREADS, (
            f"{where}: build_minimap2_index cpu={cpu}, expected the measured 1, below "
            f"_DUCKDB_THREADS={mod._DUCKDB_THREADS} — the step is blocked on its "
            f"data-plane stream, so cores matched to the pool go unused. The step's "
            f"`baseline_resources` comment in that YAML carries the measurement; if "
            f"this fires, the question is whether it still holds."
        )


def test_build_shard_index_bowtie2_cpu_pins_duckdb_threads():
    """`build-shard-index`'s `build_bowtie2_index` step: baseline `cpu:` must equal
    `build_bowtie2_index._DUCKDB_THREADS`.

    The opposite case to its minimap2 sibling directly above, on the same 987 shard
    builds: this step spends its wall time computing rather than blocked on the chunk
    stream, so the cores it asks for are cores it uses. The step's `baseline_resources`
    comment in the workflow YAML carries that measurement, including what it does and
    does not settle about the number itself.

    Equality, like the three aligner pins: the pin locks the pair so an edit to either
    side has to touch the other. It is not a claim that 4 was measured against 3 — the
    YAML says plainly that it was not. What drifting them silently costs is the same
    either way, since nothing fails at runtime when they disagree.
    """
    mod = importlib.import_module("qiita_compute_orchestrator.jobs.build_bowtie2_index")
    for where, cpu in _baseline_cpu_every_version("build-shard-index", "build_bowtie2_index"):
        assert cpu == mod._DUCKDB_THREADS, (
            f"{where} build_bowtie2_index baseline cpu={cpu} but "
            f"build_bowtie2_index._DUCKDB_THREADS={mod._DUCKDB_THREADS}; this step "
            "computes rather than waiting on its stream, so the pool and the cores it "
            "runs on must be changed together"
        )


def test_build_rype_index_cpu_stays_within_duckdb_threads():
    """`build_rype_index`'s baseline `cpu:` must not exceed its `_DUCKDB_THREADS`, in
    every workflow that declares the step.

    A BOUND, not the equality the pins above assert, because the two workflows
    declaring this step disagree: `host-reference-add` runs it at cpu 4 and
    `local-host-reference-add` at cpu 8, against a pool of 8. Whether rype's own build
    parallelism derives from the DuckDB pool has not been probed, so there is no
    measurement here to pin an equality to, and forcing the two to agree would change a
    workflow on a guess.

    What the bound does catch is the direction that is wrong under either answer: a
    `cpu:` ABOVE the pool reserves cores no part of the job can reach, since the pool is
    the only thread count this module sets.
    """
    mod = importlib.import_module("qiita_compute_orchestrator.jobs.build_rype_index")
    sites = [
        *_baseline_cpu_every_version("host-reference-add", "build_rype_index"),
        *_baseline_cpu_every_version("local-host-reference-add", "build_rype_index"),
    ]
    for where, cpu in sites:
        assert cpu <= mod._DUCKDB_THREADS, (
            f"{where} build_rype_index baseline cpu={cpu} exceeds "
            f"build_rype_index._DUCKDB_THREADS={mod._DUCKDB_THREADS}; the pool is the "
            "only thread count this module sets, so cores above it are unreachable"
        )


def test_assemble_profiles_cover_every_assembler():
    """`long-read-assembly`'s `assemble` step sizes memory per assembler through a
    `profiles:` lookup. Each profile key is the serialized `run_config.json` for one
    assembler, so the assemblers those keys NAME must be exactly the members of that
    job's `Inputs.assembler` Literal, in every workflow version using the lookup.

    The lookup has no default: a key absent from `profiles` fails at dispatch with
    `CONTRACT_VIOLATION`, rather than sizing an unknown assembler from another one's
    measurement. That turns "add a third assembler to the Literal" into a step that
    breaks on a ticket rather than here, so this pins it both ways: a member without a
    profile, or a profile without a member, fails in CI instead.
    """
    mod = importlib.import_module("qiita_compute_orchestrator.jobs.assembly_run_config")
    literal = set(get_args(mod.Inputs.model_fields["assembler"].annotation))
    assert literal, "assembly_run_config.Inputs.assembler is no longer a Literal"

    checked = 0
    for yaml_path in sorted(_WORKFLOWS_DIR.glob("long-read-assembly/*.yaml")):
        data = yaml.safe_load(yaml_path.read_text())
        steps = [e for e in data["steps"] if e.get("step") == "assemble"]
        assert len(steps) <= 1, f"{yaml_path}: more than one `assemble` step"
        if not steps:
            continue
        profiles = steps[0]["baseline_resources"].get("profiles")
        if profiles is None:
            continue  # a version still on a flat baseline
        # The keys are `run_config.json`'s stripped bytes, not bare names — see that
        # step's comment for why the lookup keys on the pre-existing output.
        named = {json.loads(k)["assembler"] for k in profiles}
        assert named == literal, (
            f"{yaml_path.relative_to(_REPO_ROOT)}: assemble profiles cover "
            f"{sorted(named)} != assembly_run_config assemblers {sorted(literal)}"
        )
        checked += 1
    assert checked, "no long-read-assembly version uses the assemble profiles lookup"
