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
from pathlib import Path

import pytest
import yaml
from qiita_common.actions import NATIVE_MODULE_PREFIX

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


def _baseline_cpu_every_version(workflow: str, step: str) -> list[tuple[Path, int]]:
    """`(yaml_path, baseline cpu)` for `step` in EVERY on-disk version of
    `workflows/<workflow>/`.

    Globbed rather than naming one filename: a pin that reads `1.0.0.yaml` stops
    covering the workflow the moment a second version lands beside it, and the
    version it keeps covering is the one nobody runs any more.
    """
    yaml_paths = sorted(_WORKFLOWS_DIR.glob(f"{workflow}/*.yaml"))
    assert yaml_paths, f"no workflow YAML under workflows/{workflow}/"
    found: list[tuple[Path, int]] = []
    for yaml_path in yaml_paths:
        data = yaml.safe_load(yaml_path.read_text())
        steps = [e for e in data["steps"] if e.get("step") == step]
        assert len(steps) == 1, (
            f"{yaml_path.relative_to(_REPO_ROOT)}: expected exactly one {step} step, "
            f"got {len(steps)}"
        )
        found.append((yaml_path, steps[0]["baseline_resources"]["cpu"]))
    return found


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
    for yaml_path, cpu in _baseline_cpu_every_version("align", "align_sharded"):
        assert cpu == mod._DUCKDB_THREADS, (
            f"{yaml_path.relative_to(_REPO_ROOT)} baseline cpu={cpu} but "
            f"align_sharded._DUCKDB_THREADS={mod._DUCKDB_THREADS}; these size the same"
            " thing (miint's concurrent-shard count) and must be changed together"
        )


def test_align_denovo_cpu_pins_duckdb_threads():
    """`align-denovo`'s baseline `cpu:` must equal `align_denovo._DUCKDB_THREADS`.

    Same pin as `align`'s above, different mechanism. There the DuckDB pool is miint's
    concurrent-SHARD count; here the job makes one non-sharded `align_minimap2` call,
    and that call draws its parallelism from the pool directly — measured on the staged
    build at 1/2/4/8 threads (17.72 / 8.91 / 4.76 / 2.40 s over 60k x 2 kb reads), with
    `MaxThreads()` tracking `SET threads`. So a `cpu:` above the literal allocates cores
    nothing uses, and one below it oversubscribes the aligner onto fewer.
    """
    mod = importlib.import_module("qiita_compute_orchestrator.jobs.align_denovo")
    for yaml_path, cpu in _baseline_cpu_every_version("align-denovo", "align_denovo"):
        assert cpu == mod._DUCKDB_THREADS, (
            f"{yaml_path.relative_to(_REPO_ROOT)} baseline cpu={cpu} but "
            f"align_denovo._DUCKDB_THREADS={mod._DUCKDB_THREADS}; the aligner's"
            " parallelism IS that pool, so these must be changed together"
        )


def test_assembly_coverage_cpu_pins_duckdb_threads():
    """`long-read-assembly`'s `assembly_coverage` step: baseline `cpu:` must equal
    `assembly_coverage._DUCKDB_THREADS`, for the reason stated on `align_denovo`'s pin.

    This step made the same non-sharded `align_minimap2` call at threads 8 against
    `cpu: 16`, reserving cores it could not use. The gap was closed by lowering the
    request, not by raising the pool: on this step the thread count is also a memory
    multiplier for the unspillable extension side, which `sacct` puts at 87% of the 64
    GB request at the median. Pinned here so the two cannot drift apart again — the
    drift is invisible at runtime, since nothing fails.

    This workflow is the one that carries more than one on-disk version, so it is
    the one where reading a single filename would have left the running version
    unpinned; `_baseline_cpu_every_version` covers each of them.
    """
    mod = importlib.import_module("qiita_compute_orchestrator.jobs.assembly_coverage")
    versions = _baseline_cpu_every_version("long-read-assembly", "assembly_coverage")
    for yaml_path, cpu in versions:
        assert cpu == mod._DUCKDB_THREADS, (
            f"{yaml_path.relative_to(_REPO_ROOT)} assembly_coverage baseline cpu={cpu}"
            f" but assembly_coverage._DUCKDB_THREADS={mod._DUCKDB_THREADS}; the"
            " aligner's parallelism IS that pool, so these must be changed together"
        )
