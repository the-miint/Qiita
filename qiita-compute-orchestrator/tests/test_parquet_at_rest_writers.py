"""Files the data plane registers are written with the at-rest Parquet options; no other file is.

`register-files` moves a step's staged Parquet into DuckLake without rewriting it, so the
job that wrote the file is the only place its compression is decided — and the at-rest
level's write cost is only worth paying for a file that stays in the lake. Neither side of
that is visible from a single job, so this derives the jobs whose output is registered from
the workflow YAMLs and checks every orchestrator module's source against it:

- a module whose output is registered names an at-rest set, or calls one of the helpers that
  write registered files;
- such a module, and those helpers, name no transfer set (their only other option is
  PARQUET_OPTS_INTERMEDIATE, for files the same job deletes);
- no other module names an at-rest set.
"""

from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

import pytest
import yaml

import qiita_compute_orchestrator

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOWS_DIR = _REPO_ROOT / "workflows"
_PACKAGE_DIR = Path(qiita_compute_orchestrator.__file__).resolve().parent

_AT_REST_SETS = frozenset({"PARQUET_OPTS_AT_REST"})
_TRANSFER_SETS = frozenset({"PARQUET_OPTS_TRANSFER", "PARQUET_OPTS_CHUNKED_TRANSFER"})

# Shared writers of registered files, and the functions through which jobs reach them.
_AT_REST_HELPERS: dict[str, frozenset[str]] = {
    "qiita_compute_orchestrator.read_staging": frozenset({"write_sorted_reads"}),
    "qiita_compute_orchestrator.jobs._feature_load": frozenset(
        {"write_feature_sequences", "write_feature_sequence_chunks"}
    ),
}
_HELPER_FUNCTIONS = frozenset().union(*_AT_REST_HELPERS.values())

# Where the sets are defined (re-exported from qiita_common.parquet), not a writer.
_DEFINING_MODULE = "qiita_compute_orchestrator.miint"


def _registering_modules() -> dict[str, set[str]]:
    """Native module → the workflows in which a register-files step consumes its output."""
    found: dict[str, set[str]] = {}
    for yaml_path in sorted(_WORKFLOWS_DIR.glob("*/*.yaml")):
        steps = yaml.safe_load(yaml_path.read_text()).get("steps", [])
        for i, step in enumerate(steps):
            if step.get("action") != "register-files":
                continue
            for name in step.get("inputs", []):
                producers = [s for s in steps[:i] if name in (s.get("outputs") or [])]
                assert producers, f"{yaml_path}: nothing before register-files outputs {name!r}"
                for producer in producers:
                    module = producer.get("module")
                    assert module, (
                        f"{yaml_path}: {name!r} is registered but produced by a non-native "
                        f"step ({producer}); this test cannot see what wrote it"
                    )
                    found.setdefault(module, set()).add(yaml_path.parent.name)
    return found


def _module_path(module: str) -> Path:
    spec = importlib.util.find_spec(module)
    assert spec is not None and spec.origin, f"cannot locate {module}"
    return Path(spec.origin)


def _names(path: Path) -> set[str]:
    """Every identifier a module reads or imports."""
    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.ImportFrom):
            names.update(alias.asname or alias.name for alias in node.names)
    return names


def _all_modules() -> dict[str, Path]:
    return {
        "qiita_compute_orchestrator."
        + ".".join(p.relative_to(_PACKAGE_DIR).with_suffix("").parts): p
        for p in sorted(_PACKAGE_DIR.rglob("*.py"))
        if p.name != "__init__.py"
    }


_REGISTERING = _registering_modules()


def test_registering_modules_are_found():
    # A YAML shape change that hid every register-files step would make the rest vacuous.
    assert {
        "qiita_compute_orchestrator.jobs.fastq_to_parquet",
        "qiita_compute_orchestrator.jobs.reference_load",
        "qiita_compute_orchestrator.jobs.align_sharded",
        "qiita_compute_orchestrator.jobs.host_filter",
    } <= _REGISTERING.keys()


@pytest.mark.parametrize("module", sorted(_REGISTERING))
def test_registered_output_is_written_at_rest(module: str):
    names = _names(_module_path(module))
    assert names & (_AT_REST_SETS | _HELPER_FUNCTIONS), (
        f"{module} writes files that {sorted(_REGISTERING[module])} register into DuckLake, "
        f"but names no at-rest option set or helper ({sorted(_AT_REST_SETS)})"
    )


@pytest.mark.parametrize("module", sorted(_AT_REST_HELPERS))
def test_at_rest_helpers_write_at_rest(module: str):
    assert _names(_module_path(module)) & _AT_REST_SETS, f"{module} names no at-rest set"


@pytest.mark.parametrize("module", sorted(_REGISTERING.keys() | _AT_REST_HELPERS.keys()))
def test_registering_modules_name_no_transfer_set(module: str):
    used = _names(_module_path(module)) & _TRANSFER_SETS
    assert not used, (
        f"{module} writes registered files, so its non-intermediate writes are at rest; "
        f"it names {sorted(used)}"
    )


def test_no_other_module_names_an_at_rest_set():
    allowed = _REGISTERING.keys() | _AT_REST_HELPERS.keys() | {_DEFINING_MODULE}
    offenders = {
        module: sorted(_names(path) & _AT_REST_SETS)
        for module, path in _all_modules().items()
        if module not in allowed and _names(path) & _AT_REST_SETS
    }
    assert not offenders, (
        f"at-rest options outside the writers of registered files: {offenders}. A file no "
        f"register-files step consumes takes PARQUET_OPTS_TRANSFER or _INTERMEDIATE."
    )
