"""Jobs whose files the data plane registers name the at-rest Parquet options; no other
job does.

Why a registered file's writer is the one place its compression is decided is stated in
`qiita_common.parquet`. Which jobs those are is not visible from a single job, so this
derives the jobs whose output is registered from the workflow YAMLs and checks every
orchestrator module's source against it:

- a module whose output is registered names an at-rest set, or calls one of the helpers that
  write registered files;
- such a module, and those helpers, name no transfer set;
- no other module names an at-rest set.

It checks which option sets a module's source names, not which set each file is written
with. A registering module may also name PARQUET_OPTS_INTERMEDIATE, for the scratch files
it deletes, so a registered file written with that set would pass. Which names are at-rest
or transfer sets is read from the sets' values, so a set added under a new name is checked
too.
"""

from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

import pytest
import yaml
from qiita_common import parquet
from qiita_common.api_paths import LibraryPrimitive

import qiita_compute_orchestrator
from qiita_compute_orchestrator import miint

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOWS_DIR = _REPO_ROOT / "workflows"
_PACKAGE_DIR = Path(qiita_compute_orchestrator.__file__).resolve().parent


def _option_sets() -> tuple[frozenset[str], frozenset[str]]:
    """The at-rest and the transfer option-set names, from every set the two defining
    modules carry, classified by what the set says: a compression level marks at rest, zstd
    without one transfer, and snappy intermediate. A set that fits none fails here, so a new
    one cannot go unchecked."""
    at_rest: set[str] = set()
    transfer: set[str] = set()
    unclassified: list[str] = []
    for module in (parquet, miint):
        for name in dir(module):
            if not name.startswith("PARQUET_OPTS"):
                continue
            value = getattr(module, name)
            if "COMPRESSION_LEVEL" in value:
                at_rest.add(name)
            elif "COMPRESSION 'zstd'" in value:
                transfer.add(name)
            elif "COMPRESSION 'snappy'" not in value:
                unclassified.append(f"{module.__name__}.{name}")
    assert not unclassified, f"option sets of no known kind: {unclassified}"
    return frozenset(at_rest), frozenset(transfer)


_AT_REST_SETS, _TRANSFER_SETS = _option_sets()

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
            if step.get("action") != LibraryPrimitive.REGISTER_FILES:
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
    """Every identifier a module reads or imports, bare or as an attribute."""
    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
        if isinstance(node, ast.Name):
            names.add(node.id)
        elif isinstance(node, ast.Attribute):
            names.add(node.attr)
        elif isinstance(node, ast.ImportFrom):
            names.update(alias.asname or alias.name for alias in node.names)
    return names


def _all_modules() -> dict[str, Path]:
    modules: dict[str, Path] = {}
    for p in sorted(_PACKAGE_DIR.rglob("*.py")):
        parts = p.relative_to(_PACKAGE_DIR).with_suffix("").parts
        if parts[-1] == "__init__":
            parts = parts[:-1]
        modules[".".join(("qiita_compute_orchestrator", *parts))] = p
    return modules


_REGISTERING = _registering_modules()


def test_option_sets_are_classified():
    assert "PARQUET_OPTS_AT_REST" in _AT_REST_SETS
    assert {"PARQUET_OPTS_TRANSFER", "PARQUET_OPTS_CHUNKED_TRANSFER"} <= _TRANSFER_SETS


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
