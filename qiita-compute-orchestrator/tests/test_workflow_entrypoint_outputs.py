"""Every container entrypoint's manifest call names exactly its step's `outputs:`.

The two halves of one contract are written in two files that nothing joins at
runtime. The YAML declares `outputs:`; the entrypoint passes `<name>=<relpath>`
pairs to the manifest writer, and those are what reach `manifest.json`. The
control-plane runner then binds them with

    {name: raw_outputs[name] for name in entry.outputs}

(`runner/_dispatch.py`, and again in `runner/_reconstruct.py` when a restart
fast-forwards an already-completed step). A name in the YAML that the entrypoint
does not emit is therefore a `KeyError` -- after the step has run, and on the
resume path after it has run *successfully*. The reverse, a pair the YAML does
not declare, is silently ignored and means the two have drifted.

Two call shapes, because one image predates the shared helper: `qiita_finish a=b`
(`_shared/_lib.sh`, which then chmods) and a direct
`python3.11 /opt/qiita/manifest_writer.py "${QIITA_OUTPUT_PATH}" a=b`
(bcl-convert, whose entrypoint asks for exactly this check in its own comment).

Set equality per call site, not per script: an entrypoint with an early
`qiita_finish; exit 0` for an empty result must name the full set on that path
too, or the empty case fails where the populated one passes.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOWS_DIR = _REPO_ROOT / "workflows"

# Every workflow shipping a container entrypoint today. Asserted as an exact set
# in `test_entrypoint_inventory_is_complete` so a new container step cannot be
# added without the parametrized checks below seeing it -- the failure mode this
# file exists for is invisible until a ticket runs.
_EXPECTED_ENTRYPOINTS = {
    ("bcl-convert", "bcl_convert"),
    ("long-read-assembly", "assemble"),
    ("long-read-assembly", "binning"),
    ("long-read-assembly", "bin_refine"),
    ("long-read-assembly", "checkm"),
    ("read-mask", "lima"),
}

# A manifest `<name>=<relpath>` pair. The output-root argument the direct
# manifest_writer.py form passes first (`"${QIITA_OUTPUT_PATH}"`) is not of this
# shape, so it drops out without needing to be positionally skipped.
_PAIR = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=([^\s]+)$")


def _container_steps() -> list[tuple[str, str, Path, set[str]]]:
    """(workflow dir name, step name, entrypoint script, declared outputs).

    Only `container:` steps with an `entrypoint:`. The entrypoint is an
    in-container path (`/opt/qiita/assemble.sh`); the def `%files`-copies it from
    the workflow directory under its own basename, so that is what resolves it
    back to a repo file.
    """
    found: list[tuple[str, str, Path, set[str]]] = []
    for yaml_path in sorted(_WORKFLOWS_DIR.rglob("*.yaml")):
        data = yaml.safe_load(yaml_path.read_text())
        # The CP loader's filter, mirrored: a YAML without `action_id` is not an
        # action definition and never syncs (workflows/amplicon/workflow.yaml is
        # pre-schema scaffolding, and carries `container:` entries with no
        # `step:` key at all).
        if not isinstance(data, dict) or "action_id" not in data:
            continue
        for step in data.get("steps", []):
            if not isinstance(step, dict) or "container" not in step:
                continue
            entrypoint = step.get("entrypoint")
            if not entrypoint:
                continue
            found.append(
                (
                    yaml_path.parent.name,
                    step["step"],
                    yaml_path.parent / Path(entrypoint).name,
                    set(step.get("outputs") or []),
                )
            )
    return found


def _manifest_calls(script: Path) -> list[dict[str, str]]:
    """The `<name>=<relpath>` map of every manifest-writing call in `script`.

    Whole-line comments are dropped and a trailing comment is cut at the first
    ` #`. Neither is a general shell parser: the only thing read here is one
    command line of `name=relpath` tokens, and no entrypoint quotes a `#` inside
    one. A qualifying line that stopped parsing cleanly would surface as a
    missing pair in the set comparison rather than as a silent skip.
    """
    calls: list[dict[str, str]] = []
    for raw in script.read_text().splitlines():
        line = raw.strip()
        if line.startswith("#"):
            continue
        line = line.split(" #", 1)[0].strip()
        tokens = line.split()
        if not tokens:
            continue
        is_helper = tokens[0] == "qiita_finish"
        is_direct = any(t.endswith("manifest_writer.py") for t in tokens)
        if not (is_helper or is_direct):
            continue
        pairs: dict[str, str] = {}
        for token in tokens[1:]:
            match = _PAIR.match(token)
            if match:
                pairs[match.group(1)] = match.group(2)
        calls.append(pairs)
    return calls


def test_entrypoint_inventory_is_complete() -> None:
    """Anti-vacuity guard: the parametrizations below are generated by walking
    `workflows/`, so a `_WORKFLOWS_DIR` that stopped resolving, or a YAML shape
    the walk no longer recognises, would silently reduce them to nothing."""
    found = {(workflow, step) for workflow, step, _, _ in _container_steps()}
    assert found == _EXPECTED_ENTRYPOINTS, (
        f"container steps with an entrypoint are {sorted(found)}, expected "
        f"{sorted(_EXPECTED_ENTRYPOINTS)}. A new one must be added here so the "
        "outputs checks below cover it."
    )


@pytest.mark.parametrize(
    ("workflow", "step", "script", "declared"),
    _container_steps(),
    ids=[f"{w}:{s}" for w, s, _, _ in _container_steps()],
)
def test_manifest_call_names_the_declared_outputs(
    workflow: str, step: str, script: Path, declared: set[str]
) -> None:
    assert script.is_file(), (
        f"{workflow}:{step} declares entrypoint {script.name}, which is not in "
        f"{script.parent} -- the check below would be vacuous"
    )
    calls = _manifest_calls(script)
    assert calls, f"{script.name} writes no manifest, which every step must do"
    for pairs in calls:
        assert set(pairs) == declared, (
            f"{script.name} writes a manifest naming {sorted(pairs)} while "
            f"{workflow}/{step} declares outputs {sorted(declared)}. The runner binds "
            "`raw_outputs[name] for name in entry.outputs`, so a declared name this "
            "call does not emit is a KeyError once the step has run."
        )


@pytest.mark.parametrize(
    ("workflow", "step", "script", "declared"),
    _container_steps(),
    ids=[f"{w}:{s}" for w, s, _, _ in _container_steps()],
)
def test_manifest_relpaths_do_not_escape_the_output_root(
    workflow: str, step: str, script: Path, declared: set[str]
) -> None:
    """Gate 4 resolves each relpath under `$QIITA_OUTPUT_PATH` and rejects
    traversal (`slurm/verify.py`). A literal `..` or absolute path here is that
    rejection, spelled as a permanent CONTRACT_VIOLATION after the step ran."""
    for pairs in _manifest_calls(script):
        for name, relpath in pairs.items():
            assert not relpath.startswith("/"), (
                f"{script.name}: output {name} is the absolute path {relpath!r}"
            )
            assert ".." not in Path(relpath).parts, (
                f"{script.name}: output {name} traverses out of the output root ({relpath!r})"
            )
