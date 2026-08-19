"""Guard the generic SIF build contract (scripts/build-sif.sh).

A container workflow opts into the single generic builder by adding a
`workflows/<workflow>/sif-build.env` declarative spec; build-sif.sh stages
into a temp root it owns and only ever READS the checkout, so a locked-down
service account can build without write access to the qiita-owned tree.

A workflow may ship its image spec in either of two layouts, both guarded here:

* SINGLE (legacy): `workflows/<wf>/sif-build.env` builds
  `workflows/<wf>/Apptainer.def` into one SIF.
* MULTI (per-tool): `workflows/<wf>/sif-build.d/<image>.env` each builds its
  own def (the spec's `DEF_FILE`, relative to the workflow dir) into its own
  SIF, so one workflow can ship N single-tool images.

These pure-unit tests (no infrastructure; run under `make test`) keep that
contract from rotting:

* no hand-rolled per-workflow `scripts/build-*-sif.sh` can reappear and
  reintroduce the checkout-write bug — the generic `build-sif.sh` is the
  only allowed builder;
* every spec (either layout) carries the keys build-sif.sh requires, the def
  it names exists, and any HASH_INPUTS it declares point at real files; and
* SIF_FILENAME matches the workflow YAML's `container:` value, so the built
  artifact name can't drift from what the orchestrator resolves at run time.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOWS = _REPO_ROOT / "workflows"
_SCRIPTS = _REPO_ROOT / "scripts"

_REQUIRED_KEYS = ("SIF_FILENAME", "VERIFY_CMD", "VERIFY_MATCH")


def _parse_env(path: Path) -> dict[str, str]:
    """Parse a `source`-able KEY="value" spec the way build-sif.sh sees it.

    Deliberately minimal: KEY=value lines, optional surrounding quotes,
    comments and blanks ignored. Mirrors the subset build-sif.sh relies on
    so a spec that parses here is one the script can source.
    """
    out: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        out[key] = value
    return out


def _files_directives(def_text: str) -> list[str]:
    """Return the non-comment directive lines inside the Apptainer.def
    `%files` section. Used so SOURCES coverage is checked against what the
    build actually copies in, not against a filename that merely appears in
    a comment elsewhere in the def."""
    out: list[str] = []
    in_files = False
    for raw in def_text.splitlines():
        stripped = raw.strip()
        if stripped.startswith("%"):
            in_files = stripped.split()[0] == "%files"
            continue
        if in_files and stripped and not stripped.startswith("#"):
            out.append(stripped)
    return out


def _sif_build_specs() -> list[Path]:
    """Every image spec, both layouts: the legacy single form at the workflow
    root and the per-tool multi form under sif-build.d/."""
    return sorted([*_WORKFLOWS.glob("*/sif-build.env"), *_WORKFLOWS.glob("*/sif-build.d/*.env")])


def _workflow_dir(spec_path: Path) -> Path:
    """The workflow directory a spec belongs to. A legacy spec sits directly in
    it; a multi spec sits one level down in sif-build.d/."""
    if spec_path.name == "sif-build.env":
        return spec_path.parent
    return spec_path.parent.parent


def _def_path(spec_path: Path, spec: dict[str, str]) -> Path:
    """The Apptainer def this spec builds — DEF_FILE (workflow-relative) if the
    spec names one, else the legacy Apptainer.def."""
    return _workflow_dir(spec_path) / spec.get("DEF_FILE", "Apptainer.def")


def _spec_id(spec_path: Path) -> str:
    """Distinct pytest id per spec — `<wf>` for a legacy spec, `<wf>:<image>`
    for a multi spec (so the four per-tool images don't collide on `sif-build.d`)."""
    if spec_path.name == "sif-build.env":
        return spec_path.parent.name
    return f"{spec_path.parent.parent.name}:{spec_path.stem}"


def test_generic_build_script_is_the_only_sif_builder() -> None:
    """No per-workflow `build-*-sif.sh` may exist — they are exactly the
    scripts that historically staged into the checkout. The single generic
    `build-sif.sh` (which does not match the `build-*-sif.sh` glob) is the
    only sanctioned builder; a new workflow ships a sif-build.env, not a
    script."""
    rogue = sorted(p.name for p in _SCRIPTS.glob("build-*-sif.sh"))
    assert rogue == [], (
        "per-workflow SIF build scripts are forbidden — fold them into the "
        f"generic scripts/build-sif.sh + a workflows/<wf>/sif-build.env: {rogue}"
    )
    assert (_SCRIPTS / "build-sif.sh").is_file(), "scripts/build-sif.sh is missing"


def test_at_least_one_workflow_uses_the_generic_flow() -> None:
    """Canary: bcl-convert is the reference consumer. If this ever drops to
    zero the parametrized tests below would vacuously pass, hiding rot."""
    assert _sif_build_specs(), "expected at least one workflows/*/sif-build.env"


@pytest.mark.parametrize("spec_path", _sif_build_specs(), ids=_spec_id)
def test_sif_build_spec_is_complete(spec_path: Path) -> None:
    spec = _parse_env(spec_path)

    missing = [k for k in _REQUIRED_KEYS if not spec.get(k)]
    assert not missing, f"{spec_path} is missing required key(s): {missing}"

    # build-sif.sh `source`s the spec, but this test only models KEY=value.
    # Forbidding `$`/backtick keeps the two views identical: a value that
    # parses here as a literal can't quietly expand (command/parameter
    # substitution) when sourced. Specs are pure data, never shell.
    for key, value in spec.items():
        assert "$" not in value and "`" not in value, (
            f"{spec_path}: value for {key} contains shell substitution "
            f"(`$`/backtick) — specs must be literal KEY=value data"
        )

    def_path = _def_path(spec_path, spec)
    assert def_path.is_file(), (
        f"{_spec_id(spec_path)} declares a build spec but its def "
        f"{def_path.relative_to(_WORKFLOWS)} does not exist"
    )

    # A multi-image spec scopes its idempotency hash to HASH_INPUTS — those must
    # be real workflow-relative files, or a typo silently drops a build input
    # from the hash (an edit to it would then never trigger a rebuild).
    workflow_dir = _workflow_dir(spec_path)
    for rel in spec.get("HASH_INPUTS", "").split():
        assert (workflow_dir / rel).is_file(), (
            f"{_spec_id(spec_path)}: HASH_INPUTS entry '{rel}' does not exist "
            f"under {workflow_dir.name}/"
        )


@pytest.mark.parametrize("spec_path", _sif_build_specs(), ids=_spec_id)
def test_sources_are_referenced_in_def(spec_path: Path) -> None:
    """Each staged SOURCES artifact must actually be consumed by the def's
    %files (by bare filename); a source listed but never copied in is dead
    config that silently bloats the build."""
    spec = _parse_env(spec_path)
    sources = spec.get("SOURCES", "").split()
    if not sources:
        return
    def_text = _def_path(spec_path, spec).read_text()
    files_block = "\n".join(_files_directives(def_text))
    for src in sources:
        assert src in files_block, (
            f"{_spec_id(spec_path)}: spec lists SOURCES entry "
            f"'{src}' but no def %files directive references it"
        )


@pytest.mark.parametrize("spec_path", _sif_build_specs(), ids=_spec_id)
def test_sif_filename_matches_workflow_container(spec_path: Path) -> None:
    """SIF_FILENAME must equal the `container:` the workflow YAML declares,
    so the built artifact name matches what the orchestrator resolves at run
    time. Regex-scan the YAML(s) rather than depend on a parser."""
    spec = _parse_env(spec_path)
    sif = spec["SIF_FILENAME"]
    workflow_dir = _workflow_dir(spec_path)

    # `_`-prefixed dirs are sentinel/helper workflows (e.g. _sif-build-smoke,
    # alongside _shared); they intentionally ship no workflow YAML — the
    # control-plane loader's rglob("*.yaml") must never pick them up as
    # actions — so there is no `container:` to match against. The other
    # checks (spec completeness, the no-rogue-scripts guard) still apply.
    if workflow_dir.name.startswith("_"):
        pytest.skip(f"{workflow_dir.name} is a sentinel workflow (no action YAML)")

    containers: list[str] = []
    for yaml_path in workflow_dir.glob("*.yaml"):
        for m in re.finditer(r"^\s*container:\s*(\S+)\s*$", yaml_path.read_text(), re.M):
            containers.append(m.group(1).strip("\"'"))

    assert containers, (
        f"{workflow_dir.name}: no `container:` found in any workflow YAML to "
        f"check SIF_FILENAME against"
    )
    assert sif in containers, (
        f"{workflow_dir.name}: sif-build.env SIF_FILENAME='{sif}' does not match "
        f"any workflow `container:' value {containers} — built SIF name would "
        f"drift from what the orchestrator resolves"
    )
