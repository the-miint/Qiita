"""Pin that `long-read-assembly` 1.0.1 is 1.0.0's computation under a new identity.

Why this exists
---------------
1.0.1 exists only to be a DIFFERENT `processing_idx`. A run's identity is
`{workflow, version, mask_idx, assembler}` and does not cover the container
images, so samples assembled before the `bin_refine` consensus fix hold a
two-binner MAG set under a 1.0.0 identity that a corrected re-run would resolve
straight back to. The version is the discriminator that makes the re-run a
distinct run.

That only holds while the two files describe the same computation. If a step,
resource or image drifts between them, a 1.0.1 re-run stops being comparable with
the 1.0.0 result it supersedes — and nothing at runtime would say so, because both
versions load and both run. So the parity is asserted here rather than trusted to
the header that claims it.

Everything below the header is compared: steps, resources, containers, modules,
context_schema, audience. The header itself (version, description) is exactly what
differs, so it is excluded by key rather than by line.
"""

from __future__ import annotations

from pathlib import Path

import yaml

_WORKFLOW_DIR = Path(__file__).resolve().parents[2] / "workflows" / "long-read-assembly"
_V100 = _WORKFLOW_DIR / "1.0.0.yaml"
_V101 = _WORKFLOW_DIR / "1.0.1.yaml"

# The two keys 1.0.1 is allowed to differ on. `version` IS the point; `description`
# carries why it exists. Anything else differing means the computations diverged.
_HEADER_KEYS = ("version", "description")


def test_both_versions_are_on_disk() -> None:
    """Anti-vacuity: the comparison below is silent if either file is missing."""
    assert _V100.is_file(), f"{_V100} is gone"
    assert _V101.is_file(), f"{_V101} is gone"


def test_1_0_1_runs_the_same_computation_as_1_0_0() -> None:
    """Identical apart from the two header keys.

    A drift here is invisible at runtime: both versions load, both submit, both
    run. It surfaces only as a comparison between a superseded run and its
    replacement that is no longer apples-to-apples.
    """
    v100 = yaml.safe_load(_V100.read_text())
    v101 = yaml.safe_load(_V101.read_text())

    assert v101["version"] == "1.0.1", v101["version"]
    assert v100["version"] == "1.0.0", v100["version"]

    body_100 = {k: v for k, v in v100.items() if k not in _HEADER_KEYS}
    body_101 = {k: v for k, v in v101.items() if k not in _HEADER_KEYS}

    assert body_101 == body_100, (
        "long-read-assembly 1.0.1 has diverged from 1.0.0. It exists only to give "
        "the same computation a distinct processing identity, so a real change "
        "belongs in a version that says so — not here, where it would silently "
        "make a re-run incomparable with the run it supersedes."
    )


def test_the_two_versions_share_their_container_images() -> None:
    """The SIFs stay `-1.0.0.sif` — they name the IMAGE, not the workflow version.

    Stated separately from the body comparison because it is the one place the
    version numbers deliberately disagree, and a reader hitting `-1.0.0.sif` in a
    1.0.1 workflow would otherwise reasonably take it for a copy-paste slip.
    """
    v101 = yaml.safe_load(_V101.read_text())
    containers = [s["container"] for s in v101["steps"] if "container" in s]

    assert containers, "1.0.1 declares no container steps"
    assert all(c.endswith("-1.0.0.sif") for c in containers), containers
