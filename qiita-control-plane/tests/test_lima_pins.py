"""Guards tying the control plane's lima constants to the vendored artifacts.

`_LIMA_ADAPTER_SET_MD5` and `_LIMA_VERSION` are folded into the read-mask identity
hash (`resolved_lima`), so they ARE the mask's claim about what trimmed the reads.
Neither can be derived at runtime — the control plane cannot hash a file inside a
SIF, nor ask a container for its lima version — so both are constants. These tests
are the only thing preventing them from drifting from the bytes lima actually sees.

Drift is silent and severe: a re-vendored adapter set or a bumped lima would keep
hashing to the OLD identity, so the new filter's output would be stored under a
mask_idx whose params describe the old one.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pytest

from qiita_control_plane.runner import _mask

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW_DIR = _REPO_ROOT / "workflows" / "read-mask"
_ADAPTER_FASTA = _WORKFLOW_DIR / "twist_adapters_231010.fasta"
_LIMA_ENV = _WORKFLOW_DIR / "sif-build.d" / "lima.env"


def _env_value(text: str, key: str) -> str:
    m = re.search(rf'^{key}="([^"]*)"$', text, flags=re.MULTILINE)
    assert m, f"{key} not found in {_LIMA_ENV}"
    return m.group(1)


def test_adapter_fasta_is_vendored():
    assert _ADAPTER_FASTA.is_file(), (
        f"{_ADAPTER_FASTA} is missing; the lima image bakes it in and the mask "
        "identity pins its md5"
    )


def test_adapter_set_md5_constant_matches_the_vendored_file():
    """The mask hash claims these adapter bytes. Re-vendoring the set MUST update
    the constant, or every new mask silently reuses the old identity."""
    actual = hashlib.md5(_ADAPTER_FASTA.read_bytes()).hexdigest()  # noqa: S324
    assert actual == _mask._LIMA_ADAPTER_SET_MD5, (
        f"twist_adapters_231010.fasta md5 is {actual} but "
        f"_LIMA_ADAPTER_SET_MD5 is {_mask._LIMA_ADAPTER_SET_MD5}; update the constant "
        "(this re-mints every lima mask, by design)"
    )


def test_lima_version_constant_matches_the_sif_verify_match():
    """`VERIFY_MATCH` is asserted against `lima --version` at build time, so it is
    the image's real lima. The CP constant must name the same version."""
    verify_match = _env_value(_LIMA_ENV.read_text(), "VERIFY_MATCH")
    assert verify_match == f"lima {_mask._LIMA_VERSION}", (
        f"sif-build.d/lima.env VERIFY_MATCH is {verify_match!r} but _LIMA_VERSION is "
        f"{_mask._LIMA_VERSION!r}; a lima bump must move both (and re-mint)"
    )


def test_sif_filename_carries_the_pinned_lima_version():
    sif = _env_value(_LIMA_ENV.read_text(), "SIF_FILENAME")
    assert sif == f"lima-{_mask._LIMA_VERSION}.sif"


def test_adapter_fasta_is_in_hash_inputs():
    """Without this, re-vendoring the adapter set does NOT rebuild the SIF: the
    two-gate skip only fires on VERIFY_MATCH (lima's version, unchanged) or the
    build-inputs content hash."""
    hash_inputs = _env_value(_LIMA_ENV.read_text(), "HASH_INPUTS").split()
    assert "twist_adapters_231010.fasta" in hash_inputs


@pytest.mark.parametrize("preset,expected", [("ASYMMETRIC", True), ("SYMMETRIC", False)])
def test_neighbors_only_on_the_twist_preset(preset, expected):
    """`--neighbors` is what makes the adapter FASTA's record ORDER load-bearing,
    and per qp-pacbio it rides the Twist/ASYMMETRIC preset only."""
    args = _mask._LIMA_PRESET_ARGS[preset]
    assert ("--neighbors" in args) is expected


def test_adapter_fasta_record_order_is_the_vendored_order():
    """lima's `--neighbors` emits a read only when its best-scoring barcode pair
    are ADJACENT records in this file. Pin the shape so a well-meaning sort or
    dedup of the vendored file fails here rather than in production: 800 records,
    strictly interleaved F/R pairs, first record `Plate_A_1_A01_F`."""
    names = [ln[1:].strip() for ln in _ADAPTER_FASTA.read_text().splitlines() if ln.startswith(">")]
    assert len(names) == 800
    assert len(set(names)) == 800
    assert names[0] == "Plate_A_1_A01_F"
    assert all(n.endswith("_F") for n in names[0::2])
    assert all(n.endswith("_R") for n in names[1::2])
