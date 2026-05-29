"""Unit tests for the Illumina BCL run-folder parser.

Shared between the user CLI's `qiita submit-bcl-convert` and the
orchestrator's `bcl_convert_prep` step, so coverage lives here at the
common-package layer.
"""

from __future__ import annotations

import pytest

from qiita_common.illumina import (
    instrument_model_from_run_folder,
    instrument_run_id_from_run_folder,
    load_instrument_prefix_table,
)

# ---------------------------------------------------------------------------
# load_instrument_prefix_table
# ---------------------------------------------------------------------------


def test_load_table_returns_only_illumina_entries_with_prefix():
    """Every entry returned must be Illumina-prefixed and have a non-empty
    machine_prefix. Families without machine_prefix (HiSeq1500, HiSeq3000,
    NextSeq, NovaSeqXPlus) are out; PacBio Revio is out (Illumina prefix
    filter), even though its machine_prefix `r` is present in the YAML."""
    table = load_instrument_prefix_table()
    for prefix, model_name in table.items():
        assert prefix, "machine_prefix must be non-empty"
        assert model_name.startswith("Illumina "), (
            f"non-Illumina model_name {model_name!r} (prefix {prefix!r}) "
            "leaked through the load-time filter"
        )


def test_load_table_carries_the_supported_families():
    """Pin the nine Illumina families that the vendored sequencer_types.yml
    covers with a parseable machine_prefix. A re-vendor that quietly drops
    or renames one surfaces here rather than as a "no profile for X" runtime
    error during dispatch."""
    table = load_instrument_prefix_table()
    expected = {
        "D": "Illumina HiSeq 2500",
        "K": "Illumina HiSeq 4000",
        "FS": "Illumina iSeq",
        "MN": "Illumina MiniSeq",
        "M": "Illumina MiSeq",
        "SL": "Illumina MiSeq i100",
        "SH": "Illumina MiSeq i100 Plus",
        "A": "Illumina NovaSeq 6000",
        "LH": "Illumina NovaSeq X",
    }
    assert table == expected


def test_load_table_excludes_pacbio_revio():
    """PacBio Revio's `r` machine_prefix lives in the YAML so a re-vendor
    of the upstream stays a clean diff, but the loader filters it out — a
    real Illumina serial number starting with lowercase r would otherwise
    map to PacBio's model_name and break dispatch later."""
    table = load_instrument_prefix_table()
    assert "r" not in table
    assert "Revio" not in table.values()


# ---------------------------------------------------------------------------
# instrument_run_id_from_run_folder
# ---------------------------------------------------------------------------


def test_run_id_returns_folder_basename_when_well_formed():
    folder = "230101_A00123_0001_BHXYZ"
    assert instrument_run_id_from_run_folder(folder) == folder


def test_run_id_rejects_underscore_count_below_three():
    """Fewer than four segments means the folder does not match the
    Illumina convention; both helpers reject identically so a partial
    parse can't slip through one path."""
    for bad in ("230101", "230101_A00123", "230101_A00123_0001"):
        with pytest.raises(ValueError, match="Illumina convention"):
            instrument_run_id_from_run_folder(bad)


# ---------------------------------------------------------------------------
# instrument_model_from_run_folder
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "folder, expected",
    [
        # Date prefix is irrelevant; only parts[1] is parsed.
        ("230101_A00123_0001_BHXYZ", "Illumina NovaSeq 6000"),
        ("260520_LH00345_0002_AXYZ12", "Illumina NovaSeq X"),
        ("260520_FS10000A_0003_AISEQ1", "Illumina iSeq"),
        ("260520_M07654_0004_AMISQ1", "Illumina MiSeq"),
        ("260520_MN00321_0005_AMNSEQ", "Illumina MiniSeq"),
        ("260520_D00567_0006_AHISQ25", "Illumina HiSeq 2500"),
        ("260520_K00567_0007_AHISQ40", "Illumina HiSeq 4000"),
        ("260520_SL00012_0008_AMI100", "Illumina MiSeq i100"),
        ("260520_SH00012_0009_AMIPLUS", "Illumina MiSeq i100 Plus"),
    ],
)
def test_model_parametrized_supported_families(folder, expected):
    assert instrument_model_from_run_folder(folder) == expected


def test_model_longest_prefix_wins_when_table_has_overlaps():
    """NovaSeq X (LH) vs HiSeq 4000 (L)... wait, there is no `L` entry,
    but `M` (MiSeq) vs `MN` (MiniSeq) is the real overlap. A serial of
    `MN00321` must return MiniSeq, not MiSeq."""
    assert instrument_model_from_run_folder("260520_MN00321_0005_X") == "Illumina MiniSeq"


def test_model_longest_prefix_lh_over_l():
    """`LH` vs any 1-char prefix that happens to start with L: ensure the
    2-char match wins. (There is no plain `L` in the table, but the sort
    order this depends on is `len desc`; this test pins the policy.)"""
    assert instrument_model_from_run_folder("260520_LH00345_0002_X") == "Illumina NovaSeq X"


def test_model_rejects_unknown_prefix():
    """A serial that does not start with any known prefix raises with a
    message that names the offending folder and tells the operator where
    to add the prefix upstream. Matches the error wording the CLI surfaces."""
    with pytest.raises(ValueError, match="unknown instrument serial prefix"):
        instrument_model_from_run_folder("260520_ZZZZZ999_0001_X")


def test_model_rejects_malformed_folder_shape():
    """Fewer than four underscore-separated segments fails before any
    prefix lookup."""
    with pytest.raises(ValueError, match="Illumina convention"):
        instrument_model_from_run_folder("230101_A00123_0001")
