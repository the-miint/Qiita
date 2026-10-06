"""Unit tests for the Illumina BCL run-folder parser.

Shared between the user CLI's `qiita submit-bcl-convert` and the
orchestrator's `bcl_convert_prep` step, so coverage lives here at the
common-package layer.
"""

from __future__ import annotations

import pytest

from qiita_common.illumina import (
    IlluminaRead,
    InstrumentRunInfo,
    _instrument_model_from_serial,
    build_amplicon_dummy_sample_sheet,
    load_instrument_prefix_table,
    read_instrument_run_info,
    read_run_reads,
)

# A minimal RunInfo.xml shaped like a real Illumina file. ``version_attrs``
# stands in for the per-version root-tag differences (the Version 5 file
# carries xsd/xsi namespace attrs the Version 6 file drops) — neither puts
# Run/Instrument in a default namespace, so both parse identically.
_RUNINFO_TEMPLATE = (
    '<?xml version="1.0"?>\n'
    "<RunInfo{version_attrs}>\n"
    '  <Run Id="{run_id}" Number="28">\n'
    "    <Flowcell>BRD91222-2611</Flowcell>\n"
    "    <Instrument>{serial}</Instrument>\n"
    "  </Run>\n"
    "</RunInfo>\n"
)


def _write_runinfo(
    bcl_input_dir,
    run_id="220913_FS10001793_28_BRD91222",
    serial="FS10001773",
    version_attrs="",
):
    """Write a RunInfo.xml into bcl_input_dir and return the directory."""
    bcl_input_dir.mkdir(parents=True, exist_ok=True)
    (bcl_input_dir / "RunInfo.xml").write_text(
        _RUNINFO_TEMPLATE.format(version_attrs=version_attrs, run_id=run_id, serial=serial),
        encoding="utf-8",
    )
    return bcl_input_dir


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
# _instrument_model_from_serial
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "serial, expected",
    [
        ("A00123", "Illumina NovaSeq 6000"),
        ("LH00345", "Illumina NovaSeq X"),
        ("FS10000A", "Illumina iSeq"),
        ("M07654", "Illumina MiSeq"),
        ("MN00321", "Illumina MiniSeq"),
        ("D00567", "Illumina HiSeq 2500"),
        ("K00567", "Illumina HiSeq 4000"),
        ("SL00012", "Illumina MiSeq i100"),
        ("SH00012", "Illumina MiSeq i100 Plus"),
    ],
)
def test__instrument_model_from_serial_supported_families(serial, expected):
    assert _instrument_model_from_serial(serial) == expected


def test__instrument_model_from_serial_longest_prefix_wins():
    """Tests the case where overlapping prefixes (`MN` MiniSeq vs `M`
    MiSeq) could both match; the longer prefix must win so `MN00321`
    resolves to MiniSeq, not MiSeq."""
    assert _instrument_model_from_serial("MN00321") == "Illumina MiniSeq"


def test__instrument_model_from_serial_rejects_unknown_prefix():
    """Tests the case where a serial number starts with no known prefix; the
    error names the offending serial number and points at the upstream table."""
    with pytest.raises(ValueError, match="unknown instrument serial prefix"):
        _instrument_model_from_serial("ZZZZZ999")


# ---------------------------------------------------------------------------
# read_instrument_run_info
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "version_attrs",
    [
        # Version 6: bare root tag.
        ' Version="6"',
        # Version 5: root carries xsd/xsi namespace attrs, which must not
        # push Run/Instrument into a default namespace.
        (
            ' xmlns:xsd="http://www.w3.org/2001/XMLSchema"'
            ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="5"'
        ),
    ],
)
def test_read_instrument_run_info_parses_both_versions(tmp_path, version_attrs):
    """Tests the case where a well-formed RunInfo.xml is present: the run ID
    comes from Run@Id verbatim and the model resolves from the Instrument
    serial number, across both Illumina RunInfo schema versions."""
    bcl_input_dir = _write_runinfo(
        tmp_path / "run",
        run_id="250606_LH00444_0355_B22VT23LT4",
        serial="LH00444",
        version_attrs=version_attrs,
    )
    expected = InstrumentRunInfo(
        instrument_run_id="250606_LH00444_0355_B22VT23LT4",
        instrument_model="Illumina NovaSeq X",
    )
    assert read_instrument_run_info(bcl_input_dir) == expected


def test_read_instrument_run_info_rejects_missing_file(tmp_path):
    """Tests the case where the run folder has no top-level RunInfo.xml."""
    (tmp_path / "run").mkdir()
    with pytest.raises(ValueError, match="RunInfo.xml not found"):
        read_instrument_run_info(tmp_path / "run")


def test_read_instrument_run_info_rejects_nested_only_runinfo(tmp_path):
    """Tests the case where a RunInfo.xml exists only in a subdirectory;
    only a top-level file counts."""
    bcl_input_dir = tmp_path / "run"
    _write_runinfo(bcl_input_dir / "nested")
    with pytest.raises(ValueError, match="RunInfo.xml not found"):
        read_instrument_run_info(bcl_input_dir)


def test_read_instrument_run_info_rejects_malformed_xml(tmp_path):
    """Tests the case where RunInfo.xml is not well-formed XML."""
    bcl_input_dir = tmp_path / "run"
    bcl_input_dir.mkdir()
    (bcl_input_dir / "RunInfo.xml").write_text("<RunInfo><Run></RunInfo>", encoding="utf-8")
    with pytest.raises(ValueError, match="not well-formed XML"):
        read_instrument_run_info(bcl_input_dir)


def test_read_instrument_run_info_rejects_missing_run_tag(tmp_path):
    """Tests the case where the XML has no <Run> tag."""
    bcl_input_dir = tmp_path / "run"
    bcl_input_dir.mkdir()
    (bcl_input_dir / "RunInfo.xml").write_text('<RunInfo Version="6" />', encoding="utf-8")
    with pytest.raises(ValueError, match="no <Run> tag"):
        read_instrument_run_info(bcl_input_dir)


def test_read_instrument_run_info_rejects_missing_id_attr(tmp_path):
    """Tests the case where the <Run> tag carries no Id attribute."""
    bcl_input_dir = tmp_path / "run"
    bcl_input_dir.mkdir()
    (bcl_input_dir / "RunInfo.xml").write_text(
        '<RunInfo Version="6"><Run><Instrument>LH00444</Instrument></Run></RunInfo>',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="no Id attribute"):
        read_instrument_run_info(bcl_input_dir)


def test_read_instrument_run_info_rejects_missing_instrument(tmp_path):
    """Tests the case where the <Run> tag has no nested <Instrument>
    serial number."""
    bcl_input_dir = tmp_path / "run"
    bcl_input_dir.mkdir()
    (bcl_input_dir / "RunInfo.xml").write_text(
        '<RunInfo Version="6"><Run Id="250606_LH00444_0355_X" /></RunInfo>',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="no <Instrument> serial number"):
        read_instrument_run_info(bcl_input_dir)


def test_read_instrument_run_info_rejects_unknown_prefix(tmp_path):
    """Tests the case where the Instrument serial number starts with no known
    prefix; the serial-resolution error surfaces unchanged."""
    bcl_input_dir = _write_runinfo(tmp_path / "run", serial="ZZZZZ999")
    with pytest.raises(ValueError, match="unknown instrument serial prefix"):
        read_instrument_run_info(bcl_input_dir)


# ---------------------------------------------------------------------------
# read_run_reads / build_amplicon_dummy_sample_sheet
# ---------------------------------------------------------------------------

# 151 / 12(index) / 151 — the real Rapid 16S MiSeq geometry, index = Golay barcode.
_READS_XML = (
    '<Read Number="1" NumCycles="151" IsIndexedRead="N"/>'
    '<Read Number="2" NumCycles="12" IsIndexedRead="Y"/>'
    '<Read Number="3" NumCycles="151" IsIndexedRead="N"/>'
)


def _write_runinfo_with_reads(bcl_input_dir, reads_xml=_READS_XML):
    bcl_input_dir.mkdir(parents=True, exist_ok=True)
    (bcl_input_dir / "RunInfo.xml").write_text(
        f'<RunInfo Version="6"><Run Id="r" Number="8"><Instrument>SL00377</Instrument>'
        f"<Reads>{reads_xml}</Reads></Run></RunInfo>",
        encoding="utf-8",
    )
    return bcl_input_dir


def test_read_run_reads_parses_geometry(tmp_path):
    reads = read_run_reads(_write_runinfo_with_reads(tmp_path / "run"))
    assert reads == (
        IlluminaRead(151, False),
        IlluminaRead(12, True),
        IlluminaRead(151, False),
    )


def test_read_run_reads_rejects_missing_reads(tmp_path):
    bcl_input_dir = _write_runinfo(tmp_path / "run")  # template has no <Reads>
    with pytest.raises(ValueError, match="no usable <Reads>"):
        read_run_reads(bcl_input_dir)


def test_build_amplicon_dummy_sample_sheet_mirrors_spp():
    reads = (IlluminaRead(151, False), IlluminaRead(12, True), IlluminaRead(151, False))
    sheet = build_amplicon_dummy_sample_sheet(reads, "run_SMPL1")
    assert "OverrideCycles,Y151;N12;Y151" in sheet
    assert "CreateFastqForIndexReads,1" in sheet
    assert "MaskShortReads,1" in sheet
    assert "[Reads]\n151\n151" in sheet
    # one placeholder sample, empty indices -> everything to Undetermined
    assert sheet.rstrip().endswith("run_SMPL1,,,,,,")


def test_build_amplicon_dummy_sample_sheet_handles_dual_index():
    reads = (
        IlluminaRead(151, False),
        IlluminaRead(8, True),
        IlluminaRead(8, True),
        IlluminaRead(151, False),
    )
    sheet = build_amplicon_dummy_sample_sheet(reads, "run_SMPL1")
    assert "OverrideCycles,Y151;N8;N8;Y151" in sheet


@pytest.mark.parametrize(
    "reads",
    [
        (IlluminaRead(151, False),),  # only one template read
        (IlluminaRead(151, False), IlluminaRead(151, False)),  # no index read
        (IlluminaRead(151, False), IlluminaRead(151, False), IlluminaRead(151, False)),
    ],
)
def test_build_amplicon_dummy_sample_sheet_rejects_abnormal_reads(reads):
    with pytest.raises(ValueError, match="2 template reads and 1-2 index reads"):
        build_amplicon_dummy_sample_sheet(reads, "run_SMPL1")
