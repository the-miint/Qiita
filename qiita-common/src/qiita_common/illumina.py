"""Illumina BCL run-folder ``RunInfo.xml`` parsing.

Reads the instrument run ID and serial number from a run folder's
``RunInfo.xml`` and resolves the serial number to a model name against a
vendored prefix table.

Prefix table: a one-time snapshot of biocore/kl-metapool's
``metapool/config/sequencer_types.yml`` lives at
``qiita-common/src/qiita_common/data/sequencer_types.yml``. The file's
header pins the source SHA + URL and documents the re-vendor protocol.

``RUNINFO_FILENAME`` is exported because the file is stat-ed before it is
opened — the control plane separates "cannot read" from "not there" for it, and
both sides have to spell it one way.
"""

from __future__ import annotations

from importlib import resources
from pathlib import Path
from typing import Any, NamedTuple
from xml.etree import ElementTree as ET

import yaml

# The sequencer-written run metadata at the top of a BCL run folder.
RUNINFO_FILENAME = "RunInfo.xml"

# bcl-convert is Illumina-only. This
# loader filters out anything whose model_name does not start with the
# Illumina prefix so a PacBio serial-prefix collision (e.g. a real
# Illumina serial number starting with "r") cannot silently route to a
# PacBio model_name downstream.
_ILLUMINA_MODEL_PREFIX = "Illumina "


def load_instrument_prefix_table() -> dict[str, str]:
    """Return ``{machine_prefix: model_name}`` for every Illumina family
    in the vendored sequencer_types.yml that carries a ``machine_prefix``.

    Filters:
      * Entries without ``machine_prefix`` are skipped. As of the
        vendored snapshot these are HiSeq1500, HiSeq3000, NextSeq, and
        NovaSeqXPlus; folder names from those families fail-fast at
        parse time with the "unknown instrument serial prefix" error
        rather than silently mis-routing.
      * Entries whose ``model_name`` does not start with ``"Illumina "``
        are skipped (PacBio Revio's ``r`` prefix is excluded — bcl-convert
        does not run on PacBio data, and an "r"-prefixed serial number
        on a real Illumina instrument would otherwise be mis-mapped).
    """
    raw_text = (
        resources.files("qiita_common.data")
        .joinpath("sequencer_types.yml")
        .read_text(encoding="utf-8")
    )
    raw: dict[str, dict[str, Any]] = yaml.safe_load(raw_text)
    table: dict[str, str] = {}
    for entry in raw.values():
        prefix = entry.get("machine_prefix")
        model_name = entry.get("model_name")
        if not prefix or not model_name:
            continue
        if not model_name.startswith(_ILLUMINA_MODEL_PREFIX):
            continue
        table[prefix] = model_name
    return table


# Module-level constant: the prefix table is read once at import time.
# Re-vendoring sequencer_types.yml requires a process restart, which
# matches the deploy lifecycle.
_INSTRUMENT_PREFIXES = load_instrument_prefix_table()


def _instrument_model_from_serial(serial: str) -> str:
    """Return the Illumina ``model_name`` string for an instrument serial number.

    Match policy: longest prefix wins. The vendored prefix table has
    overlapping entries (``LH`` vs ``L``, ``MN`` vs ``M``, ``SL`` vs
    ``S``, ``SH`` vs ``S``); without longest-match a NovaSeq X serial
    number ``LH00345`` could resolve to whatever single-character prefix is
    checked first. Iterating prefixes by length descending makes the
    match deterministic. Raises ``ValueError`` on an unrecognized prefix.
    """
    for prefix in sorted(_INSTRUMENT_PREFIXES, key=len, reverse=True):
        if serial.startswith(prefix):
            return _INSTRUMENT_PREFIXES[prefix]
    raise ValueError(
        f"unknown instrument serial prefix in {serial!r}; "
        f"add a machine_prefix entry to kl-metapool's sequencer_types.yml "
        f"and re-vendor"
    )


class InstrumentRunInfo(NamedTuple):
    """The instrument run ID and resolved model name for a BCL run folder.

    ``instrument_run_id`` is the ``Run`` tag's ``Id`` attribute verbatim;
    ``instrument_model`` is the vendored ``model_name`` resolved from the
    ``Instrument`` serial number.
    """

    instrument_run_id: str
    instrument_model: str


class IlluminaRead(NamedTuple):
    """One ``<Read>`` from RunInfo.xml: its cycle count and whether it is an index."""

    num_cycles: int
    is_indexed: bool


def read_run_reads(bcl_input_dir: Path) -> tuple[IlluminaRead, ...]:
    """Return the ordered ``<Reads>`` structure from a run folder's RunInfo.xml.

    Raises ``ValueError`` when RunInfo.xml is absent/malformed or carries no
    reads. Used to build the amplicon dummy sample sheet (`build_amplicon_dummy_
    sample_sheet`), whose Reads/OverrideCycles are a function of this geometry.
    """
    runinfo_path = bcl_input_dir / RUNINFO_FILENAME
    if not runinfo_path.is_file():
        raise ValueError(f"RunInfo.xml not found at top level of {bcl_input_dir}")
    try:
        root = ET.parse(runinfo_path).getroot()
    except ET.ParseError as exc:
        raise ValueError(f"{runinfo_path} is not well-formed XML: {exc}") from exc
    reads = [
        IlluminaRead(int(r.get("NumCycles", "0")), r.get("IsIndexedRead") == "Y")
        for r in root.findall("./Run/Reads/Read")
    ]
    if not reads or any(r.num_cycles <= 0 for r in reads):
        raise ValueError(f"{runinfo_path} has no usable <Reads> structure")
    return tuple(reads)


# The Golay barcode is a 12-nt index read.
GOLAY_BARCODE_LENGTH = 12

# The one index the amplicon dummy sheet names. bcl-convert (4.5.4) refuses
# CreateFastqForIndexReads unless the sheet has an index (and an index of only
# A/C/G/T), so the sheet names one placeholder sample with exactly this index and
# no mismatches: a read lands there only when its index read IS this sequence,
# and every other read goes to Undetermined with its I1. Its reverse complement
# (the form golay_demux decodes) is 4 bits from the nearest Golay codeword,
# beyond the correctable 3, so a read it captures is one the demux would have
# dropped anyway.
AMPLICON_PLACEHOLDER_INDEX = "A" * GOLAY_BARCODE_LENGTH


def build_amplicon_dummy_sample_sheet(reads: tuple[IlluminaRead, ...], sample_id: str) -> str:
    """Return the bcl-convert dummy sample sheet for an amplicon run.

    Sends every decodable read to Undetermined and writes its index read as I1,
    which is what carries the in-index Golay barcode out to golay_demux. OverrideCycles
    follows RunInfo.xml's read order: template reads as reads, the first index
    read as the 12-cycle Golay index (any tail masked), any other index read
    masked. See AMPLICON_PLACEHOLDER_INDEX for why the sheet names an index.
    """
    template = [r for r in reads if not r.is_indexed]
    index = [r for r in reads if r.is_indexed]
    if len(template) != 2 or len(index) not in (1, 2):
        raise ValueError(
            f"expected 2 template reads and 1-2 index reads, got {len(template)} and {len(index)}"
        )
    golay_read = index[0]
    if golay_read.num_cycles < GOLAY_BARCODE_LENGTH:
        raise ValueError(
            f"the first index read has {golay_read.num_cycles} cycles; the Golay barcode"
            f" needs {GOLAY_BARCODE_LENGTH}"
        )

    def segment(read: IlluminaRead) -> str:
        if not read.is_indexed:
            return f"Y{read.num_cycles}"
        if read is not golay_read:
            return f"N{read.num_cycles}"
        tail = read.num_cycles - GOLAY_BARCODE_LENGTH
        return f"I{GOLAY_BARCODE_LENGTH}" + (f"N{tail}" if tail else "")

    # bcl-convert reads OverrideCycles segments in RunInfo.xml read order, which
    # need not be R1/I1/I2/R2.
    override_cycles = ";".join(segment(r) for r in reads)
    lines = [
        "[Header]",
        "IEMFileVersion,4",
        "Workflow,GenerateFASTQ",
        "Application,FASTQ Only",
        "",
        "[Reads]",
        str(template[0].num_cycles),
        str(template[1].num_cycles),
        "",
        "[Settings]",
        f"OverrideCycles,{override_cycles}",
        "MaskShortReads,1",
        "CreateFastqForIndexReads,1",
        "BarcodeMismatchesIndex1,0",
        "",
        "[Data]",
        "Sample_ID,Sample_Plate,Sample_Well,I7_Index_ID,index,I5_Index_ID,index2",
        f"{sample_id},,,,{AMPLICON_PLACEHOLDER_INDEX},,",
        "",
    ]
    return "\n".join(lines)


def read_instrument_run_info(bcl_input_dir: Path) -> InstrumentRunInfo:
    """Read ``RunInfo.xml`` at the top of a BCL run folder and return the
    instrument run ID and resolved model name.

    Reading the serial number from the sequencer-written ``RunInfo.xml`` is stable
    where the folder basename is not — operators rename run folders.

    Raises ``ValueError`` when ``RunInfo.xml`` is absent or malformed, when
    the ``Run``/``Id``/``Instrument`` pieces are missing or empty, or on an
    unrecognized serial prefix.
    """
    runinfo_path = bcl_input_dir / RUNINFO_FILENAME
    if not runinfo_path.is_file():
        raise ValueError(f"RunInfo.xml not found at top level of {bcl_input_dir}")

    # Parse the sequencer-written run metadata and error if malformed.
    try:
        root = ET.parse(runinfo_path).getroot()
    except ET.ParseError as exc:
        raise ValueError(f"{runinfo_path} is not well-formed XML: {exc}") from exc

    # Pull the run ID from the Run tag's Id attribute.
    run = root.find("Run")
    if run is None:
        raise ValueError(f"{runinfo_path} has no <Run> tag")
    instrument_run_id = run.get("Id")
    if not instrument_run_id:
        raise ValueError(f"{runinfo_path} <Run> tag has no Id attribute")

    # Pull the instrument serial number from the Instrument tag nested under Run.
    instrument = run.find("Instrument")
    serial = instrument.text.strip() if instrument is not None and instrument.text else ""
    if not serial:
        raise ValueError(f"{runinfo_path} has no <Instrument> serial number under <Run>")

    return InstrumentRunInfo(instrument_run_id, _instrument_model_from_serial(serial))
