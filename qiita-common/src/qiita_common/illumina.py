"""Illumina BCL run-folder ``RunInfo.xml`` parsing.

Reads the instrument run ID and serial number from a run folder's
``RunInfo.xml`` and resolves the serial number to a model name against a
vendored prefix table.

Prefix table: a one-time snapshot of biocore/kl-metapool's
``metapool/config/sequencer_types.yml`` lives at
``qiita-common/src/qiita_common/data/sequencer_types.yml``. The file's
header pins the source SHA + URL and documents the re-vendor protocol.
"""

from __future__ import annotations

from importlib import resources
from pathlib import Path
from typing import Any, NamedTuple
from xml.etree import ElementTree as ET

import yaml

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


def read_instrument_run_info(bcl_input_dir: Path) -> InstrumentRunInfo:
    """Read ``RunInfo.xml`` at the top of a BCL run folder and return the
    instrument run ID and resolved model name.

    Reading the serial number from the sequencer-written ``RunInfo.xml`` is stable
    where the folder basename is not — operators rename run folders.

    Raises ``ValueError`` when ``RunInfo.xml`` is absent or malformed, when
    the ``Run``/``Id``/``Instrument`` pieces are missing or empty, or on an
    unrecognized serial prefix.
    """
    runinfo_path = bcl_input_dir / "RunInfo.xml"
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
