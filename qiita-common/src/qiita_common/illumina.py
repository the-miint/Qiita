"""Illumina BCL run-folder parsing shared between the CLI and the
orchestrator.

The qiita-bundled bcl-convert flow needs the instrument model in two
places: the orchestrator's bcl_convert_prep step writes it as a sidecar
file for the runner's A4 baseline_resources lookup, and the user CLI's
``qiita submit-bcl-convert`` derives it from the folder name at submit
time so it can populate ``SequencingRunCreateRequest.instrument_model``
without an extra operator flag. Both consumers parse the same Illumina
convention against the same vendored prefix table, so the parser lives
here in qiita-common rather than being duplicated.

Prefix table: a one-time snapshot of biocore/kl-metapool's
``metapool/config/sequencer_types.yml`` lives at
``qiita-common/src/qiita_common/data/sequencer_types.yml``. The file's
header pins the source SHA + URL and documents the re-vendor protocol.
"""

from __future__ import annotations

from importlib import resources
from typing import Any

import yaml

# bcl-convert is Illumina-only. The vendored prefix table preserves a
# PacBio Revio entry verbatim so re-vendoring stays a clean diff; this
# loader filters out anything whose model_name does not start with the
# Illumina prefix so a PacBio serial-prefix collision (e.g. a real
# Illumina serial starting with "r") cannot silently route to a
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


def _split_run_folder(folder_name: str) -> list[str]:
    """Split a BCL run folder name into its underscore-separated segments,
    enforcing the Illumina convention
    ``<YYMMDD>_<InstrumentSerial>_<RunNum>_<FlowcellID>`` (at least four
    segments).

    Single home for the convention string and the segment-count check so
    ``instrument_run_id_from_run_folder`` and
    ``instrument_model_from_run_folder`` validate identically — both call
    this so a partial parse cannot silently succeed in one path while the
    other rejects it. Raises ``ValueError`` on a non-conforming name.
    """
    parts = folder_name.split("_")
    if len(parts) < 4:
        raise ValueError(
            f"BCL run folder name does not match Illumina convention "
            f"<YYMMDD>_<InstrumentSerial>_<RunNum>_<FlowcellID>: {folder_name!r}"
        )
    return parts


def instrument_run_id_from_run_folder(folder_name: str) -> str:
    """Return the instrument_run_id encoded in a BCL run folder name.

    The Illumina convention is
    ``<YYMMDD>_<InstrumentSerial>_<RunNum>_<FlowcellID>`` and the
    instrument run ID IS the folder basename — they are the same value
    by definition. The CLI uses this so the operator does not have to
    re-type the folder name; the helper exists so the contract lives in
    one place if Illumina ever changes the convention.

    Raises ``ValueError`` (via ``_split_run_folder``) on a folder name
    that does not have at least four underscore-separated segments. The
    downstream ``instrument_model_from_run_folder`` enforces the same
    shape — both are checked so a partial parse cannot silently succeed.
    """
    _split_run_folder(folder_name)
    return folder_name


def instrument_model_from_run_folder(folder_name: str) -> str:
    """Return the Illumina ``model_name`` string for a BCL run folder.

    Match policy: longest prefix wins. The vendored prefix table has
    overlapping entries (``LH`` vs ``L``, ``MN`` vs ``M``, ``SL`` vs
    ``S``, ``SH`` vs ``S``); without longest-match a NovaSeq X serial
    ``LH00345`` could resolve to whatever single-character prefix is
    checked first. Iterating prefixes by length descending makes the
    match deterministic.

    Raises ``ValueError`` on malformed folder names and on unrecognized
    prefixes. Callers (the orchestrator job, the CLI submit command)
    surface this verbatim — the launcher framework maps ValueError to
    ``BackendFailure(BAD_INPUT)`` for the orchestrator path; the CLI
    prints the message and exits non-zero.
    """
    parts = _split_run_folder(folder_name)
    serial = parts[1]
    for prefix in sorted(_INSTRUMENT_PREFIXES, key=len, reverse=True):
        if serial.startswith(prefix):
            return _INSTRUMENT_PREFIXES[prefix]
    raise ValueError(
        f"unknown instrument serial prefix in {folder_name!r}; "
        f"add a machine_prefix entry to kl-metapool's sequencer_types.yml "
        f"and re-vendor, or rename the folder to use a recognized prefix"
    )
