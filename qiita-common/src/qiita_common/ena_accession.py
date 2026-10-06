"""INSDC accession-type detection and validation.

ENA, SRA and DDBJ mirror each other, and every accession below resolves through
ENA's API regardless of which archive minted it.

Validate an accession up front so a bad one fails loud here, in Python, with an
actionable message before any network/DuckDB call; miint classifies by prefix alone
(duckdb-miint#288). An accession is a known prefix followed by ASCII digits; a
sample prefix may carry one extra letter (SAMEA, SAMEG), per ENA's accession guide.
`ERS` is not accepted as a sample because `read_ena` cannot resolve it
(https://the-miint.github.io/duckdb-miint/insdc_ena/).
"""

from __future__ import annotations

import re
from enum import StrEnum


class EnaAccessionKind(StrEnum):
    STUDY = "study"
    SAMPLE = "sample"
    RUN = "run"
    EXPERIMENT = "experiment"


class InvalidEnaAccessionError(ValueError):
    """Raised when an accession is empty/blank or not a known prefix followed by digits."""


_ACCESSION_PREFIXES: dict[EnaAccessionKind, tuple[str, ...]] = {
    EnaAccessionKind.STUDY: ("PRJNA", "PRJEB", "PRJDB", "ERP", "SRP", "DRP"),
    EnaAccessionKind.SAMPLE: ("SAMN", "SAME", "SAMD"),
    EnaAccessionKind.RUN: ("SRR", "ERR", "DRR"),
    EnaAccessionKind.EXPERIMENT: ("SRX", "ERX", "DRX"),
}


_OPTIONAL_LETTER_KINDS = frozenset({EnaAccessionKind.SAMPLE})

_ACCESSION_PATTERNS: dict[EnaAccessionKind, re.Pattern[str]] = {
    kind: re.compile(
        f"(?:{'|'.join(prefixes)}){'[A-Z]?' if kind in _OPTIONAL_LETTER_KINDS else ''}[0-9]+"
    )
    for kind, prefixes in _ACCESSION_PREFIXES.items()
}


def _accepted_prefixes_message() -> str:
    forms = "; ".join(
        f"{kind.value}={'/'.join(prefixes)}"
        + (", optionally followed by one letter" if kind in _OPTIONAL_LETTER_KINDS else "")
        for kind, prefixes in _ACCESSION_PREFIXES.items()
    )
    return f"one of these prefixes followed by digits, e.g. PRJEB11419: {forms}"


def detect_accession_kind(accession: str) -> EnaAccessionKind:
    """Return the `EnaAccessionKind` matching `accession`'s format, or raise
    `InvalidEnaAccessionError` if empty/blank or matching no known format."""
    candidate = accession.strip() if accession else ""
    if not candidate:
        raise InvalidEnaAccessionError(
            "ENA accession must not be empty; expected " + _accepted_prefixes_message()
        )
    for kind, pattern in _ACCESSION_PATTERNS.items():
        if pattern.fullmatch(candidate):
            return kind
    raise InvalidEnaAccessionError(
        f"'{accession}' does not match a known INSDC accession format; "
        f"expected {_accepted_prefixes_message()}"
    )


def validate_study_accession(accession: str) -> str:
    """Validate `accession` is a well-formed INSDC STUDY accession and return it
    stripped. Raises `InvalidEnaAccessionError` on anything else — including a
    well-formed accession of the wrong kind (sample/run/experiment)."""
    kind = detect_accession_kind(accession)
    if kind is not EnaAccessionKind.STUDY:
        raise InvalidEnaAccessionError(
            f"'{accession}' is a {kind.value} accession, not a study accession "
            f"(expected one of: {', '.join(_ACCESSION_PREFIXES[EnaAccessionKind.STUDY])})"
        )
    return accession.strip()
