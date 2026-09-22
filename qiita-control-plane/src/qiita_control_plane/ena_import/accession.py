"""INSDC accession-type detection and validation.

ENA, SRA and DDBJ mirror each other, and every accession below resolves through
ENA's API regardless of which archive minted it.

Validate an accession up front so a bad one fails loud here, in Python, with an
actionable message before any network/DuckDB call. Validation-only mirror of
`duckdb-miint`'s `ENAParser::DetectAccessionType` (`duckdb-miint/src/ena_parser.cpp`) —
the prefix sets below are exactly its per-type checks, no more, no fewer. Notably `ERS`
is NOT a recognized sample prefix (miint can't resolve it either), so it is rejected
rather than silently forwarded to `read_ena`.
"""

from __future__ import annotations

from enum import StrEnum


class EnaAccessionKind(StrEnum):
    STUDY = "study"
    SAMPLE = "sample"
    RUN = "run"
    EXPERIMENT = "experiment"


class InvalidEnaAccessionError(ValueError):
    """Raised when an accession is empty/blank or matches no known INSDC prefix.
    Never a silent `None`/empty-result fallback."""


_ACCESSION_PREFIXES: dict[EnaAccessionKind, tuple[str, ...]] = {
    EnaAccessionKind.STUDY: ("PRJNA", "PRJEB", "PRJDB", "ERP", "SRP", "DRP"),
    EnaAccessionKind.SAMPLE: ("SAMN", "SAME", "SAMD"),
    EnaAccessionKind.RUN: ("SRR", "ERR", "DRR"),
    EnaAccessionKind.EXPERIMENT: ("SRX", "ERX", "DRX"),
}


def _accepted_prefixes_message() -> str:
    return "; ".join(
        f"{kind.value}={'/'.join(prefixes)}" for kind, prefixes in _ACCESSION_PREFIXES.items()
    )


def detect_accession_kind(accession: str) -> EnaAccessionKind:
    """Return the `EnaAccessionKind` matching `accession`'s prefix, or raise
    `InvalidEnaAccessionError` if empty/blank or matching no known prefix."""
    candidate = accession.strip() if accession else ""
    if not candidate:
        raise InvalidEnaAccessionError(
            "ENA accession must not be empty; expected one of: " + _accepted_prefixes_message()
        )
    for kind, prefixes in _ACCESSION_PREFIXES.items():
        if candidate.startswith(prefixes):
            return kind
    raise InvalidEnaAccessionError(
        f"'{accession}' does not match a known INSDC accession prefix; "
        f"expected one of: {_accepted_prefixes_message()}"
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
