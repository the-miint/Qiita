"""ENA/SRA accession-type detection and validation (T01-3).

Every resolver call takes a validated accession up front — fail loud on an
empty, malformed, or wrong-kind accession instead of handing it to
`read_ena`/`read_ena_attributes` (or the HTTP fallback) and getting back an
opaque zero-row result. Accepted prefixes per kind:

- study: PRJEB, PRJNA, ERP, SRP
- sample: SAMEA, SAMN, SAME, ERS
- run: ERR, SRR, DRR
- experiment: ERX, SRX

This is a validation-only mirror of (a subset of) `duckdb-miint`'s own
`ENAParser::DetectAccessionType` (`duckdb-miint/src/ena_parser.cpp`) — it
exists so a bad accession fails here, in Python, with an actionable message,
before any network/DuckDB call.
"""

from __future__ import annotations

from enum import StrEnum


class EnaAccessionKind(StrEnum):
    STUDY = "study"
    SAMPLE = "sample"
    RUN = "run"
    EXPERIMENT = "experiment"


class InvalidEnaAccessionError(ValueError):
    """Raised when an accession is empty/blank, or does not match any known
    ENA/SRA study/sample/run/experiment prefix. Carries an actionable
    message naming the offending value and the accepted prefixes — never a
    silent `None`/empty-result fallback."""


_ACCESSION_PREFIXES: dict[EnaAccessionKind, tuple[str, ...]] = {
    EnaAccessionKind.STUDY: ("PRJEB", "PRJNA", "ERP", "SRP"),
    EnaAccessionKind.SAMPLE: ("SAMEA", "SAMN", "SAME", "ERS"),
    EnaAccessionKind.RUN: ("ERR", "SRR", "DRR"),
    EnaAccessionKind.EXPERIMENT: ("ERX", "SRX"),
}


def _accepted_prefixes_message() -> str:
    return "; ".join(
        f"{kind.value}={'/'.join(prefixes)}" for kind, prefixes in _ACCESSION_PREFIXES.items()
    )


def detect_accession_kind(accession: str) -> EnaAccessionKind:
    """Return the `EnaAccessionKind` matching `accession`'s prefix.

    Raises `InvalidEnaAccessionError` if `accession` is empty/blank or
    matches none of the known study/sample/run/experiment prefixes — never
    returns an `UNKNOWN` sentinel or `None`."""
    candidate = accession.strip() if accession else ""
    if not candidate:
        raise InvalidEnaAccessionError(
            "ENA accession must not be empty; expected one of: " + _accepted_prefixes_message()
        )
    for kind, prefixes in _ACCESSION_PREFIXES.items():
        if candidate.startswith(prefixes):
            return kind
    raise InvalidEnaAccessionError(
        f"'{accession}' does not match a known ENA/SRA accession prefix; "
        f"expected one of: {_accepted_prefixes_message()}"
    )


def validate_study_accession(accession: str) -> str:
    """Validate `accession` is a well-formed ENA/SRA STUDY accession and
    return it stripped of surrounding whitespace. Raises
    `InvalidEnaAccessionError` on anything else (empty, malformed, or a
    well-formed accession of the wrong kind) — a resolver method scoped to
    "the study" must fail loud on a sample/run/experiment accession too."""
    kind = detect_accession_kind(accession)
    if kind is not EnaAccessionKind.STUDY:
        raise InvalidEnaAccessionError(
            f"'{accession}' is a {kind.value} accession, not a study accession "
            f"(expected one of: {', '.join(_ACCESSION_PREFIXES[EnaAccessionKind.STUDY])})"
        )
    return accession.strip()
