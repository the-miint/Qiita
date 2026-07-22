"""ENA/SRA accession-type detection and validation.

Every resolver call takes a validated accession up front — fail loud on an
empty, malformed, or wrong-kind accession instead of handing it to
`read_ena`/`read_ena_attributes` (or the HTTP fallback) and getting back an
opaque zero-row result. Accepted prefixes per kind:

- study: PRJNA, PRJEB, PRJDB, ERP, SRP, DRP
- sample: SAMN, SAME, SAMD
- run: SRR, ERR, DRR
- experiment: SRX, ERX, DRX

This is a validation-only mirror of `duckdb-miint`'s own
`ENAParser::DetectAccessionType` (`duckdb-miint/src/ena_parser.cpp`) — the
prefix sets above are exactly its `rfind(..., 0) == 0` checks per accession
type (INSDC/ENA/DDBJ), no more and no fewer. It exists so a bad accession
fails here, in Python, with an actionable message, before any
network/DuckDB call. Notably this does NOT accept `ERS` as a sample
prefix — `ENAParser::DetectAccessionType` doesn't recognize it either, so a
Qiita user passing `ERS*` would be an accession miint itself can't resolve;
rejecting it here (rather than silently forwarding it to `read_ena`) is the
correct pre-flight behavior.
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
