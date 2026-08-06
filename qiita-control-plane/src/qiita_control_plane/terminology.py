"""Action-layer entry points for the terminology subsystem: status
transition, manifest parsing and checksum verification, and
staged-release import."""

from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import replace
from pathlib import Path

import asyncpg
from qiita_common.models import (
    VALID_TERMINOLOGY_STATUS_TRANSITIONS,
    TerminologyManifest,
    TerminologyResponse,
    TerminologyStatus,
    TerminologyTermObsoletionKind,
)

from .repositories.terminology import (
    ParsedTerm,
    TerminologyImportAnomaly,
    TerminologyImportResult,
    fetch_terminology,
    import_terminology_release,
    update_terminology_status,
)


class TerminologyNotFound(Exception):
    """Raised when the terminology_idx doesn't exist."""


# same-pattern-ok: parallel to actions.reference.IllegalStatusTransition; N=2
# cases differing only in the target enum type. Parameterization deferred
# until a third resource (N=3) gains status-transition machinery.
class IllegalStatusTransition(Exception):
    """Raised when the current status can't transition to the target."""

    def __init__(self, *, current: str | None, target: TerminologyStatus) -> None:
        super().__init__(f"Cannot transition from {current!r} to {target!r}")
        self.current = current
        self.target = target


async def transition_terminology_status(
    pool_or_conn: asyncpg.Pool | asyncpg.Connection,
    terminology_idx: int,
    target: TerminologyStatus,
) -> TerminologyResponse:
    """Atomically transition a terminology's status, validated against
    VALID_TERMINOLOGY_STATUS_TRANSITIONS.

    Raises TerminologyNotFound if the row doesn't exist;
    IllegalStatusTransition if no source status maps to `target`, or if
    the row is in a state that cannot reach `target`.
    """
    # Derive from VALID_TERMINOLOGY_STATUS_TRANSITIONS the set of source
    # states that can reach `target`. An empty list means no source can,
    # raise error rather than letting the UPDATE silently match zero rows.
    valid_sources = [
        str(src)
        for src, targets in VALID_TERMINOLOGY_STATUS_TRANSITIONS.items()
        if target in targets
    ]
    if not valid_sources:
        raise IllegalStatusTransition(current=None, target=target)

    row = await update_terminology_status(pool_or_conn, terminology_idx, target, valid_sources)
    if row is not None:
        return TerminologyResponse(**dict(row))

    # UPDATE didn't match. Distinguish "row absent" from "row present
    # but in a state that can't reach target" via a follow-up read.
    existing = await fetch_terminology(pool_or_conn, terminology_idx)
    if existing is None:
        raise TerminologyNotFound(terminology_idx)
    raise IllegalStatusTransition(current=existing["status"], target=target)


def load_manifest(source_dir: Path) -> TerminologyManifest:
    """Read and validate `<source_dir>/manifest.json`.

    Raises FileNotFoundError if manifest.json is missing; raises
    pydantic.ValidationError if its content does not match
    TerminologyManifest.
    """
    manifest_path = source_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    payload = json.loads(manifest_path.read_text())
    return TerminologyManifest.model_validate(payload)


def verify_manifest_checksums(source_dir: Path, manifest: TerminologyManifest) -> None:
    """Compute the SHA-256 of the source file declared by the manifest
    and compare to the manifest's declared digest.

    Raises FileNotFoundError if the declared source file is missing;
    raises ValueError on digest mismatch.
    """
    source_path = source_dir / manifest.source.path
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    # Stream the file through the hasher in 1 MiB chunks; OWL files can
    # be hundreds of MB and reading the whole thing into memory just to
    # hash it is wasteful.
    digest = hashlib.sha256()
    with source_path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    actual = digest.hexdigest()
    if actual != manifest.source.sha256:
        raise ValueError(
            f"sha256 mismatch for {manifest.source.path}: "
            f"manifest declares {manifest.source.sha256!r}, computed {actual!r}"
        )


def _parse_terms_tsv(path: Path) -> list[ParsedTerm]:
    """Parse a tab-separated terms table at `path` into a list of
    ParsedTerm. Empty replaced_by_term_id / obsoletion_kind cells
    become None; is_obsolete must be 'true' or 'false' case-insensitively;
    an unrecognized obsoletion_kind raises ValueError naming the row."""
    rows: list[ParsedTerm] = []
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            term_id = row["term_id"]
            replaced_by = row.get("replaced_by_term_id") or None

            # Reject typo'd is_obsolete values explicitly so they surface
            # at parse time rather than silently coercing to False.
            is_obsolete_text = row["is_obsolete"].lower()
            if is_obsolete_text not in ("true", "false"):
                raise ValueError(
                    f"invalid is_obsolete value {row['is_obsolete']!r}"
                    f" for term_id {term_id!r}; expected 'true' or 'false'"
                )

            # Wrap the enum cast so the offending row is named in the error.
            kind_text = row.get("obsoletion_kind") or None
            try:
                obsoletion_kind = TerminologyTermObsoletionKind(kind_text) if kind_text else None
            except ValueError as exc:
                raise ValueError(
                    f"invalid obsoletion_kind {kind_text!r} for term_id {term_id!r}"
                ) from exc

            rows.append(
                ParsedTerm(
                    term_id=term_id,
                    label=row["label"],
                    is_obsolete=is_obsolete_text == "true",
                    replaced_by_term_id=replaced_by,
                    obsoletion_kind=obsoletion_kind,
                )
            )
    return rows


def _parse_closure_tsv(path: Path) -> list[tuple[str, str, int]]:
    """Parse a tab-separated closure table at `path` into a list of
    (ancestor_term_id, descendant_term_id, distance) tuples."""
    rows: list[tuple[str, str, int]] = []
    with path.open(newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            rows.append(
                (
                    row["ancestor_term_id"],
                    row["descendant_term_id"],
                    int(row["distance"]),
                )
            )
    return rows


def _check_misaligned_replaced_by(parsed_terms: list[ParsedTerm]) -> None:
    """Enforce that only an obsolete term carries a replacement pointer,
    naming the offending rows. Always raises on detection; never
    tolerated, because a non-obsolete row carrying a replacement pointer
    is malformed source data rather than a recoverable anomaly."""
    misaligned = [
        (term.term_id, term.replaced_by_term_id)
        for term in parsed_terms
        if not term.is_obsolete and term.replaced_by_term_id is not None
    ]
    if misaligned:
        raise TerminologyImportAnomaly(misaligned_replaced_by=misaligned)


def _find_unresolved_replaced_by(parsed_terms: list[ParsedTerm]) -> list[tuple[str, str]]:
    """Return (term_id, attempted_target) pairs where an obsolete term
    names a replaced_by_term_id absent from the same batch. Returns the
    empty list when every pointer resolves in-batch."""
    known_term_ids = {term.term_id for term in parsed_terms}
    return [
        (term.term_id, term.replaced_by_term_id)
        for term in parsed_terms
        if term.is_obsolete
        and term.replaced_by_term_id is not None
        and term.replaced_by_term_id not in known_term_ids
    ]


async def import_terminology(
    pool: asyncpg.Pool,
    source_dir: Path,
    *,
    tolerate_anomalies: bool = False,
) -> TerminologyImportResult:
    """Parse one staged terminology release from `source_dir` and apply
    it to the DB in a single transaction.

    Expects `manifest.json`, `terms.tsv`, and `closure.tsv` under
    `source_dir`.

    With `tolerate_anomalies=False` (default), raises
    TerminologyImportAnomaly when the release silently drops term_ids
    already in the database, or when an obsolete row's
    replaced_by_term_id is not present in the same batch.

    With `tolerate_anomalies=True`, those two anomaly kinds are absorbed:
    silent drops are auto-obsoleted (obsoletion_kind=silently_dropped,
    label carried forward), and unresolved replaced_by_term_id values
    are NULLed on the affected rows with a notes line recording the
    attempted CURIE. A misaligned replaced_by_term_id (non-obsolete row
    carrying a pointer) always raises — it is malformed source data
    rather than a tolerable anomaly.
    """

    manifest = load_manifest(source_dir)
    verify_manifest_checksums(source_dir, manifest)
    parsed_terms = _parse_terms_tsv(source_dir / "terms.tsv")
    parsed_closure = _parse_closure_tsv(source_dir / "closure.tsv")

    # Misalignment is always fatal; it indicates malformed source data.
    _check_misaligned_replaced_by(parsed_terms)

    # Validate outside the transaction so an unresolved replaced_by_term_id
    # in fail mode leaves the DB untouched (no row is created).
    unresolved_replaced_by_pairs = _find_unresolved_replaced_by(parsed_terms)
    if unresolved_replaced_by_pairs and not tolerate_anomalies:
        raise TerminologyImportAnomaly(unresolved_replaced_by=unresolved_replaced_by_pairs)

    # Tolerate mode: NULL the unresolved replaced_by_term_id on the
    # affected ParsedTerm rows so _resolve_replaced_by skips them; the
    # composer appends a notes line per pair after the upsert.
    if unresolved_replaced_by_pairs:
        unresolved_term_ids = {pair[0] for pair in unresolved_replaced_by_pairs}
        parsed_terms = [
            replace(term, replaced_by_term_id=None) if term.term_id in unresolved_term_ids else term
            for term in parsed_terms
        ]

    async with pool.acquire() as conn, conn.transaction():
        return await import_terminology_release(
            conn,
            name=manifest.name,
            version=manifest.version,
            parsed_terms=parsed_terms,
            parsed_closure=parsed_closure,
            tolerate_anomalies=tolerate_anomalies,
            unresolved_replaced_by_pairs=unresolved_replaced_by_pairs,
        )
