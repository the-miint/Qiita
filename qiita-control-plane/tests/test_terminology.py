"""Unit tests for qiita_control_plane.terminology — status transitions,
manifest parser/verifier, and the staging-dir import workflow."""

import csv
import hashlib
import json
from datetime import datetime

import pytest
from qiita_common.models import (
    TerminologyManifest,
    TerminologyManifestSource,
    TerminologyResponse,
    TerminologyStatus,
    TerminologyTermObsoletionKind,
)

from qiita_control_plane.repositories.terminology import (
    TerminologyImportAnomaly,
    TerminologyImportResult,
    fetch_terminology,
)
from qiita_control_plane.terminology import (
    IllegalStatusTransition,
    TerminologyNotFound,
    import_terminology,
    load_manifest,
    transition_terminology_status,
    verify_manifest_checksums,
)
from qiita_control_plane.testing.db_seeds import (
    SEEDED_TERMINOLOGY_LOADED_AT,
    seed_terminology,
)

pytestmark = pytest.mark.db


# =============================================================================
# transition_terminology_status
# =============================================================================


async def test_transition_terminology_status_loading_to_active(
    postgres_pool, created_terminologies
):
    """Promoting a LOADING terminology to ACTIVE succeeds and returns the
    updated row."""
    idx = await seed_terminology(
        postgres_pool, name="t1_loading_to_active", status=TerminologyStatus.LOADING
    )
    created_terminologies.append(idx)

    result = await transition_terminology_status(postgres_pool, idx, TerminologyStatus.ACTIVE)

    expected = TerminologyResponse(
        terminology_idx=idx,
        name="t1_loading_to_active",
        version="1.0.0",
        status=TerminologyStatus.ACTIVE,
        loaded_at=SEEDED_TERMINOLOGY_LOADED_AT,
    )
    assert result == expected


async def test_transition_terminology_status_loading_to_failed(
    postgres_pool, created_terminologies
):
    """Marking a LOADING terminology as FAILED succeeds and returns the
    updated row."""
    idx = await seed_terminology(
        postgres_pool, name="t2_loading_to_failed", status=TerminologyStatus.LOADING
    )
    created_terminologies.append(idx)

    result = await transition_terminology_status(postgres_pool, idx, TerminologyStatus.FAILED)

    expected = TerminologyResponse(
        terminology_idx=idx,
        name="t2_loading_to_failed",
        version="1.0.0",
        status=TerminologyStatus.FAILED,
        loaded_at=SEEDED_TERMINOLOGY_LOADED_AT,
    )
    assert result == expected


async def test_transition_terminology_status_active_to_loading(
    postgres_pool, created_terminologies
):
    """Reverting an ACTIVE terminology back to LOADING succeeds (the reload
    path)."""
    idx = await seed_terminology(
        postgres_pool, name="t3_active_to_loading", status=TerminologyStatus.ACTIVE
    )
    created_terminologies.append(idx)

    result = await transition_terminology_status(postgres_pool, idx, TerminologyStatus.LOADING)

    expected = TerminologyResponse(
        terminology_idx=idx,
        name="t3_active_to_loading",
        version="1.0.0",
        status=TerminologyStatus.LOADING,
        loaded_at=SEEDED_TERMINOLOGY_LOADED_AT,
    )
    assert result == expected


async def test_transition_terminology_status_failed_to_loading(
    postgres_pool, created_terminologies
):
    """Reverting a FAILED terminology back to LOADING succeeds (the retry
    path)."""
    idx = await seed_terminology(
        postgres_pool, name="t4_failed_to_loading", status=TerminologyStatus.FAILED
    )
    created_terminologies.append(idx)

    result = await transition_terminology_status(postgres_pool, idx, TerminologyStatus.LOADING)

    expected = TerminologyResponse(
        terminology_idx=idx,
        name="t4_failed_to_loading",
        version="1.0.0",
        status=TerminologyStatus.LOADING,
        loaded_at=SEEDED_TERMINOLOGY_LOADED_AT,
    )
    assert result == expected


async def test_transition_terminology_status_not_found(postgres_pool):
    """Transitioning a non-existent terminology idx raises
    TerminologyNotFound."""
    # 2**31 sits comfortably above any auto-generated idx in tests
    # while still fitting in BIGINT; no row will ever exist at this id.
    with pytest.raises(TerminologyNotFound):
        await transition_terminology_status(postgres_pool, 2**31, TerminologyStatus.ACTIVE)


async def test_transition_terminology_status_illegal(postgres_pool, created_terminologies):
    """ACTIVE → FAILED is rejected as an illegal transition; only LOADING can
    reach FAILED."""
    idx = await seed_terminology(
        postgres_pool, name="t5_active_to_failed_illegal", status=TerminologyStatus.ACTIVE
    )
    created_terminologies.append(idx)

    with pytest.raises(IllegalStatusTransition) as exc_info:
        await transition_terminology_status(postgres_pool, idx, TerminologyStatus.FAILED)
    assert exc_info.value.current == TerminologyStatus.ACTIVE.value
    assert exc_info.value.target == TerminologyStatus.FAILED


# =============================================================================
# load_manifest
# =============================================================================


def _write_manifest_json(source_dir, payload: dict) -> None:
    (source_dir / "manifest.json").write_text(json.dumps(payload))


def test_load_manifest(tmp_path):
    """A well-formed manifest.json parses into a TerminologyManifest."""
    payload = {
        "name": "uberon",
        "version": "2026-04-15",
        "source": {
            "path": "source.owl",
            "sha256": "a" * 64,
        },
    }
    _write_manifest_json(tmp_path, payload)

    result = load_manifest(tmp_path)

    expected = TerminologyManifest(
        name="uberon",
        version="2026-04-15",
        source=TerminologyManifestSource(path="source.owl", sha256="a" * 64),
    )
    assert result == expected


def test_load_manifest_missing_file(tmp_path):
    """An absent manifest.json in the staging dir raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        load_manifest(tmp_path)


# =============================================================================
# verify_manifest_checksums
# =============================================================================


def test_verify_manifest_checksums(tmp_path):
    """The verifier accepts a manifest whose declared sha256 matches the
    on-disk source file."""
    # Stable test content; sha256 computed eagerly so the manifest
    # carries the digest the verifier will recompute.
    content = b"@SQ\tSN:chr1\tLN:1000\n"
    (tmp_path / "source.owl").write_bytes(content)

    manifest = TerminologyManifest(
        name="uberon",
        version="2026-04-15",
        source=TerminologyManifestSource(
            path="source.owl", sha256=hashlib.sha256(content).hexdigest()
        ),
    )

    # No exception is the success criterion.
    verify_manifest_checksums(tmp_path, manifest)


def test_verify_manifest_checksums_mismatch(tmp_path):
    """A declared sha256 that does not match the on-disk source file raises
    ValueError."""
    (tmp_path / "source.owl").write_bytes(b"actual content")

    manifest = TerminologyManifest(
        name="uberon",
        version="2026-04-15",
        source=TerminologyManifestSource(path="source.owl", sha256="b" * 64),
    )

    with pytest.raises(ValueError):
        verify_manifest_checksums(tmp_path, manifest)


def test_verify_manifest_checksums_missing_source_file(tmp_path):
    """A manifest pointing at a non-existent source file raises
    FileNotFoundError."""
    manifest = TerminologyManifest(
        name="uberon",
        version="2026-04-15",
        source=TerminologyManifestSource(path="source.owl", sha256="c" * 64),
    )

    with pytest.raises(FileNotFoundError):
        verify_manifest_checksums(tmp_path, manifest)


# =============================================================================
# import_terminology — staging-dir-driven import workflow
# =============================================================================


def _write_staging(
    staging_dir,
    *,
    name: str,
    version: str,
    terms: list[tuple[str, str, bool, str | None, TerminologyTermObsoletionKind | None]],
    closure: list[tuple[str, str, int]],
) -> None:
    """Write source.owl + manifest.json + terms.tsv + closure.tsv into
    `staging_dir`. terms rows are
    (term_id, label, is_obsolete, replaced_by_term_id, obsoletion_kind);
    closure rows are (ancestor_term_id, descendant_term_id, distance)."""
    staging_dir.mkdir(parents=True, exist_ok=True)

    # source.owl content varies per (name, version) so each staging dir
    # gets a distinct sha256 and the manifest's checksum check is real.
    source_bytes = f"dummy owl content for {name} {version}".encode()
    (staging_dir / "source.owl").write_bytes(source_bytes)
    manifest = {
        "name": name,
        "version": version,
        "source": {
            "path": "source.owl",
            "sha256": hashlib.sha256(source_bytes).hexdigest(),
        },
    }
    (staging_dir / "manifest.json").write_text(json.dumps(manifest))

    with (staging_dir / "terms.tsv").open("w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(
            ["term_id", "label", "is_obsolete", "replaced_by_term_id", "obsoletion_kind"]
        )
        for term_id, label, is_obsolete, replaced_by_term_id, obsoletion_kind in terms:
            writer.writerow(
                [
                    term_id,
                    label,
                    "true" if is_obsolete else "false",
                    replaced_by_term_id or "",
                    str(obsoletion_kind) if obsoletion_kind is not None else "",
                ]
            )

    with (staging_dir / "closure.tsv").open("w", newline="") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(["ancestor_term_id", "descendant_term_id", "distance"])
        for ancestor, descendant, distance in closure:
            writer.writerow([ancestor, descendant, str(distance)])


async def _read_term_state(pool, terminology_idx: int) -> dict:
    """Returns {term_id: {label, is_obsolete, obsoletion_kind,
    obsoleted_in_version, replaced_by_term_id, notes, idx}} for every
    term in the terminology."""
    rows = await pool.fetch(
        "SELECT t.idx, t.term_id, t.label, t.is_obsolete, t.obsoletion_kind,"
        "       t.obsoleted_in_version, t.notes,"
        "       r.term_id AS replaced_by_term_id"
        "  FROM qiita.terminology_term t"
        "  LEFT JOIN qiita.terminology_term r ON t.replaced_by = r.idx"
        " WHERE t.terminology_idx = $1",
        terminology_idx,
    )
    return {row["term_id"]: dict(row) for row in rows}


def _expected_term_state(
    idx_source: dict,
    term_id: str,
    *,
    label: str,
    is_obsolete: bool = False,
    obsoletion_kind: TerminologyTermObsoletionKind | None = None,
    obsoleted_in_version: str | None = None,
    replaced_by_term_id: str | None = None,
    notes: str | None = None,
) -> dict:
    """One expected _read_term_state entry, for whole-dict comparison.

    `idx` is auto-generated, so it is copied from `idx_source` — the actual
    state whose idxs the comparison should match. Passing an earlier load's
    state there is what folds idx preservation into the equality.
    """
    return {
        "idx": idx_source[term_id]["idx"],
        "term_id": term_id,
        "label": label,
        "is_obsolete": is_obsolete,
        "obsoletion_kind": obsoletion_kind.value if obsoletion_kind is not None else None,
        "obsoleted_in_version": obsoleted_in_version,
        "notes": notes,
        "replaced_by_term_id": replaced_by_term_id,
    }


def _expected_terminology_row(
    terminology_idx: int,
    *,
    name: str,
    version: str,
    status: TerminologyStatus,
    loaded_at: datetime,
) -> dict:
    """The expected fetch_terminology projection for one terminology row.

    `loaded_at` is a parameter because a load stamps it server-side; pass the
    actual value, or an earlier read's value to assert it did not move.
    """
    return {
        "terminology_idx": terminology_idx,
        "name": name,
        "version": version,
        "loaded_at": loaded_at,
        "status": status.value,
    }


async def test_import_terminology(postgres_pool, created_terminologies, tmp_path):
    """A brand-new staging dir loads cleanly: every term is inserted, every
    closure row is written, and the terminology row ends in ACTIVE."""
    _write_staging(
        tmp_path,
        name="ldt_brand_new",
        version="1.0.0",
        terms=[
            ("UBERON:0001", "mouth", False, None, None),
            ("UBERON:0002", "tooth", False, None, None),
            ("UBERON:0003", "molar", False, None, None),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
            ("UBERON:0003", "UBERON:0003", 0),
            ("UBERON:0001", "UBERON:0002", 1),
            ("UBERON:0001", "UBERON:0003", 2),
            ("UBERON:0002", "UBERON:0003", 1),
        ],
    )

    result = await import_terminology(postgres_pool, tmp_path)
    created_terminologies.append(result.terminology_idx)

    expected = TerminologyImportResult(
        terminology_idx=result.terminology_idx,
        terms_inserted=3,
        terms_label_updated=0,
        terms_newly_obsoleted=0,
        terms_newly_merged=0,
        terms_silently_dropped=0,
        closure_rows=6,
    )
    assert result == expected

    terminology_row = await fetch_terminology(postgres_pool, result.terminology_idx)
    expected_row = _expected_terminology_row(
        result.terminology_idx,
        name="ldt_brand_new",
        version="1.0.0",
        status=TerminologyStatus.ACTIVE,
        loaded_at=terminology_row["loaded_at"],
    )
    assert dict(terminology_row) == expected_row


async def test_import_terminology_reload_preserves_idx(
    postgres_pool, created_terminologies, tmp_path
):
    """Reloading a terminology at a new version preserves terminology_idx
    and per-term idxs; a relabeled term is counted as terms_label_updated."""
    # First load establishes the terminology and term idxs.
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_reload",
        version="1.0.0",
        terms=[
            ("UBERON:0001", "mouth", False, None, None),
            ("UBERON:0002", "tooth", False, None, None),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0), ("UBERON:0002", "UBERON:0002", 0)],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir)
    created_terminologies.append(v1_result.terminology_idx)
    v1_state = await _read_term_state(postgres_pool, v1_result.terminology_idx)

    # Second load relabels one term; terminology_idx and per-term idxs must persist.
    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_reload",
        version="2.0.0",
        terms=[
            ("UBERON:0001", "oral opening", False, None, None),
            ("UBERON:0002", "tooth", False, None, None),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0), ("UBERON:0002", "UBERON:0002", 0)],
    )
    v2_result = await import_terminology(postgres_pool, v2_dir)

    assert v2_result.terminology_idx == v1_result.terminology_idx
    expected = TerminologyImportResult(
        terminology_idx=v1_result.terminology_idx,
        terms_inserted=0,
        terms_label_updated=1,
        terms_newly_obsoleted=0,
        terms_newly_merged=0,
        terms_silently_dropped=0,
        closure_rows=2,
    )
    assert v2_result == expected

    # idxs are sourced from v1_state, so this equality also proves they persisted.
    v2_state = await _read_term_state(postgres_pool, v2_result.terminology_idx)
    expected_v2_state = {
        "UBERON:0001": _expected_term_state(v1_state, "UBERON:0001", label="oral opening"),
        "UBERON:0002": _expected_term_state(v1_state, "UBERON:0002", label="tooth"),
    }
    assert v2_state == expected_v2_state

    terminology_row = await fetch_terminology(postgres_pool, v2_result.terminology_idx)
    expected_row = _expected_terminology_row(
        v2_result.terminology_idx,
        name="ldt_reload",
        version="2.0.0",
        status=TerminologyStatus.ACTIVE,
        loaded_at=terminology_row["loaded_at"],
    )
    assert dict(terminology_row) == expected_row


async def test_import_terminology_obsoleted_in_version_set_once(
    postgres_pool, created_terminologies, tmp_path
):
    """Across three sequential loads — term active in v1, obsoleted in v2,
    still obsolete in v3 — obsoleted_in_version is stamped at v2 and never
    advances on subsequent reloads."""
    for version, terms in [
        ("1.0.0", [("UBERON:0001", "mouth", False, None, None)]),
        (
            "2.0.0",
            [
                (
                    "UBERON:0001",
                    "mouth",
                    True,
                    None,
                    TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
                )
            ],
        ),
        (
            "3.0.0",
            [
                (
                    "UBERON:0001",
                    "mouth",
                    True,
                    None,
                    TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
                )
            ],
        ),
    ]:
        version_dir = tmp_path / version
        _write_staging(
            version_dir,
            name="ldt_set_once",
            version=version,
            terms=terms,
            closure=[("UBERON:0001", "UBERON:0001", 0)],
        )
        result = await import_terminology(postgres_pool, version_dir)
        created_terminologies.append(result.terminology_idx)

    state = await _read_term_state(postgres_pool, result.terminology_idx)
    expected_state = {
        "UBERON:0001": _expected_term_state(
            state,
            "UBERON:0001",
            label="mouth",
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
            obsoleted_in_version="2.0.0",
        )
    }
    assert state == expected_state


async def test_import_terminology_un_obsoletion_clears_columns(
    postgres_pool, created_terminologies, tmp_path
):
    """Reloading a term that was obsolete in v1 as non-obsolete in v2 clears
    is_obsolete, obsoletion_kind, obsoleted_in_version, and replaced_by."""
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_un_obsolete",
        version="1.0.0",
        terms=[
            ("UBERON:0001", "mouth", True, None, TerminologyTermObsoletionKind.SOURCE_DEPRECATED),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir)
    created_terminologies.append(v1_result.terminology_idx)

    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_un_obsolete",
        version="2.0.0",
        terms=[("UBERON:0001", "mouth", False, None, None)],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
    )
    await import_terminology(postgres_pool, v2_dir)

    state = await _read_term_state(postgres_pool, v1_result.terminology_idx)
    expected_state = {"UBERON:0001": _expected_term_state(state, "UBERON:0001", label="mouth")}
    assert state == expected_state


async def test_import_terminology_re_obsoletion_stamps_new_version(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a term is obsoleted, un-obsoleted, then obsoleted
    again, and the second obsoletion stamps obsoleted_in_version with the
    later release rather than restoring the earlier one.

    The set-once rule holds within one obsoletion episode, not across the
    row's whole lifetime: un-obsoletion NULLs the column, so the COALESCE in
    the UPSERT finds nothing to preserve on the next obsoletion and takes the
    version being loaded. The v2 state is asserted before v3 runs, so a v2
    that failed to stamp cannot let the v4 expectation pass vacuously.
    """
    obsolete_term = (
        "UBERON:0001",
        "mouth",
        True,
        None,
        TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
    )
    active_term = ("UBERON:0001", "mouth", False, None, None)

    async def _load(version: str, term) -> int:
        version_dir = tmp_path / version
        _write_staging(
            version_dir,
            name="ldt_re_obsolete",
            version=version,
            terms=[term],
            closure=[("UBERON:0001", "UBERON:0001", 0)],
        )
        result = await import_terminology(postgres_pool, version_dir)
        return result.terminology_idx

    # v1 active, then v2 obsoletes it — stamping this episode's first version.
    terminology_idx = await _load("1.0.0", active_term)
    created_terminologies.append(terminology_idx)
    await _load("2.0.0", obsolete_term)

    after_v2 = await _read_term_state(postgres_pool, terminology_idx)
    expected_after_v2 = {
        "UBERON:0001": _expected_term_state(
            after_v2,
            "UBERON:0001",
            label="mouth",
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
            obsoleted_in_version="2.0.0",
        )
    }
    assert after_v2 == expected_after_v2

    # v3 un-obsoletes (clearing the stamp), v4 obsoletes again.
    await _load("3.0.0", active_term)
    await _load("4.0.0", obsolete_term)

    after_v4 = await _read_term_state(postgres_pool, terminology_idx)
    expected_after_v4 = {
        "UBERON:0001": _expected_term_state(
            after_v4,
            "UBERON:0001",
            label="mouth",
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_DEPRECATED,
            obsoleted_in_version="4.0.0",
        )
    }
    assert after_v4 == expected_after_v4


async def test_import_terminology_kind_and_replaced_by_change(
    postgres_pool, created_terminologies, tmp_path
):
    """A term obsoleted in v1 as 'deprecated' is reloaded in v2 as 'merged'
    into a new survivor. obsoletion_kind flips, replaced_by points at the
    survivor, obsoleted_in_version remains v1, and terms_newly_merged
    counts the kind flip even though the row was already obsolete."""
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_kind_change",
        version="1.0.0",
        terms=[
            ("UBERON:0001", "old", True, None, TerminologyTermObsoletionKind.SOURCE_DEPRECATED),
            ("UBERON:0002", "sibling", False, None, None),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
        ],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir)
    created_terminologies.append(v1_result.terminology_idx)

    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_kind_change",
        version="2.0.0",
        terms=[
            (
                "UBERON:0001",
                "old",
                True,
                "UBERON:0003",
                TerminologyTermObsoletionKind.SOURCE_MERGED,
            ),
            ("UBERON:0002", "sibling", False, None, None),
            ("UBERON:0003", "survivor", False, None, None),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
            ("UBERON:0003", "UBERON:0003", 0),
        ],
    )
    v2_result = await import_terminology(postgres_pool, v2_dir)

    # terms_newly_merged counts the kind flip on UBERON:0001 even though
    # the row was already obsolete from v1, so terms_newly_obsoleted stays 0.
    expected = TerminologyImportResult(
        terminology_idx=v1_result.terminology_idx,
        terms_inserted=1,
        terms_label_updated=0,
        terms_newly_obsoleted=0,
        terms_newly_merged=1,
        terms_silently_dropped=0,
        closure_rows=3,
    )
    assert v2_result == expected

    state = await _read_term_state(postgres_pool, v1_result.terminology_idx)
    expected_state = {
        "UBERON:0001": _expected_term_state(
            state,
            "UBERON:0001",
            label="old",
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
            obsoleted_in_version="1.0.0",
            replaced_by_term_id="UBERON:0003",
        ),
        "UBERON:0002": _expected_term_state(state, "UBERON:0002", label="sibling"),
        "UBERON:0003": _expected_term_state(state, "UBERON:0003", label="survivor"),
    }
    assert state == expected_state


async def test_import_terminology_cross_terminology_closure_untouched(
    postgres_pool, created_terminologies, tmp_path
):
    """Loading a second, unrelated terminology leaves the closure rows of a
    previously loaded sentinel terminology untouched."""
    sentinel_dir = tmp_path / "sentinel"
    _write_staging(
        sentinel_dir,
        name="ldt_sentinel",
        version="1.0.0",
        terms=[
            ("SENT:0001", "alpha", False, None, None),
            ("SENT:0002", "beta", False, None, None),
        ],
        closure=[
            ("SENT:0001", "SENT:0001", 0),
            ("SENT:0002", "SENT:0002", 0),
            ("SENT:0001", "SENT:0002", 1),
        ],
    )
    sentinel_result = await import_terminology(postgres_pool, sentinel_dir)
    created_terminologies.append(sentinel_result.terminology_idx)

    other_dir = tmp_path / "other"
    _write_staging(
        other_dir,
        name="ldt_other",
        version="1.0.0",
        terms=[("OTHER:0001", "gamma", False, None, None)],
        closure=[("OTHER:0001", "OTHER:0001", 0)],
    )
    other_result = await import_terminology(postgres_pool, other_dir)
    created_terminologies.append(other_result.terminology_idx)

    sentinel_closure_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.terminology_closure WHERE terminology_idx = $1",
        sentinel_result.terminology_idx,
    )
    assert sentinel_closure_count == 3


async def test_import_terminology_silent_drops_raise_and_preserve_state(
    postgres_pool, created_terminologies, tmp_path
):
    """A v2 staging dir that omits a term present in v1 without an explicit
    deprecation marker raises TerminologyImportAnomaly listing the
    silently-dropped term, and the v1 row + term set survive intact (the
    v2 transaction rolls back)."""
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_silent_drops",
        version="1.0.0",
        terms=[
            ("UBERON:0001", "mouth", False, None, None),
            ("UBERON:0002", "tooth", False, None, None),
            ("UBERON:0003", "molar", False, None, None),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
            ("UBERON:0003", "UBERON:0003", 0),
        ],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir)
    created_terminologies.append(v1_result.terminology_idx)
    pre_load_row = await fetch_terminology(postgres_pool, v1_result.terminology_idx)

    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_silent_drops",
        version="2.0.0",
        terms=[
            ("UBERON:0001", "mouth", False, None, None),
            ("UBERON:0002", "tooth", False, None, None),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
        ],
    )

    with pytest.raises(TerminologyImportAnomaly) as exc_info:
        await import_terminology(postgres_pool, v2_dir)
    assert exc_info.value.silently_dropped_term_ids == ["UBERON:0003"]

    # Transaction must have rolled back: status, version, loaded_at
    # unchanged from v1, and UBERON:0003 still present and active.
    post_attempt_row = await fetch_terminology(postgres_pool, v1_result.terminology_idx)
    expected_row = _expected_terminology_row(
        v1_result.terminology_idx,
        name="ldt_silent_drops",
        version="1.0.0",
        status=TerminologyStatus.ACTIVE,
        loaded_at=pre_load_row["loaded_at"],
    )
    assert dict(post_attempt_row) == expected_row

    state = await _read_term_state(postgres_pool, v1_result.terminology_idx)
    expected_state = {
        "UBERON:0001": _expected_term_state(state, "UBERON:0001", label="mouth"),
        "UBERON:0002": _expected_term_state(state, "UBERON:0002", label="tooth"),
        "UBERON:0003": _expected_term_state(state, "UBERON:0003", label="molar"),
    }
    assert state == expected_state


async def test_import_terminology_unresolved_replaced_by_raises(
    postgres_pool, created_terminologies, tmp_path
):
    """A term that names a replaced_by term not declared anywhere in the
    TSV raises TerminologyImportAnomaly carrying the exact
    (obsolete_term_id, attempted_replaced_by) pair, without writing
    anything to the database."""
    staging_dir = tmp_path / "stage"
    _write_staging(
        staging_dir,
        name="ldt_unresolved",
        version="1.0.0",
        terms=[
            (
                "UBERON:0001",
                "orphan",
                True,
                "UBERON:9999",
                TerminologyTermObsoletionKind.SOURCE_MERGED,
            ),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
    )

    with pytest.raises(TerminologyImportAnomaly) as exc_info:
        await import_terminology(postgres_pool, staging_dir)
    assert exc_info.value.unresolved_replaced_by == [("UBERON:0001", "UBERON:9999")]

    # Nothing was inserted — name lookup returns no row.
    row = await postgres_pool.fetchrow(
        "SELECT idx FROM qiita.terminology WHERE name = $1", "ldt_unresolved"
    )
    assert row is None


async def test_import_terminology_tolerate_silent_drops(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a previously existing term is left out of a
    new release with no explicit deprecation marker, and tolerate mode
    auto-obsoletes it instead of refusing the load.

    The dropped row carries its prior label forward, picks up
    obsoletion_kind=silently_dropped, and is stamped with the new
    release as its obsoleted_in_version. The result's
    terms_silently_dropped counter surfaces the count for the caller to
    log; terms_newly_obsoleted also reflects the event because a silent
    drop is a kind of obsoletion."""
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_tolerate_silent",
        version="1.0.0",
        terms=[
            ("UBERON:0001", "mouth", False, None, None),
            ("UBERON:0002", "tooth", False, None, None),
            ("UBERON:0003", "molar", False, None, None),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
            ("UBERON:0003", "UBERON:0003", 0),
        ],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir)
    created_terminologies.append(v1_result.terminology_idx)

    # v2 omits UBERON:0003; tolerate mode auto-obsoletes instead of raising.
    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_tolerate_silent",
        version="2.0.0",
        terms=[
            ("UBERON:0001", "mouth", False, None, None),
            ("UBERON:0002", "tooth", False, None, None),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
        ],
    )
    v2_result = await import_terminology(postgres_pool, v2_dir, tolerate_anomalies=True)

    expected = TerminologyImportResult(
        terminology_idx=v1_result.terminology_idx,
        terms_inserted=0,
        terms_label_updated=0,
        terms_newly_obsoleted=1,
        terms_newly_merged=0,
        terms_silently_dropped=1,
        closure_rows=2,
    )
    assert v2_result == expected

    state = await _read_term_state(postgres_pool, v1_result.terminology_idx)
    expected_state = {
        "UBERON:0001": _expected_term_state(state, "UBERON:0001", label="mouth"),
        "UBERON:0002": _expected_term_state(state, "UBERON:0002", label="tooth"),
        "UBERON:0003": _expected_term_state(
            state,
            "UBERON:0003",
            label="molar",
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SILENTLY_DROPPED,
            obsoleted_in_version="2.0.0",
        ),
    }
    assert state == expected_state


async def test_import_terminology_tolerate_unresolved_replaced_by(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where an obsolete term names a replacement CURIE
    that isn't present anywhere else in the same batch, and tolerate
    mode records the attempted CURIE in the notes column rather than
    refusing the load.

    Fail mode treats this as a structural anomaly because there is no
    in-batch idx the loader can use to populate replaced_by. Tolerate
    mode lands the row with replaced_by=NULL on the DB side so the
    structural CHECKs hold, and writes a free-text audit line into
    notes recording the exact CURIE that was attempted so an operator
    later inspecting the row can recover the source's stated intent
    without re-parsing the staging dir."""
    staging_dir = tmp_path / "stage"
    _write_staging(
        staging_dir,
        name="ldt_tolerate_unresolved",
        version="1.0.0",
        terms=[
            (
                "UBERON:0001",
                "orphan",
                True,
                "UBERON:9999",
                TerminologyTermObsoletionKind.SOURCE_MERGED,
            ),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
    )

    result = await import_terminology(postgres_pool, staging_dir, tolerate_anomalies=True)
    created_terminologies.append(result.terminology_idx)

    state = await _read_term_state(postgres_pool, result.terminology_idx)
    expected_state = {
        "UBERON:0001": _expected_term_state(
            state,
            "UBERON:0001",
            label="orphan",
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
            obsoleted_in_version="1.0.0",
            notes="v1.0.0: attempted replaced_by=UBERON:9999 unresolved",
        )
    }
    assert state == expected_state


async def test_import_terminology_tolerate_un_obsoletion_clears_silent_drop(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where a term silently dropped in one tolerate-mode
    release reappears as a normal active row in a later release, and
    every obsoletion column on the row clears.

    A silently-dropped row is not a special kind of zombie: the same
    un-obsoletion path the existing UPSERT uses for source_deprecated
    and source_merged terms must clear is_obsolete, obsoletion_kind,
    obsoleted_in_version, and replaced_by when the term comes back as
    active. v1 has two terms; v2 tolerate-mode drops one (so it lands
    at is_obsolete=true, obsoletion_kind=silently_dropped,
    obsoleted_in_version=v2); v3 re-includes the same term as a regular
    active row."""
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_tolerate_un_obsolete",
        version="1.0.0",
        terms=[
            ("UBERON:0001", "mouth", False, None, None),
            ("UBERON:0002", "tooth", False, None, None),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
        ],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir)
    created_terminologies.append(v1_result.terminology_idx)

    # v2 tolerate: silently drop UBERON:0002.
    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_tolerate_un_obsolete",
        version="2.0.0",
        terms=[("UBERON:0001", "mouth", False, None, None)],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
    )
    await import_terminology(postgres_pool, v2_dir, tolerate_anomalies=True)

    # v3: UBERON:0002 reappears as a normal active term.
    v3_dir = tmp_path / "v3"
    _write_staging(
        v3_dir,
        name="ldt_tolerate_un_obsolete",
        version="3.0.0",
        terms=[
            ("UBERON:0001", "mouth", False, None, None),
            ("UBERON:0002", "tooth", False, None, None),
        ],
        closure=[
            ("UBERON:0001", "UBERON:0001", 0),
            ("UBERON:0002", "UBERON:0002", 0),
        ],
    )
    await import_terminology(postgres_pool, v3_dir)

    state = await _read_term_state(postgres_pool, v1_result.terminology_idx)
    expected_state = {
        "UBERON:0001": _expected_term_state(state, "UBERON:0001", label="mouth"),
        "UBERON:0002": _expected_term_state(state, "UBERON:0002", label="tooth"),
    }
    assert state == expected_state


async def test_import_terminology_tolerate_notes_accumulate(
    postgres_pool, created_terminologies, tmp_path
):
    """Tests the case where two sequential tolerate-mode loads each
    produce an unresolved-replaced_by audit line for the same row, and
    both lines remain present in notes (the first preserved, the second
    appended on a new line).

    The notes column is shared between the loader's audit lines and
    operator-added content, so the loader is not allowed to wipe it on
    reload. The trade-off is that audit lines from prior loads stay
    even after the row's structural state has moved on; in practice
    tolerate-mode runs should be rare enough that the column does not
    bloat. v1 carries an obsolete row pointing at an absent UBERON:9999;
    v2 reloads the same row pointing at a different absent UBERON:8888."""
    v1_dir = tmp_path / "v1"
    _write_staging(
        v1_dir,
        name="ldt_tolerate_notes",
        version="1.0.0",
        terms=[
            (
                "UBERON:0001",
                "orphan",
                True,
                "UBERON:9999",
                TerminologyTermObsoletionKind.SOURCE_MERGED,
            ),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
    )
    v1_result = await import_terminology(postgres_pool, v1_dir, tolerate_anomalies=True)
    created_terminologies.append(v1_result.terminology_idx)

    v2_dir = tmp_path / "v2"
    _write_staging(
        v2_dir,
        name="ldt_tolerate_notes",
        version="2.0.0",
        terms=[
            (
                "UBERON:0001",
                "orphan",
                True,
                "UBERON:8888",
                TerminologyTermObsoletionKind.SOURCE_MERGED,
            ),
        ],
        closure=[("UBERON:0001", "UBERON:0001", 0)],
    )
    await import_terminology(postgres_pool, v2_dir, tolerate_anomalies=True)

    state = await _read_term_state(postgres_pool, v1_result.terminology_idx)
    expected_notes = (
        "v1.0.0: attempted replaced_by=UBERON:9999 unresolved\n"
        "v2.0.0: attempted replaced_by=UBERON:8888 unresolved"
    )
    expected_state = {
        "UBERON:0001": _expected_term_state(
            state,
            "UBERON:0001",
            label="orphan",
            is_obsolete=True,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
            obsoleted_in_version="1.0.0",
            notes=expected_notes,
        )
    }
    assert state == expected_state


async def test_transition_terminology_status_with_connection(postgres_pool, created_terminologies):
    """transition_terminology_status accepts a Connection in addition to a
    Pool, so it can be invoked inside an outer transaction."""
    idx = await seed_terminology(
        postgres_pool, name="t6_widened_signature", status=TerminologyStatus.LOADING
    )
    created_terminologies.append(idx)

    async with postgres_pool.acquire() as conn, conn.transaction():
        result = await transition_terminology_status(conn, idx, TerminologyStatus.ACTIVE)

    expected = TerminologyResponse(
        terminology_idx=idx,
        name="t6_widened_signature",
        version="1.0.0",
        status=TerminologyStatus.ACTIVE,
        loaded_at=SEEDED_TERMINOLOGY_LOADED_AT,
    )
    assert result == expected
