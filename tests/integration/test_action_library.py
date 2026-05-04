"""Integration tests for the action-library primitives via the LIBRARY
name lookup — the same dispatch path a workflow runner will use.

Library functions are exercised indirectly by the route-level integration
tests (test_feature_split.py, test_feature_minting.py); this file
exercises them directly through the LIBRARY dict so the named-primitive
contract is verified independently of the HTTP layer.
"""

import hashlib
import uuid

import pytest

_TEST_SALT = uuid.uuid4().hex


def _md5_uuid(seq: str) -> str:
    return uuid.UUID(hashlib.md5(f"{_TEST_SALT}{seq}".encode()).hexdigest())


@pytest.fixture
async def fresh_reference(postgres_pool, human_admin_session):
    """Create a reference owned by the session admin and yield its idx,
    transitioning it to status='minting' so write-membership accepts
    feature_idxs. Cleans up at the end."""
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'minting', $2)"
        " RETURNING reference_idx",
        f"library-test-{uuid.uuid4()}",
        human_admin_session["principal_idx"],
    )
    yield idx
    await postgres_pool.execute(
        "DELETE FROM qiita.reference_membership WHERE reference_idx = $1", idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", idx
    )


async def test_library_mint_features_dispatch(postgres_pool):
    """LIBRARY['mint-features'](pool, entries) writes qiita.feature rows
    and returns a (mapping, minted, reused) tuple covering every input."""
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_common.models import FeatureHashEntry
    from qiita_control_plane.actions import LIBRARY

    entries = [FeatureHashEntry(sequence_hash=_md5_uuid(f"LIB{i}")) for i in range(5)]
    mapping, minted, reused = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, entries
    )

    assert len(mapping) == 5
    assert minted == 5
    assert reused == 0
    assert set(mapping.keys()) == {e.sequence_hash for e in entries}

    # Idempotent re-dispatch: same hashes return reused=5.
    _, minted2, reused2 = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, entries
    )
    assert minted2 == 0
    assert reused2 == 5


async def test_library_write_membership_dispatch(postgres_pool, fresh_reference):
    """LIBRARY['write-membership'](pool, idx, feature_idxs) inserts
    qiita.reference_membership rows and returns (linked, already_linked)."""
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_common.models import FeatureHashEntry
    from qiita_control_plane.actions import LIBRARY

    entries = [FeatureHashEntry(sequence_hash=_md5_uuid(f"MEM{i}")) for i in range(3)]
    mapping, _, _ = await LIBRARY[LibraryPrimitive.MINT_FEATURES](
        postgres_pool, entries
    )
    feature_idxs = list(mapping.values())

    linked, already_linked = await LIBRARY[LibraryPrimitive.WRITE_MEMBERSHIP](
        postgres_pool, fresh_reference, feature_idxs
    )
    assert linked == 3
    assert already_linked == 0

    rows = await postgres_pool.fetch(
        "SELECT feature_idx FROM qiita.reference_membership WHERE reference_idx = $1",
        fresh_reference,
    )
    assert sorted(r["feature_idx"] for r in rows) == sorted(feature_idxs)

    # Re-dispatch reports already_linked=3.
    linked2, already_linked2 = await LIBRARY[LibraryPrimitive.WRITE_MEMBERSHIP](
        postgres_pool, fresh_reference, feature_idxs
    )
    assert linked2 == 0
    assert already_linked2 == 3


async def test_library_write_membership_raises_on_unknown_feature_idx(
    postgres_pool, fresh_reference
):
    """An unknown feature_idx surfaces as ValueError (the FK violation is
    caught and re-raised as a structured error). Routes catch this and
    map to HTTP 422; runners can map it to a workflow failure with a
    useful detail."""
    from qiita_common.api_paths import LibraryPrimitive
    from qiita_control_plane.actions import LIBRARY

    with pytest.raises(ValueError, match="feature_idx"):
        await LIBRARY[LibraryPrimitive.WRITE_MEMBERSHIP](
            postgres_pool, fresh_reference, [9999999999]
        )
