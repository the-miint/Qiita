"""Tests for the sample-family field row types' global-link invariant and the
scope each derives from it.

Carries no `db` marker, unlike every other module in this package: these
assertions need no database, and the invariant they cover is enforced at
construction, so it must be checked by the tier that runs on every commit.
"""

import pytest
from qiita_common.models import FieldDataType, FieldWriteOutcome

from qiita_control_plane.repositories._sample_helpers import (
    FieldRow,
    ResolvedField,
    SampleMetadataFieldResult,
    _assert_global_link_consistent,
)

# One populated marker without the other, in both directions.
_ONE_SIDED_LINKS = [(7, None), (None, "depth_m")]


def _field_row(*, global_field_idx, internal_name):
    """A FieldRow whose non-linkage columns are fixed, varying only the two
    global-linkage markers under test.
    """
    return FieldRow(
        idx=1,
        display_name="depth",
        data_type=FieldDataType.NUMERIC,
        terminology_idx=None,
        global_field_idx=global_field_idx,
        internal_name=internal_name,
    )


def _resolved_field(*, global_field_idx, internal_name):
    """A ResolvedField whose non-linkage columns are fixed, varying only the two
    global-linkage markers under test.
    """
    return ResolvedField(
        caller_key="depth",
        global_field_idx=global_field_idx,
        study_field_idx=None,
        canonical_display="depth",
        data_type=FieldDataType.NUMERIC,
        terminology_idx=None,
        internal_name=internal_name,
    )


@pytest.mark.parametrize(
    "global_field_idx, internal_name",
    [(7, "depth_m"), (None, None)],
    ids=["globally-linked", "purely-local"],
)
def test__assert_global_link_consistent_accepts_agreeing_markers(global_field_idx, internal_name):
    """Tests the case where both global-linkage markers agree on nullness: a
    globally-linked field carries both and a purely-local field carries neither,
    so each passes.
    """
    assert _assert_global_link_consistent(global_field_idx, internal_name) is None


@pytest.mark.parametrize("global_field_idx, internal_name", _ONE_SIDED_LINKS)
def test__assert_global_link_consistent_rejects_one_sided_markers(global_field_idx, internal_name):
    """Tests the case where one global-linkage marker is populated without the
    other: the pair is rejected, and the message names both values so the
    mismatched source is identifiable.
    """
    with pytest.raises(ValueError) as exc_info:
        _assert_global_link_consistent(global_field_idx, internal_name)

    message = str(exc_info.value)
    assert repr(global_field_idx) in message
    assert repr(internal_name) in message


@pytest.mark.parametrize("global_field_idx, internal_name", _ONE_SIDED_LINKS)
def test_field_row_rejects_one_sided_global_link(global_field_idx, internal_name):
    """Tests the case where a FieldRow is built with only one of its two
    global-linkage markers populated: construction fails rather than yielding a
    row whose scope would be arbitrary.
    """
    with pytest.raises(ValueError):
        _field_row(global_field_idx=global_field_idx, internal_name=internal_name)


@pytest.mark.parametrize("global_field_idx, internal_name", _ONE_SIDED_LINKS)
def test_resolved_field_rejects_one_sided_global_link(global_field_idx, internal_name):
    """Tests the case where a ResolvedField is built with only one of its two
    global-linkage markers populated: construction fails, so no write can be
    dispatched from a row that disagrees with itself.
    """
    with pytest.raises(ValueError):
        _resolved_field(global_field_idx=global_field_idx, internal_name=internal_name)


@pytest.mark.parametrize(
    "global_field_idx, internal_name, expected_scope",
    [(7, "depth_m", "global"), (None, None, "local")],
    ids=["globally-linked", "purely-local"],
)
def test_resolved_field_scope_derives_from_global_field_idx(
    global_field_idx, internal_name, expected_scope
):
    """Tests the case where a ResolvedField's scope is read: it follows the
    global_field_idx the global write path writes through, rather than being
    stored alongside it.
    """
    resolved = _resolved_field(global_field_idx=global_field_idx, internal_name=internal_name)

    assert resolved.scope == expected_scope


@pytest.mark.parametrize(
    "internal_name, expected_scope",
    [("depth_m", "global"), (None, "local")],
    ids=["globally-linked", "purely-local"],
)
def test_sample_metadata_field_result_scope_derives_from_internal_name(
    internal_name, expected_scope
):
    """Tests the case where a write result's scope is read: it follows
    internal_name, the only global-linkage marker this type carries, so it agrees
    with the ResolvedField the write was dispatched from.
    """
    result = SampleMetadataFieldResult(
        field_key="depth",
        outcome=FieldWriteOutcome.INSERTED,
        value="5.0",
        internal_name=internal_name,
    )

    assert result.scope == expected_scope
