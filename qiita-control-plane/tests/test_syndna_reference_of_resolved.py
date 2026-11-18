"""The SynDNA count export's check that the masks the prep_samples resolved to share one
SynDNA reference, whichever member of the group was named."""

import pytest
from fastapi import HTTPException

from qiita_control_plane.routes.read_masked import syndna_reference_of_resolved


def test_one_reference_across_the_resolved_masks_is_returned():
    assert syndna_reference_of_resolved({5: 7, 12: 7, 20: 9}, {1: 5, 2: 12}) == 7


@pytest.mark.parametrize(
    "references", [{5: 7, 12: 8}, {5: 7, 12: None}], ids=["two-references", "one-without"]
)
def test_resolved_masks_with_different_references_are_refused(references):
    with pytest.raises(HTTPException) as ei:
        syndna_reference_of_resolved(references, {1: 5, 2: 12})
    assert ei.value.status_code == 409
    assert "different references" in ei.value.detail


def test_resolved_masks_without_syndna_are_refused():
    with pytest.raises(HTTPException) as ei:
        syndna_reference_of_resolved({5: None, 12: 7}, {1: 5})
    assert ei.value.status_code == 409
    assert "masks [5]" in ei.value.detail and "without SynDNA" in ei.value.detail
