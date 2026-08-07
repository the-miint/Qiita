"""Unit tests for the shared sample-label composer.

The label is the public identifier a published feature table carries in place of
`prep_sample_idx`. Two forms exist by necessity — `sequenced_sample.ena_run_accession`
is NULL until submission succeeds and `sequenced_pool_idx` is nullable — so the
tests below pin which form is chosen for which input, and pin that the pooled form
is byte-identical to the masked-read export's filename stem.
"""

import pytest

from qiita_common.sample_label import compose_sample_label, pooled_sample_label


def test_pooled_label_is_the_dotted_composite():
    assert (
        pooled_sample_label(
            biosample_accession="SAMN00000001",
            sequencing_run_idx=4,
            sequenced_pool_idx=7,
            prep_sample_idx=42,
        )
        == "SAMN00000001.4.7.42"
    )


def test_compose_prefers_the_ena_run_accession():
    """A submitted sample labels as its run accession — public, and unique per
    sequenced sample. The pool/run identifiers are ours and mean nothing to a
    reader of the published table."""
    assert (
        compose_sample_label(
            prep_sample_idx=42,
            biosample_accession="SAMN00000001",
            ena_run_accession="ERR1234567",
            sequencing_run_idx=4,
            sequenced_pool_idx=7,
        )
        == "ERR1234567"
    )


def test_compose_falls_back_to_the_pooled_composite():
    """No run accession (unsubmitted) but pooled: the composite, which is unique
    because prep_sample_idx is."""
    assert (
        compose_sample_label(
            prep_sample_idx=42,
            biosample_accession="SAMN00000001",
            ena_run_accession=None,
            sequencing_run_idx=4,
            sequenced_pool_idx=7,
        )
        == "SAMN00000001.4.7.42"
    )


def test_compose_falls_back_to_the_short_form_when_unpooled():
    """`sequenced_sample.sequenced_pool_idx` is nullable, so the four-part
    composite is not always constructible. The short form keeps the same
    leading accession and the same trailing prep_sample_idx, so it is still
    unique and still recognisably the same scheme."""
    assert (
        compose_sample_label(
            prep_sample_idx=42,
            biosample_accession="SAMN00000001",
            ena_run_accession=None,
            sequencing_run_idx=None,
            sequenced_pool_idx=None,
        )
        == "SAMN00000001.42"
    )


def test_compose_refuses_a_missing_accession():
    """Fail loud rather than emit `None.4.7.42`. The route 422s this case before
    composing, so reaching here is a bug — which is exactly why it raises."""
    with pytest.raises(ValueError, match="biosample_accession"):
        compose_sample_label(
            prep_sample_idx=42,
            biosample_accession=None,
            ena_run_accession=None,
            sequencing_run_idx=4,
            sequenced_pool_idx=7,
        )


def test_compose_refuses_a_missing_accession_even_with_a_run_accession():
    """Deliberately strict: the response ships `biosample_accession` as a column
    of its own so no consumer has to parse the label, and a NULL there is a hole
    in that contract even when `label` itself would be fine. A sample with an ENA
    run accession but no biosample accession is a submission-order anomaly worth
    surfacing, not papering over."""
    with pytest.raises(ValueError, match="biosample_accession"):
        compose_sample_label(
            prep_sample_idx=42,
            biosample_accession=None,
            ena_run_accession="ERR1234567",
            sequencing_run_idx=4,
            sequenced_pool_idx=7,
        )


def test_compose_refuses_a_half_pooled_row():
    """`sequencing_run_idx` is reached THROUGH `sequenced_pool`, so one present
    without the other means the query that produced the row is wrong. Silently
    picking a form would hide that."""
    with pytest.raises(ValueError, match="sequencing_run_idx"):
        compose_sample_label(
            prep_sample_idx=42,
            biosample_accession="SAMN00000001",
            ena_run_accession=None,
            sequencing_run_idx=None,
            sequenced_pool_idx=7,
        )
