"""Unit tests for qiita_control_plane.terminology_owl — the OBO obsoletion
conventions and the term-id prefix scoping applied to an OWL release."""

import logging

from qiita_common.models import TerminologyTermObsoletionKind

from qiita_control_plane.terminology_owl import (
    _assemble_terms,
    _collect_merge_survivors,
    build_terms,
)
from qiita_control_plane.testing.terminology import exported_class, parsed_term

_MERGED = TerminologyTermObsoletionKind.SOURCE_MERGED
_DEPRECATED = TerminologyTermObsoletionKind.SOURCE_DEPRECATED


# =============================================================================
# build_terms
# =============================================================================


def test_build_terms():
    """Tests the case where no prefix is declared: every class becomes a term
    row and a deprecated class keeps its replacement pointer."""
    exported_classes = [
        exported_class("UBERON:0001", "mouth"),
        exported_class(
            "UBERON:0002",
            "obsolete tooth",
            source_deprecated=True,
            asserted_replacement_term_id="UBERON:0001",
        ),
    ]

    result = build_terms(exported_classes, term_id_prefix=None)

    expected = [
        parsed_term("UBERON:0001", "mouth"),
        parsed_term(
            "UBERON:0002",
            "obsolete tooth",
            is_obsolete=True,
            replaced_by_term_id="UBERON:0001",
            obsoletion_kind=_DEPRECATED,
        ),
    ]
    assert result == expected


def test_build_terms_unnamed_class():
    """Tests the case where a class carries no name at all: the term row says
    the source supplied none — what the import stores a never-named term
    against — rather than carrying an empty name of its own."""
    exported_classes = [
        exported_class("UBERON:0001", ""),
        exported_class("UBERON:0002", "mouth"),
    ]

    result = build_terms(exported_classes, term_id_prefix=None)

    expected = [
        parsed_term("UBERON:0001", None),
        parsed_term("UBERON:0002", "mouth"),
    ]
    assert result == expected


def test_build_terms_prefix_filter():
    """Tests the case where a prefix is declared: classes imported from other
    vocabularies stay out, and a class carrying a pointer past the prefix
    loses it."""
    exported_classes = [
        exported_class("UBERON:0001", "mouth"),
        exported_class("CL:0000000", "cell"),
        exported_class(
            "UBERON:0002",
            "obsolete tooth",
            source_deprecated=True,
            asserted_replacement_term_id="CL:0000000",
        ),
        exported_class("UBERON:0003", "molar", alternative_term_ids=("CL:0000001", "UBERON:0004")),
    ]

    result = build_terms(exported_classes, term_id_prefix="UBERON:")

    expected = [
        parsed_term("UBERON:0001", "mouth"),
        parsed_term("UBERON:0002", "obsolete tooth", is_obsolete=True, obsoletion_kind=_DEPRECATED),
        parsed_term("UBERON:0003", "molar"),
        parsed_term(
            "UBERON:0004",
            None,
            is_obsolete=True,
            replaced_by_term_id="UBERON:0003",
            obsoletion_kind=_MERGED,
        ),
    ]
    assert result == expected


# =============================================================================
# _assemble_terms / _collect_merge_survivors
# =============================================================================


def test__assemble_terms_merge_without_class():
    """Tests the case where an absorbed term id has no class of its own: it
    carries no label, because the release names no class to take one from and
    this layer cannot decide what to store instead."""
    exported_classes = [
        exported_class("UBERON:0002", "tooth", alternative_term_ids=("UBERON:0900",))
    ]

    result = _assemble_terms(exported_classes)

    expected = [
        parsed_term("UBERON:0002", "tooth"),
        parsed_term(
            "UBERON:0900",
            None,
            is_obsolete=True,
            replaced_by_term_id="UBERON:0002",
            obsoletion_kind=_MERGED,
        ),
    ]
    assert result == expected


def test__assemble_terms_merge_with_class():
    """Tests the case where an absorbed term id also has a class of its own:
    the row keeps that class's own label instead of a synthesized one."""
    exported_classes = [
        exported_class("UBERON:0002", "tooth", alternative_term_ids=("UBERON:0900",)),
        exported_class("UBERON:0900", "obsolete tooth bud", source_deprecated=True),
    ]

    result = _assemble_terms(exported_classes)

    expected = [
        parsed_term("UBERON:0002", "tooth"),
        parsed_term(
            "UBERON:0900",
            "obsolete tooth bud",
            is_obsolete=True,
            replaced_by_term_id="UBERON:0002",
            obsoletion_kind=_MERGED,
        ),
    ]
    assert result == expected


def test__assemble_terms_deprecation_and_merge_collision(caplog):
    """Tests the case where a class is both deprecated with a replacement and
    absorbed by a different class: the row records only the absorption and
    warns about the divergent replacement."""
    exported_classes = [
        exported_class("UBERON:0002", "tooth", alternative_term_ids=("UBERON:0900",)),
        exported_class(
            "UBERON:0900",
            "obsolete tooth bud",
            source_deprecated=True,
            asserted_replacement_term_id="UBERON:0003",
        ),
    ]

    with caplog.at_level(logging.WARNING):
        result = _assemble_terms(exported_classes)

    expected = [
        parsed_term("UBERON:0002", "tooth"),
        parsed_term(
            "UBERON:0900",
            "obsolete tooth bud",
            is_obsolete=True,
            replaced_by_term_id="UBERON:0002",
            obsoletion_kind=_MERGED,
        ),
    ]
    assert result == expected
    assert len(caplog.records) == 1
    assert "UBERON:0900" in caplog.text
    assert "UBERON:0003" in caplog.text


def test__assemble_terms_duplicate_survivor_claim(caplog):
    """Tests the case where two classes claim the same absorbed term id: the
    first claim stands and the collector warns about the conflict."""
    exported_classes = [
        exported_class("UBERON:0002", "tooth", alternative_term_ids=("UBERON:0900",)),
        exported_class("UBERON:0003", "molar", alternative_term_ids=("UBERON:0900",)),
    ]

    with caplog.at_level(logging.WARNING):
        result = _assemble_terms(exported_classes)

    expected = [
        parsed_term("UBERON:0002", "tooth"),
        parsed_term("UBERON:0003", "molar"),
        parsed_term(
            "UBERON:0900",
            None,
            is_obsolete=True,
            replaced_by_term_id="UBERON:0002",
            obsoletion_kind=_MERGED,
        ),
    ]
    assert result == expected
    assert len(caplog.records) == 1
    assert "UBERON:0900" in caplog.text


def test__assemble_terms_replacement_without_deprecation():
    """Tests the case where a class carries a replacement pointer but is not
    deprecated: the row passes through as it stands, leaving the import to
    reject the contradiction."""
    exported_classes = [
        exported_class("UBERON:0002", "tooth", asserted_replacement_term_id="UBERON:0003")
    ]

    result = _assemble_terms(exported_classes)

    expected = [parsed_term("UBERON:0002", "tooth", replaced_by_term_id="UBERON:0003")]
    assert result == expected


def test__collect_merge_survivors_self_reference():
    """Tests the case where a class lists its own term id as absorbed: the map
    records no survivor for it."""
    exported_classes = [
        exported_class("UBERON:0002", "tooth", alternative_term_ids=("UBERON:0002", "UBERON:0900"))
    ]

    result = _collect_merge_survivors(exported_classes)

    assert result == {"UBERON:0900": "UBERON:0002"}
