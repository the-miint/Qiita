"""The OBO annotation conventions that decide which terms of an OWL
release are obsolete and what replaced them.

Holds no knowledge of any particular OWL toolchain: the input is already
normalized into ExportedClass rows, and the identifiers of the three
annotation properties those rows carry are defined here for whichever
extractor asks the source for them.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace

from qiita_common.models import TerminologyTermObsoletionKind

from .repositories.terminology import ParsedTerm

_log = logging.getLogger(__name__)

# Identifiers of the annotation properties an obsoletion decision reads:
# whether the source deprecated a class, which class replaces a deprecated
# one, and which term ids a class has absorbed.
OWL_DEPRECATED_PROPERTY = "owl:deprecated"
OBO_REPLACED_BY_PROPERTY = "IAO:0100001"
OBO_ALTERNATIVE_ID_PROPERTY = "oboInOwl:hasAlternativeId"

_MERGED_LABEL_TEMPLATE = "merged into {survivor_term_id}"


@dataclass(frozen=True)
class ExportedClass:
    """What an ontology release asserts about one of its classes, before any
    obsoletion decision is taken from those assertions.

    The field names mark the assertions as the source's own, because they can
    differ from the decisions derived from them: a class
    the source never deprecated is obsoleted anyway if another class
    absorbs it, and the replacement finally recorded may differ from the
    one asserted here.

    These records do not map one-to-one onto term rows. Every term id in
    `alternative_term_ids` earns a row of its own whether or not the
    release also carries a class for it, so a set of these yields at least
    as many term rows, and usually more.
    """

    term_id: str
    label: str
    source_deprecated: bool
    asserted_replacement_term_id: str | None
    alternative_term_ids: tuple[str, ...]


def build_terms(
    exported_classes: list[ExportedClass],
    *,
    term_id_prefix: str | None,
) -> list[ParsedTerm]:
    """Turn `exported_classes` into the term rows of a release.

    A `term_id_prefix` of None takes the classes whole; otherwise classes
    outside the prefix are dropped, along with any pointer reaching out of
    it.
    """
    in_scope_classes = (
        exported_classes
        if term_id_prefix is None
        else _filter_classes_to_prefix(exported_classes, term_id_prefix)
    )
    return _assemble_terms(in_scope_classes)


def _filter_classes_to_prefix(
    exported_classes: list[ExportedClass],
    term_id_prefix: str,
) -> list[ExportedClass]:
    """Keep only the classes whose term id carries `term_id_prefix`, so
    classes a release imports from other vocabularies stay out of it. A
    replacement pointer or absorbed term id reaching outside the prefix is
    dropped from the class that carries it."""
    kept: list[ExportedClass] = []
    for exported_class in exported_classes:
        if not exported_class.term_id.startswith(term_id_prefix):
            continue

        # A replacement resolves within one terminology, so a pointer at
        # another vocabulary's class cannot be recorded and is dropped.
        asserted_replacement = exported_class.asserted_replacement_term_id
        if asserted_replacement is not None and not asserted_replacement.startswith(term_id_prefix):
            _log.warning(
                "term_id %s names replacement %s outside prefix %r; dropping the pointer",
                exported_class.term_id,
                asserted_replacement,
                term_id_prefix,
            )
            asserted_replacement = None

        in_prefix_alternatives = tuple(
            alternative_id
            for alternative_id in exported_class.alternative_term_ids
            if alternative_id.startswith(term_id_prefix)
        )
        kept.append(
            replace(
                exported_class,
                asserted_replacement_term_id=asserted_replacement,
                alternative_term_ids=in_prefix_alternatives,
            )
        )
    return kept


def _assemble_terms(exported_classes: list[ExportedClass]) -> list[ParsedTerm]:
    """Apply the OBO obsoletion conventions to `exported_classes`: a
    deprecated class is obsolete, a class another one absorbed is obsolete
    and replaced by that other class, and a class encoded both ways is
    recorded only as absorbed."""
    merge_survivors = _collect_merge_survivors(exported_classes)
    terms = [
        _term_for_class(exported_class, merge_survivors) for exported_class in exported_classes
    ]

    # An absorbed term id with no class of its own has no label to carry
    # forward, so one naming the surviving class stands in for it.
    exported_term_ids = {exported_class.term_id for exported_class in exported_classes}
    terms += [
        ParsedTerm(
            term_id=merged_term_id,
            label=_MERGED_LABEL_TEMPLATE.format(survivor_term_id=survivor_term_id),
            is_obsolete=True,
            replaced_by_term_id=survivor_term_id,
            obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
        )
        for merged_term_id, survivor_term_id in sorted(merge_survivors.items())
        if merged_term_id not in exported_term_ids
    ]
    return terms


def _collect_merge_survivors(exported_classes: list[ExportedClass]) -> dict[str, str]:
    """Map each absorbed term id to the term id of the class that absorbed
    it. Warns and keeps the first when two classes claim the same one."""
    survivors: dict[str, str] = {}
    for exported_class in exported_classes:
        for merged_term_id in exported_class.alternative_term_ids:
            # A class listing its own term id records no absorption; taking
            # it would obsolete the class and point it at itself.
            if merged_term_id == exported_class.term_id:
                continue

            claimed_by = survivors.get(merged_term_id)
            if claimed_by is not None and claimed_by != exported_class.term_id:
                _log.warning(
                    "term_id %s is claimed as absorbed by both %s and %s; keeping %s",
                    merged_term_id,
                    claimed_by,
                    exported_class.term_id,
                    claimed_by,
                )
                continue
            survivors[merged_term_id] = exported_class.term_id
    return survivors


def _term_for_class(
    exported_class: ExportedClass,
    merge_survivors: dict[str, str],
) -> ParsedTerm:
    """Build the term row for one exported class. Having been absorbed takes
    precedence over the class's own deprecation, which is then recorded only
    as the absorption."""
    survivor_term_id = merge_survivors.get(exported_class.term_id)
    if survivor_term_id is None:
        return ParsedTerm(
            term_id=exported_class.term_id,
            label=exported_class.label,
            is_obsolete=exported_class.source_deprecated,
            replaced_by_term_id=exported_class.asserted_replacement_term_id,
            obsoletion_kind=(
                TerminologyTermObsoletionKind.SOURCE_DEPRECATED
                if exported_class.source_deprecated
                else None
            ),
        )

    # Encoded both ways: a replacement pointer agreeing with the absorbing
    # class is redundant, and one naming a different class contradicts it.
    if (
        exported_class.asserted_replacement_term_id is not None
        and exported_class.asserted_replacement_term_id != survivor_term_id
    ):
        _log.warning(
            "term_id %s is deprecated with replacement %s but absorbed by %s;"
            " recording the absorption",
            exported_class.term_id,
            exported_class.asserted_replacement_term_id,
            survivor_term_id,
        )
    return ParsedTerm(
        term_id=exported_class.term_id,
        label=exported_class.label,
        is_obsolete=True,
        replaced_by_term_id=survivor_term_id,
        obsoletion_kind=TerminologyTermObsoletionKind.SOURCE_MERGED,
    )
