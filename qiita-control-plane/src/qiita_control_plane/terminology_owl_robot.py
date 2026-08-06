"""ROBOT's export dialect: the command that produces an export of an OWL
release, and the reading of what that command wrote.

Everything specific to ROBOT is confined here — the column selection it
accepts, the separators it writes, and the datatype suffix it leaves on a
typed literal. The rows handed back carry no trace of it.
"""

from __future__ import annotations

import csv
from collections.abc import Sequence
from pathlib import Path

from .terminology_owl import (
    OBO_ALTERNATIVE_ID_PROPERTY,
    OBO_REPLACED_BY_PROPERTY,
    OWL_DEPRECATED_PROPERTY,
    ExportedClass,
)

# ROBOT's own keyword columns: ID yields the CURIE form of the class, LABEL
# its rdfs:label. The remaining three name annotation properties, which
# ROBOT resolves through its built-in OBO prefix map.
_ROBOT_COLUMN_TERM_ID = "ID"
_ROBOT_COLUMN_LABEL = "LABEL"
_ROBOT_EXPORT_COLUMNS = (
    _ROBOT_COLUMN_TERM_ID,
    _ROBOT_COLUMN_LABEL,
    OWL_DEPRECATED_PROPERTY,
    OBO_REPLACED_BY_PROPERTY,
    OBO_ALTERNATIVE_ID_PROPERTY,
)
# ROBOT spells the column selection and the values within one cell with the
# same character; only the cell form is configurable, and it is left alone.
_ROBOT_HEADER_SEPARATOR = "|"
_ROBOT_MULTI_VALUE_SEPARATOR = "|"
_ROBOT_EXPORT_ENTITY_TYPES = "classes"
# A typed literal arrives with its datatype after this marker.
_LITERAL_DATATYPE_SEPARATOR = "^^"

_DEFAULT_ROBOT_EXECUTABLE = ("robot",)


def robot_export_argv(
    input_filename: str,
    export_filename: str,
    *,
    executable: Sequence[str] = _DEFAULT_ROBOT_EXECUTABLE,
) -> list[str]:
    """Build the argv of a ROBOT export that parse_robot_export can read.

    Naming both files without a directory keeps the command runnable with
    the directory holding them as its working directory, so neither path
    depends on where a container mounts it. `executable` ends with the
    ROBOT executable itself, carrying whatever runs it (for example
    ("apptainer", "exec", "<sif>", "robot")).
    """
    return [
        *executable,
        "export",
        "--input",
        input_filename,
        "--header",
        _ROBOT_HEADER_SEPARATOR.join(_ROBOT_EXPORT_COLUMNS),
        "--include",
        _ROBOT_EXPORT_ENTITY_TYPES,
        "--export",
        export_filename,
    ]


def parse_robot_export(export_path: Path) -> list[ExportedClass]:
    """Read the ROBOT export at `export_path`.

    Cells hold their values separated by a pipe, and a typed literal
    carries its datatype after a '^^' marker.

    Raises FileNotFoundError when the export is absent, and ValueError when
    the header lacks a requested column, a row carries no term id, or
    owl:deprecated holds an uninterpretable value.
    """
    if not export_path.exists():
        raise FileNotFoundError(
            f"No ROBOT export at {export_path}. Produce one by running the command"
            " robot_export_argv builds, from the directory holding the source."
        )

    exported_classes: list[ExportedClass] = []
    with export_path.open(newline="") as fh:
        reader = csv.DictReader(fh, delimiter="\t")

        # Check the header up front, so a column the export never carried is
        # reported once rather than as a KeyError on the first row.
        present_columns = reader.fieldnames or ()
        missing_columns = [c for c in _ROBOT_EXPORT_COLUMNS if c not in present_columns]
        if missing_columns:
            raise ValueError(
                f"export at {export_path} is missing column(s) {missing_columns};"
                f" requested {list(_ROBOT_EXPORT_COLUMNS)}"
            )

        for row in reader:
            term_id = row[_ROBOT_COLUMN_TERM_ID].strip()
            if not term_id:
                raise ValueError(f"export at {export_path} carries a row with no term id")

            # A class may have absorbed several term ids; a replacement
            # pointer names a single class, so only the first is kept.
            replacements = _split_cell(row[OBO_REPLACED_BY_PROPERTY])
            exported_classes.append(
                ExportedClass(
                    term_id=term_id,
                    label=row[_ROBOT_COLUMN_LABEL],
                    source_deprecated=_parse_deprecated_cell(row[OWL_DEPRECATED_PROPERTY], term_id),
                    asserted_replacement_term_id=replacements[0] if replacements else None,
                    alternative_term_ids=tuple(_split_cell(row[OBO_ALTERNATIVE_ID_PROPERTY])),
                )
            )
    return exported_classes


def _parse_deprecated_cell(cell: str, term_id: str) -> bool:
    """Interpret an owl:deprecated cell, where an absent annotation arrives
    as an empty cell. Raises ValueError naming `term_id` on a value that is
    neither true nor false."""
    values = _split_cell(cell)
    if not values:
        return False
    value = values[0].lower()
    if value not in ("true", "false"):
        raise ValueError(
            f"invalid {OWL_DEPRECATED_PROPERTY} value {cell!r} for term_id {term_id!r};"
            " expected 'true', 'false', or an empty cell"
        )
    return value == "true"


def _split_cell(cell: str) -> list[str]:
    """Split a cell into its values, dropping the datatype of any typed
    literal and discarding empties."""
    values: list[str] = []
    for raw_value in cell.split(_ROBOT_MULTI_VALUE_SEPARATOR):
        value = raw_value.split(_LITERAL_DATATYPE_SEPARATOR, 1)[0].strip()
        if value:
            values.append(value)
    return values
