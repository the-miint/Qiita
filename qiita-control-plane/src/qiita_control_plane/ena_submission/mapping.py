"""Row bodies for the ENA objects this package submits.

One model per `ena` catalog table. Field names equal the column names the
catalog's specs bind, so selecting a spec's columns out of a dumped model is the
whole value mapping and no second hand-written ordering can drift from it.
"""

from __future__ import annotations

from pydantic import BaseModel, Field
from qiita_common.models import NonBlankText


class EnaProjectRow(BaseModel):
    """One `ena.projects` row body."""

    alias: NonBlankText
    title: str | None = None
    description: str | None = None
    project_type: str | None = None


class EnaSampleRow(BaseModel):
    """One `ena.samples` row body.

    `checklist` has to carry content: an empty string disables miint's
    client-side checklist validation instead of failing.
    """

    alias: NonBlankText
    taxon_id: int
    checklist: NonBlankText
    attributes: dict[str, str] = Field(default_factory=dict)
    attribute_units: dict[str, str] = Field(default_factory=dict)
