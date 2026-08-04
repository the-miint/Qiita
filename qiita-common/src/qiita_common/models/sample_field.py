"""Entity-agnostic wire shapes for the study-local field create surface.

Each sample-family-specific entity specialises these by redeclaring only the two idx
fields that carry its entity-qualified name on the wire; every other column,
and the purely-local vs globally-linked mode coupling, lives here.
"""

from typing import Annotated

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from qiita_common.auth_constants import MAX_NAME_LENGTH
from qiita_common.models.reference import FieldDataType, Tier

# Attribute name of the global-field link, whose wire spelling each subclass
# supplies as an alias. Read back off the model to name the field in a
# validation message, so the message always matches what the caller sent.
_GLOBAL_FK_ATTR = "global_field_idx"


class SampleStudyFieldCreateRequest(BaseModel):
    """Body for a study-local field create — mints a field definition on one
    study (no metadata value is written).

    The global-field link discriminates two mutually-exclusive modes.
    If omitted, purely-local: data_type is required, plus optional required /
    terminology_idx / tier_override. If set, globally-linked: only display_name
    (+ optional description); data_type / required / terminology_idx /
    tier_override are inherited from the global field and must be omitted.
    """

    model_config = ConfigDict(extra="forbid")

    display_name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    description: str | None = Field(default=None, min_length=1)
    global_field_idx: Annotated[int, Field(gt=0)] | None = None
    data_type: FieldDataType | None = None
    required: bool | None = None
    terminology_idx: Annotated[int, Field(gt=0)] | None = None
    tier_override: Tier | None = None

    @model_validator(mode="after")
    def _validate_mode_coupling(self) -> SampleStudyFieldCreateRequest:
        global_fk_name = type(self).model_fields[_GLOBAL_FK_ATTR].alias or _GLOBAL_FK_ATTR

        # Linked mode: the inherited columns live on the global-field row and
        # must be NULL on the study-field row, so reject them at the wire.
        if self.global_field_idx is not None:
            forbidden = [
                name
                for name in ("data_type", "required", "terminology_idx", "tier_override")
                if getattr(self, name) is not None
            ]
            if forbidden:
                raise ValueError(
                    f"{global_fk_name} links to a global field; "
                    f"{', '.join(forbidden)} must be omitted (inherited from the global field)"
                )
            return self

        # Local mode: data_type is required here (stricter than the DB default)
        # and terminology_idx is present iff the type is terminology.
        if self.data_type is None:
            raise ValueError(f"data_type is required when {global_fk_name} is omitted")
        if (self.data_type is FieldDataType.TERMINOLOGY) != (self.terminology_idx is not None):
            raise ValueError("terminology_idx must be set iff data_type is 'terminology'")
        return self


class SampleStudyFieldResponse(BaseModel):
    """A created study-local field (201 body is the created resource).

    Carries every stored column. For a globally-linked row, data_type /
    required / terminology_idx are the values inherited from the global-field
    row (resolved at read time), so they are always populated even though the
    study-field columns are NULL. Global-field-only columns (internal_name,
    default_tier) belong to the global-field resource and are excluded.
    """

    study_field_idx: Annotated[int, Field(gt=0)]
    study_idx: Annotated[int, Field(gt=0)]
    global_field_idx: Annotated[int, Field(gt=0)] | None
    display_name: str
    description: str | None
    data_type: FieldDataType
    required: bool
    terminology_idx: Annotated[int, Field(gt=0)] | None
    tier_override: Tier | None
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime
