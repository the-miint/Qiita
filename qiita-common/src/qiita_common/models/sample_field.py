"""Entity-agnostic wire shapes for the sample field surfaces — the study-local
field create/read shapes and the global field registry read shape.

Each sample-family-specific entity specialises these by redeclaring only the idx
fields that carry its entity-qualified name on the wire; every other column,
and the purely-local vs globally-linked mode coupling, lives here.
"""

from typing import Annotated

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from qiita_common.models._base import NonBlankName, NonBlankText
from qiita_common.models.reference import FieldDataType, Tier

# Attribute names of the two idx fields each entity subclass re-declares with
# its own wire alias.
STUDY_FIELD_IDX_ATTR = "study_field_idx"
GLOBAL_FIELD_IDX_ATTR = "global_field_idx"

# The value kinds a unique-in-study field may carry. A closed value set would
# cap the study at as many samples as the set has values, so boolean and
# terminology are excluded. Tracks the *_study_field data-type eligibility CHECK.
UNIQUE_IN_STUDY_DATA_TYPES = frozenset(
    {FieldDataType.TEXT, FieldDataType.NUMERIC, FieldDataType.DATE}
)


def field_wire_name(model: type[BaseModel], attr: str) -> str:
    """Return the wire spelling of one of model's fields: the alias it declares
    for that field, or the attribute name itself when it declares none.

    Callers naming a field on the wire must resolve it through here rather than
    reading .alias directly — a model that leaves a field unaliased carries
    alias None, which is usable neither as a payload key nor as a message
    token.
    """
    declared_alias = model.model_fields[attr].alias
    return declared_alias or attr


class SampleStudyFieldCreateRequest(BaseModel):
    """Body for a study-local field create — mints a field definition on one
    study (no metadata value is written).

    The global-field link discriminates two mutually-exclusive modes.
    If omitted, purely-local: data_type is required, plus optional required /
    terminology_idx / tier_override / unique_in_study. If set, globally-linked:
    only display_name (+ optional description); data_type / required /
    terminology_idx / tier_override are inherited from the global field and
    must be omitted, and unique_in_study must be too — it is not inherited, it
    is unavailable, since one metadata row through a global field is shared
    across every study linked to it and no single study owns the grouping.
    """

    model_config = ConfigDict(extra="forbid")

    display_name: NonBlankName
    description: NonBlankText | None = None
    global_field_idx: Annotated[int, Field(gt=0)] | None = None
    data_type: FieldDataType | None = None
    required: bool | None = None
    terminology_idx: Annotated[int, Field(gt=0)] | None = None
    tier_override: Tier | None = None
    unique_in_study: bool | None = None

    @model_validator(mode="after")
    def _validate_mode_coupling(self) -> SampleStudyFieldCreateRequest:
        global_fk_name = field_wire_name(type(self), GLOBAL_FIELD_IDX_ATTR)

        # Linked mode: the inherited columns live on the global-field row and
        # must be NULL on the study-field row, so reject them at the wire.
        if self.global_field_idx is not None:
            forbidden = [
                name
                for name in (
                    "data_type",
                    "required",
                    "terminology_idx",
                    "tier_override",
                    "unique_in_study",
                )
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
        if self.unique_in_study and self.data_type not in UNIQUE_IN_STUDY_DATA_TYPES:
            eligible = ", ".join(sorted(t.value for t in UNIQUE_IN_STUDY_DATA_TYPES))
            raise ValueError(f"unique_in_study requires data_type to be one of: {eligible}")
        return self


class SampleStudyFieldResponse(BaseModel):
    """One study-local field definition.

    Carries every stored column. For a globally-linked row, data_type /
    required / terminology_idx are the values inherited from the global-field
    row (resolved at read time), so they are always populated even though the
    study-field columns are NULL. tier_override is instead always None on a
    linked row: a global field carries default_tier, so there is nothing per-study
    to override. internal_name and default_tier belong to the global field, not
    to this row, and are excluded. unique_in_study is always False on a linked
    row, which is the stored value rather than a resolved one.
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
    unique_in_study: bool
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime


class SampleGlobalFieldResponse(BaseModel):
    """One global field definition.

    Carries every stored column. terminology_idx is populated exactly when
    data_type is terminology.
    """

    global_field_idx: Annotated[int, Field(gt=0)]
    internal_name: str
    display_name: str
    description: str | None
    data_type: FieldDataType
    default_tier: Tier
    required: bool
    terminology_idx: Annotated[int, Field(gt=0)] | None
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime
