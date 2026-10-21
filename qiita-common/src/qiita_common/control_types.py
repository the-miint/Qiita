"""Control classification of `Qiita Sample Type` terms, loaded from `data/control_types.yml`."""

from enum import StrEnum
from functools import cache
from importlib import resources

import yaml


class ControlKind(StrEnum):
    NEGATIVE = "negative"
    POSITIVE = "positive"
    NOT_CONTROL = "not_control"


@cache
def load_control_types() -> dict[str, ControlKind]:
    """Return the `term_id -> ControlKind` map; a term the file does not list is absent."""
    return parse_control_types(
        resources.files("qiita_common.data")
        .joinpath("control_types.yml")
        .read_text(encoding="utf-8")
    )


def parse_control_types(text: str) -> dict[str, ControlKind]:
    """Parse `control_types.yml` text; a term listed twice, or a wrong key set, raises."""
    raw = yaml.safe_load(text)
    expected = {kind.value for kind in ControlKind}
    if set(raw) != expected:
        raise ValueError(f"control_types keys must be {sorted(expected)}, got {sorted(raw)}")
    seen: dict[str, ControlKind] = {}
    for key, term_ids in raw.items():
        kind = ControlKind(key)
        for term_id in term_ids:
            if term_id in seen:
                first = seen[term_id].value
                raise ValueError(f"term {term_id!r} listed under {first!r} and {key!r}")
            seen[term_id] = kind
    return seen
