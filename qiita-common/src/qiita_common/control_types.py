"""Control classification of `Qiita Sample Type` terms, loaded from `data/control_types.yml`."""

from functools import cache
from importlib import resources

import yaml

_KEYS = frozenset({"negative", "positive", "not_control"})


@cache
def load_control_types() -> dict[str, frozenset[str]]:
    """Return the term_ids under `negative`, `positive` and `not_control`."""
    return parse_control_types(
        resources.files("qiita_common.data")
        .joinpath("control_types.yml")
        .read_text(encoding="utf-8")
    )


def parse_control_types(text: str) -> dict[str, frozenset[str]]:
    """Parse `control_types.yml` text; a term listed twice, or a wrong key set, raises."""
    raw = yaml.safe_load(text)
    if set(raw) != _KEYS:
        raise ValueError(f"control_types keys must be {sorted(_KEYS)}, got {sorted(raw)}")
    seen: dict[str, str] = {}
    for kind, term_ids in raw.items():
        for term_id in term_ids:
            if term_id in seen:
                raise ValueError(f"term {term_id!r} listed under {seen[term_id]!r} and {kind!r}")
            seen[term_id] = kind
    return {kind: frozenset(term_ids) for kind, term_ids in raw.items()}
