"""QC reason codes per check, loaded from `data/qc_reasons.yml`."""

from functools import cache
from importlib import resources

import yaml

from qiita_common.models import QcCheck


@cache
def load_qc_reasons() -> dict[QcCheck, frozenset[str]]:
    """Return the allowed reason codes for each `QcCheck`."""
    return parse_qc_reasons(
        resources.files("qiita_common.data").joinpath("qc_reasons.yml").read_text(encoding="utf-8")
    )


def parse_qc_reasons(text: str) -> dict[QcCheck, frozenset[str]]:
    """Parse `qc_reasons.yml` text; a repeated code within a check raises."""
    raw = yaml.safe_load(text)
    reasons = {}
    for check, codes in raw.items():
        unique = frozenset(codes)
        if len(unique) != len(codes):
            dup = next(c for c in codes if codes.count(c) > 1)
            raise ValueError(f"duplicate reason {dup!r} under check {check!r}")
        reasons[QcCheck(check)] = unique
    return reasons
