"""Tier fixtures for tests: a ranking of `Tier` to compute expected visibility
from, and a caller who may read every metadata value.

The ranking is written here rather than imported from `auth.guards`: a test that
borrowed the production ordering could not catch that ordering being wrong.
"""

from qiita_common.models import Tier

from qiita_control_plane.repositories.study_access import CallerMetadataTiers

TIER_RANK: dict[Tier, int] = {Tier.PUBLIC: 0, Tier.VIEWER: 1, Tier.MEMBER: 2, Tier.ADMIN: 3}

# Admin on every study. For a test of what a metadata read or write does, not
# of which callers it admits; the visibility tests pin the latter.
ADMIN_EVERYWHERE = CallerMetadataTiers(study_tiers={}, default=Tier.ADMIN)
