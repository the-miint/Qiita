"""A ranking of `Tier` for tests to judge expected visibility against.

Written here rather than imported from `auth.guards`: a test that borrowed the
production ordering could not catch that ordering being wrong.
"""

from qiita_common.models import Tier

TIER_RANK: dict[Tier, int] = {Tier.PUBLIC: 0, Tier.VIEWER: 1, Tier.MEMBER: 2, Tier.ADMIN: 3}
