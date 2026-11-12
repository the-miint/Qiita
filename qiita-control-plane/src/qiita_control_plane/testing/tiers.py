"""Tier fixtures for tests: a ranking of `Tier` to compute expected visibility
from, a caller who may read every metadata value, and switches for two of the
checks that run before a metadata write.

The ranking is written here rather than imported from `auth.guards`: a test that
borrowed the production ordering could not catch that ordering being wrong.
"""

import pytest
from qiita_common.models import Tier

from qiita_control_plane.repositories import _sample_helpers
from qiita_control_plane.repositories.study_access import CallerMetadataTiers

TIER_RANK: dict[Tier, int] = {Tier.PUBLIC: 0, Tier.VIEWER: 1, Tier.MEMBER: 2, Tier.ADMIN: 3}

# Admin on every study. For a test of what a metadata read or write does, not
# of which callers it admits; the visibility tests pin the latter.
ADMIN_EVERYWHERE = CallerMetadataTiers(study_tiers={}, default=Tier.ADMIN)

# The two ways a refusal test runs. "post-write-only" switches off the check
# that runs before a metadata write, so the checks after it must refuse alone.
WRITE_CHECKS = ("both-checks", "post-write-only")


def apply_write_checks(monkeypatch: pytest.MonkeyPatch, write_checks: str) -> None:
    """Under "post-write-only", switch off the check that runs before a
    metadata write. The checks after it otherwise answer only for a value or
    tier that a concurrent transaction changes in between, which a test cannot
    place there."""
    if write_checks == "post-write-only":

        async def _nothing_restricted(*_args, **_kwargs) -> list[str]:
            return []

        monkeypatch.setattr(
            _sample_helpers, "_fetch_keys_restricted_on_entity", _nothing_restricted
        )


def skip_field_tier_check(monkeypatch: pytest.MonkeyPatch) -> None:
    """Switch off the field-tier check that runs before a metadata write,
    standing in for an admin who raises a field's tier after that check passed:
    a test cannot place that commit there. A field above the caller's tier then
    reaches the value checks, the one before the write included unless
    `apply_write_checks` switches it off too."""

    async def _nothing_above(*_args, **_kwargs) -> dict:
        return {}

    monkeypatch.setattr(_sample_helpers, "fetch_fields_above_tier", _nothing_above)
