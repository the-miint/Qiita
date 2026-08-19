"""Host-filter profile catalog.

Read-only view of `qiita.host_filter_profile` — which host taxa we can deplete,
and against which reference build, on a given platform.

This is the menu. The pool roster says what each sample *would* get
(`SequencedSampleListItem.host_filter`); this says what is *available*, which is
what makes an operator override well-defined: you cannot sensibly force a sample
onto a host profile without first being able to see which profiles exist.

Managing profiles (create / repoint after a host-DB rebuild) is deliberately not
here — the live rows are seeded out-of-band by the operator, so there is no write
surface to guard yet.
"""

import asyncpg
from fastapi import APIRouter, Depends
from qiita_common.api_paths import (
    PATH_HOST_FILTER_PROFILE_PREFIX,
    PATH_HOST_FILTER_PROFILE_ROOT,
)
from qiita_common.auth_constants import Scope, SystemRole
from qiita_common.models import HostFilterProfile, Platform

from ..auth.guards import require_human, require_role_at_least, require_scope
from ..auth.principal import HumanUser, Principal
from ..deps import get_db_pool
from ..repositories.host_filter_profile import list_host_filter_profiles

router = APIRouter(prefix=PATH_HOST_FILTER_PROFILE_PREFIX, tags=["host-filter-profile"])


@router.get(PATH_HOST_FILTER_PROFILE_ROOT)
async def list_host_filter_profile(
    pool: asyncpg.Pool = Depends(get_db_pool),
    _user: HumanUser = Depends(require_human),
    _scope: Principal = Depends(require_scope(Scope.REFERENCE_READ)),
    _role: Principal = Depends(require_role_at_least(SystemRole.WET_LAB_ADMIN)),
    platform: Platform | None = None,
) -> list[HostFilterProfile]:
    """List host-filter profiles, optionally narrowed to one `platform`.

    The scope is `reference:read` rather than a new one: a profile names no
    sample, only which reference builds deplete which host, so it is reference
    config. But `reference:read` is held by every human role and service account,
    so it discriminates nothing on its own — the effective gate is the
    wet_lab_admin role, which is deliberately STRICTER than the reference reads
    this route's rows point at (those are anonymous-OK). The asymmetry is
    intentional: which organism we deplete, for which study's samples, is closer
    to sample policy than to reference metadata, and the audience that acts on it
    is the one that submits read masks. It is not the roster's gate, which pairs
    the same role with `prep_sample:read`.

    Unbounded by design, unlike the reference list: the table holds one row per
    (host, platform) pair, so it is inherently small — there is no pagination
    footgun to guard against here.
    """
    return await list_host_filter_profiles(pool, platform=platform)
