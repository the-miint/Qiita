"""Host-filter profile: the (host taxon, platform) -> reference-build mapping.

The config layer that resolves a host ORGANISM (biosample metadata — the
`host_taxon_id` global field) to the host reference BUILD we deplete against.
Mirrors the `qiita.host_filter_profile` table.
"""

from pydantic import BaseModel, ConfigDict

from qiita_common.models.reference import Platform


class HostFilterProfile(BaseModel):
    """One row of `qiita.host_filter_profile`.

    `rype_reference_idx` is required — the existence of a profile row *means*
    "deplete this host", so there is always a stage-1 index to deplete against.
    `minimap2_reference_idx` is the optional stage 2; None means this profile
    stops after stage 1.

    Both fields name a `qiita.reference`, not an on-disk path. Whether that
    reference is ACTIVE and its index actually built is a run-time question,
    answered later by the runner's reference resolution — a profile pins
    identity, not readiness.
    """

    model_config = ConfigDict(extra="forbid")

    idx: int
    host_term_idx: int
    platform: Platform
    rype_reference_idx: int
    minimap2_reference_idx: int | None = None
