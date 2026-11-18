"""Names the control plane and the krepp jobs share.

A krepp build's activation runs a control-plane action (`export-krepp-build`), a
native job on a compute node (`krepp_index_activate`) and a second action
(`activate-krepp-build`). The two actions and the job exchange the files below;
`KreppBuildRecord` and `KreppActivationReport` are their contents.
"""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

# The workflow context key naming the build (`qiita.reference_index.reference_index_idx`).
REFERENCE_INDEX_IDX_BINDING = "reference_index_idx"

KREPP_BUILD_RECORD_BASENAME = "krepp_build.json"
KREPP_ACTIVATION_REPORT_BASENAME = "krepp_activation_report.json"


class KreppBuildRecord(BaseModel):
    """What `export-krepp-build` tells the job about the build it checks: where it is
    (relative to PATH_NODE_LOCAL) and the hash its copy's SHA256SUMS must have."""

    model_config = ConfigDict(extra="forbid")

    reference_index_idx: Annotated[int, Field(gt=0)]
    reference_idx: Annotated[int, Field(gt=0)]
    fs_path: str
    sha256sums_sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class KreppActivationReport(BaseModel):
    """What `krepp_index_activate` found on a node copy. The copy passed the per-job
    check, or the job would have failed. `own_edge_share` is the share of the backbone
    tips placed whose best edge is their own; `passed` is
    `own_edge_share >= min_own_edge_share`."""

    model_config = ConfigDict(extra="forbid")

    reference_index_idx: Annotated[int, Field(gt=0)]
    node: str
    tips: Annotated[int, Field(ge=0)]
    placed: Annotated[int, Field(ge=0)]
    own_edge: Annotated[int, Field(ge=0)]
    own_edge_share: Annotated[float, Field(ge=0, le=1)]
    min_own_edge_share: Annotated[float, Field(ge=0, le=1)]
    passed: bool
