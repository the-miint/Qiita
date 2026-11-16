"""PATCH /admin/definition-settings/{definition_kind}/{definition_idx}: the audited
edit of a mask's, alignment's or processing's locked settings
(`definition_settings` holds the steps and refusals).

system_admin only, human callers only, with the kind's lifecycle scope, the scope the
pair routes and a deprecation take. A refusal is a 409 whose detail carries a
`reason` code (`DefinitionPairRefusal` or `DefinitionSettingsRefusal`); an edit that
cannot take the group lock within its bounded retry is a 503 with Retry-After.
"""

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import Field
from qiita_common.api_paths import PATH_ADMIN_DEFINITION_SETTINGS, PATH_ADMIN_PREFIX
from qiita_common.auth_constants import SystemRole
from qiita_common.models import (
    DefinitionKind,
    DefinitionSettingsEdit,
    DefinitionSettingsEditRequest,
)

from ..auth.guards import check_scope, require_human_with_role
from ..auth.principal import HumanUser
from ..auth.scopes import KIND_LIFECYCLE_SCOPE
from ..definition_settings import (
    SettingsEditInvalid,
    SettingsEditRefused,
    edit_definition_settings,
)
from ..definition_similarity import DefinitionNotFound, GroupLockBusy, PairRefused
from ..deps import TxConnFactory, get_tx_conn_factory
from ..repositories.definition_pair import decode_jsonb
from ..repositories.params_storage import ParamsDoNotSurviveStorageError
from ._helpers import pair_refused_http, raise_transient_retry

router = APIRouter(prefix=PATH_ADMIN_PREFIX, tags=["definition-settings"])


@router.patch(PATH_ADMIN_DEFINITION_SETTINGS)
async def edit_definition_settings_route(
    definition_kind: DefinitionKind,
    definition_idx: Annotated[int, Field(gt=0)],
    body: DefinitionSettingsEditRequest,
    tx: TxConnFactory = Depends(get_tx_conn_factory),
    caller: HumanUser = Depends(require_human_with_role(SystemRole.SYSTEM_ADMIN)),
) -> DefinitionSettingsEdit:
    """Replace the definition's params (and, for a mask or a processing, its workflow
    or version columns), recording the old and new values with the reason. 404 for an
    unknown definition; 422 for a workflow or version on an alignment, a workflow or
    version column that disagrees with the same key in params, or params that do not
    survive jsonb storage."""
    check_scope(caller, KIND_LIFECYCLE_SCOPE[definition_kind])
    try:
        async with tx() as conn:
            result = await edit_definition_settings(
                conn,
                kind=definition_kind,
                idx=definition_idx,
                params=body.params,
                workflow=body.workflow,
                version=body.version,
                reason=body.reason,
                principal_idx=caller.principal_idx,
            )
    except DefinitionNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (SettingsEditInvalid, ParamsDoNotSurviveStorageError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except PairRefused as exc:
        raise pair_refused_http(exc) from exc
    except SettingsEditRefused as exc:
        raise HTTPException(
            status_code=409, detail={"detail": str(exc), "reason": exc.reason.value}
        ) from exc
    except GroupLockBusy as exc:
        raise_transient_retry(str(exc))
    edit = result.edit
    return DefinitionSettingsEdit.model_validate(
        {
            **dict(edit),
            "definition_idx": edit["definition_ref"],
            "old_settings": decode_jsonb(edit["old_settings"]),
            "new_settings": decode_jsonb(edit["new_settings"]),
            "repaired_pair_idx": result.repaired_pair_idx,
        }
    )
