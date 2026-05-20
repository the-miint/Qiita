"""Control plane API routes."""

from fastapi import APIRouter
from qiita_common.auth_constants import API_PREFIX

from .admin import router as admin_router
from .auth import router as auth_router
from .biosample import biosample_router as biosample_top_level_router
from .biosample import router as biosample_router
from .reference import router as reference_router
from .sequence_range import router as sequence_range_router
from .sequenced_sample import router as sequenced_sample_run_router
from .sequenced_sample import (
    sequenced_sample_router as sequenced_sample_top_level_router,
)
from .sequenced_sample import (
    study_scoped_router as sequenced_sample_study_router,
)
from .sequencing_run import router as sequencing_router
from .study import router as study_router
from .user import router as user_router
from .work_ticket import router as work_ticket_router

api_router = APIRouter(prefix=API_PREFIX)
api_router.include_router(reference_router)
api_router.include_router(biosample_router)
api_router.include_router(biosample_top_level_router)
api_router.include_router(sequencing_router)
api_router.include_router(sequenced_sample_run_router)
api_router.include_router(sequenced_sample_study_router)
api_router.include_router(sequenced_sample_top_level_router)
api_router.include_router(sequence_range_router)
api_router.include_router(study_router)
api_router.include_router(user_router)
api_router.include_router(auth_router)
api_router.include_router(admin_router)
api_router.include_router(work_ticket_router)
