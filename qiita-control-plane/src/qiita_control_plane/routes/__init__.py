"""Control plane API routes."""

from fastapi import APIRouter
from qiita_common.auth_constants import API_PREFIX

from .admin import router as admin_router
from .auth import router as auth_router
from .reference import router as reference_router
from .user import router as user_router

api_router = APIRouter(prefix=API_PREFIX)
api_router.include_router(reference_router)
api_router.include_router(user_router)
api_router.include_router(auth_router)
api_router.include_router(admin_router)
