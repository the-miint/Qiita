"""Control plane API routes."""

from fastapi import APIRouter

from .references import router as references_router

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(references_router)
