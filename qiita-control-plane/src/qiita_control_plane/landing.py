"""Public landing page (`GET /`).

Lives outside the `/api/v1` surface intentionally: this is the page a
browser hits at the bare host (e.g. https://qiita-miint.ucsd.edu/), not
an API resource. Renders the same HTML for every visitor — the system
is CLI-only and has no persistent browser session to detect, so there
is no "logged in as X" branch.

Source of truth for the page's mailto links is `Settings.contact_email`;
changing the address is an env-file edit + restart, no code change.
"""

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .config import Settings
from .deps import get_settings

_TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
_templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

# Resolved once at import time. The installed-package version is the
# right surface for an operator reading the page footer; falling back to
# "unknown" keeps a from-source CP boot (without an installed dist)
# from 500ing the landing page.
try:
    _VERSION = version("qiita-control-plane")
except PackageNotFoundError:
    _VERSION = "unknown"


router = APIRouter()


@router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def landing(
    request: Request,
    settings: Settings = Depends(get_settings),
) -> HTMLResponse:
    """Render the public landing page. Always public, always anonymous."""
    return _templates.TemplateResponse(
        request,
        "landing.html",
        {"contact_email": settings.contact_email, "version": _VERSION},
    )
