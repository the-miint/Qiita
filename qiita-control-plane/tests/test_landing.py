"""Tests for the public landing page (`GET /`).

Pure unit, no DB marker — the landing route only depends on
`Settings.contact_email` from `app.state`. The test builds the app
without lifespan, stashes a minimal Settings, and drives the route via
httpx + ASGITransport (same pattern as the route tests under tests/routes/
but without postgres/jwks fixtures).
"""

from urllib.parse import quote

import pytest
from httpx import ASGITransport, AsyncClient


def _build_minimal_settings(contact_email: str = "qiita-help@example.org"):
    """Most Settings fields are unused by the landing route — only
    `contact_email` is read. Construct one directly (Settings.from_env()
    would also work but would require monkeypatching the full
    required-env set)."""
    from qiita_control_plane.config import Settings

    return Settings(
        database_url="unused",
        hmac_secret_key=b"\x00" * 32,
        data_plane_url="unused",
        contact_email=contact_email,
    )


@pytest.fixture
def app():
    """Yield the FastAPI app with a landing-route-sufficient Settings
    stashed on `app.state`. Save/restore the prior value so a later test
    in the same process doesn't inherit this fixture's partial Settings.
    `app` is a module-level singleton, so leaking state across tests is
    a real foot-gun — same shape the auth_client fixture in
    tests/routes/test_auth_endpoints.py uses for the verifier."""
    from qiita_control_plane.main import app as fastapi_app

    prior = getattr(fastapi_app.state, "settings", None)
    fastapi_app.state.settings = _build_minimal_settings()
    try:
        yield fastapi_app
    finally:
        if prior is None:
            # Don't leave a sentinel; mimic "field was never set".
            try:
                del fastapi_app.state.settings
            except AttributeError:
                pass
        else:
            fastapi_app.state.settings = prior


def _override_contact_email(app, contact_email: str) -> None:
    """Replace the per-test Settings with one carrying a specific
    contact_email. The outer fixture's save/restore still applies."""
    app.state.settings = _build_minimal_settings(contact_email=contact_email)


async def test_landing_returns_html(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")


async def test_landing_advertises_alpha_invitation_cli(app):
    """The page must surface the three state declarations that justify
    its existence — they're the reason a visitor isn't getting a working
    web app."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/")
    body = response.text
    assert "alpha" in body
    assert "invitation-only" in body
    assert "CLI-only" in body


async def test_landing_renders_contact_email_in_mailto_links(app):
    """Both mailto surfaces (request-access + need-help) must point at
    the configured CONTACT_EMAIL with subject-prefilled URLs so the
    recipient can triage on the subject line."""
    contact = "qiita-support@ucsd.edu"
    _override_contact_email(app, contact)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/")
    body = response.text
    assert f"mailto:{contact}?subject={quote('qiita-miint access request')}" in body
    assert f"mailto:{contact}?subject={quote('qiita-miint help')}" in body


async def test_landing_links_into_repo_docs(app):
    """The page is a hub that points into the repo — anchor-less doc URLs
    so a renamed heading on the GitHub side doesn't silently break us."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/")
    body = response.text
    assert "https://github.com/the-miint/Qiita#readme" in body
    assert (
        "https://github.com/the-miint/Qiita/blob/main/docs/runbooks/user-cli-quickstart.md" in body
    )
    assert "https://github.com/the-miint/Qiita/blob/main/docs/architecture.md" in body


async def test_landing_does_not_render_session_banner(app):
    """The codebase has no persistent browser session (the login cookie
    is single-use, scrubbed at /auth/handoff). The landing page must
    not pretend otherwise — no 'logged in as' string anywhere."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/")
    body = response.text.lower()
    assert "logged in as" not in body
    assert "log out" not in body


async def test_static_assets_served(app):
    """StaticFiles mount must serve the page's CSS + JS so the rendered
    HTML isn't quietly missing them at runtime."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        css = await ac.get("/static/landing.css")
        js = await ac.get("/static/landing.js")
    assert css.status_code == 200
    assert css.headers["content-type"].startswith("text/css")
    assert js.status_code == 200
    assert js.headers["content-type"].startswith(("text/javascript", "application/javascript"))


async def test_landing_renders_three_status_pills(app):
    """The page must carry one pill per service in the
    `qiita_control_plane.health` per-service breakdown (cp/co/dp). The
    pill IDs are the contract the JS reads from /health.services; if
    these IDs change, the JS must change in lockstep."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/")
    body = response.text
    # Three pill elements with stable IDs.
    assert 'id="status-badge-cp"' in body
    assert 'id="status-badge-co"' in body
    assert 'id="status-badge-dp"' in body
    # Each pill carries its service-name label (visitor-facing copy
    # — renaming these is a UX change, not a refactor).
    assert "Control plane" in body
    assert "Orchestrator" in body
    assert "Data plane" in body


async def test_landing_pills_start_in_checking_state(app):
    """Server-side render leaves every pill in the neutral
    'checking…' state; the JS flips to ok / degraded / unreachable
    after fetching /health. A pre-flip render must never look like
    'everything's fine' — that would mislead a visitor whose JS
    failed to run."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        response = await ac.get("/")
    body = response.text
    # Each pill has the neutral class and the placeholder text.
    # Three pills × one placeholder per pill = three occurrences.
    assert body.count("status-unknown") == 3
    assert body.count("checking") == 3


async def test_landing_js_references_three_pill_ids(app):
    """The landing.js must reference the same three pill IDs the
    HTML carries. Drifts between the two sides land a pill in
    'checking…' forever — visible but easy to miss in code review,
    so we pin the contract here."""
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        js = await ac.get("/static/landing.js")
    assert js.status_code == 200
    js_text = js.text
    # Either the IDs are constructed from a per-key suffix or they
    # appear literally — accept either pattern by checking that the
    # JS knows the three keys.
    for key in ("cp", "co", "dp"):
        assert f'"{key}"' in js_text, f"landing.js must reference service key {key!r}"
