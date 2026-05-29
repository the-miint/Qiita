"""Tests for the self-hosted OpenAPI docs surface.

FastAPI's default `/docs` and `/redoc` pull Swagger UI / ReDoc JS+CSS from
the jsdelivr CDN, which renders blank behind a strict CSP or on a host (or
viewer's browser) with no outbound internet. `main.py` disables the default
routes and re-serves the shells pointing at vendored assets under
`/static/`. These tests pin that contract: the pages must render, must load
*only* local assets, and the assets themselves must be served.

Pure unit, no DB marker — none of these routes touch `app.state`, so the
app is driven without lifespan via httpx + ASGITransport (same pattern as
test_landing.py).
"""

from httpx import ASGITransport, AsyncClient

# Substrings that betray a leak back to an external CDN / font host. The
# whole point of vendoring is that none of these appear in the rendered
# shells.
_CDN_MARKERS = ("jsdelivr", "cdn.", "unpkg", "googleapis", "gstatic")


def _client():
    from qiita_control_plane.main import app

    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def test_openapi_schema_served():
    """The spec the docs UIs fetch must be live and well-formed."""
    async with _client() as ac:
        r = await ac.get("/openapi.json")
    assert r.status_code == 200
    body = r.json()
    assert body["info"]["title"] == "qiita-control-plane"
    # Generated from the router tree, so it carries the real surface.
    assert len(body["paths"]) > 0


async def test_swagger_ui_served_from_local_assets():
    async with _client() as ac:
        r = await ac.get("/docs")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    body = r.text
    assert "/static/swagger-ui-bundle.js" in body
    assert "/static/swagger-ui.css" in body
    for marker in _CDN_MARKERS:
        assert marker not in body, f"/docs leaks external resource: {marker!r}"


async def test_redoc_served_from_local_assets():
    async with _client() as ac:
        r = await ac.get("/redoc")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    body = r.text
    assert "/static/redoc.standalone.js" in body
    for marker in _CDN_MARKERS:
        assert marker not in body, f"/redoc leaks external resource: {marker!r}"


async def test_swagger_oauth2_redirect_served():
    """The 'Authorize' flow posts back to this route; Swagger UI 404s the
    handshake without it."""
    async with _client() as ac:
        r = await ac.get("/docs/oauth2-redirect")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")


async def test_vendored_assets_present_and_typed():
    """The three vendored files must be packaged and served with the
    right content types — a missing file is exactly the failure mode
    (blank docs page) this whole change exists to prevent."""
    async with _client() as ac:
        js_bundle = await ac.get("/static/swagger-ui-bundle.js")
        css = await ac.get("/static/swagger-ui.css")
        redoc_js = await ac.get("/static/redoc.standalone.js")
    assert js_bundle.status_code == 200
    assert js_bundle.headers["content-type"].startswith(
        ("text/javascript", "application/javascript")
    )
    assert css.status_code == 200
    assert css.headers["content-type"].startswith("text/css")
    assert redoc_js.status_code == 200
    assert redoc_js.headers["content-type"].startswith(
        ("text/javascript", "application/javascript")
    )
