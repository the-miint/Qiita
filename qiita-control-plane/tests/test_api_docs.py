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

from pathlib import Path

from httpx import ASGITransport, AsyncClient

# Substrings that betray a leak back to an external CDN / font host. The
# whole point of vendoring is that none of these appear in the rendered
# shells.
_CDN_MARKERS = ("jsdelivr", "cdn.", "unpkg", "googleapis", "gstatic")

# Host markers to scan inside the vendored asset bodies themselves. Kept
# precise (full hostnames, not the bare "cdn." used for the HTML shells)
# so a minified bundle that happens to contain those bytes in code doesn't
# false-positive. A bad asset refresh (a build that pulls fonts/sourcemaps
# from a CDN) is exactly what this guards.
_ASSET_HOST_MARKERS = ("cdn.jsdelivr", "fonts.googleapis", "fonts.gstatic", "unpkg.com")

_STATIC_DIR = Path(__file__).resolve().parent.parent / "src" / "qiita_control_plane" / "static"
_VENDORED_ASSETS = (
    "swagger-ui-bundle.js",
    "swagger-ui.css",
    "redoc.standalone.js",
)


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
    handshake without it. Resolve the path from the app rather than a
    literal so a future custom redirect URL keeps this test honest."""
    from qiita_control_plane.main import app

    async with _client() as ac:
        r = await ac.get(app.swagger_ui_oauth2_redirect_url)
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


def test_vendored_assets_contain_no_cdn_references():
    """Guards the refresh procedure in docs/api-docs.md: a re-download that
    pulls a build referencing a CDN font/sourcemap would reintroduce the
    exact external dependency vendoring exists to remove. Scan the on-disk
    bundles, not just the rendered shells."""
    for name in _VENDORED_ASSETS:
        body = (_STATIC_DIR / name).read_text(encoding="utf-8", errors="ignore")
        for marker in _ASSET_HOST_MARKERS:
            assert marker not in body, f"{name} references external host: {marker!r}"
