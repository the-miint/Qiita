# API documentation surface

*Audience: maintainers (who refresh the vendored assets) and operators (who
deploy). The URL table at the top is the only end-user-facing part.*

The control plane publishes interactive, always-current OpenAPI documentation:

| URL | What it is |
|---|---|
| `/docs` | Swagger UI — interactive, "try it out" against the live API |
| `/redoc` | ReDoc — clean, read-only reference rendering |
| `/openapi.json` | The raw OpenAPI 3.1 spec the two pages render |

All three are reachable through nginx's catch-all `location /` (`deploy/nginx/qiita.conf`), so on a deploy they live at e.g. `https://<host>/docs`. The landing page links to `/docs` under "API reference".

## It updates itself

The spec is **not** a checked-in file. FastAPI builds it at request time by
walking the live router tree (`app.openapi()`), so any endpoint added to a
router shows up in `/docs` the moment the service restarts — no regeneration
step, no YAML to maintain. Routes marked `include_in_schema=False` (the
landing page, the docs routes themselves) are deliberately excluded.

## Why the assets are vendored (no CDN)

FastAPI's *default* `/docs` and `/redoc` load the Swagger UI / ReDoc
JavaScript and CSS from the public jsdelivr CDN. That makes the rendered
page depend on the **viewer's browser** being able to reach jsdelivr — which
fails (blank page) behind a strict Content-Security-Policy, on an air-gapped
or outbound-firewalled host, or during a CDN outage.

To remove that dependency, the UI assets are committed under
`qiita-control-plane/src/qiita_control_plane/static/` and `main.py` disables
the default routes (`docs_url=None`, `redoc_url=None`) and re-serves the
shells pointing at the local `/static/...` copies. ReDoc is rendered with
`with_google_fonts=False` so it doesn't pull fonts from Google either. The
result depends only on this service — the same origin already serving the
API. `tests/test_api_docs.py` asserts the rendered pages contain no
external-CDN references.

The files ride along in the wheel like any other package data (hatchling
packages everything under `src/qiita_control_plane/`), so there is
**nothing to script at deploy time** — `make build` + restart picks them up
exactly as it does `landing.css`.

## Vendored asset provenance & refreshing

Pinned versions (match what FastAPI's bundled defaults expect — Swagger UI
v5, ReDoc v2):

| File | Package | Version |
|---|---|---|
| `static/swagger-ui-bundle.js` | `swagger-ui-dist` | 5.32.6 |
| `static/swagger-ui.css` | `swagger-ui-dist` | 5.32.6 |
| `static/redoc.standalone.js` | `redoc` | 2.5.3 |

This is the only recurring maintenance, and it's rare (a security fix or a
wanted UI feature — not a per-deploy task). To refresh, re-download the
pinned versions and update this table:

```bash
cd qiita-control-plane/src/qiita_control_plane/static
SW=<new-swagger-version>; RD=<new-redoc-version>
curl -sSfL "https://cdn.jsdelivr.net/npm/swagger-ui-dist@${SW}/swagger-ui-bundle.js" -o swagger-ui-bundle.js
curl -sSfL "https://cdn.jsdelivr.net/npm/swagger-ui-dist@${SW}/swagger-ui.css"        -o swagger-ui.css
curl -sSfL "https://cdn.jsdelivr.net/npm/redoc@${RD}/bundles/redoc.standalone.js"     -o redoc.standalone.js
```

Then run `uv run pytest tests/test_api_docs.py` to confirm the pages still
render from the new files with no CDN leakage.
