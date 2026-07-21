"""Deploy-time gate for miint extension staging.

`scripts/stage-miint-extension.sh` FORCE-installs the mirror's current miint
build into `MIINT_EXTENSION_DIRECTORY` every run. That's safe but unconditional,
so `redeploy.sh` used to prompt the operator on every deploy. This module lets
the deploy skip staging when the staged build already matches what the mirror
serves — "only run when needed."

The hard part: there is **no local version pin** for miint. `qiita_common.duckdb_miint`
installs from the team mirror, which *is* the source of truth for the version
(see that module's docstring). So "is the staged build current?" splits in two:

  * **Local** — a DuckDB-version / platform / repo change is detectable without
    the network (DuckDB namespaces the extension dir by version+platform).
  * **Mirror bump** — a new build at the same DuckDB version is only detectable
    by asking the mirror. We do a cheap HTTP ``HEAD`` on the extension URL and
    compare the ``ETag`` / ``Last-Modified`` against a marker written at stage
    time.

Every uncertain path — no marker, missing validators, any network error —
returns "not current" so the caller re-stages. The gate never skips on doubt;
the worst case is an unnecessary (idempotent) FORCE INSTALL, exactly today's
behavior minus the prompt.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from pathlib import Path

import duckdb
from qiita_common.duckdb_miint import MIINT_EXTENSION_DIRECTORY_VAR, miint_repo

log = logging.getLogger(__name__)

# Marker file recording the fingerprint of the build last staged into
# MIINT_EXTENSION_DIRECTORY. Lives inside that dir so it shares the dir's
# lifecycle — clearing the extension dir clears the marker too.
MARKER_NAME = ".qiita-miint-staged.json"

# HEAD timeout (seconds). Short: a slow/unreachable mirror should fall through
# to staging quickly, not stall the deploy.
_HEAD_TIMEOUT = 10.0


def _duckdb_platform() -> str:
    """DuckDB's platform string (e.g. ``linux_amd64``, ``osx_arm64``) — the
    segment DuckDB uses in an extension download URL. Needs no extension, so a
    bare in-memory connection answers it."""
    con = duckdb.connect()
    try:
        return con.execute("PRAGMA platform").fetchone()[0]
    finally:
        con.close()


def _extension_url(repo: str, duckdb_version: str, platform: str) -> str:
    """The URL DuckDB downloads miint from, mirroring its own extension-repo
    layout: ``<repo>/v<duckdb_version>/<platform>/<name>.duckdb_extension.gz``.

    NOTE: this re-derives DuckDB's URL convention by hand — `qiita_common.duckdb_miint`
    never builds it, it hands the bare repo to ``INSTALL miint FROM '<repo>'`` and
    lets DuckDB compute the object path. So this must track DuckDB's layout. If it
    drifts, the HEAD probes a different object than the install downloads; the
    fail-safe is that a wrong/missing path 404s → ``_head_validators`` raises →
    ``staging_is_current`` re-stages (never a wrong skip). Re-verify this shape on
    a DuckDB major bump."""
    return f"{repo}/v{duckdb_version}/{platform}/miint.duckdb_extension.gz"


def _head_validators(url: str) -> dict[str, str]:
    """``HEAD`` *url*; return the ``ETag`` / ``Last-Modified`` validators (empty
    string when a header is absent). Raises ``urllib.error.URLError`` / ``OSError``
    on any network failure — callers treat that as 'cannot prove current'."""
    req = urllib.request.Request(url, method="HEAD")  # noqa: S310 — fixed mirror scheme
    with urllib.request.urlopen(req, timeout=_HEAD_TIMEOUT) as resp:
        return {
            "etag": resp.headers.get("ETag", ""),
            "last_modified": resp.headers.get("Last-Modified", ""),
        }


def staging_fingerprint() -> dict[str, str]:
    """Identity of the miint build the mirror serves *now* for this DuckDB
    version + platform: the local ``(duckdb_version, platform, repo)`` triple
    plus the mirror object's ``ETag`` / ``Last-Modified``. The HEAD can raise on
    network failure — the caller decides whether that's fatal (staging) or just
    means no marker is written."""
    platform = _duckdb_platform()
    repo = miint_repo()
    fp = {"duckdb_version": duckdb.__version__, "platform": platform, "repo": repo}
    fp.update(_head_validators(_extension_url(repo, duckdb.__version__, platform)))
    return fp


def marker_path() -> Path | None:
    """Path to the staging marker, or ``None`` when ``MIINT_EXTENSION_DIRECTORY``
    is unset (dev/test stage into the DuckDB default dir — no gate, always stage)."""
    ext_dir = os.environ.get(MIINT_EXTENSION_DIRECTORY_VAR)
    return Path(ext_dir) / MARKER_NAME if ext_dir else None


def write_staging_marker() -> None:
    """Record the just-staged build's fingerprint next to the extension. Called
    after a successful FORCE INSTALL. Non-fatal on failure: staging already
    succeeded; a missing marker only costs an unnecessary re-stage next deploy."""
    marker = marker_path()
    if marker is None:
        return
    try:
        marker.write_text(json.dumps(staging_fingerprint(), indent=2, sort_keys=True))
    except (OSError, urllib.error.URLError) as exc:
        log.warning("miint staged, but staging marker not written (%s): %s", marker, exc)


def staging_is_current() -> bool:
    """True iff the staged miint build matches what the mirror serves now.

    Returns ``False`` — re-stage — on any uncertainty: no marker, a changed
    DuckDB-version / platform / repo, a mirror that offers no ``ETag`` /
    ``Last-Modified`` to compare, a changed validator, or any network error.
    Never skips on doubt.

    The skip engages only when *both* the stored and current ``ETag`` /
    ``Last-Modified`` match. A mirror/CDN that varies either validator per
    request (a rotating ETag, a recomputed Last-Modified) will re-stage every
    deploy — safe (the FORCE INSTALL is idempotent), but the optimization
    silently never fires. If you see "always staging," check the mirror's HEAD
    validators are stable.
    """
    marker = marker_path()
    if marker is None or not marker.is_file():
        return False
    try:
        stored = json.loads(marker.read_text())
    except OSError, ValueError:
        return False

    platform = _duckdb_platform()
    repo = miint_repo()
    # Local triple — a DuckDB / platform / repo change needs no network.
    if (
        stored.get("duckdb_version"),
        stored.get("platform"),
        stored.get("repo"),
    ) != (duckdb.__version__, platform, repo):
        return False

    try:
        current = _head_validators(_extension_url(repo, duckdb.__version__, platform))
    except (urllib.error.URLError, OSError) as exc:
        log.info("miint mirror HEAD failed (%s); will re-stage to be safe", exc)
        return False

    # A mirror that returns neither validator gives us nothing to compare —
    # we cannot prove the build is unchanged, so re-stage.
    if not (current["etag"] or current["last_modified"]):
        return False

    return (stored.get("etag", ""), stored.get("last_modified", "")) == (
        current["etag"],
        current["last_modified"],
    )
