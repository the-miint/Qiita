"""`open_miint_conn` raises DuckDB's HTTP retry settings for its session so a dropped
connection to ENA's file server is retried rather than failing the run."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler

import duckdb
import pytest
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.duckdb_miint import miint_load_sql
from qiita_common.testing.loopback import serve_on_loopback

from qiita_compute_orchestrator.miint import MIINT_HTTP_RETRY_SETTINGS, open_conn, open_miint_conn

_RECORDS = 40
_FASTQ = "".join(f"@r{i}\nACGTACGTAC\n+\nIIIIIIIIII\n" for i in range(_RECORDS)).encode()
_DROPPED_CONNECTIONS = 5


@pytest.fixture
def miint_conn():
    conn = open_miint_conn()
    yield conn
    conn.close()


def _http_settings(conn) -> dict[str, tuple[float, str]]:
    """DuckDB reports the backoff as `2.0`, so values are compared as numbers."""
    rows = conn.execute(
        "SELECT name, value, scope FROM duckdb_settings() WHERE name LIKE 'http_retr%'"
    ).fetchall()
    return {name: (float(value), scope) for name, value, scope in rows}


def test_retry_settings_are_session_values_and_global_keeps_defaults(miint_conn):
    """GLOBAL, which a cursor reads, must match a plain connection's defaults."""
    session = _http_settings(miint_conn)
    assert {n: v for n, (v, _) in session.items()} == {
        n: float(v) for n, v in MIINT_HTTP_RETRY_SETTINGS.items()
    }
    assert {scope for _, scope in session.values()} == {"LOCAL"}
    plain = open_conn()
    try:
        plain.execute(miint_load_sql())
        defaults = _http_settings(plain)
    finally:
        plain.close()
    assert _http_settings(miint_conn.cursor()) == defaults


class _FlakyFastqHandler(BaseHTTPRequestHandler):
    """Drops the first connections on HEAD, then partway through a 206 body, then
    serves `_FASTQ` with Range support."""

    lock = threading.Lock()
    head_drops = 0
    body_drops = 0
    head_limit = _DROPPED_CONNECTIONS
    body_limit = _DROPPED_CONNECTIONS

    def _drop(self) -> None:
        self.close_connection = True
        self.connection.close()

    def do_HEAD(self) -> None:
        with type(self).lock:
            if type(self).head_drops < type(self).head_limit:
                type(self).head_drops += 1
                self._drop()
                return
        self.send_response(200)
        self.send_header("Content-Length", str(len(_FASTQ)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()

    def do_GET(self) -> None:
        start, end = 0, len(_FASTQ) - 1
        range_header = self.headers.get("Range")
        if range_header:
            lo, _, hi = range_header.removeprefix("bytes=").partition("-")
            start = int(lo)
            end = min(int(hi), end) if hi else end
        body = _FASTQ[start : end + 1]
        with type(self).lock:
            cut = type(self).body_drops < type(self).body_limit
            if cut:
                type(self).body_drops += 1
        self.send_response(206 if range_header else 200)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
        if range_header:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(_FASTQ)}")
        self.end_headers()
        if cut:
            self.wfile.write(body[: len(body) // 2])
            self.wfile.flush()
            self._drop()
            return
        self.wfile.write(body)

    def log_message(self, *args) -> None:
        pass


@contextmanager
def _flaky_url(head_limit: int, body_limit: int):
    handler = _FlakyFastqHandler
    handler.head_drops = handler.body_drops = 0
    handler.head_limit = head_limit
    handler.body_limit = body_limit
    with serve_on_loopback(handler) as port:
        yield f"http://{LOOPBACK_HOST}:{port}/reads.fastq"


@pytest.fixture
def flaky_fastq_url():
    with _flaky_url(_DROPPED_CONNECTIONS, _DROPPED_CONNECTIONS) as url:
        yield url


def _read_ids(conn, url: str) -> list[str]:
    return [r[0] for r in conn.execute("SELECT read_id FROM read_fastx(?)", [url]).fetchall()]


def test_read_survives_more_dropped_connections_than_duckdb_default(miint_conn, flaky_fastq_url):
    """Five dropped connections on HEAD and five cut bodies exceed DuckDB's default of
    3 retries; the raised setting recovers and every record comes back."""
    miint_conn.execute("SET threads=1")
    miint_conn.execute("SET http_retry_wait_ms=10")
    assert len(_read_ids(miint_conn, flaky_fastq_url)) == _RECORDS
    assert _FlakyFastqHandler.head_drops == _DROPPED_CONNECTIONS
    assert _FlakyFastqHandler.body_drops == _DROPPED_CONNECTIONS


@pytest.mark.parametrize(
    ("head_limit", "body_limit", "message"),
    [
        (_DROPPED_CONNECTIONS, 0, "Server returned nothing"),
        (0, _DROPPED_CONNECTIONS, "partial file"),
    ],
    ids=["head-drops", "body-cuts"],
)
def test_read_fails_at_duckdb_default_retries(miint_conn, head_limit, body_limit, message):
    """Guards the test above: at DuckDB's defaults each failure mode alone is fatal."""
    miint_conn.execute("SET threads=1")
    for name in MIINT_HTTP_RETRY_SETTINGS:
        miint_conn.execute(f"RESET {name}")
    miint_conn.execute("SET http_retry_wait_ms=10")
    with _flaky_url(head_limit, body_limit) as url:
        with pytest.raises(duckdb.IOException, match=message):
            _read_ids(miint_conn, url)
