"""Unit tests for the qiita-admin login flow's pure helpers.

Covers the loopback handler (captures ot_code, ignores probes, renders the
done page) and the token writer (atomic write, mode 0600, parent mkdir).
The full process-spawn smoke is exercised by tests/integration via the
real control plane + JwksHarness; this file's tests are stdlib-only and run
in <100ms without I/O beyond a tmp dir.

The helpers live in qiita_control_plane.cli._common; the end-user `qiita`
CLI shares them with qiita-admin.
"""

import http.client
import http.server
import os
import threading
from pathlib import Path

import pytest
from qiita_common.api_paths import LOOPBACK_HOST

from qiita_control_plane.cli._common import (
    LoopbackResult,
    bind_loopback,
    loopback_handler_factory,
    write_token,
)

# ---------------------------------------------------------------------------
# Loopback handler
# ---------------------------------------------------------------------------


@pytest.fixture
def loopback_server():
    """Spin up a one-shot loopback server bound to an OS-picked port. Yields
    `(server, port, result)`; tear-down is in the finally."""
    result = LoopbackResult()
    server, port = bind_loopback()
    server.RequestHandlerClass = loopback_handler_factory(result)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server, port, result
    finally:
        server.shutdown()
        server.server_close()


def test_loopback_captures_ot_code_and_signals_main_thread(loopback_server):
    server, port, result = loopback_server

    conn = http.client.HTTPConnection(LOOPBACK_HOST, port, timeout=2)
    conn.request("GET", "/?ot_code=hello-world-123")
    resp = conn.getresponse()
    body = resp.read()
    conn.close()

    assert resp.status == 200
    assert b"qiita login complete" in body
    assert result.event.is_set()
    assert result.ot_code == "hello-world-123"


def test_loopback_ignores_favicon_and_other_probes(loopback_server):
    """Background tabs / extensions / browsers may probe /favicon.ico or
    other paths. None of those should set the event prematurely."""
    server, port, result = loopback_server

    for path in ("/favicon.ico", "/robots.txt", "/some-random-path"):
        conn = http.client.HTTPConnection(LOOPBACK_HOST, port, timeout=2)
        conn.request("GET", path)
        resp = conn.getresponse()
        resp.read()
        conn.close()
        assert resp.status == 404
        assert not result.event.is_set()
        assert result.ot_code is None


def test_loopback_ignores_root_get_without_ot_code(loopback_server):
    """A bare GET / shouldn't trip the capture; the handler must require
    the ?ot_code= query param to set the event."""
    server, port, result = loopback_server

    conn = http.client.HTTPConnection(LOOPBACK_HOST, port, timeout=2)
    conn.request("GET", "/")
    resp = conn.getresponse()
    resp.read()
    conn.close()

    assert resp.status == 404
    assert not result.event.is_set()


# ---------------------------------------------------------------------------
# Token writer
# ---------------------------------------------------------------------------


def test_write_token_creates_file_with_mode_0600(tmp_path: Path):
    target = tmp_path / "subdir" / "token"
    write_token(target, "qk_abcdefghij")

    assert target.read_text() == "qk_abcdefghij"
    # Check mode (mask off type bits).
    mode = os.stat(target).st_mode & 0o777
    assert mode == 0o600


def test_write_token_creates_parent_dir(tmp_path: Path):
    """If the parent dir doesn't exist, we should mkdir it (with restrictive
    mode) rather than failing."""
    target = tmp_path / "fresh-dir" / "token"
    assert not target.parent.exists()

    write_token(target, "qk_xyz")

    assert target.parent.is_dir()
    assert target.read_text() == "qk_xyz"


def test_write_token_overwrites_existing(tmp_path: Path):
    """Re-running `qiita-admin login` should atomically replace the prior
    token, not append or refuse."""
    target = tmp_path / "token"
    target.write_text("qk_old_token")

    write_token(target, "qk_new_token")

    assert target.read_text() == "qk_new_token"


def test_write_token_atomic_replace_leaves_no_tmp(tmp_path: Path):
    """The implementation writes to .tmp and renames; on success the .tmp
    file should not be left behind."""
    target = tmp_path / "token"
    write_token(target, "qk_value")

    leftover = list(tmp_path.glob("*.tmp"))
    assert leftover == []
