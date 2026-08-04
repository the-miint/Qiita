"""The lifespan's logging bring-up.

Without `configure_logging()` in the lifespan, the root logger has no handler and
every `_log.info` in the service falls through to Python's WARNING-only lastResort —
which is precisely the bug that shipped: the CP's entire operational narration was
invisible in production while the code that wrote it looked correct.

Two properties, neither of which any other test covers: the lifespan calls it, and the
service emits one unconditional INFO at boot. The second is what the deploy's journal
check matches, and it has to be unconditional — every other `.info()` in either service
is work-triggered, so an idle service would narrate nothing and a journal with no app
lines could not be distinguished from a service still carrying the bug.
"""

import base64
import logging
import secrets

import pytest
from fastapi import FastAPI


class _StopAfterBoot(Exception):
    """Raised from a stubbed `get_pool` to end the lifespan once the boot narration is
    done — everything after it needs a real postgres, and none of it is under test."""


def _minimal_boot_env(monkeypatch, tmp_path):
    """Every var `Settings.from_env()` hard-requires (its `require_env` calls).

    Set here rather than in a conftest because the boot narration sits between
    from_env() and the first DB call, so these tests need from_env() to SUCCEED — a
    fail-fast on any one of these would stop the lifespan before the line under test,
    and the test would pass or fail for the wrong reason."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    monkeypatch.setenv(
        "FLIGHT_TICKET_SIGNING_KEY", base64.b64encode(secrets.token_bytes(32)).decode()
    )
    monkeypatch.setenv(
        "LOGIN_COOKIE_SECRET_KEY", base64.b64encode(secrets.token_bytes(32)).decode()
    )
    monkeypatch.setenv("PATH_SCRATCH", str(tmp_path))
    monkeypatch.setenv("CONTACT_EMAIL", "ops@example.com")


async def test_lifespan_configures_logging_and_narrates_boot(monkeypatch, caplog, tmp_path):
    from qiita_control_plane import main as cp_main

    _minimal_boot_env(monkeypatch, tmp_path)

    calls = []

    def _record(level=None):
        calls.append(level)
        return "INFO"

    # configure_logging is STUBBED rather than observed: the real one calls
    # basicConfig(force=True), which tears out caplog's handler and would take the
    # boot record with it — the assertion below would then fail against correct code.
    monkeypatch.setattr(cp_main, "configure_logging", _record)
    monkeypatch.setattr(cp_main, "install_authorization_scrub", lambda *a, **k: None)

    async def _no_pool(*a, **k):
        raise _StopAfterBoot

    monkeypatch.setattr(cp_main, "get_pool", _no_pool)

    app = FastAPI()
    with caplog.at_level(logging.INFO, logger="qiita_control_plane.main"):
        with pytest.raises(_StopAfterBoot):
            async with cp_main.lifespan(app):
                pass

    assert calls == [None], "the lifespan must call configure_logging() exactly once"
    messages = [r.getMessage() for r in caplog.records]
    boot = [m for m in messages if "control-plane up" in m]
    assert boot, f"expected one unconditional boot INFO; got {messages}"
    # The level is the payload: it is the only way an operator can tell a deliberately
    # quiet journal (LOG_LEVEL=WARNING) from a broken one.
    assert "log_level=INFO" in boot[0]


async def test_boot_line_carries_the_configured_logger_name(monkeypatch, caplog, tmp_path):
    """The deploy check greps `INFO <dotted.logger.name>`, which only a configured root
    logger produces — uvicorn's own `INFO:` + padding reaches the journal either way.
    Pin the record's logger name so a module move can't silently break that check."""
    from qiita_control_plane import main as cp_main

    _minimal_boot_env(monkeypatch, tmp_path)
    monkeypatch.setattr(cp_main, "configure_logging", lambda *a, **k: "INFO")
    monkeypatch.setattr(cp_main, "install_authorization_scrub", lambda *a, **k: None)

    async def _no_pool(*a, **k):
        raise _StopAfterBoot

    monkeypatch.setattr(cp_main, "get_pool", _no_pool)

    app = FastAPI()
    with caplog.at_level(logging.INFO, logger="qiita_control_plane.main"):
        with pytest.raises(_StopAfterBoot):
            async with cp_main.lifespan(app):
                pass

    boot = [r for r in caplog.records if "control-plane up" in r.getMessage()]
    assert boot and boot[0].name == "qiita_control_plane.main"
