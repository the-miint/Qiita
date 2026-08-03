"""Tests for the logging utilities — root-logger setup and the Authorization scrubber."""

import logging

import pytest


def test_scrub_authorization_replaces_bearer_token_in_string():
    from qiita_common.log import scrub_authorization

    raw = "GET /api/v1/auth/whoami headers={'Authorization': 'Bearer qk_AAAAAAAAAAAA'}"
    # The regex's `\S+` consumes the trailing `'}` along with the token —
    # acceptable because the goal is "the secret is gone", and the test
    # asserts the exact post-scrub form so any regex tightening surfaces.
    assert scrub_authorization(raw) == (
        "GET /api/v1/auth/whoami headers={'Authorization': 'Bearer <redacted>"
    )


def test_scrub_authorization_handles_jwt_shape():
    from qiita_common.log import scrub_authorization

    raw = "Authorization: Bearer eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.signature"
    assert scrub_authorization(raw) == "Authorization: Bearer <redacted>"


def test_scrub_authorization_is_idempotent():
    from qiita_common.log import scrub_authorization

    once = scrub_authorization("Bearer qk_X" + "A" * 50)
    assert once == "Bearer <redacted>"
    assert scrub_authorization(once) == once


def test_filter_scrubs_record_msg(caplog):
    from qiita_common.log import AuthorizationScrubFilter

    logger = logging.getLogger("test-scrub")
    logger.addFilter(AuthorizationScrubFilter())
    with caplog.at_level(logging.INFO, logger="test-scrub"):
        logger.info("outgoing: Authorization: Bearer qk_DEADBEEF" + "A" * 40)
    assert caplog.records[0].getMessage() == "outgoing: Authorization: Bearer <redacted>"


def test_filter_scrubs_record_args_tuple(caplog):
    from qiita_common.log import AuthorizationScrubFilter

    logger = logging.getLogger("test-scrub-args")
    logger.addFilter(AuthorizationScrubFilter())
    with caplog.at_level(logging.INFO, logger="test-scrub-args"):
        logger.info("header: %s", "Authorization: Bearer qk_LEAKED" + "A" * 40)
    assert caplog.records[0].getMessage() == "header: Authorization: Bearer <redacted>"


def test_filter_passes_records_through():
    """Filter must not drop records — only scrub them."""
    from qiita_common.log import AuthorizationScrubFilter

    f = AuthorizationScrubFilter()
    record = logging.LogRecord(
        name="x",
        level=logging.INFO,
        pathname="",
        lineno=0,
        msg="benign message",
        args=None,
        exc_info=None,
    )
    assert f.filter(record) is True


def test_install_authorization_scrub_catches_propagated_records():
    """Records emitted at a named logger propagate to root handlers and
    must be scrubbed there. Regression for the prior install pattern,
    which attached the filter to the root logger directly and only
    caught records originating at root — Python's logging module skips
    ancestor-logger filters on propagation.
    """
    from qiita_common.log import install_authorization_scrub

    root = logging.getLogger()
    captured: list[str] = []

    class CaptureHandler(logging.Handler):
        def emit(self, record):
            captured.append(record.getMessage())

    handler = CaptureHandler()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    root.handlers = [handler]
    root.setLevel(logging.DEBUG)
    try:
        install_authorization_scrub()
        logging.getLogger("propagation-fixture").info(
            "Authorization: Bearer qk_PROPAGATED" + "X" * 40
        )
    finally:
        root.handlers = saved_handlers
        root.setLevel(saved_level)

    assert captured == ["Authorization: Bearer <redacted>"]


def test_install_authorization_scrub_is_idempotent():
    """Calling install twice on the same handler doesn't add a duplicate
    filter — handlers that already carry the filter are skipped."""
    from qiita_common.log import (
        AuthorizationScrubFilter,
        install_authorization_scrub,
    )

    root = logging.getLogger()
    handler = logging.Handler()
    saved_handlers = root.handlers[:]
    root.handlers = [handler]
    try:
        install_authorization_scrub()
        install_authorization_scrub()
        scrub_filters = [f for f in handler.filters if isinstance(f, AuthorizationScrubFilter)]
        assert len(scrub_filters) == 1
    finally:
        root.handlers = saved_handlers


def test_install_authorization_scrub_targets_passed_logger():
    """When called with an explicit logger, install attaches to that
    logger's handlers rather than root."""
    from qiita_common.log import (
        AuthorizationScrubFilter,
        install_authorization_scrub,
    )

    target = logging.getLogger("install-target-fixture")
    handler = logging.Handler()
    saved = target.handlers[:]
    target.handlers = [handler]
    try:
        install_authorization_scrub(target)
        assert any(isinstance(f, AuthorizationScrubFilter) for f in handler.filters)
    finally:
        target.handlers = saved


# ---------------------------------------------------------------------------
# configure_logging — root-logger setup, without which app INFO never lands
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _log_level_unset(monkeypatch):
    """Unset LOG_LEVEL for every test in this module.

    These tests exercise the env-reading path, so an exported LOG_LEVEL decides what
    they assert: `test_configure_logging_defaults_to_info` asserts INFO while calling
    the very code that would honour a `LOG_LEVEL=DEBUG` in the developer's shell, and
    failed there for a reason that has nothing to do with the code under test. The
    tests that want the env path set it themselves — this only pins the baseline."""
    monkeypatch.delenv("LOG_LEVEL", raising=False)


def _restore_root(saved_handlers, saved_level):
    root = logging.getLogger()
    root.handlers = saved_handlers
    root.setLevel(saved_level)


def test_configure_logging_defaults_to_info():
    from qiita_common.log import configure_logging

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = []
    try:
        configure_logging()
        assert root.level == logging.INFO
        assert root.handlers, "configure_logging must install a root handler"
    finally:
        _restore_root(saved_handlers, saved_level)


def test_configure_logging_honours_an_explicit_level():
    from qiita_common.log import configure_logging

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = []
    try:
        configure_logging("DEBUG")
        assert root.level == logging.DEBUG
    finally:
        _restore_root(saved_handlers, saved_level)


def test_configure_logging_is_case_insensitive():
    from qiita_common.log import configure_logging

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = []
    try:
        configure_logging("warning")
        assert root.level == logging.WARNING
    finally:
        _restore_root(saved_handlers, saved_level)


def test_configure_logging_rejects_an_unknown_level():
    """Fail loud: a typo'd level must not silently leave the default in place."""
    from qiita_common.log import configure_logging

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = []
    try:
        with pytest.raises(ValueError, match="LOG_LEVEL"):
            configure_logging("VERBOSE")
    finally:
        _restore_root(saved_handlers, saved_level)


def test_configure_logging_is_idempotent():
    """Re-running must not stack duplicate handlers (double-logged lines)."""
    from qiita_common.log import configure_logging

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = []
    try:
        configure_logging()
        configure_logging()
        assert len(root.handlers) == 1
    finally:
        _restore_root(saved_handlers, saved_level)


def test_configure_logging_reads_the_log_level_env_var(monkeypatch):
    """The LOG_LEVEL path is the one production takes — both units set it or inherit
    the default through it, and nothing else exercises it."""
    from qiita_common.log import configure_logging

    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = []
    try:
        assert configure_logging() == "WARNING"
        assert root.level == logging.WARNING
    finally:
        _restore_root(saved_handlers, saved_level)


def test_configure_logging_env_var_is_case_insensitive(monkeypatch):
    """An operator writing `LOG_LEVEL=debug` in an env file must not fail the boot."""
    from qiita_common.log import configure_logging

    monkeypatch.setenv("LOG_LEVEL", "debug")
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = []
    try:
        assert configure_logging() == "DEBUG"
        assert root.level == logging.DEBUG
    finally:
        _restore_root(saved_handlers, saved_level)


def test_configure_logging_rejects_an_unknown_env_level(monkeypatch):
    """A typo in the env file must keep the unit DOWN, not silently log at the default
    — the whole point of raising rather than falling back."""
    from qiita_common.log import configure_logging

    monkeypatch.setenv("LOG_LEVEL", "CHATTY")
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = []
    try:
        with pytest.raises(ValueError, match="LOG_LEVEL"):
            configure_logging()
    finally:
        _restore_root(saved_handlers, saved_level)


def test_configure_logging_explicit_level_beats_the_env(monkeypatch):
    """The argument is the override, not a second default."""
    from qiita_common.log import configure_logging

    monkeypatch.setenv("LOG_LEVEL", "ERROR")
    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = []
    try:
        assert configure_logging("DEBUG") == "DEBUG"
        assert root.level == logging.DEBUG
    finally:
        _restore_root(saved_handlers, saved_level)


@pytest.mark.parametrize("source", ["arg", "env"])
def test_configure_logging_rejects_notset(monkeypatch, source):
    """NOTSET is in getLevelNamesMapping() but resolves to 0 — a DEBUG-and-below
    firehose, not the "leave it at the default" it reads as. Rejected from both the
    argument and the env var, since production only ever reaches it via the env."""
    from qiita_common.log import configure_logging

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = []
    try:
        with pytest.raises(ValueError, match="LOG_LEVEL"):
            if source == "arg":
                configure_logging("NOTSET")
            else:
                monkeypatch.setenv("LOG_LEVEL", "NOTSET")
                configure_logging()
    finally:
        _restore_root(saved_handlers, saved_level)


def test_configure_logging_returns_the_default_level_name():
    """The return value is what the services narrate at boot, so it must be the
    resolved name rather than whatever was passed in (here: nothing)."""
    from qiita_common.log import configure_logging

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = []
    try:
        assert configure_logging() == "INFO"
    finally:
        _restore_root(saved_handlers, saved_level)


def test_app_info_records_reach_a_handler_after_configure():
    """The regression this exists for: a module logger's INFO must be emitted.

    Without a configured root handler, `_log.info(...)` from CP modules falls
    through to Python's lastResort handler, which is WARNING-only — so every
    fan-out pump decision was invisible in production.

    Deliberately not using `caplog`: `configure_logging` calls `basicConfig(force=True)`,
    which removes caplog's own root handler, so the record would land on the real
    handler and never reach the fixture. Capture on a handler installed afterwards
    instead, which is the path a deployed service actually takes.
    """
    from qiita_common.log import configure_logging

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = []
    try:
        configure_logging()
        seen: list[str] = []

        class _Capture(logging.Handler):
            def emit(self, record):
                seen.append(record.getMessage())

        root.addHandler(_Capture())
        logging.getLogger("qiita_control_plane.fanout_dispatch").info("released 8 ticket(s)")
        assert "released 8 ticket(s)" in seen
    finally:
        _restore_root(saved_handlers, saved_level)


def test_authorization_scrub_has_a_handler_to_attach_to_after_configure():
    """`install_authorization_scrub` walks root.handlers — with none, it is inert.

    Ordering is load-bearing: configure first, then install.
    """
    from qiita_common.log import (
        AuthorizationScrubFilter,
        configure_logging,
        install_authorization_scrub,
    )

    root = logging.getLogger()
    saved_handlers, saved_level = root.handlers[:], root.level
    root.handlers = []
    try:
        configure_logging()
        install_authorization_scrub()
        assert any(
            any(isinstance(f, AuthorizationScrubFilter) for f in h.filters) for h in root.handlers
        )
    finally:
        _restore_root(saved_handlers, saved_level)
