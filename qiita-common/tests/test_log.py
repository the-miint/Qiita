"""Tests for the AuthorizationScrubFilter logging utility."""

import logging


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
