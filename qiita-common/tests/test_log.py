"""Tests for the AuthorizationScrubFilter logging utility."""

import logging


def test_scrub_authorization_replaces_bearer_token_in_string():
    from qiita_common.log import scrub_authorization

    raw = "GET /api/v1/auth/whoami headers={'Authorization': 'Bearer qk_AAAAAAAAAAAA'}"
    out = scrub_authorization(raw)
    assert "qk_AAAAAAAAAAAA" not in out
    assert "<redacted>" in out


def test_scrub_authorization_handles_jwt_shape():
    from qiita_common.log import scrub_authorization

    raw = "Authorization: Bearer eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.signature"
    out = scrub_authorization(raw)
    assert "eyJ" not in out
    assert "Bearer <redacted>" in out


def test_scrub_authorization_is_idempotent():
    from qiita_common.log import scrub_authorization

    once = scrub_authorization("Bearer qk_X" + "A" * 50)
    twice = scrub_authorization(once)
    assert once == twice


def test_filter_scrubs_record_msg(caplog):
    from qiita_common.log import AuthorizationScrubFilter

    logger = logging.getLogger("test-scrub")
    logger.addFilter(AuthorizationScrubFilter())
    with caplog.at_level(logging.INFO, logger="test-scrub"):
        logger.info("outgoing: Authorization: Bearer qk_DEADBEEF" + "A" * 40)
    rec = caplog.records[0]
    assert "qk_DEADBEEF" not in rec.getMessage()
    assert "<redacted>" in rec.getMessage()


def test_filter_scrubs_record_args_tuple(caplog):
    from qiita_common.log import AuthorizationScrubFilter

    logger = logging.getLogger("test-scrub-args")
    logger.addFilter(AuthorizationScrubFilter())
    with caplog.at_level(logging.INFO, logger="test-scrub-args"):
        logger.info("header: %s", "Authorization: Bearer qk_LEAKED" + "A" * 40)
    rec = caplog.records[0]
    assert "qk_LEAKED" not in rec.getMessage()


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
