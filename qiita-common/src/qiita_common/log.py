"""Logging utilities — primarily the Authorization-header scrubber.

httpx's `INFO`-level request logs sometimes include the request headers
verbatim. If a log line carries a `Bearer qk_...` value, the leak ends up
on disk forever. This filter walks every log record and rewrites any
`Authorization` value to `Bearer <redacted>` before the formatter sees it.

Install once at application startup:

    import logging
    from qiita_common.log import AuthorizationScrubFilter

    logging.getLogger().addFilter(AuthorizationScrubFilter())
"""

from __future__ import annotations

import logging
import re

# Match an Authorization header value in any reasonable string serialisation
# (key=value, "key": "value", JSON, repr, dict, etc.). The capture group is
# the full string up to and including "Bearer ", which we keep verbatim;
# everything after is replaced with <redacted>.
_AUTH_RE = re.compile(
    r"((?:authorization\W*)?Bearer\s+)\S+",
    re.IGNORECASE,
)


def scrub_authorization(text: str) -> str:
    """Return `text` with any `Bearer <token>` substring replaced by
    `Bearer <redacted>`. Pure; safe to call on arbitrary log strings."""
    return _AUTH_RE.sub(r"\1<redacted>", text)


class AuthorizationScrubFilter(logging.Filter):
    """Filter that rewrites Authorization values in log messages and args.

    Filters return True to allow the record through; we never drop records,
    we only scrub them in place.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        # `_scrub_value` is the same str-or-passthrough check that used to
        # live inline here; routing record.msg through it keeps the two
        # branches in sync.
        record.msg = self._scrub_value(record.msg)
        if record.args:
            record.args = self._scrub_args(record.args)
        return True

    def _scrub_args(self, args):
        if isinstance(args, dict):
            return {k: self._scrub_value(v) for k, v in args.items()}
        if isinstance(args, tuple):
            return tuple(self._scrub_value(v) for v in args)
        return args

    @staticmethod
    def _scrub_value(v):
        if isinstance(v, str):
            return scrub_authorization(v)
        return v
