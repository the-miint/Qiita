"""Logging utilities — the Authorization-header scrubber for log records.

httpx's `INFO`-level request logs sometimes include headers verbatim;
without scrubbing, any `Bearer qk_...` value ends up on disk forever.
`AuthorizationScrubFilter` rewrites the token portion of every
`Bearer <token>` substring it finds to `Bearer <redacted>` before the
formatter sees it.

**Why a visible placeholder, not a strip.** Keeping `<redacted>` rather
than dropping the token outright preserves a per-line audit signal that
auth happened, keeps the message's structural shape intact for
downstream log parsers, and turns the placeholder into a leak detector
— grepping for `<redacted>` confirms the filter is in the path, while
any raw `Bearer qk_...` or `Bearer eyJ...` in logs is an immediate red
flag.

**Where to install.** A `Logger`'s filters run only for records
originating at that logger; propagation up the tree skips ancestor
filters. Records from named loggers (`httpx`, `uvicorn`) therefore need
the filter attached to **handlers**, not loggers.
`install_authorization_scrub()` attaches to every handler on the root
logger. Call it after logging configuration is complete (e.g. inside
FastAPI's `lifespan`, once uvicorn's handlers are in place) and before
any request handlers run. Handlers added afterward are not covered.

**Who installs it.** Every long-running service that wraps httpx via
`qiita_common.client.ControlPlaneClient` (or otherwise forwards bearer
tokens). Today: the control plane and the orchestrator.

Example:

    from contextlib import asynccontextmanager
    from qiita_common.log import install_authorization_scrub

    @asynccontextmanager
    async def lifespan(app):
        install_authorization_scrub()
        yield
"""

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

    Attach to a `logging.Handler`, not a `logging.Logger` — see the module
    docstring for why. Prefer `install_authorization_scrub()` over
    instantiating this directly.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        record.msg = self._scrub_value(record.msg)
        if record.args:
            record.args = self._scrub_args(record.args)
        # Always pass the record through; we scrub, never drop.
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


def install_authorization_scrub(logger: logging.Logger | None = None) -> None:
    """Attach `AuthorizationScrubFilter` to every handler on `logger`
    (root logger if None). Idempotent — handlers that already carry the
    filter are skipped.

    The filter must live on handlers, not loggers, because Python skips
    ancestor-logger filters when records propagate up the tree. Handler
    filters are consulted for every record reaching the handler
    regardless of which logger emitted it.

    Run after logging configuration is complete; handlers added later
    will not be covered.
    """
    target = logger if logger is not None else logging.getLogger()
    for h in target.handlers:
        if not any(isinstance(f, AuthorizationScrubFilter) for f in h.filters):
            h.addFilter(AuthorizationScrubFilter())
