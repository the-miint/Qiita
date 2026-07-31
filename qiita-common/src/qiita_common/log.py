"""Logging utilities — root-logger setup and the Authorization-header scrubber.

`configure_logging()` is what makes any of this reachable: neither service
configured the root logger, so records fell through to Python's lastResort
handler at WARNING and every `_log.info` was silently dropped in production. It
also gives the scrubber below something to attach to — see the ordering note
under **Where to install**.

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
logger, so it must run **after** `configure_logging()` — uvicorn puts its
handlers on its own loggers, not root, so without that call root has no
handlers and the scrubber is inert. Handlers added afterward are not covered.

**Who installs it.** Every long-running service that wraps httpx via
`qiita_common.client.ControlPlaneClient` (or otherwise forwards bearer
tokens). Today: the control plane and the orchestrator.

Example:

    from contextlib import asynccontextmanager
    from qiita_common.log import configure_logging, install_authorization_scrub

    @asynccontextmanager
    async def lifespan(app):
        configure_logging()
        install_authorization_scrub()
        yield
"""

import logging
import os
import re

# Root-logger level when LOG_LEVEL is unset. INFO, not WARNING: the services'
# operational narration — fan-out pump decisions, dispatch lifecycle, sweeper
# passes — is all _log.info, and Python's default (no root handler, lastResort at
# WARNING) silently drops every line of it.
_DEFAULT_LOG_LEVEL = "INFO"
_LOG_FORMAT = "%(levelname)s %(name)s: %(message)s"


def configure_logging(level: str | None = None) -> None:
    """Install a root handler and set the root level.

    `level` defaults to the LOG_LEVEL env var, then INFO. Raises ValueError on an
    unknown name rather than falling back, so a typo is a boot failure instead of
    silently-missing logs.

    Idempotent: re-running replaces the handler rather than stacking a second one
    (which would double every line). Call **before**
    `install_authorization_scrub()`, which walks the handlers this installs — with
    no root handler it has nothing to attach to and is inert.

    Timestamps are deliberately absent from the format: both services run under
    systemd, and the journal already stamps every line.
    """
    name = (level or os.environ.get("LOG_LEVEL") or _DEFAULT_LOG_LEVEL).upper()
    resolved = logging.getLevelNamesMapping().get(name)
    if resolved is None:
        valid = ", ".join(sorted(logging.getLevelNamesMapping()))
        raise ValueError(f"LOG_LEVEL must be one of: {valid}; got {name!r}")
    logging.basicConfig(level=resolved, format=_LOG_FORMAT, force=True)


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
