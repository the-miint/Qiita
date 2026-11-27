"""The process-wide dispatch cap: a FIFO semaphore whose limit can change at runtime.

`asyncio.Semaphore` fixes its size at construction, and the dispatch cap has to be
retunable on a running control plane (`PATCH /work-ticket/dispatch-cap`). This keeps
the semaphore's contract — `async with`, FIFO wake order, `locked()` — and adds a
limit that is the boot value (`DISPATCH_MAX_INFLIGHT`) unless an operator override
replaces it. What the cap bounds and how it is sized against the connection pool
is in the comment above `dispatch.dispatch_cap_fits_pool`.

Raising the limit starts waiters at once. Lowering it cancels nothing: tasks already
holding a slot keep it, and waiters start only once `running` drops below the new
limit.

Not thread-safe; every caller runs on the control plane's event loop.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from types import TracebackType

from qiita_common.models import DISPATCH_MAX_INFLIGHT_CHOICES

_log = logging.getLogger(__name__)


class DispatchSemaphore:
    """FIFO counting semaphore with a boot `default` limit and an optional override.

    A slot is handed to the head waiter at the moment one frees (`running` is
    incremented on its behalf), so a task arriving later cannot take it first.
    """

    def __init__(self, default: int) -> None:
        _check_choice(default, what="default")
        self._default = default
        self._override: int | None = None
        self._running = 0
        self._waiters: deque[asyncio.Future[None]] = deque()

    @property
    def default(self) -> int:
        """The limit read from DISPATCH_MAX_INFLIGHT at boot; a restart reverts to it."""
        return self._default

    @property
    def override(self) -> int | None:
        """The operator's runtime limit, or None when the default applies."""
        return self._override

    @property
    def limit(self) -> int:
        """The limit in force: the override when set, else the default."""
        return self._default if self._override is None else self._override

    @property
    def running(self) -> int:
        """Slots held now. Can exceed `limit` after a lowering, until holders finish."""
        return self._running

    @property
    def waiting(self) -> int:
        """Tasks queued for a slot."""
        return sum(1 for w in self._waiters if not w.done())

    def locked(self) -> bool:
        """True when an `acquire` now would wait: no free slot, or others queued first."""
        return self._running >= self.limit or self.waiting > 0

    def set_override(self, value: int | None, *, ceiling: int) -> None:
        """Set the runtime limit, or clear it with None, and start any waiters it frees.

        Raises ValueError when `value` is not one of `DISPATCH_MAX_INFLIGHT_CHOICES`
        or is above `ceiling`, the largest value the connection pool built at boot
        can carry (`dispatch.max_settable_dispatch_max_inflight`).
        """
        if value is not None:
            _check_choice(value, what="override")
            if value > ceiling:
                raise ValueError(
                    f"override {value} is above {ceiling}, the most this control plane's"
                    f" connection pool (sized at boot from DISPATCH_MAX_INFLIGHT="
                    f"{self._default}) can carry; raise DISPATCH_MAX_INFLIGHT and restart"
                )
            if value < self._default:
                _log.warning(
                    "dispatch cap lowered to %d, below the DISPATCH_MAX_INFLIGHT default of"
                    " %d. The override is in-memory: a control-plane restart drops it and"
                    " dispatch resumes at %d. Re-apply after any restart (the unit is"
                    " Restart=on-failure, so a crash counts).",
                    value,
                    self._default,
                    self._default,
                )
        self._override = value
        _log.info(
            "dispatch cap now %d (default %d, override %s); %d running, %d waiting",
            self.limit,
            self._default,
            self._override,
            self._running,
            self.waiting,
        )
        self._wake()

    async def acquire(self) -> None:
        """Take a slot, waiting in FIFO order behind earlier callers."""
        if not self.locked():
            self._running += 1
            return
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        try:
            await waiter
        except asyncio.CancelledError:
            if waiter.done() and not waiter.cancelled():
                # `_wake` handed this waiter a slot in the same loop iteration the
                # cancel arrived; give it back so it is not lost.
                self._running -= 1
                self._wake()
            raise
        finally:
            try:
                self._waiters.remove(waiter)
            except ValueError:
                pass  # `_wake` already popped it

    def release(self) -> None:
        """Return a slot and hand it to the head waiter if the limit allows."""
        if self._running <= 0:
            raise RuntimeError("DispatchSemaphore released more times than acquired")
        self._running -= 1
        self._wake()

    def _wake(self) -> None:
        while self._waiters and self._running < self.limit:
            waiter = self._waiters.popleft()
            if waiter.done():
                continue  # cancelled while queued
            self._running += 1
            waiter.set_result(None)

    async def __aenter__(self) -> None:
        await self.acquire()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()


def _check_choice(value: int, *, what: str) -> None:
    if value not in DISPATCH_MAX_INFLIGHT_CHOICES:
        raise ValueError(
            f"dispatch cap {what} must be one of {list(DISPATCH_MAX_INFLIGHT_CHOICES)}, got {value}"
        )
