"""Unit tests for `DispatchSemaphore`, the resizable FIFO dispatch cap."""

from __future__ import annotations

import asyncio
import logging

import pytest

from qiita_control_plane.dispatch_semaphore import DispatchSemaphore

pytestmark = pytest.mark.asyncio


async def _settle() -> None:
    """Let woken waiters run up to their next await."""
    for _ in range(5):
        await asyncio.sleep(0)


async def _fill(sem: DispatchSemaphore) -> None:
    for _ in range(sem.limit):
        await sem.acquire()


def _waiter(sem: DispatchSemaphore, order: list[int], name: int) -> asyncio.Task:
    async def _run() -> None:
        await sem.acquire()
        order.append(name)

    return asyncio.create_task(_run())


async def test_acquire_up_to_the_limit_then_waits():
    sem = DispatchSemaphore(8)
    await _fill(sem)
    assert sem.running == 8
    assert sem.locked()
    order: list[int] = []
    task = _waiter(sem, order, 1)
    await _settle()
    assert not task.done()
    assert sem.waiting == 1
    sem.release()
    await _settle()
    assert order == [1]
    assert sem.running == 8
    assert sem.waiting == 0


async def test_waiters_start_in_arrival_order():
    sem = DispatchSemaphore(8)
    await _fill(sem)
    order: list[int] = []
    tasks = [_waiter(sem, order, i) for i in range(4)]
    await _settle()
    for _ in range(4):
        sem.release()
    await asyncio.gather(*tasks)
    assert order == [0, 1, 2, 3]


async def test_a_new_caller_does_not_take_a_slot_handed_to_a_waiter():
    """A slot freed while someone waits goes to that waiter, not to an `acquire`
    arriving in the same loop iteration."""
    sem = DispatchSemaphore(8)
    await _fill(sem)
    order: list[int] = []
    first = _waiter(sem, order, 1)
    await _settle()
    sem.release()
    late = _waiter(sem, order, 2)
    await _settle()
    assert order == [1]
    assert not late.done()
    sem.release()
    await asyncio.gather(first, late)
    assert order == [1, 2]


async def test_raising_the_override_starts_waiters_at_once():
    sem = DispatchSemaphore(8)
    await _fill(sem)
    order: list[int] = []
    tasks = [_waiter(sem, order, i) for i in range(6)]
    await _settle()
    sem.set_override(12, ceiling=12)
    await _settle()
    assert order == [0, 1, 2, 3]
    assert sem.running == 12
    assert sem.waiting == 2
    assert sem.limit == 12
    for t in tasks[4:]:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def test_lowering_cancels_nothing_and_waiters_start_once_below_the_new_limit():
    sem = DispatchSemaphore(12)
    await _fill(sem)
    order: list[int] = []
    task = _waiter(sem, order, 1)
    await _settle()
    sem.set_override(8, ceiling=12)
    assert sem.running == 12  # holders keep their slots
    # Four releases bring running to 8, still not below the limit of 8.
    for _ in range(4):
        sem.release()
    await _settle()
    assert order == []
    assert sem.running == 8
    sem.release()
    await _settle()
    assert order == [1]
    assert sem.running == 8
    await task


async def test_clearing_the_override_reverts_to_the_default():
    sem = DispatchSemaphore(8)
    sem.set_override(12, ceiling=12)
    assert sem.override == 12
    sem.set_override(None, ceiling=12)
    assert sem.override is None
    assert sem.limit == 8


async def test_a_waiter_cancelled_while_queued_leaks_no_slot_and_keeps_order():
    sem = DispatchSemaphore(8)
    await _fill(sem)
    order: list[int] = []
    first = _waiter(sem, order, 1)
    cancelled = _waiter(sem, order, 2)
    third = _waiter(sem, order, 3)
    await _settle()
    cancelled.cancel()
    await _settle()
    assert cancelled.cancelled()
    assert sem.waiting == 2
    sem.release()
    sem.release()
    await asyncio.gather(first, third)
    assert order == [1, 3]
    assert sem.running == 8
    assert sem.waiting == 0


async def test_a_waiter_cancelled_after_being_handed_a_slot_gives_it_back():
    """`release` hands the slot over and the cancel lands before the waiter runs:
    the slot must pass to the next waiter rather than be lost."""
    sem = DispatchSemaphore(8)
    await _fill(sem)
    order: list[int] = []
    handed = _waiter(sem, order, 1)
    nxt = _waiter(sem, order, 2)
    await _settle()
    sem.release()  # hands the slot to `handed` (running back to 8)
    handed.cancel()  # before `handed` has run
    await _settle()
    assert handed.cancelled()
    assert order == [2]
    assert sem.running == 8
    await nxt


async def test_release_without_acquire_raises():
    sem = DispatchSemaphore(8)
    with pytest.raises(RuntimeError, match="released more times"):
        sem.release()


async def test_context_manager_releases_on_error():
    sem = DispatchSemaphore(8)
    with pytest.raises(ValueError):
        async with sem:
            assert sem.running == 1
            raise ValueError
    assert sem.running == 0


@pytest.mark.parametrize("value", [0, 4, 10, 100])
async def test_default_outside_the_choices_is_rejected(value):
    with pytest.raises(ValueError, match="must be one of"):
        DispatchSemaphore(value)


async def test_override_outside_the_choices_is_rejected():
    sem = DispatchSemaphore(8)
    with pytest.raises(ValueError, match="must be one of"):
        sem.set_override(10, ceiling=24)
    assert sem.override is None


async def test_override_above_the_ceiling_is_rejected():
    sem = DispatchSemaphore(8)
    with pytest.raises(ValueError, match="above 12"):
        sem.set_override(16, ceiling=12)
    assert sem.override is None


async def test_lowering_below_the_default_warns_that_a_restart_reverts_it(caplog):
    sem = DispatchSemaphore(12)
    with caplog.at_level(logging.WARNING, logger="qiita_control_plane.dispatch_semaphore"):
        sem.set_override(8, ceiling=12)
    assert any("BELOW the DISPATCH_MAX_INFLIGHT default" in r.getMessage() for r in caplog.records)


async def test_raising_above_the_default_does_not_warn(caplog):
    sem = DispatchSemaphore(8)
    with caplog.at_level(logging.WARNING, logger="qiita_control_plane.dispatch_semaphore"):
        sem.set_override(12, ceiling=12)
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
