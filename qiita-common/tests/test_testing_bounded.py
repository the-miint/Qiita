"""Tests for `qiita_common.testing.bounded`."""

import threading

import pytest

from qiita_common.testing.bounded import run_bounded


def test_returns_what_the_call_returned():
    assert run_bounded(lambda: 42, timeout=5) == 42


class _NotAnException(BaseException):
    """Like `pytest.fail`'s `Failed` or `asyncio.CancelledError`: a `BaseException`
    that is not an `Exception`, which the callers' scenarios raise on the thread."""


@pytest.mark.parametrize("raised", [KeyError("x"), _NotAnException()])
def test_raises_what_the_call_raised(raised):
    def boom():
        raise raised

    with pytest.raises(type(raised)):
        run_bounded(boom, timeout=5)


def test_fails_a_call_that_does_not_return_in_time():
    release = threading.Event()
    try:
        with pytest.raises(AssertionError, match="still running after"):
            run_bounded(release.wait, timeout=0.05)
    finally:
        release.set()
