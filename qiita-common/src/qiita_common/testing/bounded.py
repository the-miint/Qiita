"""Run a call that might never return, failing the test instead of hanging the run."""

import threading
from collections.abc import Callable


def run_bounded[T](call: Callable[[], T], timeout: float) -> T:
    """`call()` on a daemon thread of its own, failing after `timeout` s. Returns what
    it returned, or raises what it raised. The thread is left behind on a timeout; it
    is a daemon, so it does not keep the interpreter alive."""
    outcome: dict[str, object] = {}

    def run() -> None:
        try:
            outcome["value"] = call()
        except BaseException as exc:
            outcome["error"] = exc

    runner = threading.Thread(target=run, daemon=True)
    runner.start()
    runner.join(timeout)
    assert not runner.is_alive(), f"still running after {timeout} s"
    if "error" in outcome:
        raise outcome["error"]
    return outcome["value"]
