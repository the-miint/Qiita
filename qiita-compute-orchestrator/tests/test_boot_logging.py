"""The lifespan's logging bring-up — the orchestrator half.

The CO is the service where this bit hardest. Its only two `.info()` sites in `src/`
are work-triggered (a SLURM JWT inside its refresh margin, a miint re-stage), so an
idle CO narrated nothing at all: a journal with no app lines was indistinguishable
from a CO whose root logger was never configured, and a deploy check for "any INFO
line" could not tell the two apart. The unconditional boot line asserted here is what
makes that check able to fail for the right reason.
"""

import logging

from fastapi import FastAPI


async def test_lifespan_configures_logging_and_narrates_boot(monkeypatch, caplog):
    from qiita_compute_orchestrator import main as co_main

    calls = []

    def _record(level=None):
        calls.append(level)
        return "INFO"

    # Stubbed, not observed: the real configure_logging calls basicConfig(force=True),
    # which removes caplog's handler and would drop the very record under test.
    monkeypatch.setattr(co_main, "configure_logging", _record)
    monkeypatch.setattr(co_main, "install_authorization_scrub", lambda *a, **k: None)

    app = FastAPI()
    with caplog.at_level(logging.INFO, logger="qiita_compute_orchestrator.main"):
        async with co_main.lifespan(app):
            pass

    assert calls == [None], "the lifespan must call configure_logging() exactly once"
    messages = [r.getMessage() for r in caplog.records]
    boot = [r for r in caplog.records if "compute-orchestrator up" in r.getMessage()]
    assert boot, f"expected one unconditional boot INFO; got {messages}"
    message = boot[0].getMessage()
    # Both fields earn their place: the resolved backend is where a "why is nothing
    # dispatching" question starts, and the level distinguishes a quiet journal from
    # a broken one.
    assert "backend=" in message and "log_level=INFO" in message
    # The deploy check greps `INFO <dotted.logger.name>`; only a configured root logger
    # emits that shape, which is what makes it fail on a CO still carrying the bug.
    assert boot[0].name == "qiita_compute_orchestrator.main"
