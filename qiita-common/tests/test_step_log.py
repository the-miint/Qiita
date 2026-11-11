"""The step-log shape: stamped narrative lines, one receipt, summary counts."""

import asyncio
import io
import json
import logging

from qiita_common.step_log import (
    COMPLETED_KIND,
    capture_to,
    listed,
    receipt,
    step_handler,
    step_scope,
    summarize,
    write_receipt,
)


def test_step_handler_stamps_each_line_with_the_running_step():
    stream = io.StringIO()
    handler = step_handler(logging.StreamHandler(stream))
    log = logging.getLogger("qiita_common.tests.step_log.stamp")
    log.setLevel(logging.INFO)
    log.addHandler(handler)
    try:
        with step_scope("denoise"):
            log.info("kept %d of %d", 3, 5)
    finally:
        log.removeHandler(handler)
    assert stream.getvalue().strip().endswith("INFO denoise: kept 3 of 5")


def test_summarize_lands_on_the_running_step_and_is_inert_outside_one():
    summarize(ignored=1)
    with step_scope("profile") as scope:
        summarize(samples=3)
        summarize(genomes=40)
    assert scope.summary == {"samples": 3, "genomes": 40}


def test_receipt_is_one_json_line():
    completed = receipt(COMPLETED_KIND, "denoise", summary={"asvs": 7})
    failed = receipt("bad_input", None, reason="no reads")
    assert json.loads(completed) == {
        "kind": "completed",
        "step_name": "denoise",
        "summary": {"asvs": 7},
    }
    assert json.loads(failed) == {"kind": "bad_input", "step_name": None, "reason": "no reads"}
    assert "\n" not in completed and "\n" not in failed


def test_capture_writes_only_its_own_steps_lines(tmp_path):
    log = logging.getLogger("qiita_common.tests.step_log.capture")
    log.setLevel(logging.INFO)

    async def step(name, logs, gate, other):
        with capture_to(logs, name):
            log.info("hello from %s", name)
            gate.set()
            await other.wait()
            summarize(rows=len(name))

    async def both():
        a, b = asyncio.Event(), asyncio.Event()
        await asyncio.gather(
            step("denoise", tmp_path / "a", a, b),
            step("mint-features", tmp_path / "b", b, a),
        )

    asyncio.run(both())
    a = (tmp_path / "a" / "stderr").read_text().splitlines()
    b = (tmp_path / "b" / "stderr").read_text().splitlines()
    assert len(a) == 1 and a[0].endswith("INFO denoise: hello from denoise")
    assert len(b) == 1 and b[0].endswith("INFO mint-features: hello from mint-features")


def test_write_receipt_appends_one_line_per_call(tmp_path):
    write_receipt(tmp_path, receipt(COMPLETED_KIND, "denoise", summary={"asvs": 7}))
    write_receipt(tmp_path, receipt("bad_input", None, reason="no reads"))
    lines = (tmp_path / "stdout").read_text().splitlines()
    assert [json.loads(line) for line in lines] == [
        {"kind": "completed", "step_name": "denoise", "summary": {"asvs": 7}},
        {"kind": "bad_input", "step_name": None, "reason": "no reads"},
    ]


def test_capture_scrubs_authorization_values(tmp_path):
    """The step-log file is served to the ticket originator, so a `Bearer` token
    logged inside the block is redacted — the boot-time scrubber covers only the
    handlers present at boot, not this one."""
    log = logging.getLogger("qiita_common.tests.step_log.scrub")
    log.setLevel(logging.INFO)
    with capture_to(tmp_path, "mint"):
        log.info("calling control plane with Authorization: Bearer qk_secret12345")
    text = (tmp_path / "stderr").read_text()
    assert "qk_secret12345" not in text
    assert "<redacted>" in text


def test_write_receipt_is_best_effort(tmp_path):
    """A receipt write that fails (the dir is gone) must not raise — the step's real
    outcome is recorded elsewhere and must not be masked by a log-file I/O error."""
    missing = tmp_path / "gone"  # parent dir absent → open() raises, caught
    write_receipt(missing, receipt(COMPLETED_KIND, "denoise"))
    assert not (missing / "stdout").exists()


def test_capture_to_is_best_effort_when_the_log_dir_cannot_be_made(tmp_path):
    """If the log dir cannot be created the block still runs and its scope still
    collects counts — a step is not failed because its log could not be written."""
    blocker = tmp_path / "blocker"
    blocker.write_text("x")  # a file where the logs dir's parent should be
    with capture_to(blocker / "logs", "mint") as scope:
        summarize(rows=3)
    assert scope.summary == {"rows": 3}


def test_no_step_scope_detaches_from_the_surrounding_step():
    """A block run under `no_step_scope` is outside any step: its `summarize` is inert,
    and the surrounding step's scope is intact afterwards."""
    from qiita_common.step_log import no_step_scope

    with step_scope("plan-shards") as parent:
        with no_step_scope():
            summarize(child=1)
        summarize(parent=1)
    assert parent.summary == {"parent": 1}


def test_listed_names_the_first_few():
    assert listed([1, 2]) == "1, 2"
    assert listed(list(range(25))).endswith(", 19, … (5 more)")
