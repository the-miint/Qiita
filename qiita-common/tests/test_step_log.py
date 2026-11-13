"""The step-log shape: stamped narrative lines, one receipt, summary counts."""

import io
import json
import logging

from qiita_common.step_log import (
    COMPLETED_KIND,
    listed,
    receipt,
    step_handler,
    step_scope,
    summarize,
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


def test_listed_names_the_first_few():
    assert listed([1, 2]) == "1, 2"
    assert listed(list(range(25))).endswith(", 19, … (5 more)")
