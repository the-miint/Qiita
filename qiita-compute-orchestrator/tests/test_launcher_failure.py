"""Unit tests for `slurm.launcher_failure.parse_launcher_failure` —
the bridge that turns the native-job launcher's structured stderr
line into a typed value SlurmBackend uses to enrich its
`BackendFailure`.
"""

from __future__ import annotations

import json

from qiita_common.backend_failure import FailureKind

from qiita_compute_orchestrator.slurm.launcher_failure import (
    LauncherFailure,
    parse_launcher_failure,
)


def _write(stderr_path, text: str) -> None:
    stderr_path.write_text(text)


def _structured(kind: str, step_name: str, reason: str) -> str:
    return json.dumps({"kind": kind, "step_name": step_name, "reason": reason})


def test_parse_returns_none_for_missing_file(tmp_path):
    """Job killed before stderr was created (NODE_FAIL, BOOT_FAIL).
    Parser must not raise — caller falls back to state-based kind."""
    assert parse_launcher_failure(tmp_path / "nonexistent") is None


def test_parse_returns_none_for_empty_file(tmp_path):
    f = tmp_path / "stderr"
    f.write_text("")
    assert parse_launcher_failure(f) is None


def test_parse_returns_none_for_container_style_stderr(tmp_path):
    """Container steps don't write the structured line. Parser sees
    arbitrary text, returns None, SlurmBackend falls back to state-based
    classification — container behavior unchanged."""
    f = tmp_path / "stderr"
    f.write_text(
        "INFO running container\n"
        "ERROR: something went wrong\n"
        "Traceback (most recent call last):\n"
        '  File "x.py", line 1\n'
        "ZeroDivisionError: division by zero\n"
    )
    assert parse_launcher_failure(f) is None


def test_parse_happy_path(tmp_path):
    f = tmp_path / "stderr"
    f.write_text(
        _structured("unknown_permanent", "fastq", "fastq_to_parquet not implemented") + "\n"
    )
    result = parse_launcher_failure(f)
    assert result == LauncherFailure(
        kind=FailureKind.UNKNOWN_PERMANENT,
        step_name="fastq",
        reason="fastq_to_parquet not implemented",
    )


def test_parse_walks_from_end(tmp_path):
    """If asyncio shutdown emits warnings AFTER the launcher's print
    (the print isn't strictly the last thing on stderr), the parser
    walks lines from the end to find the structured line. The first
    valid one it hits wins."""
    f = tmp_path / "stderr"
    f.write_text(
        "Starting native job dispatch\n"
        + _structured("bad_input", "hash", "FASTA missing")
        + "\n"
        + "RuntimeWarning: coroutine 'AsyncClient.aclose' was never awaited\n"
    )
    result = parse_launcher_failure(f)
    assert result is not None
    assert result.kind is FailureKind.BAD_INPUT
    assert result.step_name == "hash"
    assert result.reason == "FASTA missing"


def test_parse_takes_last_when_multiple_structured_lines(tmp_path):
    """If multiple valid structured lines appear (shouldn't happen in
    practice, but be robust), the LAST one is chosen — most recent
    failure wins."""
    f = tmp_path / "stderr"
    f.write_text(
        _structured("bad_input", "x", "first failure")
        + "\n"
        + _structured("contract_violation", "y", "second failure")
        + "\n"
    )
    result = parse_launcher_failure(f)
    assert result is not None
    assert result.kind is FailureKind.CONTRACT_VIOLATION
    assert result.step_name == "y"
    assert result.reason == "second failure"


def test_parse_skips_malformed_json_line(tmp_path):
    """A line that starts with `{` but doesn't parse as JSON is
    skipped — keep walking for a real one."""
    f = tmp_path / "stderr"
    f.write_text(_structured("bad_input", "x", "real failure") + "\n" + "{not actually json}\n")
    result = parse_launcher_failure(f)
    assert result is not None
    assert result.reason == "real failure"


def test_parse_skips_json_with_missing_fields(tmp_path):
    """Valid JSON object but missing required fields — skip and keep
    walking."""
    f = tmp_path / "stderr"
    f.write_text(
        _structured("bad_input", "x", "real failure")
        + "\n"
        + json.dumps({"kind": "bad_input"})
        + "\n"  # missing step_name and reason
    )
    result = parse_launcher_failure(f)
    assert result is not None
    assert result.reason == "real failure"


def test_parse_skips_unknown_failure_kind(tmp_path):
    """Forward-compatibility: a launcher that emits a kind we don't
    recognize (e.g. a future addition) is skipped rather than
    raising. Parser keeps walking for one we know."""
    f = tmp_path / "stderr"
    f.write_text(
        _structured("bad_input", "x", "real failure")
        + "\n"
        + json.dumps({"kind": "from_the_future", "step_name": "y", "reason": "z"})
        + "\n"
    )
    result = parse_launcher_failure(f)
    assert result is not None
    assert result.reason == "real failure"


def test_parse_skips_json_array_at_top_level(tmp_path):
    """A JSON array (`[...]`) starts with `[`, not `{` — caught by the
    cheap pre-filter. A `{...}` line that parses to a non-dict (Pydantic
    edge case, or similar) is also skipped."""
    f = tmp_path / "stderr"
    f.write_text("[1, 2, 3]\n")
    assert parse_launcher_failure(f) is None
