"""Unit tests for the bounded log-tail reader and OOM-signature match."""

from __future__ import annotations

from qiita_common.log_tail import contains_oom_signature, read_text_tail


def test_missing_file_is_empty_not_an_error(tmp_path):
    text, truncated = read_text_tail(tmp_path / "nope", max_lines=10, max_bytes=1000)
    assert text == ""
    assert truncated is False


def test_short_file_returned_whole_untruncated(tmp_path):
    p = tmp_path / "stderr"
    p.write_text("line1\nline2\nline3\n")
    text, truncated = read_text_tail(p, max_lines=10, max_bytes=1000)
    assert text == "line1\nline2\nline3"
    assert truncated is False


def test_line_bound_keeps_last_lines_and_flags_truncated(tmp_path):
    p = tmp_path / "stderr"
    p.write_text("\n".join(f"line{i}" for i in range(100)) + "\n")
    text, truncated = read_text_tail(p, max_lines=3, max_bytes=10_000)
    assert text == "line97\nline98\nline99"
    assert truncated is True


def test_byte_bound_drops_partial_leading_line(tmp_path):
    p = tmp_path / "stderr"
    p.write_text("aaaaaaaaaa\nbbbbbbbbbb\ncccccccccc\n")
    # max_bytes lands mid-file; the partial first line must be dropped so the
    # tail starts on a clean boundary.
    text, truncated = read_text_tail(p, max_lines=100, max_bytes=22)
    assert truncated is True
    assert text.startswith("cccccccccc") or text.startswith("bbbbbbbbbb")
    assert "aaaaaaaaaa" not in text


def test_binary_noise_does_not_raise(tmp_path):
    p = tmp_path / "stderr"
    p.write_bytes(b"good\n\xff\xfe binary \x00\nmore\n")
    text, truncated = read_text_tail(p, max_lines=10, max_bytes=1000)
    assert "good" in text
    assert "more" in text


def test_contains_oom_signature_matches_known_patterns():
    assert contains_oom_signature("Memory cgroup out of memory: Killed process 123")
    assert contains_oom_signature("oom_kill event in StepId=141763.0")
    assert contains_oom_signature("/bin/sh: line 1: 42 Killed   bcl-convert")
    assert contains_oom_signature("OUT OF MEMORY")  # case-insensitive


def test_contains_oom_signature_no_false_positive_on_normal_error():
    assert not contains_oom_signature("FileNotFoundError: missing input.fastq")
    assert not contains_oom_signature("contract violation: manifest empty")
