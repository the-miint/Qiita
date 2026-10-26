"""The `files` payload the register-files adapter sends, and resends on a replay.

The data plane moves each staged file into the lake before its catalog commit,
so a replay reads a staging dir the first call emptied, in part or in full.
"""

import json

import pytest
from qiita_common.actions import LibraryPrimitive

from qiita_control_plane.runner._reconstruct import _register_files_payload
from qiita_control_plane.workspace import action_payload_record, step_attempt_dir


@pytest.fixture
def staged(tmp_path):
    staging = tmp_path / "staging"
    (staging / "reference_sequence_chunks").mkdir(parents=True)
    (staging / "reference_membership.parquet").write_bytes(b"")
    (staging / "reference_sequence_chunks" / "part_00000.parquet").write_bytes(b"")
    return staging


_PAYLOAD = {
    "reference_membership.parquet": "reference_membership",
    "reference_sequence_chunks/part_00000.parquet": "reference_sequence_chunks",
}


def _attempt(tmp_path, attempt: int):
    return step_attempt_dir(tmp_path / "ticket", LibraryPrimitive.REGISTER_FILES, attempt)


def test_first_call_records_what_it_sends(tmp_path, staged):
    files = _register_files_payload(_attempt(tmp_path, 0), staged)

    assert files == _PAYLOAD
    record = action_payload_record(_attempt(tmp_path, 0), str(staged))
    assert json.loads(record.read_text()) == {"staging_dir": str(staged), "files": _PAYLOAD}


def test_replay_after_every_file_moved_resends_the_recorded_payload(tmp_path, staged):
    _register_files_payload(_attempt(tmp_path, 0), staged)
    for f in staged.rglob("*.parquet"):
        f.unlink()

    assert _register_files_payload(_attempt(tmp_path, 0), staged) == _PAYLOAD


def test_replay_after_some_files_moved_resends_the_whole_payload(tmp_path, staged):
    _register_files_payload(_attempt(tmp_path, 0), staged)
    (staged / "reference_membership.parquet").unlink()

    assert _register_files_payload(_attempt(tmp_path, 0), staged) == _PAYLOAD


def test_a_later_attempt_of_the_entry_finds_the_record(tmp_path, staged):
    _register_files_payload(_attempt(tmp_path, 0), staged)
    (staged / "reference_membership.parquet").unlink()

    assert _register_files_payload(_attempt(tmp_path, 1), staged) == _PAYLOAD


def test_replay_refuses_a_staged_file_the_earlier_call_did_not_send(tmp_path, staged):
    _register_files_payload(_attempt(tmp_path, 0), staged)
    (staged / "reference_annotation.parquet").write_bytes(b"")

    with pytest.raises(RuntimeError, match=r"reference_annotation\.parquet.*did not send"):
        _register_files_payload(_attempt(tmp_path, 0), staged)


def test_another_staging_dir_gets_its_own_record(tmp_path, staged):
    _register_files_payload(_attempt(tmp_path, 0), staged)
    other = tmp_path / "other"
    other.mkdir()
    (other / "read.parquet").write_bytes(b"")

    assert _register_files_payload(_attempt(tmp_path, 0), other) == {"read.parquet": "read"}


def test_an_empty_staging_dir_with_no_record_is_refused(tmp_path):
    empty = tmp_path / "staging"
    empty.mkdir()

    with pytest.raises(RuntimeError, match="contains no Parquet files"):
        _register_files_payload(_attempt(tmp_path, 0), empty)
    assert not action_payload_record(_attempt(tmp_path, 0), str(empty)).exists()
