"""`qiita biosample bulk-create`: the TSV file it reads, the request it sends, how it
names the study, and the TSV it prints."""

from pathlib import Path

import pytest
from qiita_common.api_paths import (
    URL_AUTH_WHOAMI,
    URL_BIOSAMPLE_BULK_BY_STUDY,
    URL_STUDY_LOOKUP,
)
from qiita_common.models.biosample import BIOSAMPLE_BULK_IMPORT_MAX_ROWS

from qiita_control_plane.cli.user import main

from .conftest import _stub_post

_BASE = "https://q.example.test"
_OWNER_COLUMN = "sample_name"
_STUDY_IDX = 7
_BULK_URL = URL_BIOSAMPLE_BULK_BY_STUDY.format(study_idx=_STUDY_IDX)
_TSV_TEXT = (
    "sample_name\tmatrix_tube_id\tbiosample_accession\thost taxon id\t depth \n"
    "SMK-001\t0012345678\tSAMN01\t9606\t 5 \n"
    "SMK-002\t\t\t9606\t\n"
)
_BULK_RESPONSE = {
    "results": [
        {
            "biosample_idx": 1041,
            "export_entity_id": "QB1041",
            "owner_id_biosample_study_field_idx": 5,
            "owner_id_biosample_study_field_created": True,
        },
        {
            "biosample_idx": 1042,
            "export_entity_id": "QB1042",
            "owner_id_biosample_study_field_idx": 5,
            "owner_id_biosample_study_field_created": False,
        },
    ]
}
_EXPECTED_OUTPUT = (
    "sample_name\tbiosample_idx\texport_entity_id\nSMK-001\t1041\tQB1041\nSMK-002\t1042\tQB1042\n"
)


def _write_tsv_file(tmp_path: Path, text: str = _TSV_TEXT) -> Path:
    path = tmp_path / "biosamples.tsv"
    path.write_text(text, encoding="utf-8")
    return path


def _expected_rows(owner_idx: int, **extra) -> list[dict]:
    """The request rows `_TSV_TEXT` becomes for `owner_idx`, each carrying `extra`."""
    common = {"owner_idx": owner_idx, "owner_biosample_id_field_name": _OWNER_COLUMN, **extra}
    return [
        {
            **common,
            "owner_biosample_id_value": "SMK-001",
            "metadata": {"host taxon id": "9606", "depth": "5"},
            "biosample_accession": "SAMN01",
            "matrix_tube_id": "0012345678",
        },
        {**common, "owner_biosample_id_value": "SMK-002", "metadata": {"host taxon id": "9606"}},
    ]


def _bulk_create_argv(
    tsv_file: Path,
    *study_args: str,
    extra: tuple[str, ...] = (),
    owner_column: str = _OWNER_COLUMN,
) -> list[str]:
    study = study_args or ("--study-idx", str(_STUDY_IDX))
    return [
        "--base-url",
        _BASE,
        "biosample",
        "bulk-create",
        *study,
        "--tsv-file",
        str(tsv_file),
        "--owner-biosample-id-field-name",
        owner_column,
        *extra,
    ]


def test__handle_biosample_bulk_create(monkeypatch, tmp_path, capfd):
    """Tests the case where the file carries attribute and metadata columns, a
    blank cell, and padded cells, and --owner-idx is omitted: the owner comes
    from whoami, one request carries every row, and the created biosamples print
    as a TSV in the file's row order."""
    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json=_BULK_RESPONSE,
        whoami_idx=42,
    )

    rc = main(_bulk_create_argv(_write_tsv_file(tmp_path)))

    calls = [(r["method"], r["url"], r["json"]) for r in captured["requests"]]
    expected_calls = [
        ("GET", _BASE + URL_AUTH_WHOAMI, None),
        ("POST", _BASE + _BULK_URL, {"rows": _expected_rows(42)}),
    ]
    assert (rc, calls, capfd.readouterr().out) == (0, expected_calls, _EXPECTED_OUTPUT)


def test__handle_biosample_bulk_create_explicit_owner_and_internal_names(
    monkeypatch, tmp_path, capfd
):
    """Tests the case where --owner-idx and --global-internal-names are passed:
    no whoami call is made, and every row carries that owner and the flag."""
    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BULK_RESPONSE)

    argv = _bulk_create_argv(
        _write_tsv_file(tmp_path), extra=("--owner-idx", "11", "--global-internal-names")
    )
    rc = main(argv)

    calls = [(r["method"], r["url"], r["json"]) for r in captured["requests"]]
    expected_rows = _expected_rows(11, global_internal_names=True)
    expected_calls = [("POST", _BASE + _BULK_URL, {"rows": expected_rows})]
    assert (rc, calls, capfd.readouterr().out) == (0, expected_calls, _EXPECTED_OUTPUT)


def test__handle_biosample_bulk_create_by_study_export_id(monkeypatch, tmp_path, capfd):
    """Tests the case where the study is named by --study-export-id: it is
    resolved through the study lookup, and the rows are posted to that study."""
    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json=_BULK_RESPONSE,
        whoami_idx=42,
        responses_by_url={URL_STUDY_LOOKUP: {"resolved": {"QS7": _STUDY_IDX}, "missing": []}},
    )

    rc = main(_bulk_create_argv(_write_tsv_file(tmp_path), "--study-export-id", "QS7"))

    calls = [(r["method"], r["url"], r["json"]) for r in captured["requests"]]
    expected_calls = [
        ("POST", _BASE + URL_STUDY_LOOKUP, {"key": "export_entity_id", "values": ["QS7"]}),
        ("GET", _BASE + URL_AUTH_WHOAMI, None),
        ("POST", _BASE + _BULK_URL, {"rows": _expected_rows(42)}),
    ]
    assert (rc, calls, capfd.readouterr().out) == (0, expected_calls, _EXPECTED_OUTPUT)


def test__handle_biosample_bulk_create_unknown_study_export_id(monkeypatch, tmp_path, capsys):
    """Tests the case where --study-export-id names no study: the command exits 2
    naming the id, and nothing is posted to the bulk route."""
    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json=_BULK_RESPONSE,
        responses_by_url={URL_STUDY_LOOKUP: {"resolved": {}, "missing": ["QS9"]}},
    )

    with pytest.raises(SystemExit) as exc_info:
        main(_bulk_create_argv(_write_tsv_file(tmp_path), "--study-export-id", "QS9"))

    urls = [r["url"] for r in captured["requests"]]
    stderr = capsys.readouterr().err
    assert (exc_info.value.code, urls, "--study-export-id QS9" in stderr) == (
        2,
        [_BASE + URL_STUDY_LOOKUP],
        True,
    )


@pytest.mark.parametrize(
    "study_args",
    [("--study-idx", "7", "--study-export-id", "QS7"), ()],
    ids=["both", "neither"],
)
def test__handle_biosample_bulk_create_requires_one_study_flag(tmp_path, study_args):
    """Tests the case where both study flags, or neither, are passed: argparse
    refuses the command with exit 2."""
    argv = [
        "--base-url",
        _BASE,
        "biosample",
        "bulk-create",
        *study_args,
        "--tsv-file",
        str(_write_tsv_file(tmp_path)),
        "--owner-biosample-id-field-name",
        _OWNER_COLUMN,
    ]
    with pytest.raises(SystemExit) as exc_info:
        main(argv)
    assert exc_info.value.code == 2


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("matrix_tube_id\nX\n", "no 'sample_name' column"),
        ("", "is empty"),
        ("sample_name\tnote\nSMK-001\n", "Expected Number of Columns: 2 Found: 1"),
        ("sample_name\tnote\n", "holds no rows"),
    ],
    ids=["no-owner-column", "empty-file", "ragged-row", "header-only"],
)
def test__read_biosample_tsv_refuses_bad_file(monkeypatch, tmp_path, capsys, text, message):
    """Tests the case where the file lacks the owner_biosample_id column, is
    empty, has a row of the wrong width, or holds no rows: the command exits 2
    naming the fault, without DuckDB's advice for code authors, before any
    request."""
    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BULK_RESPONSE, whoami_idx=42)

    with pytest.raises(SystemExit) as exc_info:
        main(_bulk_create_argv(_write_tsv_file(tmp_path, text)))

    stderr = capsys.readouterr().err
    outcome = (
        exc_info.value.code,
        captured["requests"],
        message in stderr,
        "Possible fixes" in stderr,
    )
    assert outcome == (2, [], True, False)


def test__handle_biosample_bulk_create_refuses_oversized_tsv(monkeypatch, tmp_path, capsys):
    """Tests the case where the file holds more rows than one bulk request
    takes: the command exits 2 before posting anything to the bulk route."""
    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BULK_RESPONSE, whoami_idx=42)
    row_count = BIOSAMPLE_BULK_IMPORT_MAX_ROWS + 1
    text = "sample_name\n" + "".join(f"SMK-{i}\n" for i in range(row_count))

    with pytest.raises(SystemExit) as exc_info:
        main(_bulk_create_argv(_write_tsv_file(tmp_path, text)))

    methods = [r["method"] for r in captured["requests"]]
    assert (exc_info.value.code, methods) == (2, ["GET"])


def test__read_biosample_tsv_refuses_unreadable_file(monkeypatch, tmp_path, capsys):
    """Tests the case where the TSV file exists but cannot be opened: the command
    exits 2 naming the file, before any request."""
    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BULK_RESPONSE, whoami_idx=42)
    unreadable_file = _write_tsv_file(tmp_path)
    unreadable_file.chmod(0)

    with pytest.raises(SystemExit) as exc_info:
        main(_bulk_create_argv(unreadable_file))

    stderr = capsys.readouterr().err
    names_file = f"--tsv-file {unreadable_file}:" in stderr
    assert (exc_info.value.code, captured["requests"], names_file) == (2, [], True)


def test__handle_biosample_bulk_create_refuses_result_count_mismatch(monkeypatch, tmp_path, capfd):
    """Tests the case where the control plane answers fewer results than the
    file has rows: the command exits naming both counts, and prints no TSV."""
    captured: dict = {}
    short_response = {"results": _BULK_RESPONSE["results"][:1]}
    _stub_post(monkeypatch, captured, response_json=short_response, whoami_idx=42)

    with pytest.raises(SystemExit) as exc_info:
        main(_bulk_create_argv(_write_tsv_file(tmp_path)))

    expected_message = "error: the control plane returned 1 result(s) for 2 row(s)"
    assert (exc_info.value.code, capfd.readouterr().out) == (expected_message, "")


def test_resolve_study_idx_refuses_blank_export_id(monkeypatch, tmp_path, capsys):
    """Tests the case where --study-export-id is blank: the command exits 2
    naming the flag, and no request is made."""
    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BULK_RESPONSE, whoami_idx=42)

    with pytest.raises(SystemExit) as exc_info:
        main(_bulk_create_argv(_write_tsv_file(tmp_path), "--study-export-id", " "))

    stderr = capsys.readouterr().err
    names_flag = "--study-export-id: must not be blank" in stderr
    assert (exc_info.value.code, captured["requests"], names_flag) == (2, [], True)


@pytest.mark.parametrize("owner_column", ["biosample_idx", "export_entity_id"])
def test__read_biosample_tsv_refuses_owner_column_named_like_output(
    monkeypatch, tmp_path, capsys, owner_column
):
    """Tests the case where the owner column shares its name with a column that
    the command's printed results already carry: the command exits 2 naming the
    flag, before any request."""
    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BULK_RESPONSE, whoami_idx=42)
    tsv_file = _write_tsv_file(tmp_path, f"{owner_column}\tnote\nSMK-001\tx\n")

    with pytest.raises(SystemExit) as exc_info:
        main(_bulk_create_argv(tsv_file, owner_column=owner_column))

    stderr = capsys.readouterr().err
    names_flag = f"--owner-biosample-id-field-name {owner_column!r}" in stderr
    assert (exc_info.value.code, captured["requests"], names_flag) == (2, [], True)
