"""`qiita biosample bulk-create` and `qiita biosample lookup`: the TSV file each reads,
the request it sends, how it names the study, and the TSV file it writes."""

import json
import os
from pathlib import Path

import pytest
from qiita_common.api_paths import (
    URL_AUTH_WHOAMI,
    URL_BIOSAMPLE_BULK_BY_STUDY,
    URL_BIOSAMPLE_LOOKUP_BY_STUDY,
    URL_STUDY_LOOKUP,
)
from qiita_common.models import EXPORT_ENTITY_ID, IDX_KEY, UNIQUE_FIELD_KEY
from qiita_common.models.biosample import BIOSAMPLE_BULK_IMPORT_MAX_ROWS

from qiita_control_plane.cli import _common as cli_common
from qiita_control_plane.cli.user import main

from .conftest import _stub_post

_BASE = "https://q.example.test"
_OUTPUT_FILE_NAME = "created.tsv"
_OWNER_BIOSAMPLE_ID_COLUMN = "sample_name"
_STUDY_IDX = 7
_BULK_URL = URL_BIOSAMPLE_BULK_BY_STUDY.format(study_idx=_STUDY_IDX)
_LOOKUP_URL = URL_BIOSAMPLE_LOOKUP_BY_STUDY.format(study_idx=_STUDY_IDX)
# lookup case -> (identifier flags, column header, request key, two values)
_LOOKUP_CASES: dict[str, tuple[tuple[str, ...], str, str, tuple[str, str]]] = {
    EXPORT_ENTITY_ID: (
        ("--key", EXPORT_ENTITY_ID),
        EXPORT_ENTITY_ID,
        EXPORT_ENTITY_ID,
        ("QB1", "QB2"),
    ),
    "biosample_idx": (("--key", "biosample_idx"), "biosample_idx", IDX_KEY, ("41", "42")),
    "biosample_accession": (
        ("--key", "biosample_accession"),
        "biosample_accession",
        "biosample_accession",
        ("SAMN01", "SAMN02"),
    ),
    "ena_sample_accession": (
        ("--key", "ena_sample_accession"),
        "ena_sample_accession",
        "ena_sample_accession",
        ("SAMEA1", "SAMEA2"),
    ),
    "matrix_tube_id": (
        ("--key", "matrix_tube_id"),
        "matrix_tube_id",
        "matrix_tube_id",
        ("0012345678", "0012345679"),
    ),
    UNIQUE_FIELD_KEY: (
        ("--unique-field-name", "sample_id"),
        "sample_id",
        UNIQUE_FIELD_KEY,
        ("S-1", "S-2"),
    ),
}
# (case, return key) pairs that look a kind up for itself, which would head both
# output columns alike and so are refused rather than resolved.
_LOOKUP_SELF_PAIRS = {(EXPORT_ENTITY_ID, EXPORT_ENTITY_ID), ("biosample_idx", "biosample_idx")}
# return key -> the two identifiers the stubbed route returns for a case's two values
_LOOKUP_IDENTIFIERS: dict[str, tuple[str | int, str | int]] = {
    EXPORT_ENTITY_ID: ("QB517", "QB518"),
    "biosample_idx": (517, 518),
}
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


def _read_output(tmp_path: Path) -> str:
    """The text of the output file that `_bulk_create_argv` names by default."""
    output_text = (tmp_path / _OUTPUT_FILE_NAME).read_text(encoding="utf-8")
    return output_text


def _expected_rows(
    owner_idx: int, owner_biosample_id_column: str = _OWNER_BIOSAMPLE_ID_COLUMN, **extra
) -> list[dict]:
    """The request rows `_TSV_TEXT` becomes for the owner principal `owner_idx`,
    with the owner_biosample_id column headed `owner_biosample_id_column`, each
    carrying `extra`."""
    common = {
        "owner_idx": owner_idx,
        "owner_biosample_id_field_name": owner_biosample_id_column,
        **extra,
    }
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
    owner_biosample_id_column: str = _OWNER_BIOSAMPLE_ID_COLUMN,
    output: Path | None = None,
) -> list[str]:
    """The bulk-create argv; the output defaults to `_OUTPUT_FILE_NAME` beside the
    TSV file."""
    study = study_args or ("--study-idx", str(_STUDY_IDX))
    output = output or tsv_file.with_name(_OUTPUT_FILE_NAME)
    return [
        "--base-url",
        _BASE,
        "biosample",
        "bulk-create",
        *study,
        "--tsv-file",
        str(tsv_file),
        "--owner-biosample-id-field-name",
        owner_biosample_id_column,
        "--output",
        str(output),
        *extra,
    ]


def test__handle_biosample_bulk_create(monkeypatch, tmp_path):
    """Tests the case where the file carries attribute and metadata columns, a
    blank cell, and padded cells, and --owner-idx is omitted: the owner comes
    from whoami, one request carries every row, and the created biosamples are
    written as a TSV in the file's row order."""
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
    assert (rc, calls, _read_output(tmp_path)) == (0, expected_calls, _EXPECTED_OUTPUT)


def test__handle_biosample_bulk_create_explicit_owner_and_internal_names(monkeypatch, tmp_path):
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
    assert (rc, calls, _read_output(tmp_path)) == (0, expected_calls, _EXPECTED_OUTPUT)


@pytest.mark.parametrize("owner_biosample_id_column", ["biosample_idx_x", "my_export_entity_id"])
def test__handle_biosample_bulk_create_accepts_owner_biosample_id_column_containing_refused_name(
    monkeypatch, tmp_path, owner_biosample_id_column
):
    """Tests the case where the owner_biosample_id column's name contains a
    refused name without equaling it: the command creates the biosamples and
    writes them under that column name."""
    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BULK_RESPONSE, whoami_idx=42)
    tsv_text = _TSV_TEXT.replace(_OWNER_BIOSAMPLE_ID_COLUMN, owner_biosample_id_column, 1)

    tsv_file = _write_tsv_file(tmp_path, tsv_text)
    rc = main(_bulk_create_argv(tsv_file, owner_biosample_id_column=owner_biosample_id_column))

    calls = [(r["method"], r["url"], r["json"]) for r in captured["requests"]]
    expected_calls = [
        ("GET", _BASE + URL_AUTH_WHOAMI, None),
        ("POST", _BASE + _BULK_URL, {"rows": _expected_rows(42, owner_biosample_id_column)}),
    ]
    expected_output = _EXPECTED_OUTPUT.replace(
        _OWNER_BIOSAMPLE_ID_COLUMN, owner_biosample_id_column, 1
    )
    assert (rc, calls, _read_output(tmp_path)) == (0, expected_calls, expected_output)


@pytest.mark.parametrize(
    ("output_name", "message"),
    [
        (_OUTPUT_FILE_NAME, "already exists"),
        (f"absent/{_OUTPUT_FILE_NAME}", "no such directory"),
        (f"locked/{_OUTPUT_FILE_NAME}", "cannot write into"),
        (f"sealed/inner/{_OUTPUT_FILE_NAME}", "cannot reach"),
    ],
    ids=["existing-file", "missing-directory", "unwritable-directory", "unreachable-directory"],
)
def test__handle_biosample_bulk_create_refuses_output_path(
    monkeypatch, tmp_path, capsys, output_name, message
):
    """Tests the case where --output names an existing file, a file in a
    missing directory, or a file in a directory that cannot be written into or
    reached: the command exits 2 naming the problem before any request, and an
    existing file is left as it was."""
    if output_name.startswith(("locked/", "sealed/")) and os.geteuid() == 0:
        pytest.skip("root writes into any directory")
    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BULK_RESPONSE, whoami_idx=42)
    (tmp_path / _OUTPUT_FILE_NAME).write_text("SENTINEL\n", encoding="utf-8")
    locked_dir = tmp_path / "locked"
    locked_dir.mkdir(mode=0o500)
    sealed_dir = tmp_path / "sealed"
    (sealed_dir / "inner").mkdir(parents=True)
    sealed_dir.chmod(0o000)
    output = tmp_path / output_name

    try:
        with pytest.raises(SystemExit) as exc_info:
            main(_bulk_create_argv(_write_tsv_file(tmp_path), output=output))
    finally:
        # Restore the modes so pytest can remove the directories afterwards.
        locked_dir.chmod(0o700)
        sealed_dir.chmod(0o700)

    outcome = (
        exc_info.value.code,
        captured["requests"],
        message in capsys.readouterr().err,
        _read_output(tmp_path),
    )
    assert outcome == (2, [], True, "SENTINEL\n")


def test__handle_biosample_bulk_create_reports_created_rows_when_output_fails(
    monkeypatch, tmp_path, capsys
):
    """Tests the case where the biosamples are created but writing the output
    fails partway: the command exits 1, gives the created rows as JSON on
    stderr, and leaves no partial output file behind."""
    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BULK_RESPONSE, whoami_idx=42)

    def _fail_after_partial_write(path, columns, rows=()):
        path.write_text("partial", encoding="utf-8")
        raise OSError("disk full")

    monkeypatch.setattr(cli_common, "write_tsv", _fail_after_partial_write)
    tsv_file = _write_tsv_file(tmp_path)
    with pytest.raises(SystemExit) as exc_info:
        main(_bulk_create_argv(tsv_file))

    # The first stderr line names the write error, whose text belongs to the OS.
    stderr_lines = capsys.readouterr().err.split("\n", 2)
    expected_rows = [
        {
            _OWNER_BIOSAMPLE_ID_COLUMN: "SMK-001",
            "biosample_idx": "1041",
            "export_entity_id": "QB1041",
        },
        {
            _OWNER_BIOSAMPLE_ID_COLUMN: "SMK-002",
            "biosample_idx": "1042",
            "export_entity_id": "QB1042",
        },
    ]
    outcome = (
        exc_info.value.code,
        stderr_lines[1],
        json.loads(stderr_lines[2]),
        sorted(path.name for path in tmp_path.iterdir()),
    )
    expected = (
        1,
        "The biosamples were created; their identifiers are:",
        expected_rows,
        [tsv_file.name],
    )
    assert outcome == expected


def test__handle_biosample_bulk_create_keeps_output_that_appears_during_run(
    monkeypatch, tmp_path, capsys
):
    """Tests the case where a file appears at the --output path after the
    command checked it: the file is left as it was, the command exits 1 giving
    the created rows on stderr, and no partial output file is left behind."""
    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BULK_RESPONSE, whoami_idx=42)
    real_write_tsv = cli_common.write_tsv

    def _write_while_output_appears(path, columns, rows=()):
        real_write_tsv(path, columns, rows)
        (tmp_path / _OUTPUT_FILE_NAME).write_text("SENTINEL\n", encoding="utf-8")

    monkeypatch.setattr(cli_common, "write_tsv", _write_while_output_appears)
    tsv_file = _write_tsv_file(tmp_path)
    with pytest.raises(SystemExit) as exc_info:
        main(_bulk_create_argv(tsv_file))

    stderr_lines = capsys.readouterr().err.split("\n", 2)
    outcome = (
        exc_info.value.code,
        stderr_lines[1],
        _read_output(tmp_path),
        sorted(path.name for path in tmp_path.iterdir()),
    )
    expected = (
        1,
        "The biosamples were created; their identifiers are:",
        "SENTINEL\n",
        sorted([tsv_file.name, _OUTPUT_FILE_NAME]),
    )
    assert outcome == expected


def test__handle_biosample_bulk_create_by_study_export_id(monkeypatch, tmp_path):
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
        (
            "POST",
            _BASE + URL_STUDY_LOOKUP,
            {"key": EXPORT_ENTITY_ID, "values": ["QS7"], "return_key": IDX_KEY},
        ),
        ("GET", _BASE + URL_AUTH_WHOAMI, None),
        ("POST", _BASE + _BULK_URL, {"rows": _expected_rows(42)}),
    ]
    assert (rc, calls, _read_output(tmp_path)) == (0, expected_calls, _EXPECTED_OUTPUT)


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
        _OWNER_BIOSAMPLE_ID_COLUMN,
        "--output",
        str(tmp_path / _OUTPUT_FILE_NAME),
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
    ids=["no-owner-biosample-id-column", "empty-file", "ragged-row", "header-only"],
)
def test__read_tsv_with_column_refuses_bad_file(monkeypatch, tmp_path, capsys, text, message):
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


def test__read_tsv_with_column_refuses_unreadable_file(monkeypatch, tmp_path, capsys):
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
    file has rows: the command exits naming both counts, and writes no TSV."""
    captured: dict = {}
    short_response = {"results": _BULK_RESPONSE["results"][:1]}
    _stub_post(monkeypatch, captured, response_json=short_response, whoami_idx=42)

    with pytest.raises(SystemExit) as exc_info:
        main(_bulk_create_argv(_write_tsv_file(tmp_path)))

    output = capfd.readouterr()
    expected_message = "error: the control plane returned 1 result(s) for 2 row(s)\n"
    outcome = (exc_info.value.code, output.err, (tmp_path / _OUTPUT_FILE_NAME).exists())
    assert outcome == (1, expected_message, False)


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


@pytest.mark.parametrize(
    "owner_biosample_id_column",
    ["biosample_idx", "export_entity_id", "Biosample_IDX", "matrix_tube_id", "Biosample_Accession"],
)
def test__handle_biosample_bulk_create_refuses_owner_biosample_id_column_name(
    monkeypatch, tmp_path, capsys, owner_biosample_id_column
):
    """Tests the case where the owner_biosample_id column's name, ignoring case, is a column
    that the command's output TSV carries or one that sets an attribute: the
    command exits 2 naming the flag, before reading the file (which does not
    exist) or sending any request."""
    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BULK_RESPONSE, whoami_idx=42)
    tsv_file = tmp_path / "absent.tsv"

    with pytest.raises(SystemExit) as exc_info:
        main(_bulk_create_argv(tsv_file, owner_biosample_id_column=owner_biosample_id_column))

    stderr = capsys.readouterr().err
    names_flag = f"--owner-biosample-id-field-name {owner_biosample_id_column!r}" in stderr
    assert (exc_info.value.code, captured["requests"], names_flag) == (2, [], True)


def _lookup_argv(tsv_file: Path, *extra: str, study_args: tuple[str, ...] = ()) -> list[str]:
    """The lookup argv; the output is `_OUTPUT_FILE_NAME` beside the TSV file."""
    study = study_args or ("--study-idx", str(_STUDY_IDX))
    return [
        "--base-url",
        _BASE,
        "biosample",
        "lookup",
        *study,
        "--tsv-file",
        str(tsv_file),
        "--output",
        str(tsv_file.with_name(_OUTPUT_FILE_NAME)),
        *extra,
    ]


@pytest.mark.parametrize(
    ("case", "return_key"),
    [
        (case, return_key)
        for case in _LOOKUP_CASES
        for return_key in _LOOKUP_IDENTIFIERS
        if (case, return_key) not in _LOOKUP_SELF_PAIRS
    ],
)
def test__handle_biosample_lookup(monkeypatch, tmp_path, case, return_key):
    """Tests the case where each identifier kind is looked up for each returned
    identifier, in a file whose first value is padded and repeats in a later row:
    one request carries every row's value, and the output maps each input row, in
    input order, to the identifier that its value resolved to."""
    flags, column, request_key, (first, second) = _LOOKUP_CASES[case]
    first_id, second_id = _LOOKUP_IDENTIFIERS[return_key]
    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json={"resolved": {second: second_id, first: first_id}, "missing": []},
    )
    tsv_text = f"{column}\tnote\n {first} \tone\n{second}\t\n{first}\tagain\n"

    rc = main(_lookup_argv(_write_tsv_file(tmp_path, tsv_text), *flags, "--return-key", return_key))

    calls = [(r["method"], r["url"], r["json"]) for r in captured["requests"]]
    expected_body = {
        "values": [first, second, first],
        "return_key": IDX_KEY if return_key == "biosample_idx" else return_key,
        "key": request_key,
        "unique_field_display_name": column if request_key == UNIQUE_FIELD_KEY else None,
    }
    expected_output = (
        f"{column}\t{return_key}\n{first}\t{first_id}\n{second}\t{second_id}\n{first}\t{first_id}\n"
    )
    outcome = (rc, calls, _read_output(tmp_path))
    assert outcome == (0, [("POST", _BASE + _LOOKUP_URL, expected_body)], expected_output)


def test__handle_biosample_lookup_by_study_export_id(monkeypatch, tmp_path):
    """Tests the case where the study is named by --study-export-id: it is
    resolved through the study lookup, and the values are looked up in that
    study."""
    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json={"resolved": {"0012345678": "QB517"}, "missing": []},
        responses_by_url={URL_STUDY_LOOKUP: {"resolved": {"QS7": _STUDY_IDX}, "missing": []}},
    )
    tsv_file = _write_tsv_file(tmp_path, "matrix_tube_id\n0012345678\n")

    rc = main(
        _lookup_argv(tsv_file, "--key", "matrix_tube_id", study_args=("--study-export-id", "QS7"))
    )

    urls = [r["url"] for r in captured["requests"]]
    expected_output = "matrix_tube_id\texport_entity_id\n0012345678\tQB517\n"
    outcome = (rc, urls, _read_output(tmp_path))
    assert outcome == (0, [_BASE + URL_STUDY_LOOKUP, _BASE + _LOOKUP_URL], expected_output)


def test__handle_biosample_lookup_reads_crlf_file(monkeypatch, tmp_path):
    """Tests the case where the file has Windows line endings: each line is one row,
    so the file is not refused, and the output maps each row in order."""
    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json={"resolved": {"QB1": 517, "QB2": 518}, "missing": []},
    )
    tsv_file = tmp_path / "biosamples.tsv"
    tsv_file.write_bytes(b"export_entity_id\tnote\r\nQB1\ta\r\nQB2\tb\r\n")

    rc = main(_lookup_argv(tsv_file, "--key", EXPORT_ENTITY_ID, "--return-key", "biosample_idx"))

    expected_output = "export_entity_id\tbiosample_idx\nQB1\t517\nQB2\t518\n"
    assert (rc, _read_output(tmp_path)) == (0, expected_output)


def test__handle_biosample_lookup_refuses_missing_values(monkeypatch, tmp_path, capsys):
    """Tests the case where some values resolve to no biosample in the study: the
    command exits 1 listing every missing value, and writes no output file."""
    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json={"resolved": {"SAMN01": "QB517"}, "missing": ["SAMN02", "SAMN03"]},
    )
    tsv_file = _write_tsv_file(tmp_path, "biosample_accession\nSAMN01\nSAMN02\nSAMN03\n")

    with pytest.raises(SystemExit) as exc_info:
        main(_lookup_argv(tsv_file, "--key", "biosample_accession"))

    stderr = capsys.readouterr().err
    outcome = (
        exc_info.value.code,
        "SAMN02" in stderr and "SAMN03" in stderr,
        (tmp_path / _OUTPUT_FILE_NAME).exists(),
    )
    assert outcome == (1, True, False)


@pytest.mark.parametrize(
    ("tsv_text", "flags", "message"),
    [
        (
            "export_entity_id\nQB1\n  \n",
            ("--key", EXPORT_ENTITY_ID, "--return-key", "biosample_idx"),
            "line(s) 3, counting",
        ),
        ("export_entity_id\nQB1\n", ("--key", EXPORT_ENTITY_ID), "same header"),
        (
            "biosample_idx\n41\n",
            ("--key", "biosample_idx", "--return-key", "biosample_idx"),
            "same header",
        ),
        ("Export_Entity_ID\nS-1\n", ("--unique-field-name", "Export_Entity_ID"), "same header"),
        ("matrix_tube_id\nX\n", ("--key", "matrix_tube_id"), "must match ^[0-9]{10}$: ['X']"),
        (
            "sample_id\nS-1\n",
            ("--key", EXPORT_ENTITY_ID, "--return-key", "biosample_idx"),
            "no 'export_entity_id' column",
        ),
        ("sample_id\nS-1\n", ("--unique-field-name", " "), "must not be blank"),
        (
            "sample_id\nS-1\n",
            ("--key", "matrix_tube_id", "--unique-field-name", "sample_id"),
            "not allowed with",
        ),
        ("export_entity_id\nQB1\n", (), "one of the arguments"),
        (
            "export_entity_id\tnote\nQB1\ta\n\nQB2\tb\n",
            ("--key", EXPORT_ENTITY_ID, "--return-key", "biosample_idx"),
            "3 data line(s) but 2 row(s)",
        ),
        (
            'export_entity_id\tnote\nQB1\t"a\nb"\nQB2\tc\n',
            ("--key", EXPORT_ENTITY_ID, "--return-key", "biosample_idx"),
            "3 data line(s) but 2 row(s)",
        ),
    ],
    ids=[
        "blank-cell",
        "returned-column-clash",
        "returned-column-clash-idx",
        "returned-column-clash-unique-field-case",
        "invalid-value",
        "missing-column",
        "blank-field-name",
        "both-identifier-flags",
        "no-identifier-flag",
        "empty-line-in-multi-column-file",
        "cell-spanning-lines",
    ],
)
def test__handle_biosample_lookup_refuses_before_request(
    monkeypatch, tmp_path, capsys, tsv_text, flags, message
):
    """Tests the case where the file or flags cannot make a valid lookup: the
    command exits 2 naming the problem, before any request."""
    captured: dict = {}
    # A consistent answer, so a lookup that wrongly reaches the route fails the assertion.
    _stub_post(monkeypatch, captured, response_json={"resolved": {}, "missing": ["QB1", "QB2"]})

    with pytest.raises(SystemExit) as exc_info:
        main(_lookup_argv(_write_tsv_file(tmp_path, tsv_text), *flags))

    stderr = capsys.readouterr().err
    assert (exc_info.value.code, captured["requests"], message in stderr) == (2, [], True), stderr


def test__handle_biosample_lookup_refuses_existing_output(monkeypatch, tmp_path, capsys):
    """Tests the case where --output names an existing file: the command exits 2
    before any request, and the file is left as it was."""
    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json={"resolved": {}, "missing": []})
    (tmp_path / _OUTPUT_FILE_NAME).write_text("SENTINEL\n", encoding="utf-8")
    tsv_file = _write_tsv_file(tmp_path, "export_entity_id\nQB1\n")

    with pytest.raises(SystemExit) as exc_info:
        main(_lookup_argv(tsv_file, "--key", EXPORT_ENTITY_ID, "--return-key", "biosample_idx"))

    outcome = (
        exc_info.value.code,
        captured["requests"],
        "already exists" in capsys.readouterr().err,
        _read_output(tmp_path),
    )
    assert outcome == (2, [], True, "SENTINEL\n")
