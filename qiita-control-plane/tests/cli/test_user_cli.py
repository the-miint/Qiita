"""Unit tests for the qiita end-user CLI scaffold + subcommands.

Subcommand-specific helpers (loopback flow, whoami, token I/O) live in
cli._common and are tested directly there or via test_cli_login.py.
This file covers the user-CLI argparse wiring and per-subcommand
dispatch.
"""

import json
import sqlite3
import sys
import types
from pathlib import Path

import pytest
from qiita_common.api_paths import (
    URL_AUTH_WHOAMI,
    URL_BIOSAMPLE_BY_IDX,
    URL_BIOSAMPLE_BY_STUDY,
    URL_BIOSAMPLE_LIST_BY_STUDY,
    URL_PREP_PROTOCOL_PREFIX,
    URL_PREP_SAMPLE_STUDY_LIST,
    URL_SEQUENCED_POOL_PREFLIGHT_UPDATE_LANE,
    URL_SEQUENCED_SAMPLE_BY_IDX,
    URL_SEQUENCED_SAMPLE_FROM_RUN,
    URL_SEQUENCED_SAMPLE_LIST_BY_RUN_FULL,
    URL_SEQUENCING_RUN_BY_IDX,
    URL_SEQUENCING_RUN_LOOKUP_BY_INSTRUMENT_RUN_ID,
    URL_SEQUENCING_RUN_PREFIX,
    URL_SEQUENCING_RUN_SEQUENCED_POOL,
    URL_STUDY_BY_IDX,
    URL_STUDY_PREFIX,
    URL_USER_ME,
    URL_WORK_TICKET_BY_IDX,
    URL_WORK_TICKET_LIST,
    URL_WORK_TICKET_PREFIX,
    URL_WORK_TICKET_STEP_LOGS,
)
from qiita_common.auth_constants import BEARER_PREFIX


def test_help_exits_cleanly(capsys):
    """`qiita --help` should print help and exit 0. Cheapest smoke test
    that the parser is well-formed and the entry point is reachable."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["--help"])
    assert exc_info.value.code == 0
    out = capsys.readouterr().out
    assert "qiita" in out
    assert "--base-url" in out


def test_no_subcommand_errors():
    """Without a subcommand argparse rejects the invocation. Locks in the
    required=True wiring on the subparser."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main([])
    # argparse exits 2 on required-arg-missing.
    assert exc_info.value.code == 2


def test_login_dispatches_to_do_login_with_qiita_command_string(monkeypatch):
    """`qiita login` calls `_common.do_login` with the parsed --base-url and
    --token-file, plus cli_command="qiita login" so error messages tell
    the user to re-run the right binary (not `qiita-admin login`)."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    captured: dict = {}

    def fake_do_login(*, base_url: str, token_file: Path, cli_command: str) -> int:
        captured["base_url"] = base_url
        captured["token_file"] = token_file
        captured["cli_command"] = cli_command
        return 0

    monkeypatch.setattr(_common, "do_login", fake_do_login)

    rc = main(
        ["--base-url", "https://qiita.example.test", "login", "--token-file", "/tmp/qiita-user"]
    )
    assert rc == 0
    assert captured["base_url"] == "https://qiita.example.test"
    assert captured["token_file"] == Path("/tmp/qiita-user")
    assert captured["cli_command"] == "qiita login"


def test_whoami_dispatches_with_base_url(monkeypatch):
    """`qiita whoami` calls `_common.whoami` with the parsed --base-url and
    the PAT loaded by run_http_subcommand."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    captured: dict = {}

    def fake_whoami(base_url: str, token: str) -> dict:
        captured["base_url"] = base_url
        captured["token"] = token
        return {"kind": "human", "principal_idx": 7}

    monkeypatch.setattr(_common, "whoami", fake_whoami)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test_user")

    rc = main(["--base-url", "https://qiita.example.test", "whoami"])
    assert rc == 0
    assert captured["base_url"] == "https://qiita.example.test"
    assert captured["token"] == "qk_test_user"


def test_whoami_without_token_errors(monkeypatch, tmp_path, capsys):
    """If QIITA_TOKEN is unset and no token file exists, whoami exits 1 with
    a message naming QIITA_TOKEN. Mirrors the admin behavior."""
    from qiita_control_plane.cli.user import main

    monkeypatch.delenv("QIITA_TOKEN", raising=False)
    monkeypatch.setattr(
        "qiita_control_plane.cli._common.TOKEN_FILE_DEFAULT",
        tmp_path / "absent",
    )
    rc = main(["whoami"])
    assert rc == 1
    assert "QIITA_TOKEN" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# profile set
# ---------------------------------------------------------------------------


def test_profile_set_sends_only_supplied_fields(monkeypatch):
    """`qiita profile set --affiliation X --phone Y` sends a body with only
    those two keys; unset fields stay absent so the server's exclude_unset
    UPDATE never touches a field the user didn't ask about."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["auth"] = headers["Authorization"]
        captured["json"] = json
        return _httpx.Response(200, json={"principal_idx": 7}, request=_httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "profile",
            "set",
            "--affiliation",
            "UCSD",
            "--phone",
            "+1-555-0100",
        ]
    )
    assert rc == 0
    assert captured["method"] == "PATCH"
    assert captured["url"] == f"https://q.example.test{URL_USER_ME}"
    assert captured["auth"] == f"{BEARER_PREFIX}qk_test"
    assert captured["json"] == {"affiliation": "UCSD", "phone": "+1-555-0100"}


def test_ticket_submit_mem_gb_sets_resource_override(monkeypatch):
    """`qiita ticket submit --mem-gb 48` carries resource_override={'mem_gb':48}
    in the POST body (the convenience flag mapping onto the model field)."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["method"] = method
        captured["json"] = json
        return _httpx.Response(
            202,
            json={"work_ticket_idx": 1, "state": "pending"},
            request=_httpx.Request(method, url),
        )

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "ticket",
            "submit",
            "--action-id",
            "fastq-to-parquet",
            "--action-version",
            "1.1.0",
            "--prep-sample-idx",
            "5",
            "--mem-gb",
            "48",
        ]
    )
    assert rc == 0
    assert captured["method"] == "POST"
    assert captured["json"]["resource_override"] == {"mem_gb": 48}
    assert captured["json"]["scope_target"] == {"kind": "prep_sample", "prep_sample_idx": 5}


def test_ticket_submit_without_mem_gb_omits_resource_override(monkeypatch):
    """Without --mem-gb the field stays absent (exclude_unset), so the server
    leaves every step on its YAML baseline."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["json"] = json
        return _httpx.Response(
            202,
            json={"work_ticket_idx": 1, "state": "pending"},
            request=_httpx.Request(method, url),
        )

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "ticket",
            "submit",
            "--action-id",
            "fastq-to-parquet",
            "--action-version",
            "1.1.0",
            "--prep-sample-idx",
            "5",
        ]
    )
    assert rc == 0
    assert "resource_override" not in captured["json"]


def test_ticket_run_posts_to_run_endpoint_with_no_body(monkeypatch):
    """`qiita ticket run <idx>` POSTs to /work-ticket/{idx}/run with no body —
    the operator resume/retry path (reset a FAILED ticket and re-dispatch,
    skipping already-completed steps)."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["method"] = method
        captured["url"] = str(url)
        captured["json"] = json
        return _httpx.Response(
            200,
            json={"work_ticket_idx": 7, "state": "pending"},
            request=_httpx.Request(method, url),
        )

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    rc = main(["--base-url", "https://q.example.test", "ticket", "run", "7"])
    assert rc == 0
    assert captured["method"] == "POST"
    assert captured["url"].endswith("/work-ticket/7/run")
    assert captured["json"] is None  # no request body


def test_profile_set_boolean_optional_action_distinguishes_unset_false_true(monkeypatch):
    """--receive-processing-emails sets True, --no-receive-processing-emails sets False,
    neither leaves the field absent from the PATCH body."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured_bodies: list[dict] = []

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured_bodies.append(json)
        return _httpx.Response(200, json={"principal_idx": 7}, request=_httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    # --receive-processing-emails → True
    main(["profile", "set", "--receive-processing-emails"])
    # --no-receive-processing-emails → False
    main(["profile", "set", "--no-receive-processing-emails"])

    assert captured_bodies == [
        {"receive_processing_emails": True},
        {"receive_processing_emails": False},
    ]


def test_profile_set_requires_at_least_one_flag(capsys):
    """`qiita profile set` with no flags should error rather than POST an
    empty body. Argparse exits 2 on parser.error()."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["profile", "set"])
    assert exc_info.value.code == 2
    assert "at least one of" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# study create
# ---------------------------------------------------------------------------


def test_study_create_minimal_sends_only_title(monkeypatch):
    """`qiita study create --title X` posts a body with only `title`.
    Optional fields stay absent so the server's column defaults apply
    rather than caller-supplied nulls overriding them."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["auth"] = headers["Authorization"]
        captured["json"] = json
        return _httpx.Response(201, json={"study_idx": 42}, request=_httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    rc = main(["--base-url", "https://q.example.test", "study", "create", "--title", "Smoke Study"])
    assert rc == 0
    assert captured["method"] == "POST"
    assert captured["url"] == f"https://q.example.test{URL_STUDY_PREFIX}"
    assert captured["auth"] == f"{BEARER_PREFIX}qk_test"
    assert captured["json"] == {"title": "Smoke Study"}


def test_study_create_passes_through_optional_fields(monkeypatch):
    """Every supplied optional flag lands in the POST body verbatim;
    snake_case translations for hyphenated CLI flags happen on the
    client side so the server contract stays clean."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["json"] = json
        return _httpx.Response(201, json={"study_idx": 42}, request=_httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    rc = main(
        [
            "study",
            "create",
            "--title",
            "T",
            "--alias",
            "T-2026",
            "--description",
            "smoke desc",
            "--abstract",
            "abs",
            "--funding",
            "NIH",
            "--ena-study-accession",
            "PRJEB99999",
            "--bioproject-accession",
            "PRJNA12345",
            "--notes",
            "note",
            "--principal-investigator-idx",
            "5",
            "--default-tier",
            "member",
        ]
    )
    assert rc == 0
    assert captured["json"] == {
        "title": "T",
        "alias": "T-2026",
        "description": "smoke desc",
        "abstract": "abs",
        "funding": "NIH",
        "ena_study_accession": "PRJEB99999",
        "bioproject_accession": "PRJNA12345",
        "notes": "note",
        "principal_investigator_idx": 5,
        "default_tier": "member",
    }


def test_study_create_requires_title(capsys):
    """--title is the only required flag; missing it should produce an
    argparse error (exit 2)."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["study", "create"])
    assert exc_info.value.code == 2
    assert "--title" in capsys.readouterr().err


def test_study_create_rejects_invalid_default_tier(capsys):
    """argparse choices= should reject a typo in --default-tier."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["study", "create", "--title", "T", "--default-tier", "owner"])
    assert exc_info.value.code == 2
    assert "default-tier" in capsys.readouterr().err


def test_study_create_pydantic_validation_error_exits_2(capsys):
    """A --title longer than StudyCreate's max_length=500 trips Pydantic
    client-side; we surface a flat error line via parser.error and exit
    2, not a traceback."""
    from qiita_control_plane.cli.user import main

    long_title = "x" * 501
    with pytest.raises(SystemExit) as exc_info:
        main(["study", "create", "--title", long_title])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "invalid StudyCreate" in err
    assert "title" in err


def test_study_create_passes_extra_metadata(monkeypatch):
    """--extra-metadata is parsed from JSON into a dict and lands in the
    POST body verbatim under the snake_case key."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["json"] = json
        return _httpx.Response(201, json={"study_idx": 42}, request=_httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    rc = main(
        [
            "study",
            "create",
            "--title",
            "T",
            "--extra-metadata",
            '{"site":"ucsd","vamps_id":"VAMPS-1"}',
        ]
    )
    assert rc == 0
    assert captured["json"] == {
        "title": "T",
        "extra_metadata": {"site": "ucsd", "vamps_id": "VAMPS-1"},
    }


def test_study_create_rejects_malformed_extra_metadata(capsys):
    """Non-JSON --extra-metadata exits 2 via parser.error rather than a
    JSONDecodeError traceback."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["study", "create", "--title", "T", "--extra-metadata", "{not-json"])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--extra-metadata" in err
    assert "not valid JSON" in err


def test_study_create_rejects_non_object_extra_metadata(capsys):
    """--extra-metadata must be a JSON object (matches the JSONB-on-server
    convention). A bare array or scalar should fail fast."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["study", "create", "--title", "T", "--extra-metadata", "[1, 2, 3]"])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--extra-metadata" in err
    assert "JSON object" in err


def test_profile_set_pydantic_validation_error_exits_2(capsys):
    """A malformed --orcid trips Pydantic client-side via UserUpdate's
    pattern constraint; surfaced as a flat parser.error."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["profile", "set", "--orcid", "not-an-orcid"])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "invalid UserUpdate" in err
    assert "orcid" in err


# ---------------------------------------------------------------------------
# Shared request-stub helper (used by every HTTP subcommand test below)
# ---------------------------------------------------------------------------


def _stub_post(
    monkeypatch,
    captured: dict,
    *,
    response_json: dict,
    status: int = 201,
    whoami_idx: int | None = None,
):
    """Patch `_common.httpx.request` to capture every call and return canned
    responses. Each call appends to `captured['requests']` (full list); the
    last call's fields also land flat on `captured` (method/url/json/auth)
    so single-call tests can assert on `captured['url']` etc. without
    indexing.

    When `whoami_idx` is supplied, a GET to `/auth/whoami` returns
    `{"kind": "human", "principal_idx": whoami_idx}` (used by handlers
    that auto-default --owner-idx). Every other request returns
    `response_json` with HTTP `status`.
    """
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured.setdefault("requests", [])

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        record = {
            "method": method,
            "url": url,
            "auth": headers["Authorization"],
            "json": json,
            "params": params,
        }
        captured["requests"].append(record)
        # Flat-shape mirror — last-wins so single-POST tests read the POST,
        # whoami-then-POST tests still see the POST as the "current" record.
        captured.update(record)
        if url.endswith("/auth/whoami"):
            assert whoami_idx is not None, (
                "test triggered whoami without supplying whoami_idx in the stub"
            )
            return _httpx.Response(
                200,
                json={"kind": "human", "principal_idx": whoami_idx},
                request=_httpx.Request(method, url),
            )
        return _httpx.Response(status, json=response_json, request=_httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")


# Canned response_json bodies for the resource creates. Hoisted out of the
# individual tests so a schema-rename only touches one site.
_BIOSAMPLE_CREATE_RESPONSE = {
    "biosample_idx": 99,
    "owner_id_biosample_study_field_idx": 5,
    "owner_id_biosample_study_field_created": True,
}
_SEQUENCED_SAMPLE_CREATE_RESPONSE = {
    "prep_sample_idx": 100,
    "sequenced_sample_idx": 200,
}


# ---------------------------------------------------------------------------
# biosample create
# ---------------------------------------------------------------------------


def test_biosample_create_defaults_owner_idx_to_caller(monkeypatch):
    """When --owner-idx is omitted, the handler resolves the caller's
    principal_idx via whoami and uses that as owner_idx on the POST body."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BIOSAMPLE_CREATE_RESPONSE, whoami_idx=42)

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "biosample",
            "create",
            "--study-idx",
            "7",
            "--owner-biosample-id-field-name",
            "owner_sample_id",
            "--owner-biosample-id-value",
            "SMK-001",
        ]
    )
    assert rc == 0
    # whoami first (to resolve owner), then the actual POST.
    assert [r["method"] for r in captured["requests"]] == ["GET", "POST"]
    whoami_req, post_req = captured["requests"]
    assert whoami_req["url"].endswith(URL_AUTH_WHOAMI)
    assert post_req["url"] == (
        f"https://q.example.test{URL_BIOSAMPLE_BY_STUDY.format(study_idx=7)}"
    )
    # Unset --metadata stays absent on the wire (default=None → not None
    # filter drops it); server's default_factory=dict fills the model.
    assert post_req["json"] == {
        "owner_idx": 42,
        "owner_biosample_id_field_name": "owner_sample_id",
        "owner_biosample_id_value": "SMK-001",
    }


def test_biosample_create_explicit_owner_idx_skips_whoami(monkeypatch):
    """When --owner-idx is supplied, no whoami round-trip — the caller
    is acting on someone else's behalf (lab-tech-on-behalf path)."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BIOSAMPLE_CREATE_RESPONSE)

    rc = main(
        [
            "biosample",
            "create",
            "--study-idx",
            "7",
            "--owner-idx",
            "11",
            "--owner-biosample-id-field-name",
            "owner_sample_id",
            "--owner-biosample-id-value",
            "SMK-002",
        ]
    )
    assert rc == 0
    assert [r["method"] for r in captured["requests"]] == ["POST"]
    assert captured["requests"][0]["json"]["owner_idx"] == 11


def test_biosample_create_metadata_pairs_become_dict(monkeypatch):
    """Repeated --metadata KEY=VALUE collects into a dict on the wire,
    keyed verbatim on the user-supplied display_name strings."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BIOSAMPLE_CREATE_RESPONSE, whoami_idx=42)

    rc = main(
        [
            "biosample",
            "create",
            "--study-idx",
            "7",
            "--owner-biosample-id-field-name",
            "owner_sample_id",
            "--owner-biosample-id-value",
            "SMK-001",
            "--metadata",
            "host_subject_id=mouse-1",
            "--metadata",
            "collection_date=2026-05-19",
        ]
    )
    assert rc == 0
    assert captured["requests"][-1]["json"]["metadata"] == {
        "host_subject_id": "mouse-1",
        "collection_date": "2026-05-19",
    }


def test_biosample_create_passes_through_optional_fields(monkeypatch):
    """Tests the case where every CLI-exposed optional field
    (metadata_checklist_name, biosample_accession, ena_sample_accession,
    matrix_tube_id) flows into the POST body when supplied."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_BIOSAMPLE_CREATE_RESPONSE, whoami_idx=42)

    rc = main(
        [
            "biosample",
            "create",
            "--study-idx",
            "7",
            "--owner-biosample-id-field-name",
            "owner_sample_id",
            "--owner-biosample-id-value",
            "SMK-001",
            "--metadata-checklist-name",
            "ERC000015",
            "--biosample-accession",
            "SAMN12345678",
            "--ena-sample-accession",
            "ERS1234567",
            "--matrix-tube-id",
            "0123456789",
        ]
    )
    assert rc == 0
    body = captured["requests"][-1]["json"]
    assert body["metadata_checklist_name"] == "ERC000015"
    assert body["biosample_accession"] == "SAMN12345678"
    assert body["ena_sample_accession"] == "ERS1234567"
    assert body["matrix_tube_id"] == "0123456789"


def test_biosample_create_requires_required_flags(capsys):
    """Missing any of --study-idx / --owner-biosample-id-field-name /
    --owner-biosample-id-value should produce an argparse error (exit 2)."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["biosample", "create"])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--study-idx" in err


def test_biosample_create_rejects_malformed_metadata(capsys):
    """A --metadata entry without '=' is a typo, not a key with empty
    value; reject loudly via parser.error (exit 2)."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "biosample",
                "create",
                "--study-idx",
                "7",
                "--owner-biosample-id-field-name",
                "owner_sample_id",
                "--owner-biosample-id-value",
                "SMK-001",
                "--metadata",
                "no_equals_sign",
            ]
        )
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--metadata" in err
    assert "missing '='" in err


def test_biosample_create_rejects_duplicate_metadata_key(capsys):
    """Duplicate --metadata KEY entries are almost always a typo; reject
    rather than silently last-wins."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "biosample",
                "create",
                "--study-idx",
                "7",
                "--owner-biosample-id-field-name",
                "owner_sample_id",
                "--owner-biosample-id-value",
                "SMK-001",
                "--metadata",
                "k=v1",
                "--metadata",
                "k=v2",
            ]
        )
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--metadata" in err
    assert "repeated" in err


# ---------------------------------------------------------------------------
# sequencing-run create
# ---------------------------------------------------------------------------


def test_sequencing_run_create_minimal(monkeypatch):
    """Only --instrument-run-id + --platform should be necessary; optional
    columns stay absent so the server's defaults apply."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json={"sequencing_run_idx": 4})

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "sequencing-run",
            "create",
            "--instrument-run-id",
            "240301_MN12345_0001_AAATEST",
            "--platform",
            "illumina",
        ]
    )
    assert rc == 0
    assert captured["method"] == "POST"
    assert captured["url"] == f"https://q.example.test{URL_SEQUENCING_RUN_PREFIX}"
    assert captured["json"] == {
        "instrument_run_id": "240301_MN12345_0001_AAATEST",
        "platform": "illumina",
    }


def test_sequencing_run_create_passes_through_optional_fields(monkeypatch):
    """All optional flags surface verbatim in the body. --extra-metadata
    is parsed from JSON into a dict before send."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json={"sequencing_run_idx": 4})

    rc = main(
        [
            "sequencing-run",
            "create",
            "--instrument-run-id",
            "240301_MN12345_0001_AAATEST",
            "--platform",
            "oxford_nanopore",
            "--instrument-model",
            "MinION Mk1C",
            "--instrument-serial",
            "MN12345",
            "--run-performed-at",
            "2026-05-19T15:30:00Z",
            "--extra-metadata",
            '{"chemistry":"R10.4.1"}',
        ]
    )
    assert rc == 0
    assert captured["json"] == {
        "instrument_run_id": "240301_MN12345_0001_AAATEST",
        "platform": "oxford_nanopore",
        "instrument_model": "MinION Mk1C",
        "instrument_serial": "MN12345",
        # Pydantic normalizes the trailing Z to +00:00 on AwareDatetime round-trip
        "run_performed_at": "2026-05-19T15:30:00Z",
        "extra_metadata": {"chemistry": "R10.4.1"},
    }


def test_sequencing_run_create_requires_instrument_run_id_and_platform(capsys):
    """Argparse should refuse the call without the two required flags."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["sequencing-run", "create"])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--instrument-run-id" in err


def test_sequencing_run_create_rejects_unknown_platform(capsys):
    """choices= guards a typo in --platform before any HTTP round-trip."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "sequencing-run",
                "create",
                "--instrument-run-id",
                "X",
                "--platform",
                "iontorrent",  # missing underscore
            ]
        )
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--platform" in err


def test_sequencing_run_create_rejects_malformed_extra_metadata(capsys):
    """Non-JSON --extra-metadata exits 2 via parser.error rather than a
    JSONDecodeError traceback."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "sequencing-run",
                "create",
                "--instrument-run-id",
                "X",
                "--platform",
                "illumina",
                "--extra-metadata",
                "{not-json",
            ]
        )
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--extra-metadata" in err
    assert "not valid JSON" in err


# ---------------------------------------------------------------------------
# sequencing-run get
# ---------------------------------------------------------------------------


_SEQUENCING_RUN_RESPONSE = {
    "sequencing_run_idx": 4,
    "instrument_run_id": "240301_MN12345_0001_AAATEST",
    "platform": "illumina",
    "instrument_model": "Illumina NovaSeq 6000",
    "instrument_serial": None,
    "run_performed_at": None,
    "extra_metadata": None,
    "created_by_idx": 7,
    "created_at": "2026-05-20T00:00:00+00:00",
    "retired": False,
    "retired_by_idx": None,
    "retired_at": None,
    "retire_reason": None,
}


def test_sequencing_run_get_issues_get_against_the_idx(monkeypatch):
    """`sequencing-run get --sequencing-run-idx N` GETs the by-idx path and
    issues no body."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_SEQUENCING_RUN_RESPONSE, status=200)

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "sequencing-run",
            "get",
            "--sequencing-run-idx",
            "4",
        ]
    )
    assert rc == 0
    assert captured["method"] == "GET"
    assert captured["url"] == (
        f"https://q.example.test{URL_SEQUENCING_RUN_BY_IDX.format(sequencing_run_idx=4)}"
    )
    assert captured["json"] is None


def test_sequencing_run_get_requires_idx(capsys):
    """Omitting --sequencing-run-idx exits 2 via argparse."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["sequencing-run", "get"])
    assert exc_info.value.code == 2
    assert "--sequencing-run-idx" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# sequencing-run lookup
# ---------------------------------------------------------------------------


def test_sequencing_run_lookup_posts_instrument_run_ids(monkeypatch):
    """`sequencing-run lookup --instrument-run-id A --instrument-run-id B`
    POSTs the lookup path with both ids in the body list."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json={"resolved": {"A": 1, "B": 2}, "missing": []},
        status=200,
    )

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "sequencing-run",
            "lookup",
            "--instrument-run-id",
            "A",
            "--instrument-run-id",
            "B",
        ]
    )
    assert rc == 0
    assert captured["method"] == "POST"
    assert captured["url"] == (
        f"https://q.example.test{URL_SEQUENCING_RUN_LOOKUP_BY_INSTRUMENT_RUN_ID}"
    )
    assert captured["json"] == {"instrument_run_ids": ["A", "B"]}


def test_sequencing_run_lookup_requires_at_least_one_id(capsys):
    """Omitting --instrument-run-id exits 2 via argparse (required=True)."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["sequencing-run", "lookup"])
    assert exc_info.value.code == 2
    assert "--instrument-run-id" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# prep-sample list-studies
# ---------------------------------------------------------------------------


def test_prep_sample_list_studies_issues_get_against_the_idx(monkeypatch):
    """`prep-sample list-studies --prep-sample-idx N` GETs the study-list path
    and issues no body."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json={
            "studies": [],
            "count": 0,
            "truncated": False,
            "caller_system_role": "wet_lab_admin",
        },
        status=200,
    )

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "prep-sample",
            "list-studies",
            "--prep-sample-idx",
            "9",
        ]
    )
    assert rc == 0
    assert captured["method"] == "GET"
    assert captured["url"] == (
        f"https://q.example.test{URL_PREP_SAMPLE_STUDY_LIST.format(prep_sample_idx=9)}"
    )
    assert captured["json"] is None


def test_prep_sample_list_studies_requires_idx(capsys):
    """Omitting --prep-sample-idx exits 2 via argparse."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["prep-sample", "list-studies"])
    assert exc_info.value.code == 2
    assert "--prep-sample-idx" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# sequenced-sample list
# ---------------------------------------------------------------------------


def test_sequenced_sample_list_issues_get_against_the_run(monkeypatch):
    """`sequenced-sample list --sequencing-run-idx N` GETs the run-scoped
    full-list path and issues no body; it prints the envelope verbatim."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json={
            "samples": [],
            "count": 0,
            "truncated": False,
            "caller_system_role": "wet_lab_admin",
        },
        status=200,
    )

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "sequenced-sample",
            "list",
            "--sequencing-run-idx",
            "7",
        ]
    )
    assert rc == 0
    assert captured["method"] == "GET"
    assert captured["url"] == (
        f"https://q.example.test{URL_SEQUENCED_SAMPLE_LIST_BY_RUN_FULL.format(sequencing_run_idx=7)}"
    )
    assert captured["json"] is None


def test_sequenced_sample_list_requires_run_idx(capsys):
    """Omitting --sequencing-run-idx exits 2 via argparse."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["sequenced-sample", "list"])
    assert exc_info.value.code == 2
    assert "--sequencing-run-idx" in capsys.readouterr().err


def test_sequencing_run_create_rejects_non_object_extra_metadata(capsys):
    """--extra-metadata must be a JSON object (matches the JSONB-on-server
    convention). A bare array or scalar should fail fast."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "sequencing-run",
                "create",
                "--instrument-run-id",
                "X",
                "--platform",
                "illumina",
                "--extra-metadata",
                "[1, 2, 3]",
            ]
        )
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--extra-metadata" in err
    assert "JSON object" in err


# ---------------------------------------------------------------------------
# sequenced-pool create
# ---------------------------------------------------------------------------


def test_sequenced_pool_create_minimal_no_preflight(monkeypatch):
    """No --run-preflight-blob / --run-preflight-filename means a pool
    with no preflight; the pair is optional."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json={"sequenced_pool_idx": 9})

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "sequenced-pool",
            "create",
            "--run-idx",
            "4",
        ]
    )
    assert rc == 0
    assert captured["method"] == "POST"
    assert captured["url"] == (
        f"https://q.example.test{URL_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=4)}"
    )
    # extra_metadata=None gets stripped by exclude_unset; nothing else to send.
    assert captured["json"] == {}


def test_sequenced_pool_create_with_preflight_blob_and_explicit_filename(monkeypatch, tmp_path):
    """--run-preflight-blob reads bytes from the file, --run-preflight-filename
    overrides the auto-default."""
    import base64

    from qiita_control_plane.cli.user import main

    blob_path = tmp_path / "RunPreflight.db"
    blob_bytes = b"fake-sqlite-preflight-content"
    blob_path.write_bytes(blob_bytes)

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json={"sequenced_pool_idx": 9})

    rc = main(
        [
            "sequenced-pool",
            "create",
            "--run-idx",
            "4",
            "--run-preflight-blob",
            str(blob_path),
            "--run-preflight-filename",
            "uploaded.db",
        ]
    )
    assert rc == 0
    body = captured["json"]
    assert body["run_preflight_filename"] == "uploaded.db"
    # On the wire, Pydantic re-encodes the bytes as base64.
    assert body["run_preflight_blob"] == base64.b64encode(blob_bytes).decode("ascii")


def test_sequenced_pool_create_defaults_filename_from_blob_path(monkeypatch, tmp_path):
    """When --run-preflight-filename is omitted, the handler defaults it to
    the basename of --run-preflight-blob so a half-populated pair never
    reaches the wire."""
    from qiita_control_plane.cli.user import main

    blob_path = tmp_path / "RunPreflight.db"
    blob_path.write_bytes(b"x")

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json={"sequenced_pool_idx": 9})

    rc = main(
        [
            "sequenced-pool",
            "create",
            "--run-idx",
            "4",
            "--run-preflight-blob",
            str(blob_path),
        ]
    )
    assert rc == 0
    assert captured["json"]["run_preflight_filename"] == "RunPreflight.db"


def test_sequenced_pool_create_refuses_filename_without_blob(capsys):
    """A half-populated pair would be a 422 server-side; refuse before HTTP."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "sequenced-pool",
                "create",
                "--run-idx",
                "4",
                "--run-preflight-filename",
                "stranded.db",
            ]
        )
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--run-preflight-filename" in err
    assert "--run-preflight-blob" in err


def test_sequenced_pool_create_refuses_missing_blob_file(capsys, tmp_path):
    """A --run-preflight-blob path that doesn't exist fails before HTTP."""
    from qiita_control_plane.cli.user import main

    missing = tmp_path / "not-there.db"
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "sequenced-pool",
                "create",
                "--run-idx",
                "4",
                "--run-preflight-blob",
                str(missing),
            ]
        )
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--run-preflight-blob" in err
    assert "not a regular file" in err


def test_sequenced_pool_create_refuses_empty_blob_file(capsys, tmp_path):
    """An empty file would trip the model's min_length=1; surface as a
    clean argparse error."""
    from qiita_control_plane.cli.user import main

    blob_path = tmp_path / "empty.db"
    blob_path.write_bytes(b"")

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "sequenced-pool",
                "create",
                "--run-idx",
                "4",
                "--run-preflight-blob",
                str(blob_path),
            ]
        )
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "is empty" in err


def test_sequenced_pool_create_requires_run_idx(capsys):
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["sequenced-pool", "create"])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--run-idx" in err


# ---------------------------------------------------------------------------
# sequenced-sample create
# ---------------------------------------------------------------------------


def test_sequenced_sample_create_minimal_with_caller_owner(monkeypatch):
    """Owner defaults to the caller via whoami; primary-only (no secondary
    studies, no metadata, no checklist) sends the smallest valid body."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(
        monkeypatch, captured, response_json=_SEQUENCED_SAMPLE_CREATE_RESPONSE, whoami_idx=42
    )

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "sequenced-sample",
            "create",
            "--run-idx",
            "4",
            "--pool-idx",
            "9",
            "--biosample-idx",
            "55",
            "--prep-protocol-idx",
            "3",
            "--pool-item-id",
            "WELL-A1",
            "--primary-study-idx",
            "7",
        ]
    )
    assert rc == 0
    assert [r["method"] for r in captured["requests"]] == ["GET", "POST"]
    post = captured["requests"][1]
    assert post["url"] == (
        f"https://q.example.test"
        f"{URL_SEQUENCED_SAMPLE_FROM_RUN.format(sequencing_run_idx=4, sequenced_pool_idx=9)}"
    )
    # --metadata stays absent (default=None → filtered, server fills {}).
    # secondary_study_idxs always lands on the wire because the model's
    # dedupe_secondary_study_idxs validator reassigns it, marking the field
    # as "set" even when the caller didn't pass --secondary-study-idx.
    assert post["json"] == {
        "biosample_idx": 55,
        "prep_protocol_idx": 3,
        "owner_idx": 42,
        "sequenced_pool_item_id": "WELL-A1",
        "primary_study_idx": 7,
        "secondary_study_idxs": [],
    }


def test_sequenced_sample_create_with_secondary_studies_and_metadata(monkeypatch):
    """Repeated --secondary-study-idx accumulates into a list; metadata
    KEY=VALUE entries collect into a dict."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(
        monkeypatch, captured, response_json=_SEQUENCED_SAMPLE_CREATE_RESPONSE, whoami_idx=42
    )

    rc = main(
        [
            "sequenced-sample",
            "create",
            "--run-idx",
            "4",
            "--pool-idx",
            "9",
            "--biosample-idx",
            "55",
            "--prep-protocol-idx",
            "3",
            "--pool-item-id",
            "WELL-A1",
            "--primary-study-idx",
            "7",
            "--secondary-study-idx",
            "8",
            "--secondary-study-idx",
            "12",
            "--metadata",
            "library_prep_kit=Nextera XT",
            "--metadata",
            "barcode=AAGCTT",
        ]
    )
    assert rc == 0
    body = captured["requests"][-1]["json"]
    assert body["secondary_study_idxs"] == [8, 12]
    assert body["metadata"] == {
        "library_prep_kit": "Nextera XT",
        "barcode": "AAGCTT",
    }


def test_sequenced_sample_create_explicit_owner_skips_whoami(monkeypatch):
    """When --owner-idx is supplied, no whoami round-trip — the caller is
    acting on someone else's behalf."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_SEQUENCED_SAMPLE_CREATE_RESPONSE)

    rc = main(
        [
            "sequenced-sample",
            "create",
            "--run-idx",
            "4",
            "--pool-idx",
            "9",
            "--biosample-idx",
            "55",
            "--prep-protocol-idx",
            "3",
            "--owner-idx",
            "11",
            "--pool-item-id",
            "WELL-A1",
            "--primary-study-idx",
            "7",
        ]
    )
    assert rc == 0
    assert [r["method"] for r in captured["requests"]] == ["POST"]
    assert captured["requests"][0]["json"]["owner_idx"] == 11


def test_sequenced_sample_create_metadata_checklist_passes_through(monkeypatch):
    """--metadata-checklist-name flows verbatim; ENA accession fields stay
    absent when their flags are not supplied."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(
        monkeypatch, captured, response_json=_SEQUENCED_SAMPLE_CREATE_RESPONSE, whoami_idx=42
    )

    rc = main(
        [
            "sequenced-sample",
            "create",
            "--run-idx",
            "4",
            "--pool-idx",
            "9",
            "--biosample-idx",
            "55",
            "--prep-protocol-idx",
            "3",
            "--pool-item-id",
            "WELL-A1",
            "--primary-study-idx",
            "7",
            "--metadata-checklist-name",
            "ERC000015",
        ]
    )
    assert rc == 0
    body = captured["requests"][-1]["json"]
    assert body["metadata_checklist_name"] == "ERC000015"
    assert "ena_experiment_accession" not in body
    assert "ena_run_accession" not in body


def test_sequenced_sample_create_passes_ena_accessions(monkeypatch):
    """Tests the case where --ena-experiment-accession and --ena-run-accession
    flow into the POST body — a sequenced sample may already carry ENA
    accessions at create time."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(
        monkeypatch, captured, response_json=_SEQUENCED_SAMPLE_CREATE_RESPONSE, whoami_idx=42
    )

    rc = main(
        [
            "sequenced-sample",
            "create",
            "--run-idx",
            "4",
            "--pool-idx",
            "9",
            "--biosample-idx",
            "55",
            "--prep-protocol-idx",
            "3",
            "--pool-item-id",
            "WELL-A1",
            "--primary-study-idx",
            "7",
            "--ena-experiment-accession",
            "ERX9999999",
            "--ena-run-accession",
            "ERR9999999",
        ]
    )
    assert rc == 0
    body = captured["requests"][-1]["json"]
    assert body["ena_experiment_accession"] == "ERX9999999"
    assert body["ena_run_accession"] == "ERR9999999"


def test_sequenced_sample_create_requires_required_flags(capsys):
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["sequenced-sample", "create"])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    # argparse names the first missing required flag in its standard error
    # message; just check one of ours is mentioned so the test isn't tied
    # to argparse's choice of "which one".
    assert "required" in err
    assert "--run-idx" in err or "--pool-idx" in err or "--biosample-idx" in err


def test_sequenced_sample_create_rejects_primary_in_secondary(monkeypatch, capsys):
    """The model's primary-in-secondary validator fires client-side via
    _build_body and surfaces as a parser.error."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(
        monkeypatch, captured, response_json=_SEQUENCED_SAMPLE_CREATE_RESPONSE, whoami_idx=42
    )

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "sequenced-sample",
                "create",
                "--run-idx",
                "4",
                "--pool-idx",
                "9",
                "--biosample-idx",
                "55",
                "--prep-protocol-idx",
                "3",
                "--pool-item-id",
                "WELL-A1",
                "--primary-study-idx",
                "7",
                "--secondary-study-idx",
                "7",
            ]
        )
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "invalid SequencedSampleCreateRequest" in err
    assert "primary_study_idx" in err


# ---------------------------------------------------------------------------
# ticket submit
# ---------------------------------------------------------------------------


def test_ticket_submit_minimal_prep_sample_scope(monkeypatch):
    """--prep-sample-idx is the smoke-flow convenience; constructs the
    scope_target dict and POSTs an action_context of {} by default."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json={"work_ticket_idx": 12, "state": "pending"},
        status=202,
    )

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "ticket",
            "submit",
            "--action-id",
            "fastq-to-parquet",
            "--action-version",
            "1.0.0",
            "--prep-sample-idx",
            "55",
        ]
    )
    assert rc == 0
    assert captured["method"] == "POST"
    assert captured["url"] == f"https://q.example.test{URL_WORK_TICKET_PREFIX}"
    assert captured["json"] == {
        "action_id": "fastq-to-parquet",
        "action_version": "1.0.0",
        "scope_target": {"kind": "prep_sample", "prep_sample_idx": 55},
    }


def test_ticket_submit_with_context_json(monkeypatch):
    """A paired-end --context-json is parsed before POST; both fastq paths
    land on the wire as action_context."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json={"work_ticket_idx": 12, "state": "pending"},
        status=202,
    )

    rc = main(
        [
            "ticket",
            "submit",
            "--action-id",
            "fastq-to-parquet",
            "--action-version",
            "1.0.0",
            "--prep-sample-idx",
            "55",
            "--context-json",
            '{"fastq_path": "/scratch/filename_prefix_R1.fastq",'
            ' "reverse_fastq_path": "/scratch/filename_prefix_R2.fastq"}',
        ]
    )
    assert rc == 0
    assert captured["json"]["action_context"] == {
        "fastq_path": "/scratch/filename_prefix_R1.fastq",
        "reverse_fastq_path": "/scratch/filename_prefix_R2.fastq",
    }


def test_ticket_submit_with_scope_target_json(monkeypatch):
    """--scope-target-json is the escape hatch for non-prep_sample scopes."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json={"work_ticket_idx": 12, "state": "pending"},
        status=202,
    )

    rc = main(
        [
            "ticket",
            "submit",
            "--action-id",
            "reference-add",
            "--action-version",
            "1.0.0",
            "--scope-target-json",
            '{"kind": "reference", "reference_idx": 8}',
        ]
    )
    assert rc == 0
    assert captured["json"]["scope_target"] == {"kind": "reference", "reference_idx": 8}


def test_ticket_submit_requires_a_scope_target(capsys):
    """The mutex group is required=True; neither flag → exit 2."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "ticket",
                "submit",
                "--action-id",
                "fastq-to-parquet",
                "--action-version",
                "1.0.0",
            ]
        )
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--prep-sample-idx" in err or "--scope-target-json" in err


def test_ticket_submit_rejects_both_scope_target_forms(capsys):
    """The mutex group rejects supplying both flags."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "ticket",
                "submit",
                "--action-id",
                "fastq-to-parquet",
                "--action-version",
                "1.0.0",
                "--prep-sample-idx",
                "55",
                "--scope-target-json",
                '{"kind": "reference", "reference_idx": 8}',
            ]
        )
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "not allowed" in err


def test_ticket_submit_rejects_malformed_context_json(capsys):
    """Malformed --context-json exits 2 via parser.error rather than a
    JSONDecodeError traceback."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "ticket",
                "submit",
                "--action-id",
                "fastq-to-parquet",
                "--action-version",
                "1.0.0",
                "--prep-sample-idx",
                "55",
                "--context-json",
                "{not-json",
            ]
        )
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--context-json" in err
    assert "not valid JSON" in err


# ---------------------------------------------------------------------------
# ticket status
# ---------------------------------------------------------------------------


_TICKET_STATUS_RESPONSE = {
    "work_ticket_idx": 12,
    "action_id": "fastq-to-parquet",
    "action_version": "1.0.0",
    "originator_principal_idx": 7,
    "scope_target": {"kind": "prep_sample", "prep_sample_idx": 55},
    "action_context": {"fastq_path": "/scratch/sample.fastq"},
    "state": "processing",
    "retry_count": 0,
    "max_retries": 3,
    "failure_type": None,
    "failure_stage": None,
    "failure_step_name": None,
    "failure_reason": None,
    "created_at": "2026-05-20T00:00:00+00:00",
    "updated_at": "2026-05-20T00:00:01+00:00",
}


def test_ticket_status_issues_get_against_the_idx(monkeypatch):
    """Positional `work_ticket_idx` lands on the path; the handler issues
    a GET (not a POST) and the captured response shape carries the full
    WorkTicket fields the route returns."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_TICKET_STATUS_RESPONSE, status=200)

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "ticket",
            "status",
            "12",
        ]
    )
    assert rc == 0
    assert captured["method"] == "GET"
    assert captured["url"] == (
        f"https://q.example.test{URL_WORK_TICKET_BY_IDX.format(work_ticket_idx=12)}"
    )
    # A GET has no body — assert the stub captured no JSON payload.
    assert captured["json"] is None


def test_ticket_status_requires_idx(capsys):
    """Omitting the positional argument exits 2 with the standard
    argparse error pointing at `work_ticket_idx`."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["ticket", "status"])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "work_ticket_idx" in err


# ---------------------------------------------------------------------------
# ticket logs
# ---------------------------------------------------------------------------


_TICKET_LOGS_RESPONSE = {
    "work_ticket_idx": 12,
    "step_index": 0,
    "attempt": 1,
    "step_name": "stage_local_fasta",
    "stdout": "starting\n",
    "stderr": "oom_kill event\n",
    "stdout_truncated": False,
    "stderr_truncated": False,
}


def test_ticket_logs_issues_get_against_the_step_path(monkeypatch):
    """`ticket logs <idx> --step-index N` GETs the step-logs path; with no
    --attempt / --tail-lines, no query params are sent so the server defaults
    (latest attempt, 200 lines) apply."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_TICKET_LOGS_RESPONSE, status=200)

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "ticket",
            "logs",
            "12",
            "--step-index",
            "0",
        ]
    )
    assert rc == 0
    assert captured["method"] == "GET"
    expected_path = URL_WORK_TICKET_STEP_LOGS.format(work_ticket_idx=12, step_index=0)
    assert captured["url"] == f"https://q.example.test{expected_path}"
    assert captured["json"] is None
    assert captured["params"] == {}


def test_ticket_logs_forwards_attempt_and_tail_lines(monkeypatch):
    """--attempt and --tail-lines ride along as query params."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_TICKET_LOGS_RESPONSE, status=200)

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "ticket",
            "logs",
            "12",
            "--step-index",
            "0",
            "--attempt",
            "0",
            "--tail-lines",
            "50",
        ]
    )
    assert rc == 0
    assert captured["params"] == {"attempt": "0", "tail_lines": "50"}


def test_ticket_logs_renders_streams_raw_not_json(monkeypatch, capsys):
    """The logs verb prints each stream raw (newlines rendered) instead of a
    JSON dump, so a fetched stack trace is actually readable."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_TICKET_LOGS_RESPONSE, status=200)

    rc = main(["--base-url", "https://q.example.test", "ticket", "logs", "12", "--step-index", "0"])
    assert rc == 0
    out = capsys.readouterr().out
    # Raw newline rendering, not a JSON object with escaped \n.
    assert "oom_kill event" in out
    assert "\\n" not in out
    assert '"stderr"' not in out
    assert "===== stderr =====" in out


def test_ticket_logs_requires_step_index(capsys):
    """--step-index is required; omitting it exits 2 via argparse."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["ticket", "logs", "12"])
    assert exc_info.value.code == 2
    assert "step-index" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# ticket list
# ---------------------------------------------------------------------------


_TICKET_LIST_RESPONSE = [
    {
        "work_ticket_idx": 12,
        "action_id": "fastq-to-parquet",
        "action_version": "1.0.0",
        "originator_principal_idx": 7,
        "scope_target": {"kind": "prep_sample", "prep_sample_idx": 55},
        "action_context": {},
        "state": "processing",
        "retry_count": 0,
        "max_retries": 3,
        "failure_type": None,
        "failure_stage": None,
        "failure_step_name": None,
        "failure_reason": None,
        "created_at": "2026-05-20T00:00:00+00:00",
        "updated_at": "2026-05-20T00:00:01+00:00",
        "current_step_index": 0,
        "current_step_name": "convert",
        "compute_target": "slurm",
        "slurm_job_id": 4242,
        "step_state": "running",
    }
]


def test_ticket_list_issues_get_with_no_filter_params(monkeypatch):
    """Bare `ticket list` GETs the work-ticket root with no query params
    (server defaults: own tickets, all states, default limit)."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_TICKET_LIST_RESPONSE, status=200)

    rc = main(["--base-url", "https://q.example.test", "ticket", "list"])
    assert rc == 0
    assert captured["method"] == "GET"
    assert captured["url"] == f"https://q.example.test{URL_WORK_TICKET_LIST}"
    assert captured["json"] is None
    assert captured["params"] == {}


def test_ticket_list_passes_filter_params(monkeypatch):
    """--state / --active / --all / --limit map onto the query params; --all
    is sent as `all=true` (the route's alias)."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=_TICKET_LIST_RESPONSE, status=200)

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "ticket",
            "list",
            "--state",
            "processing",
            "--active",
            "--all",
            "--limit",
            "10",
        ]
    )
    assert rc == 0
    assert captured["params"] == {
        "state": "processing",
        "active": "true",
        "all": "true",
        "limit": "10",
    }


def test_ticket_list_rejects_unknown_state(capsys):
    """--state is constrained to the WorkTicketState values (argparse
    choices), so a bogus value exits 2 before any HTTP call."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["ticket", "list", "--state", "bogus"])
    assert exc_info.value.code == 2
    assert "bogus" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# HTTP-error handling (run_http_subcommand)
# ---------------------------------------------------------------------------


def test_http_error_response_prints_to_stderr_and_exits_1(monkeypatch, capsys):
    """A non-2xx response surfaces through run_http_subcommand: `call`'s
    raise_for_status() throws httpx.HTTPStatusError, the CLI prints
    `http error <code>: <body>` to stderr and returns exit code 1."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json={"detail": "requires study access at tier 'admin' or higher"},
        status=403,
    )

    rc = main(["study", "create", "--title", "denied-study"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "http error 403" in err
    # The server's response body is echoed so the user sees the reason.
    assert "requires study access" in err


def test_connection_error_prints_friendly_message_and_exits_1(monkeypatch, capsys):
    """A transport-level failure (control plane down, wrong --base-url) raises
    httpx.RequestError, which has no HTTP response. run_http_subcommand catches
    it and prints a friendly, actionable message naming the target URL — not a
    raw traceback — and returns exit code 1."""
    import httpx

    from qiita_control_plane.cli.user import main

    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    def boom(base_url, token):
        # Mirror how httpx raises a connect failure: the exception carries the
        # request it was attempting, so `.request.url` is populated.
        request = httpx.Request("GET", f"{base_url}{URL_AUTH_WHOAMI}")
        raise httpx.ConnectError("Connection refused", request=request)

    monkeypatch.setattr("qiita_control_plane.cli._common.whoami", boom)

    rc = main(["--base-url", "http://localhost:9999", "whoami"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "could not reach" in err
    # The target URL is named so the user can spot a wrong base-url.
    assert "localhost:9999" in err
    assert "QIITA_CONTROL_PLANE_URL" in err


# ---------------------------------------------------------------------------
# --base-url http-to-non-localhost guard
# ---------------------------------------------------------------------------


def test_http_to_non_localhost_refused_without_insecure(capsys):
    """Plain http:// to a non-localhost host would send the PAT in
    cleartext. The CLI refuses (exit 2) unless --insecure is set."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["--base-url", "http://qiita.example.com", "whoami"])
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "cleartext" in err
    assert "qiita.example.com" in err
    assert "--insecure" in err


def test_http_to_non_localhost_allowed_with_insecure(monkeypatch, capsys):
    """--insecure suppresses the refuse, prints a warning to stderr, and
    proceeds with the call."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    def fake_whoami(base_url, token):
        return {"kind": "human"}

    monkeypatch.setattr(_common, "whoami", fake_whoami)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    rc = main(["--base-url", "http://qiita.example.com", "--insecure", "whoami"])
    assert rc == 0
    err = capsys.readouterr().err
    assert "warning" in err
    assert "cleartext" in err


def test_http_to_localhost_allowed_without_insecure(monkeypatch):
    """Plain http:// to localhost / 127.0.0.1 / ::1 / 127.x.x.x is always
    permitted — traffic stays on the host."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    monkeypatch.setattr(_common, "whoami", lambda base_url, token: {})
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    # Default base-url is already http://localhost — verify a few hostnames.
    for url in (
        "http://localhost:8080",
        "http://127.0.0.1:8080",
        "http://127.0.0.2:8080",
        "http://[::1]:8080",
    ):
        rc = main(["--base-url", url, "whoami"])
        assert rc == 0, f"expected localhost URL to be allowed: {url}"


def test_https_to_non_localhost_allowed(monkeypatch):
    """https:// is always fine regardless of hostname — the bearer is
    encrypted in transit."""
    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    monkeypatch.setattr(_common, "whoami", lambda base_url, token: {})
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    rc = main(["--base-url", "https://qiita.example.com", "whoami"])
    assert rc == 0


# ---------------------------------------------------------------------------
# submit-bcl-convert
# ---------------------------------------------------------------------------
# The bundled bcl-convert flow chains three POSTs in one CLI gesture, with
# RunInfo.xml parsing in front of the network calls. Each test stubs httpx
# with a multi-response queue so the three legs (sequencing-run,
# sequenced-pool, work-ticket) can be sequenced independently — _stub_post
# reuses one body across calls and is not enough here.


def _stub_multi_response(monkeypatch, captured: dict, *, responses):
    """Patch httpx.request to return canned ``(status, body)`` responses in
    the order supplied. `captured['requests']` collects every call so a
    test can pin per-leg URL + body + auth."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured.setdefault("requests", [])
    queue = list(responses)

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["requests"].append(
            {
                "method": method,
                "url": url,
                "auth": headers["Authorization"],
                "json": json,
            }
        )
        if not queue:
            raise AssertionError(
                f"submit-bcl-convert made an extra request beyond the stubbed responses: "
                f"{method} {url}"
            )
        status, body = queue.pop(0)
        return _httpx.Response(status, json=body, request=_httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")


def _seed_bcl_folder(tmp_path: Path, name: str, *, with_runinfo: bool = True) -> Path:
    """Create a BCL run folder. When ``with_runinfo``, also write a top-level
    RunInfo.xml whose ``Run@Id`` is the folder name and whose ``Instrument``
    serial number is the name's second underscore segment (the real run-ID
    convention), so the CLI's RunInfo reader derives the run ID + model."""
    folder = tmp_path / name
    folder.mkdir()
    if with_runinfo:
        serial = name.split("_")[1]
        (folder / "RunInfo.xml").write_text(
            '<?xml version="1.0"?>\n'
            f'<RunInfo Version="6"><Run Id="{name}">'
            f"<Instrument>{serial}</Instrument></Run></RunInfo>\n",
            encoding="utf-8",
        )
    return folder


@pytest.fixture
def preflight_stub(monkeypatch, tmp_path):
    """Install a fake `run_preflight` module + return a blob-path builder.

    The handler imports `open_db_file`, `get_illumina_sample_info`, and
    `get_illumina_sample_rows` from `run_preflight` at call time, and reads
    `project.human_filtering` off the opened connection; the fixture patches
    `sys.modules["run_preflight"]` and returns a connection whose `.execute`
    serves canned project rows, so the test controls every per-sample input
    without depending on the upstream library or a real SQLite schema.

    Returns a callable
    `_install(rows=None, raises=None, project_by_idx=None,
    filtering_by_project=None) -> Path` that writes a non-empty marker blob at
    `tmp_path/preflight.db` and yields its path. `rows` is a list of 4-tuples
    matching `get_illumina_sample_info`; `raises` is an exception the stub raises
    when invoked. `project_by_idx` maps illumina_sample_idx -> project_name
    (default: every row -> 'STUB_PROJECT'); `filtering_by_project` maps
    project_name -> human_filtering bool (default: every project -> False, so
    existing tests need no host reference args). The stub builds
    get_illumina_sample_rows 6-tuples from `project_by_idx` and serves the
    `SELECT project_name, human_filtering FROM project` rows from
    `filtering_by_project`.
    """

    def _install(
        rows: list[tuple[int, str, str, list[str]]] | None = None,
        raises: Exception | None = None,
        project_by_idx: dict[int, str] | None = None,
        filtering_by_project: dict[str, bool] | None = None,
    ) -> Path:
        blob = tmp_path / "preflight.db"
        blob.write_bytes(b"\x00stub-preflight-marker")
        captured_rows = list(rows or [])
        # Default project wiring: every row -> one project, not human-filtered.
        idx_to_project = dict(project_by_idx or {})
        for row in captured_rows:
            idx_to_project.setdefault(row[0], "STUB_PROJECT")
        project_filtering = dict(filtering_by_project or {})
        for name in idx_to_project.values():
            project_filtering.setdefault(name, False)

        class _StubCursor:
            def __init__(self, result):
                self._result = result

            def fetchall(self):
                return self._result

        class _StubConn:
            # The handler runs exactly one raw query:
            # SELECT project_name, human_filtering FROM project. SQLite returns
            # the boolean as 0/1, so serve ints to mirror it (the handler bool()s).
            # Assert the shape so a future second raw query can't be silently fed
            # project rows.
            def execute(self, sql):
                assert "FROM project" in sql, f"unexpected preflight query: {sql!r}"
                return _StubCursor([(n, int(f)) for n, f in project_filtering.items()])

            def close(self):
                pass

        stub_module = types.ModuleType("run_preflight")
        stub_module.open_db_file = lambda _blob: _StubConn()
        if raises is not None:
            captured_exc = raises

            def _get(_conn):
                raise captured_exc

            stub_module.get_illumina_sample_info = _get
        else:
            stub_module.get_illumina_sample_info = lambda _conn: list(captured_rows)
        # get_illumina_sample_rows lives in run_preflight.db (NOT top-level), so
        # mirror that layout — the handler reaches it via `from run_preflight
        # import db`. Tuple is (idx, lane, i7, i5, project_name, sample_name); the
        # handler reads only idx ([0]) and project_name ([4]).
        stub_module.db = types.SimpleNamespace(
            get_illumina_sample_rows=lambda _conn: [
                (idx, 1, "", "", idx_to_project[idx], f"s{idx}") for idx in idx_to_project
            ]
        )
        monkeypatch.setitem(sys.modules, "run_preflight", stub_module)
        return blob

    return _install


def test_submit_bcl_convert_happy_path_chains_full_flow(
    monkeypatch, tmp_path, capsys, preflight_stub
):
    """The full bundled flow:
      1. whoami (resolve owner_idx for per-sample composer);
      2. POST /biosample/lookup-by-accession (every biosample resolves);
      3. POST /study/lookup-by-accession (every primary + secondary
         study accession resolves);
      4. POST /sequencing-run (201);
      5. POST /sequencing-run/{R}/sequenced-pool (201);
      6. POST sequenced-sample composer once per preflight row (201);
      7. POST /work-ticket (202).
    Pin each leg's URL + body; check the summary echoes per-sample
    results including resolved secondary_study_idxs."""
    import base64 as _b64
    import json as _json

    from qiita_control_plane.cli.user import main

    folder = _seed_bcl_folder(tmp_path, "230101_A00123_0001_BHXYZ")
    # Row 1: single-study sample (primary PRJ001, no secondaries).
    # Row 2: control on plate PRJ001, also linked to PRJ002 as a
    #        secondary — exercises the primary+secondary fan-out.
    # Row 3: control bridging PRJ002 + PRJ003 — exercises a row whose
    #        primary appears as another row's secondary (and vice-versa)
    #        and a row with multiple secondaries.
    blob = preflight_stub(
        rows=[
            (1, "SAMN001", "PRJ001", []),
            (2, "SAMN002", "PRJ001", ["PRJ002"]),
            (3, "SAMN003", "PRJ002", ["PRJ001", "PRJ003"]),
        ],
    )

    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            # whoami
            (200, {"kind": "human", "principal_idx": 99}),
            # biosample lookup-by-accession
            (
                200,
                {
                    "resolved": {"SAMN001": 41, "SAMN002": 42, "SAMN003": 43},
                    "missing": [],
                },
            ),
            # study lookup-by-accession
            (
                200,
                {
                    "resolved": {"PRJ001": 7, "PRJ002": 8, "PRJ003": 9},
                    "missing": [],
                },
            ),
            # sequencing-run, sequenced-pool
            (201, {"sequencing_run_idx": 12}),
            (201, {"sequenced_pool_idx": 34}),
            # pool roster GET (create-missing: fresh pool -> empty, all created)
            (200, {"samples": []}),
            # sequenced-sample x3
            (201, {"sequenced_sample_idx": 71, "prep_sample_idx": 81}),
            (201, {"sequenced_sample_idx": 72, "prep_sample_idx": 82}),
            (201, {"sequenced_sample_idx": 73, "prep_sample_idx": 83}),
            # work-ticket
            (202, {"work_ticket_idx": 56, "state": "pending"}),
        ],
    )

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "submit-bcl-convert",
            "--bcl-input-dir",
            str(folder),
            "--preflight-blob",
            str(blob),
            "--prep-protocol-idx",
            "7",
        ]
    )
    assert rc == 0
    requests = captured["requests"]
    # whoami + biosample-lookup + study-lookup + run + pool + roster-GET + 3 samples
    # + ticket = 10 calls.
    assert len(requests) == 10

    # Leg 1: whoami.
    assert requests[0]["method"] == "GET"
    assert requests[0]["url"].endswith("/auth/whoami")

    # Leg 2: biosample lookup-by-accession with the deduped preflight
    # accessions (here the same as row order since each is unique).
    assert requests[1]["method"] == "POST"
    assert requests[1]["url"].endswith("/biosample/lookup-by-accession")
    assert requests[1]["json"] == {
        "accessions": ["SAMN001", "SAMN002", "SAMN003"],
        "accession_field": "biosample_accession",
    }

    # Leg 3: study lookup-by-accession with the order-preserving dedup
    # of every row's primary + secondary project accessions. First
    # appearance order: row1 primary PRJ001, row2 secondary PRJ002,
    # row3 secondary PRJ003 (PRJ001 and PRJ002 already seen).
    assert requests[2]["method"] == "POST"
    assert requests[2]["url"].endswith("/study/lookup-by-accession")
    assert requests[2]["json"] == {
        "accessions": ["PRJ001", "PRJ002", "PRJ003"],
        "accession_field": "bioproject_accession",
    }

    # Leg 4: POST /sequencing-run.
    assert requests[3]["method"] == "POST"
    assert requests[3]["url"] == f"https://q.example.test{URL_SEQUENCING_RUN_PREFIX}"
    assert requests[3]["json"] == {
        "instrument_run_id": "230101_A00123_0001_BHXYZ",
        "platform": "illumina",
        "instrument_model": "Illumina NovaSeq 6000",
    }

    # Leg 5: POST /sequencing-run/{R}/sequenced-pool. Blob round-trips
    # byte-equal through base64.
    assert requests[4]["method"] == "POST"
    assert requests[4]["url"] == (
        f"https://q.example.test{URL_SEQUENCING_RUN_SEQUENCED_POOL.format(sequencing_run_idx=12)}"
    )
    pool_body = requests[4]["json"]
    assert pool_body["run_preflight_filename"] == "preflight.db"
    assert _b64.b64decode(pool_body["run_preflight_blob"]) == blob.read_bytes()

    # Leg 6: GET the pool roster (create-missing) — empty here, so all 3 samples
    # are created next.
    assert requests[5]["method"] == "GET"
    assert requests[5]["url"].endswith("/sequencing-run/12/sequenced-pool/34/sequenced-sample/list")

    # Legs 7..9: one sequenced-sample composer POST per preflight row.
    # secondary_study_idxs preserves the row's secondary order (after
    # the model's dedup; here no row has duplicates).
    expected_per_sample = [
        (1, 41, 7, []),
        (2, 42, 7, [8]),
        (3, 43, 8, [7, 9]),
    ]
    for offset, (illumina, biosample_idx, primary_study, secondary_studies) in enumerate(
        expected_per_sample
    ):
        req = requests[6 + offset]
        assert req["method"] == "POST"
        assert req["url"].endswith("/sequencing-run/12/sequenced-pool/34/sequenced-sample")
        assert req["json"] == {
            "biosample_idx": biosample_idx,
            "owner_idx": 99,
            "prep_protocol_idx": 7,
            "sequenced_pool_item_id": str(illumina),
            "primary_study_idx": primary_study,
            "secondary_study_idxs": secondary_studies,
        }

    # Leg 10: POST /work-ticket.
    assert requests[9]["method"] == "POST"
    assert requests[9]["url"] == f"https://q.example.test{URL_WORK_TICKET_PREFIX}"
    ticket_body = requests[9]["json"]
    assert ticket_body["action_id"] == "bcl-convert"
    assert ticket_body["action_version"] == "1.0.0"
    assert ticket_body["scope_target"] == {
        "kind": "sequenced_pool",
        "sequenced_pool_idx": 34,
        "sequencing_run_idx": 12,
    }
    # action_context now carries a sample_map (one entry per pool sample,
    # pool_item_id == str(illumina_sample_idx)) built from the composer
    # responses' prep_sample_idx.
    assert ticket_body["action_context"] == {
        "bcl_input_dir": str(folder),
        "sample_map": [
            {"prep_sample_idx": 81, "pool_item_id": "1"},
            {"prep_sample_idx": 82, "pool_item_id": "2"},
            {"prep_sample_idx": 83, "pool_item_id": "3"},
        ],
    }

    # CLI summary echoes per-sample idxs alongside the run/pool/ticket
    # idxs, including resolved secondary_study_idxs.
    summary = _json.loads(capsys.readouterr().out)
    assert summary["sequencing_run"]["status"] == "created"
    assert summary["sequenced_pool"]["status"] == "created"
    assert summary["work_ticket"]["work_ticket_idx"] == 56
    assert [s["sequenced_sample_idx"] for s in summary["sequenced_samples"]] == [71, 72, 73]
    assert summary["sequenced_samples"][0]["biosample_accession"] == "SAMN001"
    assert summary["sequenced_samples"][0]["biosample_idx"] == 41
    assert summary["sequenced_samples"][0]["illumina_sample_idx"] == 1
    assert summary["sequenced_samples"][0]["primary_study_idx"] == 7
    assert summary["sequenced_samples"][0]["secondary_study_idxs"] == []
    assert summary["sequenced_samples"][2]["secondary_study_idxs"] == [7, 9]


@pytest.mark.parametrize(
    (
        "biosample_resolved",
        "biosample_missing",
        "study_resolved",
        "study_missing",
        "expected_substrings",
    ),
    [
        # Biosample misses only — two of three biosample accessions
        # missing; every study accession resolves. Combined-error block
        # carries only the biosample sub-section.
        pytest.param(
            {"SAMN001": 41},
            ["SAMN999", "SAMN1000"],
            {"PRJ001": 7, "PRJ888": 8, "PRJ777": 9},
            [],
            (
                "2 distinct preflight biosample accessions not found in qiita,"
                " affecting 2 illumina_sample rows",
                "SAMN999 (illumina_sample_idx=5)",
                "SAMN1000 (illumina_sample_idx=8)",
            ),
            id="biosample_missing_only",
        ),
        # Study misses only — every biosample resolves; one secondary
        # study and one primary study missing. The bullet for the row
        # with both misses names every offending accession on that row.
        pytest.param(
            {"SAMN001": 41, "SAMN999": 42, "SAMN1000": 43},
            [],
            {"PRJ001": 7},
            ["PRJ888", "PRJ777"],
            (
                "2 distinct preflight study accessions not found in qiita,"
                " affecting 2 illumina_sample rows",
                "PRJ888 (illumina_sample_idx=5)",
                "PRJ777 (illumina_sample_idx=8)",
            ),
            id="study_missing_only",
        ),
        # Both classes missing — combined block carries both labelled
        # sub-sections so the operator fixes everything in one pass.
        pytest.param(
            {"SAMN001": 41},
            ["SAMN999", "SAMN1000"],
            {"PRJ001": 7},
            ["PRJ888", "PRJ777"],
            (
                "2 distinct preflight biosample accessions not found in qiita,"
                " affecting 2 illumina_sample rows",
                "SAMN999 (illumina_sample_idx=5)",
                "2 distinct preflight study accessions not found in qiita,"
                " affecting 2 illumina_sample rows",
                "PRJ777 (illumina_sample_idx=8)",
            ),
            id="both_classes_missing",
        ),
    ],
)
def test_submit_bcl_convert_fails_fast_when_accessions_missing(
    monkeypatch,
    tmp_path,
    capsys,
    preflight_stub,
    biosample_resolved,
    biosample_missing,
    study_resolved,
    study_missing,
    expected_substrings,
):
    """When either lookup-by-accession response carries a non-empty
    `missing` list, the CLI prints a combined stderr block naming the
    offending preflight rows for each class and exits 1 with no
    sequencing-run / sequenced-pool / sequenced-sample / ticket POSTs.

    Parametrized over biosample-missing-only, study-missing-only, and
    both-missing — they share the same bail mechanics, so one body
    drives every case.
    """
    from qiita_control_plane.cli.user import main

    folder = _seed_bcl_folder(tmp_path, "230101_A00123_0001_BHXYZ")
    blob = preflight_stub(
        rows=[
            (1, "SAMN001", "PRJ001", []),
            (5, "SAMN999", "PRJ001", ["PRJ888"]),
            (8, "SAMN1000", "PRJ777", []),
        ],
    )

    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (200, {"kind": "human", "principal_idx": 99}),
            (200, {"resolved": biosample_resolved, "missing": biosample_missing}),
            (200, {"resolved": study_resolved, "missing": study_missing}),
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "submit-bcl-convert",
                "--bcl-input-dir",
                str(folder),
                "--preflight-blob",
                str(blob),
                "--prep-protocol-idx",
                "7",
            ]
        )
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    for fragment in expected_substrings:
        assert fragment in err, f"expected {fragment!r} in stderr; got:\n{err}"
    # No write-side legs ran — whoami + both lookups are the only calls.
    assert len(captured["requests"]) == 3


def test_submit_bcl_convert_rejects_preflight_without_illumina_samples(
    tmp_path, capsys, preflight_stub
):
    """An empty preflight (library returns no rows) is a misuse — the
    bcl-convert submission needs at least one row to demultiplex.
    parser.error exits 2 before any network round-trip."""
    from qiita_control_plane.cli.user import main

    folder = _seed_bcl_folder(tmp_path, "230101_A00123_0001_BHXYZ")
    blob = preflight_stub(rows=[])
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "submit-bcl-convert",
                "--bcl-input-dir",
                str(folder),
                "--preflight-blob",
                str(blob),
                "--prep-protocol-idx",
                "7",
            ]
        )
    assert exc_info.value.code == 2
    assert "no illumina_sample rows" in capsys.readouterr().err


def test_submit_bcl_convert_rejects_non_sqlite_preflight(tmp_path, capsys, preflight_stub):
    """A library failure parsing the preflight blob (stubbed here via
    `get_illumina_sample_info` raising) surfaces as a clean parser.error
    rather than a stack trace."""
    from qiita_control_plane.cli.user import main

    folder = _seed_bcl_folder(tmp_path, "230101_A00123_0001_BHXYZ")
    blob = preflight_stub(
        raises=sqlite3.DatabaseError("file is not a database"),
    )
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "submit-bcl-convert",
                "--bcl-input-dir",
                str(folder),
                "--preflight-blob",
                str(blob),
                "--prep-protocol-idx",
                "7",
            ]
        )
    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "preflight query failed" in err


def test_submit_bcl_convert_dedups_repeated_accessions_in_lookup(
    monkeypatch, tmp_path, preflight_stub
):
    """Two preflight rows pointing at the same biosample_accession and
    primary_project_accession (replicates) → the lookup bodies carry
    each accession once; both rows still get their own sequenced-sample
    POST."""
    from qiita_control_plane.cli.user import main

    folder = _seed_bcl_folder(tmp_path, "230101_A00123_0001_BHXYZ")
    blob = preflight_stub(
        rows=[
            (1, "SAMN001", "PRJ001", []),
            # Same accession + study, different illumina_sample_idx —
            # a replicate.
            (2, "SAMN001", "PRJ001", []),
        ],
    )

    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (200, {"kind": "human", "principal_idx": 99}),
            (200, {"resolved": {"SAMN001": 41}, "missing": []}),
            (200, {"resolved": {"PRJ001": 7}, "missing": []}),
            (201, {"sequencing_run_idx": 12}),
            (201, {"sequenced_pool_idx": 34}),
            (200, {"samples": []}),  # pool roster GET (create-missing)
            (201, {"sequenced_sample_idx": 71, "prep_sample_idx": 81}),
            (201, {"sequenced_sample_idx": 72, "prep_sample_idx": 82}),
            (202, {"work_ticket_idx": 56, "state": "pending"}),
        ],
    )

    rc = main(
        [
            "submit-bcl-convert",
            "--bcl-input-dir",
            str(folder),
            "--preflight-blob",
            str(blob),
            "--prep-protocol-idx",
            "7",
        ]
    )
    assert rc == 0
    # Each lookup body carries its accession exactly once.
    assert captured["requests"][1]["json"] == {
        "accessions": ["SAMN001"],
        "accession_field": "biosample_accession",
    }
    assert captured["requests"][2]["json"] == {
        "accessions": ["PRJ001"],
        "accession_field": "bioproject_accession",
    }
    # Both rows still produce a sequenced-sample composer POST with the
    # row's distinct illumina_sample_idx (after the run/pool/roster-GET legs).
    sample_bodies = [r["json"] for r in captured["requests"][6:8]]
    assert sample_bodies[0]["sequenced_pool_item_id"] == "1"
    assert sample_bodies[1]["sequenced_pool_item_id"] == "2"
    # Both rows resolve to the same biosample_idx (replicate convention).
    assert sample_bodies[0]["biosample_idx"] == 41
    assert sample_bodies[1]["biosample_idx"] == 41
    # Per-row owner_idx comes from the single up-front whoami round trip.
    assert sample_bodies[0]["owner_idx"] == 99
    assert sample_bodies[1]["owner_idx"] == 99


def test_submit_bcl_convert_reports_reused_when_run_post_returns_200(
    monkeypatch, tmp_path, capsys, preflight_stub
):
    """A retry that hits an existing sequencing_run row returns 200 from
    the sequencing-run POST; the CLI summary surfaces `status: "reused"`
    so the operator can confirm the find-or-create branch."""
    import json as _json

    from qiita_control_plane.cli.user import main

    folder = _seed_bcl_folder(tmp_path, "230101_A00123_0001_BHXYZ")
    blob = preflight_stub(rows=[(1, "SAMN001", "PRJ001", [])])
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (200, {"kind": "human", "principal_idx": 99}),
            (200, {"resolved": {"SAMN001": 41}, "missing": []}),
            (200, {"resolved": {"PRJ001": 7}, "missing": []}),
            (200, {"sequencing_run_idx": 12}),
            (200, {"sequenced_pool_idx": 34}),
            (200, {"samples": []}),  # pool roster GET (create-missing)
            (201, {"sequenced_sample_idx": 71, "prep_sample_idx": 81}),
            (202, {"work_ticket_idx": 56, "state": "pending"}),
        ],
    )

    rc = main(
        [
            "submit-bcl-convert",
            "--bcl-input-dir",
            str(folder),
            "--preflight-blob",
            str(blob),
            "--prep-protocol-idx",
            "7",
        ]
    )
    assert rc == 0
    summary = _json.loads(capsys.readouterr().out)
    assert summary["sequencing_run"]["status"] == "reused"
    assert summary["sequenced_pool"]["status"] == "reused"


def test_submit_bcl_convert_reuses_existing_roster_samples(
    monkeypatch, tmp_path, capsys, preflight_stub
):
    """Convergent re-run: when the pool roster already holds a sample, bcl-convert
    creates NO sequenced-sample (create-missing) and reuses its prep_sample_idx in
    the work-ticket sample_map. Pins the CHANGELOG 'convergent re-run' claim."""
    import json as _json

    from qiita_control_plane.cli.user import main

    folder = _seed_bcl_folder(tmp_path, "230101_A00123_0001_BHXYZ")
    blob = preflight_stub(rows=[(1, "SAMN001", "PRJ001", [])])
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (200, {"kind": "human", "principal_idx": 99}),
            (200, {"resolved": {"SAMN001": 41}, "missing": []}),
            (200, {"resolved": {"PRJ001": 7}, "missing": []}),
            (200, {"sequencing_run_idx": 12}),
            (200, {"sequenced_pool_idx": 34}),
            # roster already has item "1" (biosample_idx matches the resolved 41).
            (
                200,
                {
                    "samples": [
                        {
                            "sequenced_pool_item_id": "1",
                            "prep_sample_idx": 81,
                            "sequenced_sample_idx": 71,
                            "biosample_idx": 41,
                        }
                    ]
                },
            ),
            (202, {"work_ticket_idx": 56, "state": "pending"}),
        ],
    )

    rc = main(
        [
            "submit-bcl-convert",
            "--bcl-input-dir",
            str(folder),
            "--preflight-blob",
            str(blob),
            "--prep-protocol-idx",
            "7",
        ]
    )
    assert rc == 0
    # No sequenced-sample was CREATED (POST) — the existing one is reused.
    assert not [
        r
        for r in captured["requests"]
        if r["method"] == "POST" and r["url"].endswith("/sequenced-sample")
    ]
    # The work-ticket sample_map carries the reused prep_sample_idx.
    ticket = next(r for r in captured["requests"] if r["url"].endswith("/work-ticket"))
    assert ticket["json"]["action_context"]["sample_map"] == [
        {"prep_sample_idx": 81, "pool_item_id": "1"}
    ]
    summary = _json.loads(capsys.readouterr().out)
    assert summary["sequenced_samples"][0]["prep_sample_idx"] == 81


def test_submit_bcl_convert_rejects_relative_bcl_input_dir(capsys, preflight_stub):
    """A relative --bcl-input-dir cannot be passed through to the
    orchestrator's container bind logic safely; fail at argparse time
    rather than letting the server return 422."""
    from qiita_control_plane.cli.user import main

    blob = preflight_stub()
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "submit-bcl-convert",
                "--bcl-input-dir",
                "relative/path",
                "--preflight-blob",
                str(blob),
                "--prep-protocol-idx",
                "7",
            ]
        )
    assert exc_info.value.code == 2
    assert "must be absolute" in capsys.readouterr().err


def test_submit_bcl_convert_rejects_missing_bcl_input_dir(tmp_path, capsys, preflight_stub):
    """A path that does not exist on disk cannot be the run folder."""
    from qiita_control_plane.cli.user import main

    blob = preflight_stub()
    bogus = tmp_path / "does-not-exist"
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "submit-bcl-convert",
                "--bcl-input-dir",
                str(bogus),
                "--preflight-blob",
                str(blob),
                "--prep-protocol-idx",
                "7",
            ]
        )
    assert exc_info.value.code == 2
    assert "is not a directory" in capsys.readouterr().err


def test_submit_bcl_convert_rejects_empty_preflight_blob(tmp_path, capsys):
    """A zero-byte preflight file cannot be a kl-run-preflight SQLite."""
    from qiita_control_plane.cli.user import main

    folder = _seed_bcl_folder(tmp_path, "230101_A00123_0001_BHXYZ")
    blob = tmp_path / "empty.db"
    blob.write_bytes(b"")
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "submit-bcl-convert",
                "--bcl-input-dir",
                str(folder),
                "--preflight-blob",
                str(blob),
                "--prep-protocol-idx",
                "7",
            ]
        )
    assert exc_info.value.code == 2
    assert "is empty" in capsys.readouterr().err


def test_submit_bcl_convert_rejects_missing_runinfo(tmp_path, capsys, preflight_stub):
    """Tests the case where the run folder has no top-level RunInfo.xml;
    the reader fails before any server round-trip."""
    from qiita_control_plane.cli.user import main

    folder = _seed_bcl_folder(tmp_path, "230101_A00123_0001_BHXYZ", with_runinfo=False)
    blob = preflight_stub()
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "submit-bcl-convert",
                "--bcl-input-dir",
                str(folder),
                "--preflight-blob",
                str(blob),
                "--prep-protocol-idx",
                "7",
            ]
        )
    assert exc_info.value.code == 2
    assert "RunInfo.xml not found" in capsys.readouterr().err


def test_submit_bcl_convert_rejects_unknown_instrument_prefix(tmp_path, capsys, preflight_stub):
    """A serial number that does not start with any known Illumina prefix
    surfaces the parser's "unknown instrument serial prefix" error.
    Same path catches PacBio folders, because the parser filters
    PacBio out at table-load time."""
    from qiita_control_plane.cli.user import main

    folder = _seed_bcl_folder(tmp_path, "230101_ZZZZZ999_0001_BHXYZ")
    blob = preflight_stub()
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "submit-bcl-convert",
                "--bcl-input-dir",
                str(folder),
                "--preflight-blob",
                str(blob),
                "--prep-protocol-idx",
                "7",
            ]
        )
    assert exc_info.value.code == 2
    assert "unknown instrument serial prefix" in capsys.readouterr().err


def test_submit_bcl_convert_pacbio_folder_rejected_as_unknown_prefix(
    tmp_path, capsys, preflight_stub
):
    """A PacBio Revio serial number starts with lowercase r. The parser filters
    PacBio out at load time so this surfaces as the same
    "unknown prefix" error a malformed Illumina serial number would — by design,
    because bcl-convert is Illumina-only."""
    from qiita_control_plane.cli.user import main

    folder = _seed_bcl_folder(tmp_path, "230101_r00012_0001_BHXYZ")
    blob = preflight_stub()
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "submit-bcl-convert",
                "--bcl-input-dir",
                str(folder),
                "--preflight-blob",
                str(blob),
                "--prep-protocol-idx",
                "7",
            ]
        )
    assert exc_info.value.code == 2
    assert "unknown instrument serial prefix" in capsys.readouterr().err


def test__read_preflight_rows_round_trips_library_tuples(preflight_stub):
    """Tests the case where the reader wraps `get_illumina_sample_info`
    4-tuples into `_PreflightRow` instances — illumina_sample_idx is
    cast to int and secondary_project_accessions is copied into a fresh
    list."""
    import argparse

    from qiita_control_plane.cli.user import _PreflightRow, _read_preflight_rows

    library_secondaries = ["PRJ002", "PRJ003"]
    blob = preflight_stub(
        rows=[(7, "SAMN001", "PRJ001", library_secondaries)],
        project_by_idx={7: "PROJ_A"},
        filtering_by_project={"PROJ_A": True},
    )
    parser = argparse.ArgumentParser()
    rows = _read_preflight_rows(blob, parser)
    assert rows == [
        _PreflightRow(
            illumina_sample_idx=7,
            biosample_accession="SAMN001",
            primary_project_accession="PRJ001",
            secondary_project_accessions=["PRJ002", "PRJ003"],
            human_filtering=True,
        )
    ]
    # NamedTuple field carries a fresh list, not an alias to the
    # library's return — mutating downstream does not contaminate the
    # caller's data.
    assert rows[0].secondary_project_accessions is not library_secondaries


def test_submit_bcl_convert_records_no_host_refs(monkeypatch, tmp_path, capsys, preflight_stub):
    """bcl-convert only demultiplexes the run: no sequenced-sample composer POST
    carries a host reference (host filtering is chosen later at
    submit-host-filter-pool). The preflight's per-project human_filtering flag is
    still echoed per sample for operator reference."""
    import json as _json

    from qiita_control_plane.cli.user import main

    folder = _seed_bcl_folder(tmp_path, "230101_A00123_0001_BHXYZ")
    blob = preflight_stub(
        rows=[(1, "SAMN001", "PRJ001", []), (2, "SAMN002", "PRJ001", [])],
        project_by_idx={1: "HUMAN", 2: "NOFILT"},
        filtering_by_project={"HUMAN": True, "NOFILT": False},
    )
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (200, {"kind": "human", "principal_idx": 99}),  # whoami
            (200, {"resolved": {"SAMN001": 41, "SAMN002": 42}, "missing": []}),  # biosample
            (200, {"resolved": {"PRJ001": 70}, "missing": []}),  # study
            (201, {"sequencing_run_idx": 12}),
            (201, {"sequenced_pool_idx": 34}),
            (200, {"samples": []}),  # pool roster GET (create-missing)
            (201, {"sequenced_sample_idx": 71, "prep_sample_idx": 81}),
            (201, {"sequenced_sample_idx": 72, "prep_sample_idx": 82}),
            (202, {"work_ticket_idx": 56, "state": "pending"}),
        ],
    )

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "submit-bcl-convert",
            "--bcl-input-dir",
            str(folder),
            "--preflight-blob",
            str(blob),
            "--prep-protocol-idx",
            "7",
        ]
    )
    assert rc == 0
    # No host-reference readiness GET is made — bcl-convert does not host-filter.
    assert not [r for r in captured["requests"] if "/reference/" in r["url"]]
    sample_posts = [
        r for r in captured["requests"] if r["method"] == "POST" and "sequenced-sample" in r["url"]
    ]
    for post in sample_posts:
        assert "host_rype_reference_idx" not in post["json"]
        assert "host_minimap2_reference_idx" not in post["json"]
    # Summary still echoes the per-project human_filtering flag for reference.
    summary = _json.loads(capsys.readouterr().out)
    by_illumina = {s["illumina_sample_idx"]: s for s in summary["sequenced_samples"]}
    assert by_illumina[1]["human_filtering"] is True
    assert by_illumina[2]["human_filtering"] is False
    assert "host_rype_reference_idx" not in by_illumina[1]


def test_submit_bcl_convert_rejects_host_ref_args(monkeypatch, tmp_path, capsys, preflight_stub):
    """submit-bcl-convert no longer accepts host-reference args — they belong to
    submit-host-filter-pool. argparse rejects the unknown flag (exit 2)."""
    from qiita_control_plane.cli.user import main

    folder = _seed_bcl_folder(tmp_path, "230101_A00123_0001_BHXYZ")
    blob = preflight_stub(
        rows=[(1, "SAMN001", "PRJ001", [])],
        project_by_idx={1: "HUMAN"},
        filtering_by_project={"HUMAN": True},
    )
    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--base-url",
                "https://q.example.test",
                "submit-bcl-convert",
                "--bcl-input-dir",
                str(folder),
                "--preflight-blob",
                str(blob),
                "--prep-protocol-idx",
                "7",
                "--host-rype-reference-idx",
                "7",
            ]
        )
    assert exc_info.value.code == 2
    assert "--host-rype-reference-idx" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# submit-host-filter-pool
# ---------------------------------------------------------------------------
# Fans out one read-mask/1.0.0 ticket per pool sample over the reads
# bcl-convert already stored — this command does NOT parse FASTQ or re-store
# reads. The flow makes GET the pool sample-list, GET /reference/{idx} +
# GET /reference/{idx}/index per given host reference, GET /sequencing-run,
# then one POST /work-ticket per resolved sample — stubbed with the same
# multi-response queue as submit-bcl-convert.


def _ref_active_body(reference_idx=7):
    return {
        "reference_idx": reference_idx,
        "name": "human-t2t",
        "version": "v2.0",
        "kind": "sequence_reference",
        "status": "active",
        "is_host": True,
        "created_by_idx": 1,
        "created_at": "2026-06-16T00:00:00Z",
    }


def _both_indexes_body(reference_idx=7):
    return [
        {
            "reference_index_idx": 1,
            "reference_idx": reference_idx,
            "index_type": "rype",
            "fs_path": "/derived/7/rype/index.ryxdi",
            "params": {},
            "created_at": "2026-06-16T00:00:00Z",
        },
        {
            "reference_index_idx": 2,
            "reference_idx": reference_idx,
            "index_type": "minimap2",
            "fs_path": "/derived/7/minimap2/index.mmi",
            "params": {},
            "created_at": "2026-06-16T00:00:00Z",
        },
    ]


def _pool_samples_body(samples):
    """samples: list of (sequenced_sample_idx, prep_sample_idx, pool_item_id[,
    human_filtering]).

    Host references are no longer a sample property — they are chosen at
    submit-host-filter-pool time, so the sample-list rows carry none. The roster
    DOES carry each sample's intake `human_filtering` intent, which the route
    derives server-side from the pool's stored preflight; supply it as the 4th
    tuple entry. A 3-tuple leaves `human_filtering` None (the route's "no stored
    intent for this item" case), which the host-filter-pool guard rejects."""

    def _row(entry):
        ss, ps, item = entry[0], entry[1], entry[2]
        return {
            "sequenced_sample_idx": ss,
            "prep_sample_idx": ps,
            "sequenced_pool_item_id": item,
            "human_filtering": entry[3] if len(entry) > 3 else None,
            # 5th tuple entry → has_read_mask_ticket (default False = no ticket).
            "has_read_mask_ticket": entry[4] if len(entry) > 4 else False,
        }

    return {
        "samples": [_row(e) for e in samples],
        "count": len(samples),
        "truncated": False,
        "caller_system_role": "wet_lab_admin",
    }


def _seq_run_body(*, sequencing_run_idx=3, instrument_model="NextSeq 550"):
    """A GET /sequencing-run/{idx} response body. The CLI only reads
    instrument_model; the rest mirrors the SequencingRunResponse shape."""
    return {
        "sequencing_run_idx": sequencing_run_idx,
        "instrument_run_id": "run-001",
        "platform": "illumina",
        "instrument_model": instrument_model,
        "instrument_serial": None,
        "run_performed_at": None,
        "extra_metadata": None,
        "created_by_idx": 1,
        "created_at": "2026-06-16T00:00:00Z",
        "retired": False,
        "retired_by_idx": None,
        "retired_at": None,
        "retire_reason": None,
    }


def _run_submit_host_filter_pool(
    *, run=3, pool=5, rype=None, minimap2=None, force=False, only_missing=False
):
    from qiita_control_plane.cli.user import main

    argv = [
        "submit-host-filter-pool",
        "--sequencing-run-idx",
        str(run),
        "--sequenced-pool-idx",
        str(pool),
    ]
    if rype is not None:
        argv += ["--host-rype-reference-idx", str(rype)]
    if minimap2 is not None:
        argv += ["--host-minimap2-reference-idx", str(minimap2)]
    if force:
        argv += ["--force"]
    if only_missing:
        argv += ["--only-missing"]
    return main(argv)


def test_submit_host_filter_pool_fans_out_one_ticket_per_sample(monkeypatch, capsys):
    """Two samples, host-filtered against the submission's rype reference 7 →
    two read-mask/1.0.0 POSTs, each with host_filter_enabled against that
    reference, the run's instrument_model forwarded, and scoped to the sample's
    prep_sample_idx. The rype reference is pre-flighted exactly once (at
    submission, not per-sample). Reads were stored by ingest, so no fastq paths
    ride in the context."""
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (200, _pool_samples_body([(100, 1000, "10", True), (101, 1001, "11", True)])),
            (200, _ref_active_body()),  # rype reference 7 (pre-flighted once)
            (200, _both_indexes_body()),  # its index list (carries rype)
            (200, _seq_run_body(instrument_model="NextSeq 550")),  # run metadata
            (202, {"work_ticket_idx": 900}),
            (202, {"work_ticket_idx": 901}),
        ],
    )

    rc = _run_submit_host_filter_pool(rype=7)
    assert rc == 0

    # The shared rype reference is GET-pre-flighted once, not once per sample.
    ref_gets = [
        r for r in captured["requests"] if r["method"] == "GET" and "/reference/" in r["url"]
    ]
    assert len([r for r in ref_gets if r["url"].endswith("/reference/7")]) == 1

    posts = [r for r in captured["requests"] if r["method"] == "POST"]
    assert len(posts) == 2
    by_prep = {p["json"]["scope_target"]["prep_sample_idx"]: p["json"] for p in posts}
    assert set(by_prep) == {1000, 1001}
    for prep_idx in (1000, 1001):
        body = by_prep[prep_idx]
        assert body["action_id"] == "read-mask"
        assert body["action_version"] == "1.0.0"
        assert body["scope_target"] == {"kind": "prep_sample", "prep_sample_idx": prep_idx}
        ctx = body["action_context"]
        assert ctx["host_filter_enabled"] is True
        assert ctx["host_rype_reference_idx"] == 7
        # minimap2 not recorded → its key is omitted (rype-only host filter).
        assert "host_minimap2_reference_idx" not in ctx
        assert ctx["instrument_model"] == "NextSeq 550"
        # Reads were stored by ingest; no fastq paths ride in the context.
        assert "fastq_path" not in ctx
        assert "reverse_fastq_path" not in ctx


def test_submit_host_filter_pool_one_failure_does_not_strand_the_rest(monkeypatch, capsys):
    """A 5xx on one sample's POST is recorded and the fan-out CONTINUES — every
    other sample is still attempted (the fan-out-fragility fix). The command
    exits non-zero and the printed summary lists the submitted and the failed
    samples."""
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (
                200,
                _pool_samples_body(
                    [
                        (100, 1000, "10", True),
                        (101, 1001, "11", True),
                        (102, 1002, "12", True),
                    ]
                ),
            ),
            (200, _ref_active_body()),  # rype reference 7 (pre-flighted once)
            (200, _both_indexes_body()),  # its index list
            (200, _seq_run_body()),  # run metadata
            (202, {"work_ticket_idx": 900}),  # sample 1000 → ok
            (502, {"detail": "bad gateway"}),  # sample 1001 → transient 5xx
            (202, {"work_ticket_idx": 902}),  # sample 1002 → ok (NOT stranded)
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        _run_submit_host_filter_pool(rype=7)
    assert exc_info.value.code == 1

    # All three samples were attempted — the 1001 failure did not abort 1002.
    posts = [r for r in captured["requests"] if r["method"] == "POST"]
    assert [p["json"]["scope_target"]["prep_sample_idx"] for p in posts] == [1000, 1001, 1002]

    summary = json.loads(capsys.readouterr().out)
    assert summary["samples_submitted"] == 2
    assert summary["samples_failed"] == 1
    assert summary["samples_skipped_existing"] == 0
    assert [f["prep_sample_idx"] for f in summary["failed"]] == [1001]
    assert summary["failed"][0]["status_code"] == 502


def test_submit_host_filter_pool_only_missing_skips_ticketed_samples(monkeypatch, capsys):
    """--only-missing submits only samples with no existing read-mask ticket
    (roster has_read_mask_ticket flag), so a re-run fills a partially-submitted
    pool without duplicating the samples already handled."""
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (
                200,
                _pool_samples_body(
                    [
                        (100, 1000, "10", True, True),  # already ticketed → skip
                        (101, 1001, "11", True, False),  # missing → submit
                        (102, 1002, "12", True, True),  # already ticketed → skip
                    ]
                ),
            ),
            (200, _ref_active_body()),
            (200, _both_indexes_body()),
            (200, _seq_run_body()),
            (202, {"work_ticket_idx": 900}),  # only the one missing sample
        ],
    )

    rc = _run_submit_host_filter_pool(rype=7, only_missing=True)
    assert rc == 0

    posts = [r for r in captured["requests"] if r["method"] == "POST"]
    assert [p["json"]["scope_target"]["prep_sample_idx"] for p in posts] == [1001]

    summary = json.loads(capsys.readouterr().out)
    assert summary["samples_submitted"] == 1
    assert summary["samples_skipped_existing"] == 2
    assert summary["samples_failed"] == 0


def test_submit_host_filter_pool_only_missing_all_present_no_posts(monkeypatch, capsys):
    """--only-missing on a fully-ticketed pool submits nothing and exits 0."""
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (
                200,
                _pool_samples_body([(100, 1000, "10", True, True), (101, 1001, "11", True, True)]),
            ),
        ],
    )

    rc = _run_submit_host_filter_pool(rype=7, only_missing=True)
    assert rc == 0
    assert [r for r in captured["requests"] if r["method"] == "POST"] == []
    summary = json.loads(capsys.readouterr().out)
    assert summary["samples_submitted"] == 0
    assert summary["samples_skipped_existing"] == 2


def test_submit_host_filter_pool_two_reference_forwards_both(monkeypatch):
    """A submission giving both a rype (7) and a minimap2 (8) reference →
    each is pre-flighted (reference + its index) and both flow into the
    per-sample action_context."""
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (200, _pool_samples_body([(100, 1000, "10", True)])),
            (200, _ref_active_body(reference_idx=7)),  # rype reference
            (200, [_both_indexes_body()[0]]),  # rype-only index list
            (200, _ref_active_body(reference_idx=8)),  # minimap2 reference
            (200, [_both_indexes_body(reference_idx=8)[1]]),  # minimap2-only index list
            (200, _seq_run_body(instrument_model="NovaSeq 6000")),
            (202, {"work_ticket_idx": 900}),
        ],
    )

    rc = _run_submit_host_filter_pool(rype=7, minimap2=8)
    assert rc == 0
    post = next(r for r in captured["requests"] if r["method"] == "POST")
    ctx = post["json"]["action_context"]
    assert ctx["host_filter_enabled"] is True
    assert ctx["host_rype_reference_idx"] == 7
    assert ctx["host_minimap2_reference_idx"] == 8
    assert ctx["instrument_model"] == "NovaSeq 6000"


def test_submit_host_filter_pool_no_refs_is_passthrough(monkeypatch):
    """Omitting both host-ref args → every sample gets a QC-only ticket with
    host_filter_enabled=False and no reference keys. With no host ref given, NO
    reference is pre-flighted."""
    # No host ref applied → every sample's intake intent must be not-human-filtered.
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (200, _pool_samples_body([(100, 1000, "10", False)])),
            (200, _seq_run_body(instrument_model="NextSeq 550")),
            (202, {"work_ticket_idx": 900}),
        ],
    )

    rc = _run_submit_host_filter_pool()
    assert rc == 0
    # No /reference/ pre-flight GET at all when nothing is host-filtered.
    assert not [r for r in captured["requests"] if "/reference/" in r["url"]]
    post = next(r for r in captured["requests"] if r["method"] == "POST")
    ctx = post["json"]["action_context"]
    assert ctx["host_filter_enabled"] is False
    assert "host_rype_reference_idx" not in ctx
    assert "host_minimap2_reference_idx" not in ctx


def test_submit_host_filter_pool_applies_ref_to_every_sample(monkeypatch):
    """The submission's host reference applies uniformly across the whole pool:
    every sample's ticket is host_filter_enabled against the given rype 7, and
    the reference is pre-flighted exactly once at submission."""
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (200, _pool_samples_body([(100, 1000, "10", True), (101, 1001, "11", True)])),
            (200, _ref_active_body()),  # rype reference 7
            (200, _both_indexes_body()),
            (200, _seq_run_body(instrument_model="NextSeq 550")),
            (202, {"work_ticket_idx": 900}),
            (202, {"work_ticket_idx": 901}),
        ],
    )

    rc = _run_submit_host_filter_pool(rype=7)
    assert rc == 0
    posts = [r for r in captured["requests"] if r["method"] == "POST"]
    by_prep = {
        p["json"]["scope_target"]["prep_sample_idx"]: p["json"]["action_context"] for p in posts
    }
    assert by_prep[1000]["host_filter_enabled"] is True
    assert by_prep[1000]["host_rype_reference_idx"] == 7
    assert by_prep[1001]["host_filter_enabled"] is True
    assert by_prep[1001]["host_rype_reference_idx"] == 7
    ref_gets = [
        r for r in captured["requests"] if r["method"] == "GET" and "/reference/" in r["url"]
    ]
    assert len([r for r in ref_gets if r["url"].endswith("/reference/7")]) == 1


def test_submit_host_filter_pool_instrument_model_absent_omitted(monkeypatch):
    """When the run records no instrument_model (null), the per-sample context
    omits the key — QC then defaults polyG OFF."""
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (200, _pool_samples_body([(100, 1000, "10", True)])),
            (200, _ref_active_body()),
            (200, _both_indexes_body()),
            (200, _seq_run_body(instrument_model=None)),
            (202, {"work_ticket_idx": 900}),
        ],
    )

    rc = _run_submit_host_filter_pool(rype=7)
    assert rc == 0
    post = next(r for r in captured["requests"] if r["method"] == "POST")
    assert "instrument_model" not in post["json"]["action_context"]


def test_submit_host_filter_pool_minimap2_without_rype_errors(capsys):
    """--host-minimap2-reference-idx without --host-rype-reference-idx is rejected
    before any network call (minimap2 is the optional second stage, never
    standalone)."""
    with pytest.raises(SystemExit) as exc_info:
        _run_submit_host_filter_pool(minimap2=8)
    assert exc_info.value.code == 2
    assert "--host-minimap2-reference-idx requires --host-rype-reference-idx" in (
        capsys.readouterr().err
    )


def test_submit_host_filter_pool_reference_not_active_no_posts(monkeypatch, capsys):
    captured: dict = {}
    inactive = _ref_active_body()
    inactive["status"] = "indexing"
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[(200, _pool_samples_body([(100, 1000, "10", True)])), (200, inactive)],
    )

    with pytest.raises(SystemExit) as exc_info:
        _run_submit_host_filter_pool(rype=7)
    assert exc_info.value.code == 1
    assert "not active" in capsys.readouterr().err
    assert not [r for r in captured["requests"] if r["method"] == "POST"]


def test_submit_host_filter_pool_rype_ref_missing_rype_index_no_posts(monkeypatch, capsys):
    """The given rype reference is active but carries no rype index →
    abort before any ticket (only a minimap2 index present here)."""
    captured: dict = {}
    minimap2_only = [_both_indexes_body()[1]]
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (200, _pool_samples_body([(100, 1000, "10", True)])),
            (200, _ref_active_body()),
            (200, minimap2_only),
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        _run_submit_host_filter_pool(rype=7)
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "rype" in err
    assert "--host-rype-reference-idx" in err
    assert not [r for r in captured["requests"] if r["method"] == "POST"]


def test_submit_host_filter_pool_minimap2_ref_missing_minimap2_index_no_posts(monkeypatch, capsys):
    """The given minimap2 reference is active but carries no minimap2 index →
    abort before any ticket (the rype reference passed first)."""
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (200, _pool_samples_body([(100, 1000, "10", True)])),
            (200, _ref_active_body(reference_idx=7)),  # rype reference OK
            (200, [_both_indexes_body()[0]]),  # rype index present
            (200, _ref_active_body(reference_idx=8)),  # minimap2 reference active
            (200, [_both_indexes_body(reference_idx=8)[0]]),  # but only a rype index
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        _run_submit_host_filter_pool(rype=7, minimap2=8)
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "minimap2" in err
    assert "--host-minimap2-reference-idx" in err
    assert not [r for r in captured["requests"] if r["method"] == "POST"]


# ---------------------------------------------------------------------------
# submit-host-filter-pool: intent-mismatch guard (+ --force override)
# ---------------------------------------------------------------------------


def test_submit_host_filter_pool_all_human_no_ref_errors_no_posts(monkeypatch, capsys):
    """Every sample's intake intent is human_filtering=True, but the submission
    gives NO host reference (a pass-through) → the dangerous case: human reads
    would not be depleted. Abort with zero POSTs before any host-ref preflight."""
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[(200, _pool_samples_body([(100, 1000, "10", True), (101, 1001, "11", True)]))],
    )

    with pytest.raises(SystemExit) as exc_info:
        _run_submit_host_filter_pool()
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "intake" in err and "human_filtering" in err
    # Both mismatched samples named.
    assert "sequenced_pool_item_id 10" in err
    assert "sequenced_pool_item_id 11" in err
    assert not [r for r in captured["requests"] if r["method"] == "POST"]


def test_submit_host_filter_pool_all_nonhuman_with_ref_errors_no_posts(monkeypatch, capsys):
    """Every sample's intake intent is human_filtering=False, but the submission
    gives a host reference → samples would be filtered against their intent.
    Abort with zero POSTs."""
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[(200, _pool_samples_body([(100, 1000, "10", False)]))],
    )

    with pytest.raises(SystemExit) as exc_info:
        _run_submit_host_filter_pool(rype=7)
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "sequenced_pool_item_id 10" in err
    assert "apply host reference 7" in err
    # No host-ref preflight GET happened — the guard aborts first.
    assert not [r for r in captured["requests"] if "/reference/" in r["url"]]
    assert not [r for r in captured["requests"] if r["method"] == "POST"]


def test_submit_host_filter_pool_roster_item_missing_intent_errors_no_posts(monkeypatch, capsys):
    """A pool roster item the route resolved no human_filtering intent for (a
    broken bcl-convert/preflight coupling, surfaced as a null roster field)
    aborts fail-fast before any POST. Every pool member is checked, so a single
    null intent fails the submission."""
    # Roster carries 10 (intent True) and 11 (no stored intent → null).
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[(200, _pool_samples_body([(100, 1000, "10", True), (101, 1001, "11")]))],
    )

    with pytest.raises(SystemExit) as exc_info:
        _run_submit_host_filter_pool(rype=7)
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    assert "no stored preflight intent" in err
    assert "11" in err
    assert not [r for r in captured["requests"] if r["method"] == "POST"]


def test_submit_host_filter_pool_mismatch_force_warns_and_proceeds(monkeypatch, capsys):
    """--force downgrades the mismatch to a warning and proceeds with the
    pool-wide host-ref choice (POSTs happen)."""
    # Intake intent says not-human-filtered, but the submission applies rype 7.
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (200, _pool_samples_body([(100, 1000, "10", False)])),
            (200, _ref_active_body()),
            (200, _both_indexes_body()),
            (200, _seq_run_body()),
            (202, {"work_ticket_idx": 900}),
        ],
    )

    rc = _run_submit_host_filter_pool(rype=7, force=True)
    assert rc == 0
    err = capsys.readouterr().err
    assert "WARNING (--force)" in err
    assert "sequenced_pool_item_id 10" in err
    posts = [r for r in captured["requests"] if r["method"] == "POST"]
    assert len(posts) == 1
    assert posts[0]["json"]["action_context"]["host_filter_enabled"] is True


def test_submit_host_filter_pool_mixed_pool_errors_then_force_proceeds(monkeypatch, capsys):
    """A mixed pool (one human, one not) with a host reference → without --force
    it errors naming only the disagreeing sample (the not-human one) and POSTs
    nothing; with --force it warns and submits every sample."""
    mixed_roster = [(100, 1000, "10", True), (101, 1001, "11", False)]

    # Without --force: error, zero POSTs.
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[(200, _pool_samples_body(mixed_roster))],
    )
    with pytest.raises(SystemExit) as exc_info:
        _run_submit_host_filter_pool(rype=7)
    assert exc_info.value.code == 1
    err = capsys.readouterr().err
    # Only the disagreeing sample (item 11, not-human) is flagged.
    assert "sequenced_pool_item_id 11" in err
    assert "sequenced_pool_item_id 10" not in err
    assert not [r for r in captured["requests"] if r["method"] == "POST"]

    # With --force: warns, submits all samples.
    captured2: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured2,
        responses=[
            (200, _pool_samples_body(mixed_roster)),
            (200, _ref_active_body()),
            (200, _both_indexes_body()),
            (200, _seq_run_body()),
            (202, {"work_ticket_idx": 900}),
            (202, {"work_ticket_idx": 901}),
        ],
    )
    rc = _run_submit_host_filter_pool(rype=7, force=True)
    assert rc == 0
    assert "WARNING (--force)" in capsys.readouterr().err
    assert len([r for r in captured2["requests"] if r["method"] == "POST"]) == 2


# ---------------------------------------------------------------------------
# submit-block-mask-pool
# ---------------------------------------------------------------------------
# Same client-side preflight as submit-host-filter-pool (host-ref coherence +
# intent check + host-ref readiness), but the submission is ONE POST to the
# block-mask-plan endpoint, not a per-sample fan-out. instrument_model + the
# only-missing filter are server-side, so there is no GET /sequencing-run and no
# client-side sample skipping.


def _run_submit_block_mask_pool(
    *, run=3, pool=5, rype=None, minimap2=None, force=False, only_missing=False
):
    from qiita_control_plane.cli.user import main

    argv = [
        "submit-block-mask-pool",
        "--sequencing-run-idx",
        str(run),
        "--sequenced-pool-idx",
        str(pool),
    ]
    if rype is not None:
        argv += ["--host-rype-reference-idx", str(rype)]
    if minimap2 is not None:
        argv += ["--host-minimap2-reference-idx", str(minimap2)]
    if force:
        argv += ["--force"]
    if only_missing:
        argv += ["--only-missing"]
    return main(argv)


def test_submit_block_mask_pool_single_plan_call_passthrough(monkeypatch, capsys):
    """No host reference (a pass-through): after the roster GET, exactly ONE POST
    to the block-mask-plan endpoint (no per-sample fan-out, no host-ref GETs, no
    GET /sequencing-run), carrying only_missing + null host refs."""
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            # Pass-through intent (human_filtering=False) matches "no host ref".
            (200, _pool_samples_body([(100, 1000, "10", False), (101, 1001, "11", False)])),
            (202, {"blocks_created": 1, "samples_planned": 2, "partitions": [], "blocks": []}),
        ],
    )
    rc = _run_submit_block_mask_pool()
    assert rc == 0

    posts = [r for r in captured["requests"] if r["method"] == "POST"]
    assert len(posts) == 1
    assert posts[0]["url"].endswith("/sequenced-pool/5/block-mask-plan")
    assert posts[0]["json"] == {
        "host_rype_reference_idx": None,
        "host_minimap2_reference_idx": None,
        "only_missing": False,
    }
    # No host-ref preflight GETs and no GET /sequencing-run for a pass-through.
    assert not [r for r in captured["requests"] if "/reference/" in r["url"]]


def test_submit_block_mask_pool_host_filtered_preflights_and_posts(monkeypatch, capsys):
    """With a rype reference: the reference is pre-flighted (GET /reference/{idx}
    + its index list), then ONE plan POST carrying the host ref."""
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        responses=[
            (200, _pool_samples_body([(100, 1000, "10", True), (101, 1001, "11", True)])),
            (200, _ref_active_body()),  # rype reference 7
            (200, _both_indexes_body()),  # its index list (carries rype)
            (202, {"blocks_created": 2, "samples_planned": 2, "partitions": [], "blocks": []}),
        ],
    )
    rc = _run_submit_block_mask_pool(rype=7, only_missing=True)
    assert rc == 0

    posts = [r for r in captured["requests"] if r["method"] == "POST"]
    assert len(posts) == 1
    assert posts[0]["json"] == {
        "host_rype_reference_idx": 7,
        "host_minimap2_reference_idx": None,
        "only_missing": True,
    }


def test_submit_block_mask_pool_intent_mismatch_aborts_no_post(monkeypatch, capsys):
    """The shared intent preflight fires for the block path too: a sample whose
    intake intent disagrees with the pool-wide choice aborts before any POST
    (no --force)."""
    captured: dict = {}
    _stub_multi_response(
        monkeypatch,
        captured,
        # Intake says human-filtered (True) but the submission applies no host ref.
        responses=[(200, _pool_samples_body([(100, 1000, "10", True)]))],
    )
    with pytest.raises(SystemExit) as exc_info:
        _run_submit_block_mask_pool()
    assert exc_info.value.code == 1
    assert not [r for r in captured["requests"] if r["method"] == "POST"]


def test_submit_block_mask_pool_minimap2_without_rype_errors(capsys):
    """argparse-time coherence: minimap2 requires rype (exit 2), before any call."""
    with pytest.raises(SystemExit):
        _run_submit_block_mask_pool(minimap2=9)


# ---------------------------------------------------------------------------
# pool-completion (two-idx GET read command)
# ---------------------------------------------------------------------------


def test_pool_completion_issues_get_against_run_and_pool(monkeypatch):
    """pool-completion GETs the run+pool-scoped completion route and returns the
    decoded body (exit 0)."""
    import httpx as _httpx
    from qiita_common.api_paths import URL_SEQUENCED_POOL_COMPLETION

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        return _httpx.Response(
            200,
            json={"sequenced_pool_idx": 5, "complete": False},
            request=_httpx.Request(method, url),
        )

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    rc = main(
        [
            "pool-completion",
            "--sequencing-run-idx",
            "3",
            "--sequenced-pool-idx",
            "5",
        ]
    )
    assert rc == 0
    assert captured["method"] == "GET"
    assert captured["url"].endswith(
        URL_SEQUENCED_POOL_COMPLETION.format(sequencing_run_idx=3, sequenced_pool_idx=5)
    )


def test_pool_completion_requires_both_idxs(capsys):
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["pool-completion", "--sequencing-run-idx", "3"])
    assert exc_info.value.code == 2
    assert "--sequenced-pool-idx" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# study get / biosample get / biosample list-idxs (read subcommands)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("argv", "expected_url"),
    [
        (["study", "get", "--study-idx", "42"], URL_STUDY_BY_IDX.format(study_idx=42)),
        (
            ["biosample", "get", "--biosample-idx", "100"],
            URL_BIOSAMPLE_BY_IDX.format(biosample_idx=100),
        ),
        (
            ["biosample", "list-idxs", "--study-idx", "42"],
            URL_BIOSAMPLE_LIST_BY_STUDY.format(study_idx=42),
        ),
    ],
)
def test_read_subcommand_issues_get(monkeypatch, argv, expected_url):
    """Tests the case where a read subcommand issues an authenticated GET to
    the resource's URL and returns the decoded body (exit 0)."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["auth"] = headers["Authorization"]
        return _httpx.Response(200, json={"ok": True}, request=_httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    rc = main(["--base-url", "https://q.example.test", *argv])
    assert rc == 0
    assert captured["method"] == "GET"
    assert captured["url"] == f"https://q.example.test{expected_url}"
    assert captured["auth"] == f"{BEARER_PREFIX}qk_test"


@pytest.mark.parametrize(
    "argv",
    [
        ["study", "get"],
        ["biosample", "get"],
        ["biosample", "list-idxs"],
    ],
)
def test_read_subcommand_requires_idx(argv):
    """Tests the case where a read subcommand's required idx flag is omitted;
    argparse rejects the invocation with exit 2."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(argv)
    assert exc_info.value.code == 2


def test_read_subcommand_http_error_exits_1(monkeypatch, capsys):
    """Tests the case where the GET returns a non-2xx status; the command
    exits 1 and names the status on stderr."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        return _httpx.Response(
            404, json={"detail": "not found"}, request=_httpx.Request(method, url)
        )

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    rc = main(["--base-url", "https://q.example.test", "study", "get", "--study-idx", "999"])
    assert rc == 1
    assert "404" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# study patch / biosample patch / sequenced-sample patch (write subcommands)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("argv", "expected_url", "expected_body"),
    [
        (
            ["study", "patch", "--study-idx", "42", "--ena-study-accession", "PRJEB1"],
            URL_STUDY_BY_IDX.format(study_idx=42),
            {"ena_study_accession": "PRJEB1"},
        ),
        (
            ["study", "patch", "--study-idx", "42", "--bioproject-accession", "PRJNA1"],
            URL_STUDY_BY_IDX.format(study_idx=42),
            {"bioproject_accession": "PRJNA1"},
        ),
        (
            ["biosample", "patch", "--biosample-idx", "100", "--ena-sample-accession", "ERS1"],
            URL_BIOSAMPLE_BY_IDX.format(biosample_idx=100),
            {"ena_sample_accession": "ERS1"},
        ),
        (
            [
                "sequenced-sample",
                "patch",
                "--sequenced-sample-idx",
                "5",
                "--ena-experiment-accession",
                "ERX1",
                "--ena-run-accession",
                "ERR1",
            ],
            URL_SEQUENCED_SAMPLE_BY_IDX.format(sequenced_sample_idx=5),
            {"ena_experiment_accession": "ERX1", "ena_run_accession": "ERR1"},
        ),
    ],
)
def test_patch_subcommand_get_etag_then_patch_if_match(
    monkeypatch, argv, expected_url, expected_body
):
    """Tests the case where a patch subcommand GETs the resource to read its
    ETag, then PATCHes with that ETag as If-Match and the supplied fields as
    the body (exit 0)."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    requests: list[dict] = []

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        requests.append({"method": method, "url": url, "headers": headers, "json": json})
        if method == "GET":
            return _httpx.Response(
                200, json={}, headers={"ETag": "etag-v1"}, request=_httpx.Request(method, url)
            )
        return _httpx.Response(200, json={"ok": True}, request=_httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    rc = main(["--base-url", "https://q.example.test", *argv])
    assert rc == 0
    assert [r["method"] for r in requests] == ["GET", "PATCH"]
    get_req, patch_req = requests
    assert get_req["url"] == f"https://q.example.test{expected_url}"
    assert patch_req["url"] == f"https://q.example.test{expected_url}"
    assert patch_req["headers"]["If-Match"] == "etag-v1"
    assert patch_req["headers"]["Authorization"] == f"{BEARER_PREFIX}qk_test"
    assert patch_req["json"] == expected_body


@pytest.mark.parametrize(
    "argv",
    [
        ["study", "patch", "--study-idx", "42"],
        ["biosample", "patch", "--biosample-idx", "100"],
        ["sequenced-sample", "patch", "--sequenced-sample-idx", "5"],
    ],
)
def test_patch_subcommand_empty_update_exits_2(argv):
    """Tests the case where a patch subcommand is invoked with no field flags;
    the PatchRequestModel's at-least-one-field rule rejects it (exit 2)."""
    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(["--base-url", "https://q.example.test", *argv])
    assert exc_info.value.code == 2


def test_patch_subcommand_conflict_exits_1(monkeypatch, capsys):
    """Tests the case where the PATCH is rejected with 412 (stale If-Match);
    the command exits 1 and surfaces the status on stderr."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common
    from qiita_control_plane.cli.user import main

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        if method == "GET":
            return _httpx.Response(
                200, json={}, headers={"ETag": "etag-stale"}, request=_httpx.Request(method, url)
            )
        return _httpx.Response(
            412, json={"detail": "If-Match did not match"}, request=_httpx.Request(method, url)
        )

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "study",
            "patch",
            "--study-idx",
            "42",
            "--ena-study-accession",
            "PRJEB1",
        ]
    )
    assert rc == 1
    assert "412" in capsys.readouterr().err


def test_run_preflight_update_lane_posts_body(monkeypatch):
    """`qiita run-preflight update-lane` POSTs platform/from_lane/to_lane/reason
    to the preflight update-lane route, with integer lanes preserved."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["method"] = method
        captured["url"] = url
        captured["json"] = json
        return _httpx.Response(
            200,
            json={"sequenced_pool_idx": 5, "rows_updated": 3},
            request=_httpx.Request(method, url),
        )

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "run-preflight",
            "update-lane",
            "--sequencing-run-idx",
            "7",
            "--sequenced-pool-idx",
            "5",
            "--platform",
            "illumina",
            "--from-lane",
            "1",
            "--to-lane",
            "2",
            "--reason",
            "fix stale lane",
        ]
    )
    assert rc == 0
    assert captured["method"] == "POST"
    expected_url = URL_SEQUENCED_POOL_PREFLIGHT_UPDATE_LANE.format(
        sequencing_run_idx=7, sequenced_pool_idx=5
    )
    assert captured["url"] == f"https://q.example.test{expected_url}"
    assert captured["json"] == {
        "platform": "illumina",
        "from_lane": 1,
        "to_lane": 2,
        "reason": "fix stale lane",
    }


def test_run_preflight_update_lane_sends_explicit_null(monkeypatch):
    """`--to-lane none` sends a JSON null (a NULL lane is a real value), not a
    dropped field — so update_lane can clear lanes."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured["json"] = json
        return _httpx.Response(
            200,
            json={"sequenced_pool_idx": 5, "rows_updated": 1},
            request=_httpx.Request(method, url),
        )

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    rc = main(
        [
            "run-preflight",
            "update-lane",
            "--sequencing-run-idx",
            "7",
            "--sequenced-pool-idx",
            "5",
            "--platform",
            "illumina",
            "--from-lane",
            "1",
            "--to-lane",
            "none",
            "--reason",
            "clear lanes",
        ]
    )
    assert rc == 0
    assert captured["json"]["to_lane"] is None
    assert captured["json"]["from_lane"] == 1


def test_run_preflight_update_lane_identical_lanes_errors(monkeypatch, capsys):
    """from_lane == to_lane is rejected client-side (exit 2) before any HTTP
    call, so the SQLite change_log never gains a spurious no-op entry."""
    from qiita_control_plane.cli import _common

    def boom(*a, **k):  # pragma: no cover - must never be reached
        raise AssertionError("no HTTP request should be made for an identical-lane no-op")

    monkeypatch.setattr(_common.httpx, "request", boom)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "run-preflight",
                "update-lane",
                "--sequencing-run-idx",
                "7",
                "--sequenced-pool-idx",
                "5",
                "--platform",
                "illumina",
                "--from-lane",
                "2",
                "--to-lane",
                "2",
                "--reason",
                "noop",
            ]
        )
    assert exc_info.value.code == 2
    assert "identical" in capsys.readouterr().err


def test_run_preflight_update_lane_blank_reason_errors(monkeypatch, capsys):
    """A whitespace-only --reason is rejected client-side (exit 2) before any HTTP
    call, so the preflight change_log audit trail never gets a blank reason."""
    from qiita_control_plane.cli import _common

    def boom(*a, **k):  # pragma: no cover - must never be reached
        raise AssertionError("no HTTP request should be made for a blank reason")

    monkeypatch.setattr(_common.httpx, "request", boom)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")

    from qiita_control_plane.cli.user import main

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "run-preflight",
                "update-lane",
                "--sequencing-run-idx",
                "7",
                "--sequenced-pool-idx",
                "5",
                "--platform",
                "illumina",
                "--from-lane",
                "1",
                "--to-lane",
                "2",
                "--reason",
                "   ",
            ]
        )
    assert exc_info.value.code == 2
    assert "reason" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# prep-protocol list / reference list — discovery commands (#162)
# ---------------------------------------------------------------------------


def test_prep_protocol_list_default_excludes_retired(monkeypatch):
    """`prep-protocol list` GETs /prep-protocol with no include_retired param."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(
        monkeypatch,
        captured,
        response_json=[
            {
                "prep_protocol_idx": 3,
                "name": "short_read_metagenomics",
                "description": None,
                "retired": False,
                "created_by_idx": 1,
                "created_at": "2026-01-01T00:00:00Z",
            }
        ],
        status=200,
    )
    rc = main(["--base-url", "https://q.example.test", "prep-protocol", "list"])
    assert rc == 0
    assert captured["method"] == "GET"
    assert captured["url"] == f"https://q.example.test{URL_PREP_PROTOCOL_PREFIX}"
    assert captured["params"] is None
    assert captured["json"] is None


def test_prep_protocol_list_all_includes_retired(monkeypatch):
    """`prep-protocol list --all` passes include_retired=true."""
    from qiita_control_plane.cli.user import main

    captured: dict = {}
    _stub_post(monkeypatch, captured, response_json=[], status=200)
    rc = main(["--base-url", "https://q.example.test", "prep-protocol", "list", "--all"])
    assert rc == 0
    assert captured["method"] == "GET"
    assert captured["params"] == {"include_retired": "true"}


def _stub_reference_list(monkeypatch, *, references, indexes_by_idx):
    """Route GET /reference -> `references` and GET /reference/{idx}/index ->
    `indexes_by_idx[idx]`, recording each request. Returns the request log."""
    import httpx as _httpx

    from qiita_control_plane.cli import _common

    requests: list[dict] = []

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        requests.append({"method": method, "url": url, "params": params})
        if url.endswith("/index"):
            idx = int(url.rsplit("/reference/", 1)[1].split("/")[0])
            body = indexes_by_idx.get(idx, [])
        else:
            body = references
        return _httpx.Response(200, json=body, request=_httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    monkeypatch.setenv("QIITA_TOKEN", "qk_test")
    return requests


def _ref_row(idx, **over):
    row = {
        "reference_idx": idx,
        "name": f"ref{idx}",
        "version": "v1",
        "kind": "sequence_reference",
        "status": "active",
        "is_host": True,
        "created_by_idx": 1,
        "created_at": "2026-01-01T00:00:00Z",
    }
    row.update(over)
    return row


def test_reference_list_filters_by_index_type(monkeypatch, capsys):
    """`reference list --host --active --index-type rype` sends is_host/status
    filters, enriches each row with its built index types, and drops references
    lacking the requested index — exactly the readiness-gate set."""
    from qiita_control_plane.cli.user import main

    requests = _stub_reference_list(
        monkeypatch,
        references=[_ref_row(10), _ref_row(11)],
        indexes_by_idx={
            10: [{"index_type": "rype"}, {"index_type": "minimap2"}],
            11: [{"index_type": "minimap2"}],
        },
    )
    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "reference",
            "list",
            "--host",
            "--active",
            "--index-type",
            "rype",
        ]
    )
    assert rc == 0
    list_req = next(r for r in requests if r["url"].endswith("/reference"))
    assert list_req["method"] == "GET"
    assert list_req["params"] == {"is_host": "true", "status": "active"}
    body = json.loads(capsys.readouterr().out)
    assert [r["reference_idx"] for r in body] == [10]
    assert body[0]["index_types"] == ["minimap2", "rype"]


def test_reference_list_no_filters_enriches_all(monkeypatch, capsys):
    """Without filters every reference is returned, each with its index types
    (empty list when it has none) and no query params on the list call."""
    from qiita_control_plane.cli.user import main

    requests = _stub_reference_list(
        monkeypatch, references=[_ref_row(5, is_host=False)], indexes_by_idx={}
    )
    rc = main(["--base-url", "https://q.example.test", "reference", "list"])
    assert rc == 0
    list_req = next(r for r in requests if r["url"].endswith("/reference"))
    assert list_req["params"] is None
    body = json.loads(capsys.readouterr().out)
    assert body[0]["reference_idx"] == 5
    assert body[0]["index_types"] == []


def test_reference_list_index_type_no_match_returns_empty(monkeypatch, capsys):
    """`reference list --index-type rype` over references that carry only other
    index types yields an empty list (every row is filtered out)."""
    from qiita_control_plane.cli.user import main

    _stub_reference_list(
        monkeypatch,
        references=[_ref_row(20), _ref_row(21)],
        indexes_by_idx={20: [{"index_type": "minimap2"}], 21: []},
    )
    rc = main(
        [
            "--base-url",
            "https://q.example.test",
            "reference",
            "list",
            "--index-type",
            "rype",
        ]
    )
    assert rc == 0
    assert json.loads(capsys.readouterr().out) == []
