"""`qiita-admin definition edit-settings`: PATCHes the definition-settings route with
the params file, the reason and only the workflow / version given."""

import json

import httpx
from qiita_common.api_paths import URL_ADMIN_DEFINITION_SETTINGS

from qiita_control_plane.cli import admin as cli

_EDIT = {
    "definition_settings_edit_idx": 3,
    "definition_kind": "processing",
    "definition_idx": 7,
    "actor_principal_idx": 2,
    "old_settings": {},
    "new_settings": {},
    "reason": "typo",
    "occurred_at": "2026-11-16T00:00:00Z",
    "repaired_pair_idx": [4],
}


def _capture(monkeypatch, status=200, body=_EDIT):
    from qiita_control_plane.cli import _common

    monkeypatch.setenv("QIITA_TOKEN", "qk_admin")
    captured: dict = {}

    def fake_request(method, url, headers=None, json=None, params=None, timeout=None):
        captured.update(method=method, url=url, json=json)
        return httpx.Response(status, json=body, request=httpx.Request(method, url))

    monkeypatch.setattr(_common.httpx, "request", fake_request)
    return captured


def test_edit_settings_sends_the_params_file_and_only_the_columns_given(
    monkeypatch, capsys, tmp_path
):
    params = tmp_path / "params.json"
    params.write_text(
        json.dumps({"workflow": "long-read-assembly", "version": "1.1.0", "assembler": "hifiasm"})
    )
    captured = _capture(monkeypatch)

    argv = ["definition", "edit-settings", "processing", "7", "--params-file", str(params)]
    assert cli.main([*argv, "--version", "1.1.0", "--reason", "typo"]) == 0

    assert captured["method"] == "PATCH"
    assert captured["url"].endswith(
        URL_ADMIN_DEFINITION_SETTINGS.format(definition_kind="processing", definition_idx=7)
    )
    assert captured["json"] == {
        "params": {"workflow": "long-read-assembly", "version": "1.1.0", "assembler": "hifiasm"},
        "reason": "typo",
        "version": "1.1.0",
    }
    assert "pairs recomputed: [4]" in capsys.readouterr().err


def test_a_refused_edit_exits_1(monkeypatch, tmp_path):
    params = tmp_path / "params.json"
    params.write_text("{}")
    _capture(monkeypatch, status=409, body={"detail": {"detail": "x", "reason": "no_change"}})
    argv = ["definition", "edit-settings", "mask", "7", "--params-file", str(params)]
    assert cli.main([*argv, "--reason", "typo"]) == 1
