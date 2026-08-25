"""Unit tests for the `qiita <entity> list-fields` / `list-global-fields` verbs.

Each verb is one GET with no body and no filters, so what these lock in is the
wiring rather than any request shaping. Both entities are driven off one
surface, since the two differ only in their bindings.
"""

import pytest

from qiita_control_plane.cli.user import main

from .test_user_cli import _BASE, _FIELD_CLI_SURFACES, _run

_LIST_BODY: list[dict] = []


@pytest.mark.parametrize("surface", _FIELD_CLI_SURFACES)
def test_list_global_fields_gets_the_registry_with_no_parameters(monkeypatch, surface):
    """Tests the case where the registry list runs with no flags: it dials its
    own top-level route, and neither a body nor query params reach the wire."""
    captured = _run(
        monkeypatch, [surface.subcommand, "list-global-fields"], response_json=_LIST_BODY
    )

    assert captured["method"] == "GET"
    assert captured["url"] == f"{_BASE}{surface.global_field_url}"
    assert captured["json"] is None
    assert captured["params"] is None


@pytest.mark.parametrize("surface", _FIELD_CLI_SURFACES)
def test_list_fields_fills_the_path_from_study_idx(monkeypatch, surface):
    """Tests the case where the study-scoped list runs with --study-idx: the idx
    fills the path template rather than travelling as a query param."""
    captured = _run(
        monkeypatch,
        [surface.subcommand, "list-fields", "--study-idx", "5"],
        response_json=_LIST_BODY,
    )

    assert captured["method"] == "GET"
    assert captured["url"] == f"{_BASE}{surface.url_template.format(study_idx=5)}"
    assert captured["json"] is None
    assert captured["params"] is None


@pytest.mark.parametrize("surface", _FIELD_CLI_SURFACES)
def test_list_fields_requires_study_idx(surface):
    """Tests the case where the study-scoped list omits its required idx flag:
    argparse rejects the invocation with exit 2, before any request."""
    with pytest.raises(SystemExit) as exc_info:
        main([surface.subcommand, "list-fields"])
    assert exc_info.value.code == 2
