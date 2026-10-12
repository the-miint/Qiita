"""Fixtures and builders shared by the `ena_submission` tests."""

from pathlib import Path

import pytest

from qiita_control_plane.ena_submission.mapping import EnaProjectRow, EnaSampleRow

_OPEN_SESSION = "qiita_control_plane.ena_submission.catalog._open_ena_submission_session"

_WEBIN_USER = "Webin-00000"
_ENDPOINT = "test"
_CHECKLIST = "ERC000011"

_CHECKLIST_VALIDATION_DETAIL = f"sample alias 'QB11': checklist '{_CHECKLIST}' marks field 'collection date' as mandatory but it is missing or empty"  # noqa: E501


@pytest.fixture
def password_file(tmp_path: Path) -> Path:
    """A readable Webin password file; its contents are miint's to consume."""
    path = tmp_path / "webin_password.txt"
    path.write_text("not-a-real-password\n")
    return path


def _valid_kwargs(password_file: Path, **overrides) -> dict:
    """A configuration that passes validation, with any field replaced."""
    return {
        "webin_user": _WEBIN_USER,
        "webin_password_file": password_file,
        "endpoint": _ENDPOINT,
        **overrides,
    }


def _already_exists_error(object_noun: str, alias: str, accession: str) -> str:
    """ENA's rejection of an object already in the account, worded as its one
    documented example of it (for an analysis) shows."""
    return f'In {object_noun}, alias:"{alias}", accession:"". The object being added already exists in the submission account with accession: "{accession}".'  # noqa: E501


def _project_row() -> EnaProjectRow:
    """One project body, with the unpopulated columns left at their default."""
    return EnaProjectRow(alias="QS7", title="A study", project_type="other")


def _sample_row(alias: str) -> EnaSampleRow:
    """One sample body carrying a single checklist attribute and no units."""
    return EnaSampleRow(
        alias=alias,
        taxon_id=408170,
        checklist=_CHECKLIST,
        attributes={"collection date": "2026-01-15"},
    )
