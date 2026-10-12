"""Contract tests for `EnaSubmissionCatalog` against real miint.

Each test runs the catalog's own session setup, inserts and teardown through the
staged miint extension, with a local stand-in answering in ENA's place, so they
pin what the recording-connection tests cannot see: that miint accepts the
statements the catalog builds and hands back what the catalog expects.
"""

import json
import re
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from qiita_common.api_paths import LOOPBACK_HOST

from qiita_control_plane.ena_submission import catalog as catalog_module
from qiita_control_plane.ena_submission.catalog import EnaSubmissionCatalog
from qiita_control_plane.testing.loopback import serve_on_loopback

from .conftest import _CHECKLIST, _OPEN_SESSION, _project_row, _sample_row, _valid_kwargs

# Where miint sends each request; the portal and checklist bases reach miint
# through these environment variables, the submit URL through the session.
_SUBMIT_PATH = "/submit"
_PORTAL_BASE_PATH = "/portal/api"
_CHECKLIST_BASE_PATH = "/browser/api/xml"
_PORTAL_BASE_ENV = "MIINT_ENA_PORTAL_URL_BASE"
_CHECKLIST_BASE_ENV = "MIINT_ENA_CHECKLIST_URL_BASE"

# Per envelope key: the receipt element, its primary and secondary accession
# prefixes, and the EXT_ID type the secondary accession is reported under.
_RECEIPT_KINDS = {
    "projects": ("PROJECT", "PRJEB", "ERP", "study"),
    "samples": ("SAMPLE", "ERS", "SAMEA", "biosample"),
}
_FIRST_ACCESSION_NUMBER = 1000

_LATITUDE_LABEL = "geographic location (latitude)"
_LATITUDE_UNIT = "DD"

# miint caches a fetched checklist per process by accession, so this is the one
# checklist the stand-in ever serves for it.
_CHECKLIST_XML = f"""<?xml version="1.0" encoding="UTF-8"?>
<CHECKLIST_SET>
<CHECKLIST accession="{_CHECKLIST}" checklistType="Sample">
  <IDENTIFIERS><PRIMARY_ID>{_CHECKLIST}</PRIMARY_ID></IDENTIFIERS>
  <DESCRIPTOR>
    <LABEL>Stand-in checklist</LABEL>
    <NAME>stand_in</NAME>
    <FIELD_GROUP restrictionType="Any number or none of the fields">
      <FIELD>
        <LABEL>collection date</LABEL>
        <NAME>collection_date</NAME>
        <FIELD_TYPE><TEXT_FIELD/></FIELD_TYPE>
        <MANDATORY>mandatory</MANDATORY>
      </FIELD>
      <FIELD>
        <LABEL>{_LATITUDE_LABEL}</LABEL>
        <NAME>geographic_location_latitude</NAME>
        <FIELD_TYPE><TEXT_FIELD/></FIELD_TYPE>
        <UNITS><UNIT>{_LATITUDE_UNIT}</UNIT></UNITS>
        <MANDATORY>optional</MANDATORY>
      </FIELD>
    </FIELD_GROUP>
  </DESCRIPTOR>
</CHECKLIST>
</CHECKLIST_SET>
"""

_XML_ACTION_RE = re.compile(r"<ACTION>\s*<(\w+)")
_XML_SAMPLE_ALIAS_RE = re.compile(r'<SAMPLE\b[^>]*\balias="([^"]*)"')


def _parse_envelope(content_type: str, body: str) -> tuple[str, str, list[str]]:
    """The envelope key, the action, and the aliases of one submission body:
    projects arrive as JSON, samples as XML."""
    if "json" in content_type:
        envelope = json.loads(body)
        actions = [action["type"] for action in envelope["submission"]["actions"]]
        aliases = [project["alias"] for project in envelope["projects"]]
        return "projects", actions[-1], aliases
    action_match = _XML_ACTION_RE.search(body)
    action = "ADD" if action_match is None else action_match.group(1)
    aliases = _XML_SAMPLE_ALIAS_RE.findall(body)
    return "samples", action, aliases


def _build_receipt(kind: str, action: str, aliases: list[str]) -> str:
    """A successful receipt numbering each alias's accessions by its position;
    a VALIDATE receipt names no objects, since validation assigns none."""
    element, primary_prefix, secondary_prefix, ext_type = _RECEIPT_KINDS[kind]
    objects = []
    if action != "VALIDATE":
        for position, alias in enumerate(aliases):
            number = _FIRST_ACCESSION_NUMBER + position
            objects.append(
                f'<{element} accession="{primary_prefix}{number}" alias="{alias}"'
                f' status="PRIVATE"><EXT_ID accession="{secondary_prefix}{number}"'
                f' type="{ext_type}"/></{element}>'
            )
    receipt = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<RECEIPT receiptDate="2026-10-12T12:00:00.000Z" submissionFile="stand-in" success="true">'
        f"{''.join(objects)}"
        '<SUBMISSION accession="ERA1000" alias="stand-in"/>'
        f"<ACTIONS>{action}</ACTIONS>"
        "</RECEIPT>"
    )
    return receipt


def _portal_search_tsv(path: str) -> str:
    """A portal search answer reporting none of the queried aliases as taken."""
    query = parse_qs(urlparse(path).query)
    header = query["fields"][0]
    return f"{header}\n"


class _EnaStandInHandler(BaseHTTPRequestHandler):
    """Answers miint's submit, alias-check and checklist requests in ENA's place."""

    def do_GET(self) -> None:
        if self.path.startswith(f"{_PORTAL_BASE_PATH}/search"):
            self._respond("text/tab-separated-values", _portal_search_tsv(self.path))
        elif self.path == f"{_CHECKLIST_BASE_PATH}/{_CHECKLIST}":
            self._respond("application/xml", _CHECKLIST_XML)
        else:
            self.send_error(404)

    def do_POST(self) -> None:
        if self.path != _SUBMIT_PATH:
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8")
        kind, action, aliases = _parse_envelope(self.headers.get("Content-Type", ""), body)
        self._respond("application/xml", _build_receipt(kind, action, aliases))

    def _respond(self, content_type: str, text: str) -> None:
        payload = text.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args) -> None:
        pass


@pytest.fixture
def contract_catalog(monkeypatch, password_file: Path):
    """A factory for catalogs whose sessions run on real miint with the stand-in
    answering for ENA; enter one to submit."""
    with serve_on_loopback(_EnaStandInHandler) as port:
        base_url = f"http://{LOOPBACK_HOST}:{port}"
        monkeypatch.setenv(_PORTAL_BASE_ENV, f"{base_url}{_PORTAL_BASE_PATH}")
        monkeypatch.setenv(_CHECKLIST_BASE_ENV, f"{base_url}{_CHECKLIST_BASE_PATH}")

        # Point the real session opener at the stand-in; everything else is unchanged
        open_session = catalog_module._open_ena_submission_session

        def open_against_stand_in(**kwargs):
            session = open_session(**kwargs, endpoint_url=base_url)
            return session

        monkeypatch.setattr(_OPEN_SESSION, open_against_stand_in)

        def make_catalog(*, dry_run: bool = False) -> EnaSubmissionCatalog:
            catalog = EnaSubmissionCatalog(**_valid_kwargs(password_file, dry_run=dry_run))
            return catalog

        yield make_catalog


def _sample_row_with_latitude(alias: str):
    """A sample body whose attributes include one carrying a unit."""
    row = _sample_row(alias)
    attributes = {**row.attributes, _LATITUDE_LABEL: "-64.8"}
    with_latitude = row.model_copy(
        update={"attributes": attributes, "attribute_units": {_LATITUDE_LABEL: _LATITUDE_UNIT}}
    )
    return with_latitude


def test_EnaSubmissionCatalog_submit_project(contract_catalog):
    """Tests the case where one project is submitted through real miint: the
    accessions in ENA's receipt come back as (prjeb, erp)."""
    with contract_catalog() as catalog:
        assigned = catalog.submit_project(_project_row())

    assert assigned == ("PRJEB1000", "ERP1000")


def test_EnaSubmissionCatalog_submit_samples(contract_catalog):
    """Tests the case where two samples carrying attributes and units are
    submitted through real miint: both maps bind, and each row gets the
    accessions ENA's receipt names for its alias."""
    rows = [_sample_row_with_latitude("QB11"), _sample_row_with_latitude("QB12")]

    with contract_catalog() as catalog:
        assigned = catalog.submit_samples(rows)

    assert assigned == [("ERS1000", "SAMEA1000"), ("ERS1001", "SAMEA1001")]


def test_EnaSubmissionCatalog_submit_samples_dry_run(contract_catalog):
    """Tests the case where samples are submitted under dry-run through real
    miint: validation assigns no accessions, so every row comes back None."""
    rows = [_sample_row("QB11"), _sample_row("QB12")]

    with contract_catalog(dry_run=True) as catalog:
        assigned = catalog.submit_samples(rows)

    assert assigned == [None, None]
