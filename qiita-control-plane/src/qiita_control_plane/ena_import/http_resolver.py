"""`HttpEnaResolver` — the experimental plain-HTTP fallback (T01-4, D2's
escape hatch). Implements the same `EnaResolver` contract as
`MiintEnaResolver` by talking directly to ENA's public APIs over `httpx`
instead of DuckDB + miint: the Portal API (`/search`, TSV) for the study
header and runs, and the Browser API (`/xml/...`) for per-sample attributes.

Off by default (see `ena_import.get_resolver`) — the swap from
`MiintEnaResolver` is config-level, never a callers change. URL/query shape
mirrors `duckdb-miint`'s own `ENAParser::BuildSearchURL` /
`ENAParser::BuildXMLURL` (`duckdb-miint/src/ena_parser.cpp`) so the two
resolvers agree on results; unlike miint's `read_ena_attributes`, this
fallback does not implement the `/search`-pushdown optimization for
attribute filtering — it always resolves the study's sample accessions and
fetches their XML.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from urllib.parse import quote

import httpx
from qiita_common.models.ena import EnaRunRecord, EnaSampleAttributes, EnaStudyHeader

from .accession import validate_study_accession
from .resolver import EnaAccessionNotFoundError, EnaResolver, pivot_sample_attributes

PORTAL_BASE = "https://www.ebi.ac.uk/ena/portal/api"
BROWSER_BASE = "https://www.ebi.ac.uk/ena/browser/api"

# Mirrors MiintEnaResolver._RUN_FIELDS so the two resolvers map the same
# EnaRunRecord fields from the same underlying Portal API columns.
_RUN_FIELDS = (
    "run_accession,experiment_accession,sample_accession,study_accession,"
    "library_layout,library_strategy,library_source,library_selection,"
    "instrument_platform,"
    "fastq_ftp,fastq_aspera,fastq_bytes,fastq_md5,read_count,base_count"
)
_STUDY_FIELDS = (
    "study_accession,secondary_study_accession,study_title,study_description,"
    "center_name,first_public,last_updated,scientific_name,tax_id"
)

_HTTP_TIMEOUT_SECONDS = 60


def _parse_tsv(text: str) -> tuple[list[str], list[list[str]]]:
    """Parse a `format=tsv` Portal API response into (columns, rows). ENA
    always returns a header line even for zero data rows."""
    lines = [line for line in text.splitlines() if line != ""]
    if not lines:
        return [], []
    columns = lines[0].split("\t")
    rows = [line.split("\t") for line in lines[1:]]
    return columns, rows


def _parse_sample_attributes_xml(xml_text: str) -> list[list[str]]:
    """Parse a Browser API `/xml/<accession,...>` response into
    (sample_accession, tag, value) rows — the same narrow shape
    `read_ena_attributes` returns (see `ENAParser::ParseSampleAttributesXML`,
    `duckdb-miint/src/ena_parser.cpp`)."""
    root = ET.fromstring(xml_text)  # noqa: S314 - trusted ENA response, not user input
    rows: list[list[str]] = []
    for sample in root.findall(".//SAMPLE"):
        sample_accession = sample.get("accession", "")
        for attribute in sample.findall(".//SAMPLE_ATTRIBUTE"):
            tag = attribute.findtext("TAG")
            if not tag:
                continue
            value = attribute.findtext("VALUE") or ""
            rows.append([sample_accession, tag, value])
    return rows


class HttpEnaResolver(EnaResolver):
    """Experimental — talks to ENA's public HTTP APIs directly, no miint /
    DuckDB dependency. Off by default; see `ena_import.get_resolver`."""

    def __init__(self, *, http_client: httpx.Client | None = None) -> None:
        if http_client is not None:
            self._http = http_client
            self._owns_http = False
        else:
            self._http = httpx.Client(timeout=_HTTP_TIMEOUT_SECONDS)
            self._owns_http = True

    def close(self) -> None:
        if self._owns_http:
            self._http.close()

    def __enter__(self) -> HttpEnaResolver:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _search(
        self, accession: str, result: str, fields: str
    ) -> tuple[list[str], list[list[str]]]:
        url = (
            f"{PORTAL_BASE}/search?result={result}"
            f"&query=study_accession%3D%22{quote(accession)}%22"
            f"&fields={fields}&limit=0&format=tsv"
        )
        response = self._http.get(url)
        response.raise_for_status()
        return _parse_tsv(response.text)

    def resolve_study_header(self, accession: str) -> EnaStudyHeader:
        accession = validate_study_accession(accession)
        columns, rows = self._search(accession, "study", _STUDY_FIELDS)
        if not rows:
            raise EnaAccessionNotFoundError(f"no ENA study found for accession {accession!r}")
        return EnaStudyHeader(**dict(zip(columns, rows[0], strict=True)))

    def resolve_runs(self, accession: str) -> list[EnaRunRecord]:
        accession = validate_study_accession(accession)
        columns, rows = self._search(accession, "read_run", _RUN_FIELDS)
        if not rows:
            raise EnaAccessionNotFoundError(f"no ENA runs found for study {accession!r}")
        return [EnaRunRecord(**dict(zip(columns, row, strict=True))) for row in rows]

    def resolve_sample_attributes(self, accession: str) -> list[EnaSampleAttributes]:
        accession = validate_study_accession(accession)
        # This first check is a genuine existence check (mirrors
        # resolve_runs): zero samples for a well-formed study accession
        # means nothing resolved, and stays a hard raise.
        _, sample_rows = self._search(accession, "sample", "sample_accession")
        if not sample_rows:
            raise EnaAccessionNotFoundError(
                f"no ENA sample attributes found for study {accession!r}"
            )
        sample_accessions = [row[0] for row in sample_rows]

        url = f"{BROWSER_BASE}/xml/{quote(','.join(sample_accessions))}"
        response = self._http.get(url)
        response.raise_for_status()
        rows = _parse_sample_attributes_xml(response.text)
        if not rows:
            # Unlike the sample-existence check above, zero
            # <SAMPLE_ATTRIBUTE> rows across a known-real sample set is a
            # legitimate "no attributes" result, not "nonexistent" -- a
            # real ENA/DDBJ sample can carry no submitter-defined
            # attributes at all. Return no entries rather than raise;
            # registration.register_ena_study's attrs_by_sample_accession
            # lookup already treats a missing sample as an empty attribute
            # map.
            return []
        return pivot_sample_attributes(["sample_accession", "tag", "value"], rows)
