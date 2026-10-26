"""Unit tests for `ena_submission.catalog`.

Network-free and DuckDB-free: `connect_with_miint_staged` is replaced with a
recording connection, and the module-level `_open_ena_submission_session` is
replaced by fully-qualified name.
"""

from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from qiita_control_plane import miint as miint_module
from qiita_control_plane.ena_submission import catalog as catalog_module
from qiita_control_plane.ena_submission.catalog import (
    EnaAliasCheckBlockedError,
    EnaAlreadySubmittedError,
    EnaChecklistValidationError,
    EnaExistingAccession,
    EnaObjectKind,
    EnaObjectSpec,
    EnaSubmissionCatalog,
    EnaSubmissionError,
)
from qiita_control_plane.ena_submission.mapping import EnaProjectRow, EnaSampleRow

from .conftest import (
    _CHECKLIST,
    _CHECKLIST_VALIDATION_DETAIL,
    _ENDPOINT,
    _OPEN_SESSION,
    _WEBIN_USER,
    _already_exists_error,
    _project_row,
    _sample_row,
    _valid_kwargs,
)

# Statement prefixes a recording connection can be told to fail on.
_INSERT = "INSERT INTO "
_ATTACH = "ATTACH "
_DETACH = "DETACH "

_INVALID_ENDPOINTS = ["", "https://wwwdev.ebi.ac.uk/ena/submit/webin-v2/", "Production"]

# Every spec the module defines, paired with the model whose fields its columns
# bind from. A spec missing from here fails its own registration test.
_SPEC_ROW_MODELS = [
    (catalog_module._PROJECT_SPEC, EnaProjectRow),
    (catalog_module._SAMPLE_SPEC, EnaSampleRow),
]

# The statements the two submit paths are pinned to, written out rather than
# rebuilt from the specs so a change to the builder has to be restated here.
_PROJECT_INSERT_SQL = (
    "INSERT INTO ena.projects (alias, title, description, project_type)"
    " VALUES (?, ?, ?, ?)"
    " RETURNING prjeb_accession, erp_accession"
)
_TWO_SAMPLE_INSERT_SQL = (
    "INSERT INTO ena.samples (alias, taxon_id, checklist, attributes, attribute_units)"
    " VALUES (?, ?, ?, ?, ?), (?, ?, ?, ?, ?)"
    " RETURNING alias, ers_accession, samea_accession"
)

_RECEIPT_ALREADY_SUBMITTED = (
    f"INSERT INTO ena.projects failed: {_already_exists_error('project', 'QS7', 'PRJEB77')}"
)
_MIINT_ALIAS_CHECK_BLOCKED = (
    "INSERT INTO ena.samples: aliases already exist in submission account: 'QB11', 'QB12'"
)
_MIINT_CHECKLIST_VALIDATION = (
    f"INSERT INTO ena.samples: checklist validation failed:\n  {_CHECKLIST_VALIDATION_DETAIL}"
)


class _RecordingConn:
    """Stands in for a DuckDB connection, recording every statement executed
    and handing back a canned result set; a statement starting with a key of
    `failures` raises that key's error text instead."""

    def __init__(
        self, returned_rows: list[tuple] | None = None, *, failures: dict[str, str] | None = None
    ) -> None:
        self.statements: list[str] = []
        self.parameters: list[list | None] = []
        self.closed = False
        self._returned_rows = [] if returned_rows is None else returned_rows
        self._failures = failures or {}

    def execute(self, sql: str, params: list | None = None) -> _RecordingConn:
        self.statements.append(sql)
        self.parameters.append(params)
        for prefix, error in self._failures.items():
            if sql.startswith(prefix):
                raise duckdb.Error(error)
        return self

    def fetchall(self) -> list[tuple]:
        return self._returned_rows

    def close(self) -> None:
        self.closed = True


def _catalog_over(
    monkeypatch, password_file: Path, conn: _RecordingConn, *, dry_run: bool = False
) -> EnaSubmissionCatalog:
    """A catalog whose session is `conn`; enter it to submit."""
    monkeypatch.setattr(_OPEN_SESSION, lambda **kwargs: conn)
    return EnaSubmissionCatalog(**_valid_kwargs(password_file, dry_run=dry_run))


def _open(
    monkeypatch, password_file: Path, *, dry_run: bool = False, endpoint_url: str | None = None
) -> _RecordingConn:
    """Open a session against a recording connection and return the recorder."""
    conn = _RecordingConn()
    monkeypatch.setattr(catalog_module, "connect_with_miint_staged", lambda: conn)
    catalog_module._open_ena_submission_session(
        **_valid_kwargs(password_file, dry_run=dry_run, endpoint_url=endpoint_url)
    )
    return conn


def test__open_ena_submission_session_creates_secret(monkeypatch, password_file: Path):
    """Tests the case where a session is opened: the Webin secret is registered
    with the user, the password file, and the endpoint, as a TYPE ENA secret."""
    conn = _open(monkeypatch, password_file)
    secret = next(s for s in conn.statements if "CREATE SECRET" in s)
    assert catalog_module._SECRET_NAME in secret
    assert "TYPE ENA" in secret
    assert f"'{_WEBIN_USER}'" in secret
    assert f"'{password_file}'" in secret
    assert f"ENDPOINT '{_ENDPOINT}'" in secret


@pytest.mark.parametrize(
    ("endpoint_url", "url_option"),
    [
        (None, ""),
        ("http://127.0.0.1:8080", ", ENDPOINT_URL 'http://127.0.0.1:8080'"),
    ],
)
def test__open_ena_submission_session_attaches_catalog(
    monkeypatch, password_file: Path, endpoint_url: str | None, url_option: str
):
    """Tests the case where a session is opened without a URL and the case where
    it is opened with one: the catalog is attached at the endpoint, under the
    module's alias, bound to the registered secret, and points at the URL only
    when one is given."""
    conn = _open(monkeypatch, password_file, endpoint_url=endpoint_url)
    attach = next(s for s in conn.statements if s.startswith("ATTACH "))
    expected = (
        f"ATTACH '{_ENDPOINT}' AS {catalog_module._CATALOG_ALIAS}"
        f" (TYPE ENA, SECRET {catalog_module._SECRET_NAME}{url_option})"
    )
    assert attach == expected


@pytest.mark.parametrize(
    ("setting", "expected_value"),
    [
        ("threads", str(catalog_module._SESSION_THREADS)),
        ("memory_limit", catalog_module._SESSION_MEMORY_LIMIT),
    ],
)
def test__open_ena_submission_session_applies_resource_bounds(
    monkeypatch, password_file: Path, setting: str, expected_value: str
):
    """Tests the case where a session is opened: each resource bound is set to
    the module constant's value, and set before the catalog is attached."""
    conn = _open(monkeypatch, password_file)
    bound = next(s for s in conn.statements if s.startswith(f"SET {setting}"))
    assert expected_value in bound
    attach_at = next(i for i, s in enumerate(conn.statements) if s.startswith("ATTACH "))
    assert conn.statements.index(bound) < attach_at


@pytest.mark.parametrize("dry_run", [True, False])
def test__open_ena_submission_session_validate_only(
    monkeypatch, password_file: Path, dry_run: bool
):
    """Tests the case where dry-run is requested and the case where it is not:
    validate-only is set only under dry-run."""
    conn = _open(monkeypatch, password_file, dry_run=dry_run)
    emitted = any("miint_ena_validate_only" in s for s in conn.statements)
    assert emitted is dry_run


def test__open_ena_submission_session_setup_failure_closes(monkeypatch, password_file: Path):
    """Tests the case where a setup statement fails: the error propagates and
    the connection is closed rather than leaked."""
    conn = _RecordingConn(failures={_ATTACH: "attach refused"})
    monkeypatch.setattr(catalog_module, "connect_with_miint_staged", lambda: conn)

    with pytest.raises(duckdb.Error, match="attach refused"):
        catalog_module._open_ena_submission_session(**_valid_kwargs(password_file, dry_run=False))

    assert conn.closed is True


def test__open_ena_submission_session_emits_no_install(monkeypatch, password_file: Path):
    """Tests the case where a session is opened: no extension statement is
    issued, since the staged connect helper already loads miint and httpfs."""
    conn = _open(monkeypatch, password_file)
    for statement in conn.statements:
        assert "INSTALL" not in statement.upper(), statement
        assert "LOAD " not in statement.upper(), statement


@pytest.mark.parametrize("endpoint", _INVALID_ENDPOINTS)
def test__open_ena_submission_session_invalid_endpoint_raises(
    monkeypatch, password_file: Path, endpoint: str
):
    """Tests the case where the session is opened directly with an endpoint
    outside ENA's two environments: it fails naming the endpoint, and no
    connection is opened."""
    monkeypatch.setattr(
        catalog_module,
        "connect_with_miint_staged",
        lambda: pytest.fail("no connection may be opened"),
    )
    with pytest.raises(EnaSubmissionError, match="endpoint"):
        catalog_module._open_ena_submission_session(
            **_valid_kwargs(password_file, endpoint=endpoint, dry_run=False)
        )


@pytest.mark.parametrize(
    ("field", "overrides"),
    [
        ("webin_user", {"webin_user": ""}),
        ("webin_user", {"webin_user": "   "}),
        *[("endpoint", {"endpoint": endpoint}) for endpoint in _INVALID_ENDPOINTS],
        ("password", {"webin_password_file": Path("/nonexistent/webin_password.txt")}),
    ],
)
def test_EnaSubmissionCatalog_invalid_configuration_raises(
    monkeypatch, password_file: Path, field: str, overrides: dict
):
    """Tests the case where a required parameter is blank or absent: construction
    fails loud naming the field, and no session is opened."""
    monkeypatch.setattr(_OPEN_SESSION, lambda **kwargs: pytest.fail("no session may be opened"))
    with pytest.raises(EnaSubmissionError, match=field):
        EnaSubmissionCatalog(**_valid_kwargs(password_file, **overrides))


@pytest.mark.parametrize("body_raises", [False, True])
def test_EnaSubmissionCatalog_detaches_and_closes(
    monkeypatch, password_file: Path, body_raises: bool
):
    """Tests the case where the block exits cleanly and the case where it exits
    on an exception: the catalog is detached and the connection closed either
    way, and an exception from the body is not swallowed."""
    conn = _RecordingConn()
    monkeypatch.setattr(_OPEN_SESSION, lambda **kwargs: conn)
    catalog = EnaSubmissionCatalog(**_valid_kwargs(password_file))

    if body_raises:
        with pytest.raises(ValueError, match="from the body"), catalog:
            raise ValueError("from the body")
    else:
        with catalog:
            pass

    assert any(s.startswith("DETACH ") for s in conn.statements)
    assert conn.closed is True


@pytest.mark.parametrize(
    ("body_raises", "expected_error"),
    [(False, duckdb.Error), (True, ValueError)],
    ids=["clean body", "raising body"],
)
def test_EnaSubmissionCatalog_detach_failure(
    monkeypatch, password_file: Path, body_raises: bool, expected_error: type[Exception]
):
    """Tests the case where the detach fails after a clean block and the case
    where it fails after the block raised: the connection is closed either way,
    and a detach error surfaces only when no error was already leaving the block."""
    conn = _RecordingConn(failures={_DETACH: "detach refused"})
    monkeypatch.setattr(_OPEN_SESSION, lambda **kwargs: conn)
    catalog = EnaSubmissionCatalog(**_valid_kwargs(password_file))

    with pytest.raises(expected_error), catalog:
        if body_raises:
            raise ValueError("from the body")

    assert conn.closed is True


def test_EnaSubmissionCatalog_instances_are_independent(monkeypatch, password_file: Path):
    """Tests the case where two catalogs are open at once: each holds its own
    session, and closing one leaves the other's untouched."""
    first_conn, second_conn = _RecordingConn(), _RecordingConn()
    handed = iter((first_conn, second_conn))
    monkeypatch.setattr(_OPEN_SESSION, lambda **kwargs: next(handed))
    first = EnaSubmissionCatalog(**_valid_kwargs(password_file))
    second = EnaSubmissionCatalog(**_valid_kwargs(password_file))
    with first:
        with second:
            assert first._con is first_conn
            assert second._con is second_conn
        # The inner block's teardown must not have touched the outer session.
        assert second_conn.closed is True
        assert first_conn.closed is False
    assert first_conn.closed is True


def test_catalog_uses_staged_connect():
    """Tests the case where the module's connect helper is inspected: it is the
    LOAD-only staged form, with the INSTALL-based client helper absent."""
    assert catalog_module.connect_with_miint_staged is miint_module.connect_with_miint_staged
    assert not hasattr(catalog_module, "connect_with_miint")


@pytest.mark.parametrize(("spec", "row_model"), _SPEC_ROW_MODELS)
def test_EnaObjectSpec_columns_bind_from_model_fields(spec: EnaObjectSpec, row_model: type):
    """Tests the case where a spec's columns are checked against its row model:
    the two name exactly the same set, so every column has a field to bind from
    and a field added without its column cannot go silently unsent."""
    assert set(spec.columns) == set(row_model.model_fields)


def test_EnaObjectSpec_every_spec_is_registered():
    """Tests the case where a spec is added to the module but not paired with a
    row model: the omission fails here rather than leaving the new object
    silently unchecked."""
    defined_specs = {
        value for value in vars(catalog_module).values() if isinstance(value, EnaObjectSpec)
    }
    registered_specs = {spec for spec, _ in _SPEC_ROW_MODELS}
    assert defined_specs == registered_specs


def test_EnaObjectSpec_returning_batch_leads_with_alias():
    """Tests the case where a batch insert's RETURNING list is built: the alias
    leads, ahead of the single-row form's columns unchanged."""
    spec = EnaObjectSpec(
        object_kind=EnaObjectKind.SAMPLE,
        table="ena.widgets",
        columns=("alias", "size"),
        returning=("first_accession", "second_accession"),
    )
    assert spec.returning_batch == ("alias", "first_accession", "second_accession")


def test_submit_project(monkeypatch, password_file: Path):
    """Tests the case where a project is submitted: one INSERT carries the
    spec's columns with a placeholder each, the row's values bind in column
    order, and ENA's assigned accessions come back as the spec's tuple."""
    conn = _RecordingConn([("PRJEB77", "ERP77")])
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        assigned = catalog.submit_project(_project_row())
        # Read before the block exits, whose teardown statement would be last.
        assert conn.statements[-1] == _PROJECT_INSERT_SQL
        assert conn.parameters[-1] == ["QS7", "A study", None, "other"]
    assert assigned == ("PRJEB77", "ERP77")


def test_submit_project_dry_run_returns_none(monkeypatch, password_file: Path):
    """Tests the case where a validate-only session returns no row: the project
    submit reports no accession rather than failing on an empty result."""
    conn = _RecordingConn([])
    with _catalog_over(monkeypatch, password_file, conn, dry_run=True) as catalog:
        assigned = catalog.submit_project(_project_row())
    assert assigned is None


def test_submit_project_no_result_raises(monkeypatch, password_file: Path):
    """Tests the case where a session that is not validate-only returns no row:
    the submit fails rather than reporting an accepted project as unassigned."""
    conn = _RecordingConn([])
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaSubmissionError, match="QS7"):
            catalog.submit_project(_project_row())


def test_submit_samples(monkeypatch, password_file: Path):
    """Tests the case where several samples are submitted: a single INSERT
    carries one placeholder group per row, every row's values bind in column
    order, and the batch RETURNING asks for the alias alongside."""
    conn = _RecordingConn([("QB11", "ERS11", "SAMEA11"), ("QB12", "ERS12", "SAMEA12")])
    rows = [_sample_row("QB11"), _sample_row("QB12")]
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        catalog.submit_samples(rows)
        # Read before the block exits, whose teardown statement would be last.
        assert conn.statements[-1] == _TWO_SAMPLE_INSERT_SQL
        assert conn.parameters[-1] == [
            "QB11",
            408170,
            _CHECKLIST,
            {"collection date": "2026-01-15"},
            {},
            "QB12",
            408170,
            _CHECKLIST,
            {"collection date": "2026-01-15"},
            {},
        ]


def test_submit_samples_maps_results_by_alias(monkeypatch, password_file: Path):
    """Tests the case where the returned rows arrive in an order other than the
    one submitted: each row's accessions are matched to it by alias."""
    conn = _RecordingConn([("QB12", "ERS12", "SAMEA12"), ("QB11", "ERS11", "SAMEA11")])
    rows = [_sample_row("QB11"), _sample_row("QB12")]
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        assigned = catalog.submit_samples(rows)
    assert assigned == [("ERS11", "SAMEA11"), ("ERS12", "SAMEA12")]


def test_submit_samples_missing_alias_raises(monkeypatch, password_file: Path):
    """Tests the case where a session that is not validate-only returns no row
    for one submitted alias: the submit fails naming it, rather than reporting
    an accepted sample as unassigned."""
    conn = _RecordingConn([("QB11", "ERS11", "SAMEA11")])
    rows = [_sample_row("QB11"), _sample_row("QB13")]
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaSubmissionError, match="QB13"):
            catalog.submit_samples(rows)


def test_submit_samples_dry_run_returns_none_per_row(monkeypatch, password_file: Path):
    """Tests the case where a validate-only session returns no rows: every
    submitted sample reports no accession, one entry per input row."""
    conn = _RecordingConn([])
    rows = [_sample_row("QB11"), _sample_row("QB12")]
    with _catalog_over(monkeypatch, password_file, conn, dry_run=True) as catalog:
        assigned = catalog.submit_samples(rows)
    assert assigned == [None, None]


def test_submit_samples_empty_submits_nothing(monkeypatch, password_file: Path):
    """Tests the case where an empty list is submitted: no statement is sent,
    since an INSERT with no VALUES group is not valid SQL."""
    conn = _RecordingConn([])
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        before = list(conn.statements)
        assigned = catalog.submit_samples([])
        assert conn.statements == before
    assert assigned == []


def test_submit_project_outside_the_block_raises(monkeypatch, password_file: Path):
    """Tests the case where a submit is attempted on an unentered catalog: it
    fails loud rather than opening a session of its own."""
    conn = _RecordingConn([])
    catalog = _catalog_over(monkeypatch, password_file, conn)
    with pytest.raises(EnaSubmissionError, match="not open"):
        catalog.submit_project(_project_row())


def test__insert_returning_rows_already_submitted(monkeypatch, password_file: Path):
    """Tests the case where ENA's receipt reports the alias already deposited:
    the alias and the accession it names ride together on a distinguishable
    error, so a caller can record it rather than treat the submission as lost."""
    conn = _RecordingConn(failures={_INSERT: _RECEIPT_ALREADY_SUBMITTED})
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaAlreadySubmittedError) as raised:
            catalog.submit_project(_project_row())
    assert (raised.value.object_kind, raised.value.existing) == (
        EnaObjectKind.PROJECT,
        (EnaExistingAccession("QS7", "PRJEB77"),),
    )


def test__insert_returning_rows_alias_check_blocked(monkeypatch, password_file: Path):
    """Tests the case where miint's own pre-POST alias check refuses the insert:
    every colliding alias is carried, and the error says no accession is
    available from this path."""
    conn = _RecordingConn(failures={_INSERT: _MIINT_ALIAS_CHECK_BLOCKED})
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaAliasCheckBlockedError) as raised:
            catalog.submit_samples([_sample_row("QB11"), _sample_row("QB12")])
    assert (raised.value.object_kind, raised.value.aliases) == (
        EnaObjectKind.SAMPLE,
        ("QB11", "QB12"),
    )
    assert "no accession" in str(raised.value)


def test__insert_returning_rows_checklist_validation(monkeypatch, password_file: Path):
    """Tests the case where miint's checklist validator refuses the insert: the
    per-attribute detail is carried on its own error, since it means incomplete
    data rather than an object already deposited."""
    conn = _RecordingConn(failures={_INSERT: _MIINT_CHECKLIST_VALIDATION})
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaChecklistValidationError) as raised:
            catalog.submit_samples([_sample_row("QB11")])
    assert (raised.value.object_kind, raised.value.detail) == (
        EnaObjectKind.SAMPLE,
        _CHECKLIST_VALIDATION_DETAIL,
    )


def test__insert_returning_rows_unclassified_failure(monkeypatch, password_file: Path):
    """Tests the case where a failure matches none of the three known shapes:
    the base error carries the exception's text, so an unrecognized rejection
    is reported as itself rather than reshaped into one of them."""
    suspended = "INSERT INTO ena.samples failed: Webin account is suspended"
    conn = _RecordingConn(failures={_INSERT: suspended})
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaSubmissionError, match="Webin account is suspended") as raised:
            catalog.submit_samples([_sample_row("QB11")])
    assert type(raised.value) is EnaSubmissionError


def test__insert_returning_rows_already_submitted_names_every_collision(
    monkeypatch, password_file: Path
):
    """Tests the case where one envelope collides on several of its rows: every
    alias ENA's receipt names is carried, since a caller recording only the
    first would lose the rest with nothing saying so."""
    # miint joins ENA's messages with "; "
    rejections = [
        _already_exists_error("sample", "QB11", "ERS11"),
        _already_exists_error("sample", "QB12", "ERS12"),
    ]
    receipt = f"INSERT INTO ena.samples failed: {'; '.join(rejections)}"
    conn = _RecordingConn(failures={_INSERT: receipt})
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaAlreadySubmittedError) as raised:
            catalog.submit_samples([_sample_row("QB11"), _sample_row("QB12")])
    assert raised.value.existing == (
        EnaExistingAccession("QB11", "ERS11"),
        EnaExistingAccession("QB12", "ERS12"),
    )


def test__classify_submission_failure_accession_without_its_alias():
    """Tests the case where a receipt names an accession but not the alias it
    belongs to: the accession is still carried, marked as unattributable rather
    than dropped or paired with a guess."""
    message = 'INSERT INTO ena.projects failed: The object being added already exists in the submission account with accession: "PRJEB77".'  # noqa: E501
    classified = catalog_module._classify_submission_failure(catalog_module._PROJECT_SPEC, message)
    assert isinstance(classified, EnaAlreadySubmittedError)
    assert classified.existing == (EnaExistingAccession(None, "PRJEB77"),)


def test__classify_submission_failure_alias_check_without_quoted_aliases():
    """Tests the case where the alias-check sentence matches but names nothing
    extractable: the base error reports the disagreement instead of an
    alias-check error carrying the empty payload it exists to deliver."""
    message = "INSERT INTO ena.samples: aliases already exist in submission account: QB11, QB12"
    classified = catalog_module._classify_submission_failure(catalog_module._SAMPLE_SPEC, message)
    assert type(classified) is EnaSubmissionError
    assert "no alias could be read" in str(classified)


def test__bind_values_orders_to_spec_columns():
    """Tests the case where a spec's column order differs from its model's field
    order: values are selected by name, so each lands under its own column
    rather than following the model's declaration order."""
    spec = EnaObjectSpec(
        object_kind=EnaObjectKind.PROJECT,
        table="ena.projects",
        columns=("project_type", "alias"),
        returning=("prjeb_accession",),
    )
    values = catalog_module._bind_values(spec, _project_row())
    assert values == ("other", "QS7")


def test_submit_samples_outside_the_block_raises(monkeypatch, password_file: Path):
    """Tests the case where an empty list is submitted on an unentered catalog:
    it fails loud like its non-empty and single-row siblings, rather than
    reporting success for a session that was never opened."""
    conn = _RecordingConn([])
    catalog = _catalog_over(monkeypatch, password_file, conn)
    with pytest.raises(EnaSubmissionError, match="not open"):
        catalog.submit_samples([])


def test_submit_samples_duplicate_alias_raises(monkeypatch, password_file: Path):
    """Tests the case where two rows share an alias: the submission is refused
    before any statement is sent, since results are keyed by alias and the two
    would otherwise both be handed the same accessions."""
    conn = _RecordingConn([])
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        before = list(conn.statements)
        with pytest.raises(EnaSubmissionError, match="distinct alias"):
            catalog.submit_samples([_sample_row("QB11"), _sample_row("QB11")])
        assert conn.statements == before


@pytest.mark.parametrize("blank", ["", "   ", "\t\n"])
@pytest.mark.parametrize("field", ["alias", "checklist"])
def test_EnaSampleRow_blank_text_is_refused(field: str, blank: str):
    """Tests the case where a sample's alias or checklist carries no content: it
    is refused at construction."""
    overrides = {"alias": "QB11", "checklist": _CHECKLIST, field: blank}
    with pytest.raises(ValidationError):
        EnaSampleRow(taxon_id=408170, **overrides)


@pytest.mark.parametrize("blank", ["", "   "])
def test_EnaProjectRow_blank_alias_is_refused(blank: str):
    """Tests the case where a project's alias carries no content: it is refused
    at construction rather than reaching miint, whose own guard passes
    whitespace through."""
    with pytest.raises(ValidationError):
        EnaProjectRow(alias=blank, title="A study", project_type="other")


@pytest.mark.parametrize(
    ("model", "overrides"),
    [
        (EnaSampleRow, {"taxon_id": 408170, "checklist": _CHECKLIST}),
        (EnaProjectRow, {}),
    ],
    ids=["sample", "project"],
)
def test_ena_row_alias_is_stripped(model: type, overrides: dict):
    """Tests the case where an alias arrives with stray padding: it is stored
    stripped, so one alias written two ways keys to the same result."""
    row = model(alias="  QB11  ", **overrides)
    assert row.alias == "QB11"
