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

_OPEN_SESSION = "qiita_control_plane.ena_submission.catalog._open_ena_submission_session"

_WEBIN_USER = "Webin-00000"
# miint's ENA endpoint is an environment selector, not a URL.
_ENDPOINT = "test"

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

_CHECKLIST = "ERC000011"

# The three failure wordings, quoted from their sources: ENA's own receipt, and
# miint's two client-side refusals (`ena_object_insert_op.hpp` for the alias
# check, `ena_samples_insert_op.cpp` for the checklist validator).
_RECEIPT_ALREADY_SUBMITTED = (
    'ENA receipt: alias "7" already exists in the submission account with accession: "PRJEB77"'
)
_MIINT_ALIAS_CHECK_BLOCKED = (
    "INSERT INTO ena.samples: aliases already exist in submission account: '11', '12'"
)
_CHECKLIST_VALIDATION_DETAIL = "sample alias '11': mandatory attribute 'collection date' is missing"
_MIINT_CHECKLIST_VALIDATION = (
    f"INSERT INTO ena.samples: checklist validation failed:\n  {_CHECKLIST_VALIDATION_DETAIL}"
)

# A failure the insert reports directly, with nothing in it to classify.
_OPAQUE_INSERT_FAILURE = "INSERT INTO ena.samples: submission failed"


class _RecordingConn:
    """Stands in for a DuckDB connection, recording every statement executed
    and handing back a canned result set."""

    def __init__(self, returned_rows: list[tuple] | None = None) -> None:
        self.statements: list[str] = []
        self.parameters: list[list | None] = []
        self.closed = False
        self._returned_rows = [] if returned_rows is None else returned_rows

    def execute(self, sql: str, params: list | None = None) -> _RecordingConn:
        self.statements.append(sql)
        self.parameters.append(params)
        return self

    def fetchall(self) -> list[tuple]:
        return self._returned_rows

    def fetchone(self) -> tuple | None:
        """An empty submission log — this connection's statements all succeed,
        so the high-water read ahead of each insert finds nothing."""
        return (None,)

    def close(self) -> None:
        self.closed = True


class _SubmissionLogConn:
    """Stands in for a DuckDB connection whose INSERT fails, answering the
    submission-log reads that follow out of a canned list of failed rows.

    `failed_rows` are (submitted_at, error_messages) pairs already in the log
    when the block opens; `logged_on_failure` is the row the failing INSERT
    writes, which the two client-side refusals do not write at all.
    `error_messages` is a list of messages, matching the column's own type, so a
    failed row with nothing to say carries an empty list rather than NULL.
    """

    def __init__(
        self,
        insert_error: str,
        *,
        failed_rows: list[tuple[int, list[str]]] | None = None,
        logged_on_failure: tuple[int, list[str]] | None = None,
        log_readable: bool = True,
    ) -> None:
        self._insert_error = insert_error
        self._failed_rows = list(failed_rows or [])
        self._logged_on_failure = logged_on_failure
        self._log_readable = log_readable
        self._pending_row: tuple | None = None
        self.statements: list[str] = []
        self.closed = False

    def execute(self, sql: str, params: list | None = None) -> _SubmissionLogConn:
        self.statements.append(sql)
        if sql.startswith("INSERT INTO "):
            if self._logged_on_failure is not None:
                self._failed_rows.append(self._logged_on_failure)
            raise duckdb.Error(self._insert_error)
        if not sql.startswith("SELECT "):
            return self
        if not self._log_readable:
            raise duckdb.Error("ena.submission_log is unavailable")
        self._pending_row = self._read_log(sql, params)
        return self

    def _read_log(self, sql: str, params: list | None) -> tuple | None:
        """The row the high-water read or the bounded error read comes back
        with, honouring the `submitted_at > ?` bound when one is bound."""
        if "max(submitted_at)" in sql:
            return (max((at for at, _ in self._failed_rows), default=None),)
        after = params[0] if params else None
        newer = [row for row in self._failed_rows if after is None or row[0] > after]
        newest = max(newer, default=None)
        return None if newest is None else (newest[1],)

    def fetchone(self) -> tuple | None:
        return self._pending_row

    def close(self) -> None:
        self.closed = True


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


def _catalog_over(monkeypatch, password_file: Path, conn: _RecordingConn) -> EnaSubmissionCatalog:
    """A catalog whose session is `conn`; enter it to submit."""
    monkeypatch.setattr(_OPEN_SESSION, lambda **kwargs: conn)
    return EnaSubmissionCatalog(**_valid_kwargs(password_file))


def _project_row() -> EnaProjectRow:
    """One project body, with the unpopulated columns left at their default."""
    return EnaProjectRow(alias="7", title="A study", project_type="other")


def _sample_row(alias: str) -> EnaSampleRow:
    """One sample body carrying a single checklist attribute and no units."""
    return EnaSampleRow(
        alias=alias,
        taxon_id=408170,
        checklist=_CHECKLIST,
        attributes={"collection date": "2026-01-15"},
    )


def _open(monkeypatch, password_file: Path, *, dry_run: bool = False) -> _RecordingConn:
    """Open a session against a recording connection and return the recorder."""
    conn = _RecordingConn()
    monkeypatch.setattr(catalog_module, "connect_with_miint_staged", lambda: conn)
    catalog_module._open_ena_submission_session(**_valid_kwargs(password_file, dry_run=dry_run))
    return conn


def test__open_ena_submission_session_creates_secret(monkeypatch, password_file: Path):
    """Tests the case where a session is opened: the Webin secret is registered
    with the user, the password file, and the endpoint, as a TYPE ENA secret.

    The endpoint rides on the secret as well as the attach because the secret is
    the only side miint validates it on.
    """
    conn = _open(monkeypatch, password_file)
    secret = next(s for s in conn.statements if "CREATE SECRET" in s)
    assert catalog_module._SECRET_NAME in secret
    assert "TYPE ENA" in secret
    assert f"'{_WEBIN_USER}'" in secret
    assert f"'{password_file}'" in secret
    assert f"ENDPOINT '{_ENDPOINT}'" in secret


def test__open_ena_submission_session_attaches_catalog(monkeypatch, password_file: Path):
    """Tests the case where a session is opened: the catalog is attached at the
    endpoint, under the module's alias, bound to the registered secret."""
    conn = _open(monkeypatch, password_file)
    attach = next(s for s in conn.statements if s.startswith("ATTACH "))
    assert f"'{_ENDPOINT}'" in attach
    assert f"AS {catalog_module._CATALOG_ALIAS}" in attach
    assert "TYPE ENA" in attach
    assert catalog_module._SECRET_NAME in attach


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


def test__open_ena_submission_session_emits_no_install(monkeypatch, password_file: Path):
    """Tests the case where a session is opened: no extension statement is
    issued, since the staged connect helper already loads miint and httpfs."""
    conn = _open(monkeypatch, password_file)
    for statement in conn.statements:
        assert "INSTALL" not in statement.upper(), statement
        assert "LOAD " not in statement.upper(), statement


@pytest.mark.parametrize(
    ("field", "overrides"),
    [
        ("webin_user", {"webin_user": ""}),
        ("webin_user", {"webin_user": "   "}),
        ("endpoint", {"endpoint": ""}),
        # ATTACH accepts any string, so a wrong endpoint has to fail here.
        ("endpoint", {"endpoint": "https://wwwdev.ebi.ac.uk/ena/submit/webin-v2/"}),
        ("endpoint", {"endpoint": "Production"}),
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
        assert conn.parameters[-1] == ["7", "A study", None, "other"]
    assert assigned == ("PRJEB77", "ERP77")


def test_submit_project_dry_run_returns_none(monkeypatch, password_file: Path):
    """Tests the case where a validate-only session returns no row: the project
    submit reports no accession rather than failing on an empty result."""
    conn = _RecordingConn([])
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        assigned = catalog.submit_project(_project_row())
    assert assigned is None


def test_submit_samples(monkeypatch, password_file: Path):
    """Tests the case where several samples are submitted: a single INSERT
    carries one placeholder group per row, every row's values bind in column
    order, and the batch RETURNING asks for the alias alongside."""
    conn = _RecordingConn([("11", "ERS11", "SAMEA11"), ("12", "ERS12", "SAMEA12")])
    rows = [_sample_row("11"), _sample_row("12")]
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        catalog.submit_samples(rows)
        # Read before the block exits, whose teardown statement would be last.
        assert conn.statements[-1] == _TWO_SAMPLE_INSERT_SQL
        assert conn.parameters[-1] == [
            "11",
            408170,
            _CHECKLIST,
            {"collection date": "2026-01-15"},
            {},
            "12",
            408170,
            _CHECKLIST,
            {"collection date": "2026-01-15"},
            {},
        ]


def test_submit_samples_maps_results_by_alias(monkeypatch, password_file: Path):
    """Tests the case where the returned rows arrive in an order other than the
    one submitted: each row's accessions are matched to it by alias, and a row
    no result names reports none."""
    conn = _RecordingConn([("12", "ERS12", "SAMEA12"), ("11", "ERS11", "SAMEA11")])
    rows = [_sample_row("11"), _sample_row("12"), _sample_row("13")]
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        assigned = catalog.submit_samples(rows)
    assert assigned == [("ERS11", "SAMEA11"), ("ERS12", "SAMEA12"), None]


def test_submit_samples_dry_run_returns_none_per_row(monkeypatch, password_file: Path):
    """Tests the case where a validate-only session returns no rows: every
    submitted sample reports no accession, one entry per input row."""
    conn = _RecordingConn([])
    rows = [_sample_row("11"), _sample_row("12")]
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
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
    conn = _SubmissionLogConn(
        _OPAQUE_INSERT_FAILURE, logged_on_failure=(2, [_RECEIPT_ALREADY_SUBMITTED])
    )
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaAlreadySubmittedError) as raised:
            catalog.submit_project(_project_row())
    assert (raised.value.object_kind, raised.value.existing) == (
        EnaObjectKind.PROJECT,
        (EnaExistingAccession("7", "PRJEB77"),),
    )


def test__insert_returning_rows_alias_check_blocked(monkeypatch, password_file: Path):
    """Tests the case where miint's own pre-POST alias check refuses the insert:
    every colliding alias is carried, and the error says no accession is
    available from this path."""
    conn = _SubmissionLogConn(_MIINT_ALIAS_CHECK_BLOCKED)
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaAliasCheckBlockedError) as raised:
            catalog.submit_samples([_sample_row("11"), _sample_row("12")])
    assert (raised.value.object_kind, raised.value.aliases) == (
        EnaObjectKind.SAMPLE,
        ("11", "12"),
    )
    assert "no accession" in str(raised.value)


def test__insert_returning_rows_checklist_validation(monkeypatch, password_file: Path):
    """Tests the case where miint's checklist validator refuses the insert: the
    per-attribute detail is carried on its own error, since it means incomplete
    data rather than an object already deposited."""
    conn = _SubmissionLogConn(_MIINT_CHECKLIST_VALIDATION)
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaChecklistValidationError) as raised:
            catalog.submit_samples([_sample_row("11")])
    assert (raised.value.object_kind, raised.value.detail) == (
        EnaObjectKind.SAMPLE,
        _CHECKLIST_VALIDATION_DETAIL,
    )


def test__insert_returning_rows_unclassified_failure(monkeypatch, password_file: Path):
    """Tests the case where a failure matches none of the three known shapes:
    the base error carries the log's detail, so an unrecognized rejection is
    reported as itself rather than reshaped into one of them."""
    conn = _SubmissionLogConn(
        _OPAQUE_INSERT_FAILURE,
        logged_on_failure=(2, ["ENA receipt: Webin account is suspended"]),
    )
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaSubmissionError, match="Webin account is suspended") as raised:
            catalog.submit_samples([_sample_row("11")])
    assert type(raised.value) is EnaSubmissionError


@pytest.mark.parametrize(
    ("logged_on_failure", "expected_detail"),
    [
        (None, _OPAQUE_INSERT_FAILURE),
        ((2, ["ENA receipt: Webin account is suspended"]), "Webin account is suspended"),
    ],
    ids=["older row rejected", "newer row accepted"],
)
def test__last_submission_error_bounded_by_high_water(
    monkeypatch, password_file: Path, logged_on_failure, expected_detail: str
):
    """Tests the case where the log already holds an older failed row: only a
    row written after this insert began enriches the error, since a pooled,
    long-lived session's newest failed row may belong to an earlier request."""
    conn = _SubmissionLogConn(
        _OPAQUE_INSERT_FAILURE,
        failed_rows=[(1, ["an earlier request's rejection"])],
        logged_on_failure=logged_on_failure,
    )
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaSubmissionError) as raised:
            catalog.submit_samples([_sample_row("11")])
    assert expected_detail in str(raised.value)
    assert "an earlier request" not in str(raised.value)


def test__last_submission_error_unreadable_log_does_not_mask(monkeypatch, password_file: Path):
    """Tests the case where the submission log cannot be read at all: the
    insert's own error still reaches the caller, unenriched rather than
    replaced by the log's."""
    conn = _SubmissionLogConn(_OPAQUE_INSERT_FAILURE, log_readable=False)
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaSubmissionError, match=_OPAQUE_INSERT_FAILURE) as raised:
            catalog.submit_samples([_sample_row("11")])
    assert type(raised.value) is EnaSubmissionError


def test__insert_returning_rows_already_submitted_names_every_collision(
    monkeypatch, password_file: Path
):
    """Tests the case where one envelope collides on several of its rows: every
    alias ENA's receipt names is carried, since a caller recording only the
    first would lose the rest with nothing saying so."""
    receipt = (
        'ENA receipt: alias "11" already exists in the submission account with'
        ' accession: "ERS11"; alias "12" already exists in the submission account'
        ' with accession: "ERS12"'
    )
    conn = _SubmissionLogConn(_OPAQUE_INSERT_FAILURE, logged_on_failure=(2, [receipt]))
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaAlreadySubmittedError) as raised:
            catalog.submit_samples([_sample_row("11"), _sample_row("12")])
    assert raised.value.existing == (
        EnaExistingAccession("11", "ERS11"),
        EnaExistingAccession("12", "ERS12"),
    )


def test__classify_submission_failure_accession_without_its_alias():
    """Tests the case where a receipt names an accession but not the alias it
    belongs to: the accession is still carried, marked as unattributable rather
    than dropped or paired with a guess."""
    message = 'ENA receipt: already exists in the submission account with accession: "PRJEB77"'
    classified = catalog_module._classify_submission_failure(catalog_module._PROJECT_SPEC, message)
    assert isinstance(classified, EnaAlreadySubmittedError)
    assert classified.existing == (EnaExistingAccession(None, "PRJEB77"),)


def test__classify_submission_failure_alias_check_without_quoted_aliases():
    """Tests the case where the alias-check sentence matches but names nothing
    extractable: the base error reports the disagreement instead of an
    alias-check error carrying the empty payload it exists to deliver."""
    message = "INSERT INTO ena.samples: aliases already exist in submission account: 11, 12"
    classified = catalog_module._classify_submission_failure(catalog_module._SAMPLE_SPEC, message)
    assert type(classified) is EnaSubmissionError
    assert "no alias could be read" in str(classified)


def test__last_submission_error_empty_message_list_does_not_mask(monkeypatch, password_file: Path):
    """Tests the case where the failed row carries no messages: the insert's own
    error reaches the caller, since an empty list is no detail rather than
    detail that happens to render as one."""
    conn = _SubmissionLogConn(_OPAQUE_INSERT_FAILURE, logged_on_failure=(2, []))
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaSubmissionError, match=_OPAQUE_INSERT_FAILURE) as raised:
            catalog.submit_samples([_sample_row("11")])
    assert "[]" not in str(raised.value)


def test__last_submission_error_joins_every_logged_message(monkeypatch, password_file: Path):
    """Tests the case where the failed row carries several messages: all of them
    reach the caller, joined, rather than arriving as a rendered list."""
    conn = _SubmissionLogConn(
        _OPAQUE_INSERT_FAILURE,
        logged_on_failure=(2, ["first complaint", "second complaint"]),
    )
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaSubmissionError) as raised:
            catalog.submit_samples([_sample_row("11")])
    assert "first complaint; second complaint" in str(raised.value)
    assert "['" not in str(raised.value)


@pytest.mark.parametrize(
    ("failed_rows", "expected_bound"),
    [
        ([], ""),
        ([(1, ["an earlier request's rejection"])], " AND submitted_at > ?"),
    ],
    ids=["log empty when the insert began", "log already held a failed row"],
)
def test__last_submission_error_bounded_read_is_filtered_and_ordered(
    monkeypatch, password_file: Path, failed_rows, expected_bound: str
):
    """Tests the case where the enrichment read is issued: it selects only
    failed rows and only the newest one, bounded to rows newer than the
    pre-insert mark whenever the log held anything to mark.

    These clauses are answered by the fake rather than executed by it, so the
    statement is asserted directly; nothing else would notice them changing.
    """
    conn = _SubmissionLogConn(
        _OPAQUE_INSERT_FAILURE, failed_rows=failed_rows, logged_on_failure=(2, ["anything"])
    )
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaSubmissionError):
            catalog.submit_samples([_sample_row("11")])
    bounded_read = next(sql for sql in conn.statements if sql.startswith("SELECT error_messages"))
    assert bounded_read == (
        "SELECT error_messages FROM ena.submission_log WHERE NOT success"
        f"{expected_bound} ORDER BY submitted_at DESC LIMIT 1"
    )


def test__insert_returning_rows_checklist_detail_excludes_the_exception(
    monkeypatch, password_file: Path
):
    """Tests the case where the log carries the checklist wording and the raised
    error does not: the detail stops at the log's own text, since a caller fixes
    metadata from it rather than re-parsing a message."""
    conn = _SubmissionLogConn(
        _OPAQUE_INSERT_FAILURE,
        logged_on_failure=(2, [f"checklist validation failed:\n  {_CHECKLIST_VALIDATION_DETAIL}"]),
    )
    with _catalog_over(monkeypatch, password_file, conn) as catalog:
        with pytest.raises(EnaChecklistValidationError) as raised:
            catalog.submit_samples([_sample_row("11")])
    assert raised.value.detail == _CHECKLIST_VALIDATION_DETAIL


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
    assert values == ("other", "7")


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
            catalog.submit_samples([_sample_row("11"), _sample_row("11")])
        assert conn.statements == before


@pytest.mark.parametrize("blank", ["", "   ", "\t\n"])
@pytest.mark.parametrize("field", ["alias", "checklist"])
def test_EnaSampleRow_blank_text_is_refused(field: str, blank: str):
    """Tests the case where a sample's alias or checklist carries no content: it
    is refused at construction, since an empty checklist turns off miint's own
    validation and an empty alias is the key results are matched on."""
    overrides = {"alias": "11", "checklist": _CHECKLIST, field: blank}
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
    row = model(alias="  11  ", **overrides)
    assert row.alias == "11"
