"""The miint ENA (Webin V2) submission catalog — this package's one I/O boundary.

Opens a DuckDB session with a Webin secret registered and the fixed-schema `ena`
catalog attached, and tears both down again. Every statement this boundary needs
lives here; it touches no Qiita database and knows nothing of Qiita rows.
"""

from __future__ import annotations

from pathlib import Path
from types import TracebackType

import duckdb
from qiita_common.sql import sql_string_literal

from qiita_control_plane.miint import connect_with_miint_staged

# Fixed names, never caller input, so they interpolate into SQL unescaped.
_SECRET_NAME = "qiita_ena_submission"
_CATALOG_ALIAS = "ena"

# miint's ENA endpoint is an environment selector, not a URL. Both CREATE SECRET
# and ATTACH reject anything outside this set; checking it here fails before a
# connection is opened and raises this module's own error type rather than a
# DuckDB binder error.
ENA_ENDPOINTS = frozenset({"test", "production"})

# Per-session DuckDB bounds. A connection left at DuckDB's defaults takes every
# core and roughly 80% of system RAM, which inside the process that also serves
# the REST API is a ceiling worth naming rather than inheriting. These are
# conservative bounds, not measurements: a session carries one project row and
# some per-sample attribute maps, so it is sized for metadata rather than for
# anything that streams sequence data, and several sessions may run at once.
_SESSION_THREADS = 2
_SESSION_MEMORY_LIMIT = "1GB"


class EnaSubmissionError(RuntimeError):
    """A submission session could not be configured or opened."""


def _open_ena_submission_session(
    *,
    webin_user: str,
    webin_password_file: Path,
    endpoint: str,
    dry_run: bool,
) -> duckdb.DuckDBPyConnection:
    """Open a session with the Webin secret registered and the catalog attached.

    Under `dry_run` every insert on the returned session is validated against
    ENA instead of submitted. The caller owns the connection and must close it.
    """
    con = connect_with_miint_staged()
    try:
        con.execute(f"SET threads = {_SESSION_THREADS}")
        con.execute(f"SET memory_limit = {sql_string_literal(_SESSION_MEMORY_LIMIT)}")
        con.execute(
            f"CREATE SECRET {_SECRET_NAME} ("
            " TYPE ENA,"
            f" USER {sql_string_literal(webin_user)},"
            f" PASSWORD_FILE {sql_string_literal(webin_password_file)},"
            f" ENDPOINT {sql_string_literal(endpoint)})"
        )
        con.execute(
            f"ATTACH {sql_string_literal(endpoint)} AS {_CATALOG_ALIAS}"
            f" (TYPE ENA, SECRET {_SECRET_NAME})"
        )
        if dry_run:
            con.execute("SET miint_ena_validate_only = true")
    except Exception:
        con.close()
        raise
    return con


class EnaSubmissionCatalog:
    """One prepared ENA submission session, held for the life of a `with` block.

    A DuckDB connection cannot be used from more than one thread, so a caller
    submitting in parallel gives each worker its own catalog; instances share no
    state and may be opened concurrently. Parameters are validated before any
    connection is opened.
    """

    def __init__(
        self,
        *,
        webin_user: str,
        webin_password_file: Path,
        endpoint: str,
        dry_run: bool = False,
    ) -> None:
        if not webin_user.strip():
            raise EnaSubmissionError("webin_user is blank; cannot register a Webin secret")
        if endpoint not in ENA_ENDPOINTS:
            raise EnaSubmissionError(
                f"endpoint must be one of {sorted(ENA_ENDPOINTS)}; got {endpoint!r}"
            )
        if not webin_password_file.is_file():
            raise EnaSubmissionError(f"webin password file not found: {webin_password_file}")
        self._webin_user = webin_user
        self._webin_password_file = webin_password_file
        self._endpoint = endpoint
        self._dry_run = dry_run
        self._con: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> EnaSubmissionCatalog:
        self._con = _open_ena_submission_session(
            webin_user=self._webin_user,
            webin_password_file=self._webin_password_file,
            endpoint=self._endpoint,
            dry_run=self._dry_run,
        )
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        con = self._con
        self._con = None
        if con is None:
            return
        # Close even if the detach fails, or a failed teardown leaks the connection.
        try:
            con.execute(f"DETACH {_CATALOG_ALIAS}")
        finally:
            con.close()
