"""Parity checks between qiita-common Python enums and their Postgres ENUM twins.

The system deliberately duplicates many closed value sets: once as a Python
`StrEnum` in `qiita-common` (so Pydantic models get strong typing without a DB
connection at import time) and once as a Postgres `CREATE TYPE ... AS ENUM` (so
the database itself rejects bad values). Per issue #37 this duplication is a
chosen compromise — the DB is *not* treated as the single source of truth — and
this module is the automated guard that keeps the two copies from drifting.

Not every closed value set is a Postgres ENUM — `auth_event.event_type`,
`reference.status`, and `reference.kind` are intentionally plain `TEXT`/`CHECK`
columns (see those migrations) and are out of scope here. This module covers
only value sets that are `CREATE TYPE ... AS ENUM`.

Two failure modes are covered:

* `test_enum_parity` — for each registered (Python enum, Postgres ENUM) pair,
  the two value sets must be exactly equal.
* `test_all_postgres_enums_are_covered` — every ENUM type in the `qiita` schema
  must be registered in `ENUM_PAIRS`, so a newly added Postgres ENUM cannot
  silently escape the parity check.

When you add or change a mirrored enum, change both sides and update
`ENUM_PAIRS`. See the "Enum parity" section in the repo-root CLAUDE.md.
"""

import pytest
from qiita_common.auth_constants import SystemRole
from qiita_common.models import (
    FailureType,
    FieldDataType,
    Platform,
    ProcessingKind,
    ScopeTargetKind,
    Tier,
    WorkTicketFailureStage,
    WorkTicketState,
)

pytestmark = pytest.mark.db


# Every Python StrEnum that mirrors a Postgres `CREATE TYPE ... AS ENUM` maps
# to its Postgres type name (without the `qiita.` schema prefix) here. The
# two-way comment on each Postgres ENUM definition points back at its Python
# twin. Adding a new mirrored enum: define both sides, then append the pair.
ENUM_PAIRS = [
    (SystemRole, "system_role"),
    (Platform, "platform"),
    (Tier, "tier"),
    (FieldDataType, "field_data_type"),
    (ScopeTargetKind, "scope_target_kind"),
    (ProcessingKind, "processing_kind"),
    (WorkTicketState, "work_ticket_state"),
    (FailureType, "failure_type"),
    (WorkTicketFailureStage, "work_ticket_failure_stage"),
]


async def _fetch_pg_enum_values(postgres_pool, pg_type: str) -> set[str]:
    """Return the value set of the Postgres ENUM `qiita.<pg_type>`."""
    rows = await postgres_pool.fetch(
        "SELECT e.enumlabel"
        "  FROM pg_enum e"
        "  JOIN pg_type t ON t.oid = e.enumtypid"
        "  JOIN pg_namespace n ON n.oid = t.typnamespace"
        " WHERE n.nspname = 'qiita' AND t.typname = $1",
        pg_type,
    )
    return {r["enumlabel"] for r in rows}


@pytest.mark.parametrize(
    "py_enum,pg_type",
    ENUM_PAIRS,
    ids=[pg_type for _, pg_type in ENUM_PAIRS],
)
async def test_enum_parity(py_enum, pg_type, postgres_pool):
    """A Python enum and its Postgres ENUM twin must carry the same values."""
    pg_values = await _fetch_pg_enum_values(postgres_pool, pg_type)
    assert pg_values, (
        f"Postgres ENUM qiita.{pg_type} not found — ENUM_PAIRS names a type "
        f"that does not exist in the schema."
    )
    py_values = {member.value for member in py_enum}
    assert py_values == pg_values, (
        f"Enum drift between {py_enum.__module__}.{py_enum.__name__} and "
        f"qiita.{pg_type}: Python has {sorted(py_values)}, Postgres has "
        f"{sorted(pg_values)}. Update both the Python StrEnum and the Postgres "
        f"CREATE TYPE so the value sets match (issue #37)."
    )


async def test_all_postgres_enums_are_covered(postgres_pool):
    """Every ENUM type in the qiita schema must be registered in ENUM_PAIRS.

    Catches the failure mode where a new `CREATE TYPE ... AS ENUM` is added to
    a migration but its Python twin / parity check is forgotten."""
    rows = await postgres_pool.fetch(
        "SELECT t.typname"
        "  FROM pg_type t"
        "  JOIN pg_namespace n ON n.oid = t.typnamespace"
        " WHERE n.nspname = 'qiita' AND t.typtype = 'e'"
    )
    pg_enum_types = {r["typname"] for r in rows}
    covered = {pg_type for _, pg_type in ENUM_PAIRS}
    uncovered = pg_enum_types - covered
    assert not uncovered, (
        f"Postgres ENUM type(s) {sorted(uncovered)} in schema qiita have no "
        f"entry in ENUM_PAIRS. Define a mirroring Python StrEnum in "
        f"qiita-common and register the (enum, type) pair here (issue #37)."
    )
