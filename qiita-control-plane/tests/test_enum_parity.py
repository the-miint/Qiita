"""Parity checks between qiita-common Python enums and their Postgres ENUM twins.

See CLAUDE.md for the rationale and the rules. Not every closed value set is a
Postgres ENUM, and this module covers only value sets that are
`CREATE TYPE ... AS ENUM`.

The checks run in **two tiers**, deliberately:

* No DB (`make test`) — the Postgres value sets are reconstructed by replaying
  the enum DDL in `db/migrations/*.sql`. This is the tier that matters for the
  common failure: adding a value to a Python `StrEnum` and forgetting the
  `ALTER TYPE ... ADD VALUE` migration (or vice versa). Catching it here means
  catching it before the push, not on a CI round trip.
* DB (`-m db`) — the same two assertions against a live `qiita` schema. These
  additionally prove the migration replay above models Postgres faithfully, so
  a parser that quietly drifts from reality cannot leave the cheap tier green.

Two failure modes are covered in each tier: value-set drift for every entry in
`ENUM_PAIRS`, and a Postgres ENUM that exists but was never registered in
`ENUM_PAIRS` (so a new `CREATE TYPE` cannot silently escape the parity check).

When you add a new mirrored enum, register its (Python class, Postgres type
name) pair in `ENUM_PAIRS` below.
"""

import re
from functools import cache

import pytest
from qiita_common.auth_constants import SystemRole
from qiita_common.models import (
    FailureType,
    FieldDataType,
    Platform,
    ProcessingKind,
    ScopeTargetKind,
    TerminologyStatus,
    TerminologyTermObsoletionKind,
    Tier,
    WorkTicketFailureStage,
    WorkTicketState,
)

from qiita_control_plane.testing.migrations import migrate_up_sql, migration_files

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
    (TerminologyStatus, "terminology_status"),
    (TerminologyTermObsoletionKind, "terminology_term_obsoletion_kind"),
]

_CREATE_ENUM = re.compile(
    r"CREATE\s+TYPE\s+(?:qiita\.)?(\w+)\s+AS\s+ENUM\s*\(([^)]*)\)",
    re.IGNORECASE | re.DOTALL,
)
_ADD_VALUE = re.compile(
    r"ALTER\s+TYPE\s+(?:qiita\.)?(\w+)\s+ADD\s+VALUE\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?'([^']+)'",
    re.IGNORECASE,
)
_RENAME_VALUE = re.compile(
    r"ALTER\s+TYPE\s+(?:qiita\.)?(\w+)\s+RENAME\s+VALUE\s+'([^']+)'\s+TO\s+'([^']+)'",
    re.IGNORECASE,
)
# Enum DDL this replay does NOT model. Reaching one means the reconstructed value
# set has silently diverged from what Postgres builds, so it is a hard error, not
# a skip — a false green here is exactly the drift the no-DB tier exists to catch.
_ANY_ALTER_TYPE = re.compile(r"ALTER\s+TYPE\s+(?:qiita\.)?\w+[^;]*", re.IGNORECASE)
_DROP_TYPE = re.compile(r"DROP\s+TYPE\s+(?:IF\s+EXISTS\s+)?(?:qiita\.)?(\w+)", re.IGNORECASE)


@cache
def enum_values_from_migrations() -> dict[str, set[str]]:
    """Replay the enum DDL in `db/migrations/` into `{pg_type: {values}}`.

    Migrations are applied in filename (= version) order, exactly as dbmate
    applies them, so a later `ADD VALUE` lands on the type its `CREATE TYPE`
    established. Only each file's `migrate:up` half is replayed — the down half
    is the rollback path and would, e.g., `DROP TYPE` everything back out.
    """
    enums: dict[str, set[str]] = {}
    for path in migration_files():
        up = migrate_up_sql(path)

        for name, body in _CREATE_ENUM.findall(up):
            enums[name] = set(re.findall(r"'([^']+)'", body))

        for name, value in _ADD_VALUE.findall(up):
            assert name in enums, (
                f"{path.name}: ALTER TYPE qiita.{name} ADD VALUE, but no earlier "
                f"migration CREATEs that type."
            )
            enums[name].add(value)

        for name, old, new in _RENAME_VALUE.findall(up):
            assert name in enums and old in enums[name], (
                f"{path.name}: ALTER TYPE qiita.{name} RENAME VALUE '{old}', but "
                f"'{old}' is not a value of that type at this point in history."
            )
            enums[name].discard(old)
            enums[name].add(new)

        for stmt in _ANY_ALTER_TYPE.findall(up):
            if not (_ADD_VALUE.search(stmt) or _RENAME_VALUE.search(stmt)):
                raise AssertionError(
                    f"{path.name}: unmodelled `ALTER TYPE` — {stmt.strip()!r}. "
                    f"Teach enum_values_from_migrations() this form; leaving it "
                    f"unparsed would desync the no-DB parity check from Postgres."
                )

        # A DROP TYPE in an *up* half genuinely retires an enum. Unmodelled, it
        # would leave a phantom in the replay that no longer exists in Postgres —
        # so honour it rather than letting the two drift.
        for name in _DROP_TYPE.findall(up):
            enums.pop(name, None)

    return enums


def _drift_message(py_enum, pg_type: str, py_values: set[str], pg_values: set[str]) -> str:
    return (
        f"Enum drift between {py_enum.__module__}.{py_enum.__name__} and "
        f"qiita.{pg_type}: Python has {sorted(py_values)}, Postgres has "
        f"{sorted(pg_values)}. Update both the Python StrEnum and the Postgres "
        f"CREATE TYPE so the value sets match."
    )


def _uncovered_message(uncovered: set[str]) -> str:
    return (
        f"Postgres ENUM type(s) {sorted(uncovered)} in schema qiita have no "
        f"entry in ENUM_PAIRS. Define a mirroring Python StrEnum in "
        f"qiita-common and register the (enum, type) pair here."
    )


# --------------------------------------------------------------------------
# Tier 1 — no DB. Runs in `make test`.
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "py_enum,pg_type",
    ENUM_PAIRS,
    ids=[pg_type for _, pg_type in ENUM_PAIRS],
)
def test_enum_parity_against_migrations(py_enum, pg_type):
    """A Python enum and the ENUM its migrations build must carry the same values."""
    pg_values = enum_values_from_migrations().get(pg_type, set())
    assert pg_values, (
        f"No `CREATE TYPE ... AS ENUM` for qiita.{pg_type} in db/migrations/ — "
        f"ENUM_PAIRS names a type no migration defines."
    )
    py_values = {member.value for member in py_enum}
    assert py_values == pg_values, _drift_message(py_enum, pg_type, py_values, pg_values)


def test_all_migration_enums_are_covered():
    """Every ENUM a migration CREATEs must be registered in ENUM_PAIRS."""
    uncovered = set(enum_values_from_migrations()) - {pg for _, pg in ENUM_PAIRS}
    assert not uncovered, _uncovered_message(uncovered)


# --------------------------------------------------------------------------
# Tier 2 — live schema. Runs under `make test-control-plane-with-db`.
# Same assertions, but they also pin the migration replay above to reality.
# --------------------------------------------------------------------------


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


@pytest.mark.db
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
    assert py_values == pg_values, _drift_message(py_enum, pg_type, py_values, pg_values)


@pytest.mark.db
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
    uncovered = {r["typname"] for r in rows} - {pg for _, pg in ENUM_PAIRS}
    assert not uncovered, _uncovered_message(uncovered)


@pytest.mark.db
async def test_migration_replay_matches_live_schema(postgres_pool):
    """The no-DB migration replay must reproduce the live schema exactly.

    This is what lets the cheap tier be trusted: if `enum_values_from_migrations()`
    ever drifts from what Postgres actually builds, the cheap tier would go green
    on a lie. Here that shows up as a failure instead."""
    rows = await postgres_pool.fetch(
        "SELECT t.typname, e.enumlabel"
        "  FROM pg_type t"
        "  JOIN pg_namespace n ON n.oid = t.typnamespace"
        "  JOIN pg_enum e ON e.enumtypid = t.oid"
        " WHERE n.nspname = 'qiita' AND t.typtype = 'e'"
    )
    live: dict[str, set[str]] = {}
    for row in rows:
        live.setdefault(row["typname"], set()).add(row["enumlabel"])

    assert enum_values_from_migrations() == live, (
        "The migration replay in enum_values_from_migrations() disagrees with the "
        "live qiita schema. Either a migration uses enum DDL the replay doesn't "
        "model, or the database has drifted from db/migrations/."
    )
