"""Parity between the Python terminal/non-terminal split and the SQL that
hardcodes it.

`NON_TERMINAL_WORK_TICKET_STATES` is *derived* in Python as the complement of
the terminal set, so a new `WorkTicketState` joins it automatically. The
database does NOT inherit that: the `work_ticket_one_in_flight_per_*` unique
indexes spell the in-flight set out as SQL literals, and they are the atomic
backstop for disallow-without-delete (the route's SELECT-then-INSERT check races
with nothing behind it if the index has a hole).

So a state added to the enum and forgotten in these predicates would let a second
in-flight ticket be minted for the same (scope_target, action) — which is issue
#284's failure mode relocated from Python into the schema. These tests fail loudly
instead. No DB needed; they read the migration files.
"""

import re
from pathlib import Path

from qiita_common.models import NON_TERMINAL_WORK_TICKET_STATES

_MIGRATIONS = Path(__file__).resolve().parent.parent / "db" / "migrations"

# Every migration is globbed, so an index added in a NEW migration is covered the
# moment it lands — no test edit, no opt-in.
_IN_FLIGHT_INDEX = re.compile(
    r"CREATE UNIQUE INDEX (?:IF NOT EXISTS )?(work_ticket_one_in_flight_per_\w+)(.*?);",
    re.DOTALL,
)
_STATE_IN_LIST = re.compile(r"state IN \(([^)]*)\)")

# The set as it stood when this test was written. A lower bound, not an
# allow-list: it only guards against the regex silently matching nothing (a
# renamed index convention) and reporting vacuous success.
_KNOWN_INDEXES = {
    "work_ticket_one_in_flight_per_reference",
    "work_ticket_one_in_flight_per_study_prep",
    "work_ticket_one_in_flight_per_prep_sample",
    "work_ticket_one_in_flight_per_sequenced_pool",
    "work_ticket_one_in_flight_per_block",
}


def _in_flight_indexes() -> dict[str, set[str]]:
    """{index_name: {states its predicate enumerates}} across every migration.

    Only the `migrate:up` half of each file is scanned. The natural shape of an
    add-a-state migration is DROP + CREATE(new set) in up and DROP + CREATE(old
    set) in down — so scanning the whole file would read the DOWN block's stale
    predicate (later in the file, and these are last-write-wins) and fail the very
    migration this test exists to ask for.
    """
    found: dict[str, set[str]] = {}
    for sql_file in sorted(_MIGRATIONS.glob("*.sql")):
        up = sql_file.read_text().split("-- migrate:down", 1)[0]
        for name, body in _IN_FLIGHT_INDEX.findall(up):
            states = _STATE_IN_LIST.search(body)
            assert states, f"{name} in {sql_file.name} has no `state IN (...)` predicate"
            # Last CREATE wins *within the up block*, which is what we want: a
            # migration that drops and recreates an index defines its current shape.
            found[name] = {s.strip().strip("'") for s in states.group(1).split(",")}
    return found


def test_every_in_flight_index_is_found():
    # Guards the regex itself: a vacuous pass would make the real assertion below
    # meaningless.
    found = _in_flight_indexes()
    assert _KNOWN_INDEXES <= set(found), f"missing: {_KNOWN_INDEXES - set(found)}"


def test_in_flight_index_predicates_match_the_non_terminal_set():
    expected = set(NON_TERMINAL_WORK_TICKET_STATES)
    for name, states in _in_flight_indexes().items():
        assert states == expected, (
            f"{name} enumerates {sorted(states)} but NON_TERMINAL_WORK_TICKET_STATES is"
            f" {sorted(expected)}. The Python set is derived from WorkTicketState and this"
            " index is not — add a migration that recreates it with the new state, or the"
            " one-in-flight uniqueness backstop has a hole for it."
        )
