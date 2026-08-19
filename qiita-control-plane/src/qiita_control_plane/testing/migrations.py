"""Reading the migration files as a source of truth, without a database.

Several tests assert that Python and the schema agree (enum parity, the in-flight
index predicates) by *replaying the SQL* rather than querying Postgres, so the
check runs in the no-infrastructure tier. They all need the same two primitives,
and they must agree on them — two scanners with different ideas of where a
migration's `up` half ends is the silent-inconsistency shape, and the answer only
shows up as a false green.

`migrate_up()` is the load-bearing one, and it matches the marker **anchored to the
start of a line**, as dbmate itself does. Two weaker forms are tempting and both
are wrong:

* Splitting on the bare substring `migrate:down` truncates any file whose up-half
  *prose* merely mentions the marker — `20260624000000_drop_sequenced_sample_host_references.sql`
  says "migrate:down re-adds the columns" mid-comment, which would cut its up half
  from 28 lines to 19 and silently drop the DDL below.
* Splitting on `-- migrate:down` is safe against that, but would also fire on an
  *indented* `  -- migrate:down`, which dbmate does not treat as the marker.

Scope: these primitives are shaped for scanning **DDL**. `strip_sql_comments` does
not model dollar-quoting, so a `--` inside a `DO $$ ... $$` body is stripped like
an ordinary comment. That is harmless for the DDL-shaped scans here; a future
consumer replaying function or trigger *bodies* would need to teach it `$$`.
"""

import re
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "db" / "migrations"

# dbmate's down marker: a line that *starts* with `-- migrate:down`. Anchored, so
# prose mentioning migrate:down mid-comment doesn't end the up half early.
_DOWN_MARKER = re.compile(r"(?m)^--\s*migrate:down")


def migration_files() -> list[Path]:
    """Every migration, in filename (= version) order — the order dbmate applies.

    Fails loudly on a missing directory rather than returning an empty list. This
    module ships inside the installed package, so under a non-editable install
    `MIGRATIONS_DIR` resolves into site-packages, where `db/migrations/` does not
    exist — and `glob()` on a missing dir returns `[]` with no error. An empty
    migration set makes a coverage assertion pass *vacuously*, which is the exact
    false green these tests exist to prevent.
    """
    if not MIGRATIONS_DIR.is_dir():
        raise RuntimeError(
            f"No migrations directory at {MIGRATIONS_DIR}. These helpers read the "
            f"repo's db/migrations/ and only work from a source checkout (or an "
            f"editable install); an empty scan would pass coverage checks vacuously."
        )
    return sorted(MIGRATIONS_DIR.glob("*.sql"))


def migrate_up(sql: str) -> str:
    """The `migrate:up` half of a dbmate migration.

    Everything from the `-- migrate:down` line onward is the rollback path and
    never reaches a live schema. Scanning it would replay statements that undo
    the migration — a `DROP TYPE`, or the *old* form of an index a migration is
    rewriting — so every schema-replaying test must cut it off here.
    """
    return _DOWN_MARKER.split(sql, maxsplit=1)[0]


def strip_sql_comments(sql: str, *, source: str = "<sql>") -> str:
    """Remove `--` line comments, refusing to corrupt a string literal.

    A naive `--.*$` strip is unsafe here: the migrations genuinely contain `--`
    *inside* `COMMENT ON ... IS '...'` literals (e.g.
    `'cross-study governance -- the protocol is defined once ...'`), and cutting
    at that `--` truncates the literal and leaves an unbalanced quote for the
    caller to misparse. So the scan is quote-aware, and it asserts the result is
    quote-balanced rather than trusting itself.
    """
    out: list[str] = []
    in_string = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        if in_string:
            # '' is an escaped quote inside a literal, not a close.
            if ch == "'" and sql[i + 1 : i + 2] == "'":
                out.append("''")
                i += 2
                continue
            if ch == "'":
                in_string = False
            out.append(ch)
            i += 1
            continue
        if ch == "'":
            in_string = True
            out.append(ch)
            i += 1
            continue
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            i = len(sql) if j == -1 else j  # keep the newline; drop the comment
            continue
        out.append(ch)
        i += 1

    stripped = "".join(out)
    assert stripped.count("'") % 2 == 0, (
        f"{source}: stripping SQL comments left an odd number of quotes, so a "
        f"string literal was cut in half. Parsing this would silently misread the "
        f"migration — fix the stripper rather than trusting the result."
    )
    return stripped


def migrate_up_sql(path: Path) -> str:
    """`migrate_up` + `strip_sql_comments` for a migration file — the usual pairing."""
    return strip_sql_comments(migrate_up(path.read_text()), source=path.name)
