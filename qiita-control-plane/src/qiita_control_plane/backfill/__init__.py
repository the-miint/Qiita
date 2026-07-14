"""One-off data backfills, reachable as `qiita-admin backfill <name>`.

These are NOT part of the running service. A backfill exists to bring the corpus
that predates a field up to the shape the runtime already assumes, and it lives here
— rather than beside `host_filter_resolver.py` and `preflight.py` at the package top
level — so that "code the API serves requests with" and "code an operator runs once
against the corpus" are not the same drawer.

They stay INSIDE the control-plane package rather than moving to `scripts/` because a
backfill has to reuse the real thing: the repositories layer that owns the writes, the
metadata spec that types them, and (here) the pre-flight's own definition of a control
sample. A script outside the package could only re-implement those, and a
re-implementation that drifts from the runtime is precisely how a backfill writes rows
the runtime then disagrees with.

Every backfill in here must be:

  * **dry-run by default** — it reports what it would write and writes nothing until
    an explicit `--execute`;
  * **idempotent** — a row that already carries the field is skipped, so a re-run
    after curation lands is safe and is the expected way to use it;
  * **fail-open-to-the-report, never to a guess** — anything the rules do not settle
    is listed as residue, not filled in with a default. The residue is the curation
    worklist.

**Retirement.** A backfill is done when its residue is empty and no new rows can
arrive without the field. Delete it then; it is not load-bearing and nothing imports
it but its CLI entry point.
"""
