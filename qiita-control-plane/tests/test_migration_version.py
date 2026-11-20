"""Every migration has a version of its own.

dbmate keys a migration on its filename's numeric prefix. On a database that
has recorded a version, as the live one has, a second file with that version
is never run, and `dbmate status` lists it as applied. `deploy/activate.sh`
checks the same prefix against `schema_migrations`, so it passes too. Only a
`dbmate up` that holds both files pending fails, on the duplicate version.
"""

from collections import defaultdict

from qiita_control_plane.testing.migrations import migration_files


def test_no_two_migrations_share_a_version():
    by_version: dict[str, list[str]] = defaultdict(list)
    for path in migration_files():
        by_version[path.name.split("_", 1)[0]].append(path.name)
    shared = {version: names for version, names in by_version.items() if len(names) > 1}
    assert not shared, (
        f"migrations sharing a version, of which dbmate runs at most one: {shared}. "
        f"Give the newer one a version after every migration on main."
    )
