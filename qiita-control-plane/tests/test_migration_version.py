"""Every migration has a version of its own.

dbmate keys a migration on its filename's numeric prefix. Of two files that
share one, it runs one and records the version, then treats the other
as applied without running it. `deploy/activate.sh` checks the same prefix
against `schema_migrations`, so it does not catch the skipped file either.
"""

from collections import defaultdict

from qiita_control_plane.testing.migrations import migration_files


def test_no_two_migrations_share_a_version():
    by_version: dict[str, list[str]] = defaultdict(list)
    for path in migration_files():
        by_version[path.name.split("_", 1)[0]].append(path.name)
    shared = {version: names for version, names in by_version.items() if len(names) > 1}
    assert not shared, (
        f"migrations sharing a version, of which dbmate runs only the first: {shared}. "
        f"Give the newer one a version after every migration on main."
    )
