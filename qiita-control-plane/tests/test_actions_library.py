"""Unit tests for the action-library registry shape (no DB).

Catches drift between qiita_common.api_paths.LibraryPrimitive (the closed
set of names workflow YAML can reference via `action:` entries) and the
qiita_control_plane.actions.library.LIBRARY dispatch dict (what the
runner actually calls).
"""

import inspect


def test_library_exposes_every_named_primitive():
    from qiita_common.api_paths import LibraryPrimitive

    from qiita_control_plane.actions import LIBRARY

    assert set(LIBRARY.keys()) == set(LibraryPrimitive)


def test_library_primitives_are_async_callables():
    """The runner does `await LIBRARY[name](...)` uniformly — every entry
    must be an async callable."""
    from qiita_control_plane.actions import LIBRARY

    for name, fn in LIBRARY.items():
        assert callable(fn), f"{name!r} entry is not callable"
        assert inspect.iscoroutinefunction(fn), f"{name!r} is not async"


def test_library_re_exports_match_module_callables():
    """The names in LIBRARY map 1:1 to the module-level functions of the
    same role — adding a named primitive without a same-named function
    (or vice-versa) is a smell."""
    from qiita_common.api_paths import LibraryPrimitive

    from qiita_control_plane.actions import LIBRARY
    from qiita_control_plane.actions import library as lib

    assert LIBRARY[LibraryPrimitive.MINT_FEATURES] is lib.mint_features
    assert LIBRARY[LibraryPrimitive.WRITE_MEMBERSHIP] is lib.write_membership
    assert LIBRARY[LibraryPrimitive.REGISTER_FILES] is lib.register_files
    assert LIBRARY[LibraryPrimitive.REGISTER_INDEX] is lib.register_index
    assert LIBRARY[LibraryPrimitive.PERSIST_READ_METRICS] is lib.persist_read_metrics
