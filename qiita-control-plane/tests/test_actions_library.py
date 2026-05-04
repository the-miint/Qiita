"""Unit tests for the action-library registry shape (no DB)."""

import inspect


def test_library_exposes_three_named_primitives():
    """LIBRARY contains exactly the LibraryPrimitive members. Adding or
    removing entries is a contract change visible to every workflow YAML,
    so this assertion guards against accidental drift between the enum
    and the dict."""
    from qiita_common.api_paths import LibraryPrimitive

    from qiita_control_plane.actions import LIBRARY

    assert set(LIBRARY.keys()) == set(LibraryPrimitive)


def test_library_primitives_are_async_callables():
    """Every LIBRARY entry must be an async callable so the runner can
    `await library[name](...)` uniformly."""
    from qiita_control_plane.actions import LIBRARY

    for name, fn in LIBRARY.items():
        assert callable(fn), f"{name!r} entry is not callable"
        assert inspect.iscoroutinefunction(fn), f"{name!r} is not async"


def test_library_re_exports_match_module_callables():
    """The names in LIBRARY map to the module-level functions of the same
    role — adding a named primitive without a same-named module function
    (or vice-versa) is a smell."""
    from qiita_common.api_paths import LibraryPrimitive

    from qiita_control_plane.actions import LIBRARY
    from qiita_control_plane.actions import library as lib

    assert LIBRARY[LibraryPrimitive.MINT_FEATURES] is lib.mint_features
    assert LIBRARY[LibraryPrimitive.WRITE_MEMBERSHIP] is lib.write_membership
    assert LIBRARY[LibraryPrimitive.REGISTER_FILES] is lib.register_files
