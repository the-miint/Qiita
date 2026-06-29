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
    assert LIBRARY[LibraryPrimitive.PERSIST_QC_REPORT] is lib.persist_qc_report


async def test_delete_pool_reads_data_empty_set_short_circuits():
    """An empty prep_sample set returns {} without a Flight call — so an
    empty pool delete never touches the data plane."""
    from qiita_control_plane.actions import library as lib

    result = await lib.delete_pool_reads_data(
        prep_sample_idxs=[],
        hmac_secret=b"\x00" * 32,
        data_plane_url="grpc://unreachable:1",
    )
    assert result == {}


def test_reap_staged_reads_none_root_is_noop():
    """CP-only/dev (no shared scratch) reaps nothing and never raises."""
    from qiita_control_plane.actions.sequenced_pool import reap_staged_reads

    assert reap_staged_reads(None, [1, 2, 3]) == 0


def test_reap_staged_reads_removes_files_and_empty_dirs(tmp_path):
    from qiita_common.api_paths import compute_reads_staging_path

    from qiita_control_plane.actions.sequenced_pool import reap_staged_reads

    present = compute_reads_staging_path(tmp_path, 11)
    present.parent.mkdir(parents=True)
    present.write_bytes(b"x")
    # idx 22 has no staged copy — reaper must tolerate the gap (idempotent).
    reaped = reap_staged_reads(tmp_path, [11, 22])
    assert reaped == 1
    assert not present.exists()
    assert not present.parent.exists()
