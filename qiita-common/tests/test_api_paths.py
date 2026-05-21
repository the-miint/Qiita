"""Cross-service path-contract tests for qiita_common.api_paths.

The Rust data plane and the Python control plane share filesystem
conventions where the data plane writes and the control plane reads.
These tests pin those conventions on the Python side so a drift in
either direction surfaces as a test failure rather than a mysterious
"file not found" at workflow run time.
"""

from pathlib import Path

from qiita_common.api_paths import compute_upload_staging_path


def test_compute_upload_staging_path_matches_rust_layout():
    """Locked layout: ``{root}/uploads/{idx}/upload.parquet``.

    Mirrors the Rust unit test ``staging_path_for_layout`` in
    ``qiita-data-plane/src/flight_service.rs``. If you change one,
    change the other in the same commit; both sides will move together
    or not at all."""
    assert compute_upload_staging_path(Path("/scratch/ephemeral/staging"), 42) == Path(
        "/scratch/ephemeral/staging/uploads/42/upload.parquet"
    )


def test_compute_upload_staging_path_accepts_relative_root():
    """A relative ``staging_root`` is preserved verbatim — used by tests
    that synthesize an isolated tmpdir-rooted staging area."""
    assert compute_upload_staging_path(Path("tmp/staging"), 7) == Path(
        "tmp/staging/uploads/7/upload.parquet"
    )
