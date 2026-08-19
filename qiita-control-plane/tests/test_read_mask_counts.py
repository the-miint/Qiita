"""Pure-unit tests for `_read_mask_counts`' bucket whitelist (no DB).

Lives apart from test_persist_read_metrics_action.py, which is `pytestmark =
pytest.mark.db` at module scope — these need no database and belong in the fast
tier, where a wrong bucket fails immediately.
"""

import duckdb
from qiita_common.models import ReadMaskReason

# --- spike-in + twist_no_adaptor bucketing ------------------------------------
#
# The predicate this replaced was `reason NOT LIKE 'qc_%'` — fail-OPEN, so BOTH of
# these reasons would have been silently counted as biological.


def _mask_with_new_reasons(path, *, n_pass, n_host, n_qc, n_spikein, n_twist):
    """Single-end mask carrying every bucket, so one query exercises the whitelist."""
    sidx = 0
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TABLE m(mask_idx BIGINT, prep_sample_idx BIGINT, sequence_idx BIGINT, "
            "reason VARCHAR, left_trim1 UINTEGER, right_trim1 UINTEGER, "
            "left_trim2 UINTEGER, right_trim2 UINTEGER)"
        )
        for reason, n in (
            (ReadMaskReason.PASS.value, n_pass),
            (ReadMaskReason.HOST_RYPE.value, n_host),
            (ReadMaskReason.QC_TOO_SHORT.value, n_qc),
            (ReadMaskReason.SPIKEIN_SYNDNA.value, n_spikein),
            (ReadMaskReason.TWIST_NO_ADAPTOR.value, n_twist),
        ):
            for _ in range(n):
                conn.execute(
                    f"INSERT INTO m VALUES (1, 1, ?, '{reason}', 0, 0, NULL, NULL)", [sidx]
                )
                sidx += 1
        conn.execute(f"COPY m TO '{path}' (FORMAT PARQUET)")
    return path


def test_read_mask_counts_buckets_spikein_and_twist_out_of_biological(tmp_path):
    """biological = pass + host_*. A spike-in is added in the lab; a read with no
    Twist adaptor is artifactual. Neither is a molecule from the sample."""
    from qiita_control_plane.actions.library import _read_mask_counts

    mask = _mask_with_new_reasons(
        tmp_path / "m.parquet", n_pass=10, n_host=3, n_qc=5, n_spikein=7, n_twist=2
    )
    raw, biological, quality_filtered, spikein = _read_mask_counts(mask)
    assert raw == 27  # every row
    assert biological == 13  # pass + host_rype, NOT spikein/twist/qc
    assert quality_filtered == 10  # the pass subset
    assert spikein == 7
    # The invariants the sequenced_sample CHECK enforces.
    assert quality_filtered <= biological
    assert biological + spikein <= raw


def test_read_mask_counts_spikein_is_zero_without_syndna(tmp_path):
    """An Illumina mask has no spike-ins; the bucket is 0, not NULL, and the old
    three counts are unchanged by the whitelist rewrite."""
    from qiita_control_plane.actions.library import _read_mask_counts

    mask = _mask_with_new_reasons(
        tmp_path / "m.parquet", n_pass=4, n_host=1, n_qc=2, n_spikein=0, n_twist=0
    )
    assert _read_mask_counts(mask) == (7, 5, 4, 0)


# --- Python <-> Rust bucket lockstep ------------------------------------------


def _rust_const(name: str) -> str:
    """Extract a `const NAME: &str = "...";` literal from the data plane source."""
    import re
    from pathlib import Path

    src = (
        Path(__file__).resolve().parents[2] / "qiita-data-plane" / "src" / "flight_service.rs"
    ).read_text()
    m = re.search(rf'^const {name}: &str = "(.*)";$', src, flags=re.MULTILINE)
    assert m, f"{name} not found in flight_service.rs"
    return m.group(1)


def test_rust_reason_lists_match_the_python_bucket_map():
    """`mask_metrics_counts` (Rust) is the block-path twin of `_read_mask_counts`.
    Rust cannot import `READ_MASK_BUCKET`, so it hardcodes the reason lists — and
    the block e2e fixture never emits a `spikein_syndna` or a `host_minimap2` read,
    so a typo there would miscount SILENTLY rather than fail a test.

    This is that test: compare the Rust literals against the Python source of
    truth, character for character. Adding a ReadMaskReason means editing both.
    """
    from qiita_common.models import ReadMaskBucket, read_mask_reason_sql_list

    assert _rust_const("BIOLOGICAL_REASONS") == read_mask_reason_sql_list(ReadMaskBucket.BIOLOGICAL)
    assert _rust_const("SPIKEIN_REASONS") == read_mask_reason_sql_list(ReadMaskBucket.SPIKEIN)
