"""Real-miint contract pins for the fastp-port QC functions the `qc` native job
(Phase 2) builds on: `filter_read`, `trim_adapters` / `trim_adapters_pe`,
`trim_polyg`.

These run against the team-mirror miint build (staged by the session-autouse
`_stage_miint_extension` fixture in tests/conftest.py; `open_miint_conn` is
LOAD-only). They pin the facts the job depends on that a stubbed unit test
cannot see and that the upstream docs get wrong:

  * miint QC functions take POSITIONAL args, not named (`min_length := 100`
    raises a BinderException) — `filter_read` has only a 2-arg and an 8-arg
    overload; `trim_adapters_pe` only a 4-arg and an 11-arg one.
  * `filter_read`'s 2-arg defaults equal `(15, 0, 15, 40, 5, 0)` == fastp's
    defaults, so a faithful `fastp -l 100` is the 8-arg call
    `(seq, qual, 100, 0, 15, 40, 5, 0)`.
  * `trim_adapters_pe`'s fastp overlap defaults are `(30, 5, 20, false, 0,
    false)` — the 11-arg form with an EMPTY adapter list reproduces the 4-arg
    (overlap-only) result exactly, which is how the job can add an adapter
    fallback without changing overlap behavior.
  * `trim_polyg` trims a G run only when its quality is LOW (2-color no-signal);
    a high-quality G run is left intact — matching fastp, and why the job gates
    it on a 2-color `instrument_model`.

If a future miint build changes any of these, this file fails — the signal to
re-pin the job's SQL.
"""

from __future__ import annotations

from qiita_compute_orchestrator.miint import open_miint_conn

# A 32 nt insert and the standard Illumina TruSeq adapter prefix.
_INSERT = "ACGTGGCATTGACCTGATCAGTTACGGCATTGA"
_ADAPTER = "AGATCGGAAGAGC"


def _q(n: int, val: int = 35) -> str:
    """A length-`n` UTINYINT[] quality literal, all `val` (Q35 = high)."""
    return "[" + ",".join([str(val)] * n) + "]::UTINYINT[]"


def _revcomp(seq: str) -> str:
    return seq.translate(str.maketrans("ACGT", "TGCA"))[::-1]


def test_filter_read_length_gate_is_first_positional_arg():
    """`filter_read(seq, qual, min_length, ...)`: min_length is the 1st int param.
    A 32 nt read fails at min_length=100 (fail_reason 'length') and passes at 10."""
    with open_miint_conn() as conn:
        fail = conn.execute(
            f"SELECT filter_read('{_INSERT}', {_q(len(_INSERT))}, 100, 0, 15, 40, 5, 0)"
        ).fetchone()[0]
        assert fail["passed"] is False
        assert fail["fail_reason"] == "length"
        assert fail["length"] == len(_INSERT)

        keep = conn.execute(
            f"SELECT filter_read('{_INSERT}', {_q(len(_INSERT))}, 10, 0, 15, 40, 5, 0)"
        ).fetchone()[0]
        assert keep["passed"] is True
        assert keep["fail_reason"] is None


def test_filter_read_defaults_match_fastp_defaults():
    """The 2-arg form equals the 8-arg form with (15, 0, 15, 40, 5, 0), so the
    only knob the job overrides for `fastp -l 100` is min_length."""
    with open_miint_conn() as conn:
        two, eight = conn.execute(
            f"SELECT filter_read('{_INSERT}', {_q(len(_INSERT))}),"
            f" filter_read('{_INSERT}', {_q(len(_INSERT))}, 15, 0, 15, 40, 5, 0)"
        ).fetchone()
        assert two == eight


def test_filter_read_n_base_gate():
    """6 Ns with max_n=5 fails with fail_reason 'n_base' (pins max_n position)."""
    seq = "NNNNNN" + _INSERT
    with open_miint_conn() as conn:
        r = conn.execute(
            f"SELECT filter_read('{seq}', {_q(len(seq))}, 10, 0, 15, 40, 5, 0)"
        ).fetchone()[0]
        assert r["passed"] is False
        assert r["fail_reason"] == "n_base"
        assert r["n_bases"] == 6


def test_trim_adapters_se_removes_3p_adapter():
    """SE `trim_adapters(seq, qual, [adapters])` recovers the insert exactly."""
    read = _INSERT + _ADAPTER
    with open_miint_conn() as conn:
        r = conn.execute(
            f"SELECT trim_adapters('{read}', {_q(len(read))}, ['{_ADAPTER}'])"
        ).fetchone()[0]
        assert r["sequence"] == _INSERT
        assert r["trimmed_3p"] == len(_ADAPTER)


def test_trim_adapters_se_empty_adapters_is_noop():
    """SE `trim_adapters(seq, qual, [])` with an EMPTY adapter list is a no-op:
    0 trims, sequence unchanged — even when an adapter IS present in the read. This
    is the PacBio / long-read QC path: the `qc` job binds no adapter set and renders
    `[]::VARCHAR[]`, so QC runs the length/quality filter with no adapter trim. Pins
    the miint behavior the optional-`adapter_parquet` design relies on (a bump that
    started trimming on an empty set would silently clip HiFi reads)."""
    read = _INSERT + _ADAPTER
    with open_miint_conn() as conn:
        r = conn.execute(
            f"SELECT trim_adapters('{read}', {_q(len(read))}, []::VARCHAR[])"
        ).fetchone()[0]
        assert r["sequence"] == read
        assert r["trimmed_5p"] == 0
        assert r["trimmed_3p"] == 0


def test_trim_adapters_pe_11arg_empty_adapters_equals_4arg():
    """The 11-arg form with an empty adapter list and the fastp overlap defaults
    (30, 5, 20, false, 0, false) reproduces the 4-arg overlap-only result — pins
    those defaults so the job's adapter-fallback call keeps fastp overlap behavior."""
    r1 = _INSERT + _ADAPTER
    r2 = _revcomp(_INSERT) + _ADAPTER
    with open_miint_conn() as conn:
        four = conn.execute(
            f"SELECT trim_adapters_pe('{r1}', {_q(len(r1))}, '{r2}', {_q(len(r2))})"
        ).fetchone()[0]
        eleven = conn.execute(
            f"SELECT trim_adapters_pe('{r1}', {_q(len(r1))}, '{r2}', {_q(len(r2))},"
            " []::VARCHAR[], 30, 5, 20, false, 0, false)"
        ).fetchone()[0]
        assert four == eleven
        # The adaptered overlapping pair is detected and trimmed.
        assert four["adapter_trimmed"] is True
        assert four["sequence1"] == _INSERT
        assert four["sequence2"] == _revcomp(_INSERT)


def test_trim_polyg_trims_only_low_quality_g_run():
    """A high-quality G run is left intact; a low-quality (2-color no-signal) G
    run is trimmed from the 3' end — fastp's polyG behavior."""
    seq = _INSERT + "G" * 16
    with open_miint_conn() as conn:
        hi = conn.execute(f"SELECT trim_polyg('{seq}', {_q(len(seq))})").fetchone()[0]
        assert hi["trimmed_3p"] == 0
        assert hi["sequence"] == seq

        qlo = "[" + ",".join(["35"] * len(_INSERT) + ["2"] * 16) + "]::UTINYINT[]"
        lo = conn.execute(f"SELECT trim_polyg('{seq}', {qlo})").fetchone()[0]
        assert lo["trimmed_3p"] > 0
        assert lo["sequence"].startswith(_INSERT[: len(lo["sequence"])])


def test_qc_functions_report_fastp_port_version():
    """Sanity: the build self-identifies as the fastp algorithm port."""
    with open_miint_conn() as conn:
        version = conn.execute("SELECT qc_version()").fetchone()[0]
        assert "fastp" in version.lower()
