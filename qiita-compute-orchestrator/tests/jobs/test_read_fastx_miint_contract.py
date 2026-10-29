"""Real-miint contract pins for `read_fastx`, on the build Qiita stages.

The first two pin the `sequence_index` it assigns, which `assembly_hash` composes
into its synthetic per-contig `read_id`. Two halves of the column's upstream
contract (https://the-miint.github.io/duckdb-miint/reading/) are what the id rests
on: dense 1..N over ONE file, restarting at 1 in the next file of a multi-path
scan; and the same record taking the same ordinal in a second scan of the same
bytes. Why the id needs each is in `assembly_hash`.

The third pins that one multi-path scan closes each file once it has read it,
which `stage_local_fasta` relies on when it hands its whole manifest to one call.
The reading page does not state it.

Runs against the team-mirror miint build staged by the session-autouse fixture in
tests/conftest.py (`open_miint_conn` is LOAD-only).
"""

from __future__ import annotations

import gzip
import json
import subprocess
import sys
import textwrap
from pathlib import Path

from qiita_compute_orchestrator.miint import open_miint_conn

# `assembly_hash`'s scan: a list of paths, `include_filepath` on so each row carries
# the file its ordinal is relative to.
_SCAN = (
    "SELECT filepath, sequence_index, read_id"
    " FROM read_fastx(?, include_filepath:=true{opts})"
    " ORDER BY filepath, sequence_index"
)


def _fasta(path: Path, records: list[tuple[str, str]]) -> str:
    """Write `records` as FASTA at `path`, gzipped when it ends in `.gz`."""
    text = "".join(f">{rid}\n{seq}\n" for rid, seq in records)
    if path.suffix == ".gz":
        with gzip.open(path, "wt") as fh:
            fh.write(text)
    else:
        path.write_text(text)
    return str(path)


def test_sequence_index_restarts_at_one_in_each_file(tmp_path):
    """Dense 1..N per file, 1-based, restarting in the next file of one scan.

    The two files hold different record counts, so a scan-global counter would put
    `b.fa` at 4..5 rather than 1..2.
    """
    a = _fasta(tmp_path / "a.fa", [("a1", "ACGTACGT"), ("a2", "AAAACCCC"), ("a3", "TTTTGGGG")])
    b = _fasta(tmp_path / "b.fa", [("b1", "CCCCAAAA"), ("b2", "GGGGTTTT")])

    with open_miint_conn() as conn:
        rows = conn.execute(_SCAN.format(opts=""), [[a, b]]).fetchall()

    assert rows == [(a, 1, "a1"), (a, 2, "a2"), (a, 3, "a3"), (b, 1, "b1"), (b, 2, "b2")]


def test_sequence_index_is_the_same_in_a_second_scan(tmp_path):
    """A second scan of the same files pairs every ordinal with the same record.

    The second scan reads under a 4 KB batch budget, below either file's size
    (~12 kB and ~6 kB of 2 kb records), so the two runs do not divide the files the
    same way.
    """
    a = _fasta(tmp_path / "a.fa", [(f"a{i}", "ACGT" * 500 + "A" * i) for i in range(6)])
    b = _fasta(tmp_path / "b.fa", [(f"b{i}", "TGCA" * 500 + "T" * i) for i in range(3)])
    paths = [a, b]

    with open_miint_conn() as conn:
        first = conn.execute(_SCAN.format(opts=""), [paths]).fetchall()
        second = conn.execute(_SCAN.format(opts=", max_batch_bytes:='4KB'"), [paths]).fetchall()

    assert first == [(a, i + 1, f"a{i}") for i in range(6)] + [
        (b, i + 1, f"b{i}") for i in range(3)
    ]
    assert second == first


# More paths than the scan's process may hold open at once. `read_fastx` reads one
# file per thread, so the threads are pinned well under the limit: a large host's
# default would hold that many files open on any build.
_DESCRIPTOR_LIMIT = 128
_MANY_PATHS = 300
_THREADS = 8


def test_a_scan_of_many_paths_closes_each_file_once_read(tmp_path):
    """One scan of more gzipped files than the process may open at once reads them all.

    A build that holds every listed file open until the call ends (duckdb-miint#260,
    fixed by duckdb-miint#261) fails this scan at the descriptor limit, so the child's
    exit status is what tells the builds apart. The scan runs in a child process so
    the lowered limit touches nothing else, and the child reports the limit it ran
    under, so nothing that raised it can pass the test.
    """
    paths = [
        _fasta(tmp_path / f"g{i:04d}.fa.gz", [(f"g{i}", "ACGTACGT")]) for i in range(_MANY_PATHS)
    ]
    script = textwrap.dedent(
        f"""
        import json, resource, sys
        _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, ({_DESCRIPTOR_LIMIT}, hard))
        from qiita_compute_orchestrator.miint import open_miint_conn
        with open_miint_conn() as conn:
            conn.execute("SET threads = {_THREADS}")
            [(reads,)] = conn.execute(
                "SELECT count(*) FROM read_fastx(?)", [json.load(sys.stdin)]
            ).fetchall()
        print(json.dumps([reads, resource.getrlimit(resource.RLIMIT_NOFILE)[0]]))
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        input=json.dumps(paths),
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == [_MANY_PATHS, _DESCRIPTOR_LIMIT]
