"""A DuckLake registration made by one process is visible to the next connection
another process opens.

Data-plane instances are separate processes on one catalog, and each request opens
its own DuckDB connection and ATTACH (`open_ducklake` in the data plane), so a read
routed to one instance after a write through another depends on this. It is DuckDB
and DuckLake behaviour, pinned here against the versions this repo ships. The two
workers below mirror that pattern: a fresh in-memory connection per operation, and
`CALL ducklake_add_data_files`, the call `register_files` makes.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

from conftest import DUCKLAKE_CATALOG_CONNSTR

_WORKER = textwrap.dedent(
    """
    import sys, duckdb
    CONN, DATA = sys.argv[1], sys.argv[2]
    def fresh():
        c = duckdb.connect()
        c.execute("LOAD ducklake; LOAD postgres;")
        c.execute(f"ATTACH 'ducklake:postgres:{CONN}' AS qiita_lake (DATA_PATH '{DATA}/')")
        return c
    held = None
    for line in sys.stdin:
        cmd, *arg = line.split()
        if cmd == "init":
            c = fresh()
            c.execute("CREATE OR REPLACE TABLE qiita_lake.cross_process_probe (i INTEGER)")
            c.close(); out = "ok"
        elif cmd == "add":
            i = int(arg[0]); path = f"{DATA}/cross_process_probe_{i}.parquet"
            c = fresh()
            c.execute(f"COPY (SELECT {i}::INTEGER AS i) TO '{path}' (FORMAT PARQUET)")
            c.execute("CALL ducklake_add_data_files('qiita_lake', 'cross_process_probe', ?)", [path])
            c.close(); out = "ok"
        elif cmd == "count":
            c = fresh()
            out = c.execute("SELECT count(*) FROM qiita_lake.cross_process_probe").fetchone()[0]
            c.close()
        elif cmd == "hold_begin":
            held = fresh(); held.execute("BEGIN TRANSACTION")
            out = held.execute("SELECT count(*) FROM qiita_lake.cross_process_probe").fetchone()[0]
        elif cmd == "hold_count":
            out = held.execute("SELECT count(*) FROM qiita_lake.cross_process_probe").fetchone()[0]
        elif cmd == "hold_end":
            held.execute("COMMIT"); held.close(); held = None; out = "ok"
        print(out, flush=True)
    """
)

_WRITES = 25


class _Worker:
    def __init__(self, data_path: str) -> None:
        self.proc = subprocess.Popen(
            [sys.executable, "-c", _WORKER, DUCKLAKE_CATALOG_CONNSTR, data_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )

    def ask(self, command: str) -> str:
        assert self.proc.stdin and self.proc.stdout
        self.proc.stdin.write(command + "\n")
        self.proc.stdin.flush()
        answer = self.proc.stdout.readline().strip()
        assert answer, f"worker exited on {command!r} (rc={self.proc.poll()})"
        return answer

    def close(self) -> None:
        if self.proc.stdin:
            self.proc.stdin.close()
        self.proc.wait(timeout=30)


def test_a_registration_is_visible_to_another_process_next_connection(
    data_plane,
) -> None:
    data_path = data_plane["data_path"]
    writer, reader = _Worker(data_path), _Worker(data_path)
    try:
        writer.ask("init")
        for i in range(1, _WRITES + 1):
            writer.ask(f"add {i}")
            assert int(reader.ask("count")) == i, (
                f"write {i} not visible to the other process"
            )

        # Control: a read inside a transaction opened before the write keeps its
        # snapshot, so the check above could have failed.
        before = int(reader.ask("hold_begin"))
        writer.ask(f"add {_WRITES + 1}")
        assert int(reader.ask("hold_count")) == before
        reader.ask("hold_end")
        assert int(reader.ask("count")) == _WRITES + 1
    finally:
        writer.close()
        reader.close()
