"""A DuckLake registration made by one process is visible to another process's
next query, both on a connection it opens then and on one it attached earlier.

Data-plane instances are separate processes on one catalog, and each serves
requests from a pool of connections attached once and reused (`pool.rs`), so a
read routed to one instance after a write through another depends on this. It is
DuckDB and DuckLake behaviour, pinned here against the versions this repo ships.
The writer opens a fresh in-memory connection per write and calls
`ducklake_add_data_files`, the call `register_files` makes; the reader checks
with a fresh connection and with one it keeps attached, as a pool member does.
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
    COUNT = "SELECT count(*) FROM qiita_lake.cross_process_probe"
    held = None
    pooled = None
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
            add = "CALL ducklake_add_data_files('qiita_lake', 'cross_process_probe', ?)"
            c.execute(add, [path])
            c.close(); out = "ok"
        elif cmd == "count":
            c = fresh()
            out = c.execute("SELECT count(*) FROM qiita_lake.cross_process_probe").fetchone()[0]
            c.close()
        elif cmd == "pooled_count":
            if pooled is None:
                pooled = fresh()
            out = pooled.execute(COUNT).fetchone()[0]
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


def test_a_registration_is_visible_to_another_process_next_query(
    data_plane,
) -> None:
    data_path = data_plane["data_path"]
    writer, reader = _Worker(data_path), _Worker(data_path)
    try:
        writer.ask("init")
        for i in range(1, _WRITES + 1):
            writer.ask(f"add {i}")
            assert int(reader.ask("count")) == i, (
                f"write {i} not visible to the other process's new connection"
            )
            assert int(reader.ask("pooled_count")) == i, (
                f"write {i} not visible to the other process's attached connection"
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
