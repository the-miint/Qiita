"""SIGTERM drains the data plane: a DoGet stream open when the signal lands is
served to its last row, a new request is refused, and the process exits 0. A
second SIGTERM during the drain does not cut it short; `systemctl kill -s KILL`
is what does (docs/deployments.md).

`systemctl stop` and `restart` send SIGTERM, so this is what a deploy does to a
stream in flight. Its own module because it stops the module's data plane.
"""

import signal
import threading
import time

import pyarrow.flight as flight
from qiita_common.api_paths import LOOPBACK_HOST

from conftest import ducklake_connect

N_ROWS = 50_000
SEQ_LEN = 1000
# Slow enough that the stream is still open when the signal lands and when the
# second request is made, SIGNAL_AFTER_S + 0.5 s in. At 0.05 s a stream ended
# 6.5 ms before that request.
READ_SLEEP_S = 0.2
SIGNAL_AFTER_S = 1.0
SECOND_SIGNAL_AFTER_S = 0.25


def _seed(data_path: str) -> None:
    conn = ducklake_connect(data_path)
    try:
        conn.execute(
            f"INSERT INTO qiita_lake.reference_sequence_chunks "
            f"SELECT 900000000 + i, 0, repeat('ACGT', {SEQ_LEN // 4}) FROM range({N_ROWS}) t(i)"
        )
        conn.execute(
            f"INSERT INTO qiita_lake.reference_membership "
            f"SELECT 1, 900000000 + i FROM range({N_ROWS}) t(i)"
        )
    finally:
        conn.close()


def test_sigterm_finishes_an_open_stream_refuses_new_requests_and_exits_zero_when_repeated(
    data_plane,
):
    from qiita_control_plane.auth.tickets import sign_ticket

    proc = data_plane["process"]
    _seed(data_plane["data_path"])
    ticket = flight.Ticket(
        sign_ticket(
            table="reference_sequence_chunks",
            filter={"reference_idx": [1]},
            secret=data_plane["secret"],
        )
    )
    location = f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"
    during_drain: dict[str, object] = {}

    def signal_then_request() -> None:
        time.sleep(SIGNAL_AFTER_S)
        proc.send_signal(signal.SIGTERM)
        time.sleep(SECOND_SIGNAL_AFTER_S)
        proc.send_signal(signal.SIGTERM)
        time.sleep(0.5 - SECOND_SIGNAL_AFTER_S)
        client = flight.FlightClient(location)
        during_drain["requested_at"] = time.monotonic()
        try:
            client.do_get(ticket).read_all()
            during_drain["outcome"] = "served"
        except flight.FlightUnavailableError as exc:
            during_drain["outcome"] = exc
        finally:
            client.close()

    signaller = threading.Thread(target=signal_then_request)
    signaller.start()
    client = flight.FlightClient(location)
    rows = 0
    reader = client.do_get(ticket)
    while True:
        try:
            rows += reader.read_chunk().data.num_rows
        except StopIteration:
            break
        time.sleep(READ_SLEEP_S)
    stream_ended_at = time.monotonic()
    client.close()
    signaller.join()

    assert proc.wait(timeout=60) == 0
    assert rows == N_ROWS
    # The request was made while the first stream was still being read, so the
    # refusal is the drain's, not a process that had already exited.
    assert during_drain["requested_at"] < stream_ended_at
    assert isinstance(during_drain["outcome"], flight.FlightUnavailableError), (
        during_drain
    )
