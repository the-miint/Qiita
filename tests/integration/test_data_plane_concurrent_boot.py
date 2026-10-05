"""Data-plane processes booting together against one DuckLake catalog.

A deploy restarts every instance in QIITA_DATA_PLANE_PORTS back to back, and a reboot
starts every enabled instance at once, so boots overlap. Every boot writes the catalog
(`ducklake::setup_catalog` in the data plane, which carries the conflicts it retries);
these pin that overlapping boots all come up, on an empty catalog and on one that
already exists.
"""

from __future__ import annotations

import subprocess
import time
from pathlib import Path

from qiita_common.api_paths import LOOPBACK_HOST

from conftest import (
    DATA_PLANE_BINARY,
    DATA_PLANE_START_TIMEOUT_S,
    alloc_free_port,
    data_plane_env,
    reset_ducklake_catalog,
    wait_for_grpc,
)

_INSTANCES = 4


def _boot_together(signing_key: bytes, root: Path, count: int) -> dict[int, str]:
    """Start `count` data planes at once and wait for each to listen or exit.
    Returns {port: the last lines of its output} for every one that did not come up."""
    scratch, persistent = root / "scratch", root / "persistent"
    (scratch / "staging").mkdir(parents=True, exist_ok=True)
    (persistent / "ducklake").mkdir(parents=True, exist_ok=True)
    ports = [alloc_free_port() for _ in range(count)]
    procs = {
        port: subprocess.Popen(
            [str(DATA_PLANE_BINARY)],
            env=data_plane_env(signing_key, scratch, persistent, port),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        for port in ports
    }
    try:
        pending, failed = set(ports), {}
        deadline = time.monotonic() + DATA_PLANE_START_TIMEOUT_S
        while pending and time.monotonic() < deadline:
            for port in list(pending):
                if procs[port].poll() is not None:
                    failed[port] = f"exited {procs[port].returncode}"
                    pending.discard(port)
                elif wait_for_grpc(LOOPBACK_HOST, port, timeout=0.2):
                    pending.discard(port)
        failed.update({port: "did not listen in time" for port in pending})
    finally:
        for proc in procs.values():
            proc.terminate()
        for port, proc in procs.items():
            output = proc.communicate(timeout=30)[0].decode(errors="replace")
            if port in failed:
                failed[port] += ": " + " | ".join(output.strip().splitlines()[-3:])
    return failed


def test_data_planes_booting_together_on_an_empty_catalog_all_come_up(
    signing_key: bytes, tmp_path: Path
) -> None:
    reset_ducklake_catalog()
    assert _boot_together(signing_key, tmp_path, _INSTANCES) == {}


def test_data_planes_booting_together_on_an_existing_catalog_all_come_up(
    signing_key: bytes, tmp_path: Path
) -> None:
    reset_ducklake_catalog()
    assert _boot_together(signing_key, tmp_path, 1) == {}, (
        "the catalog could not be created"
    )
    assert _boot_together(signing_key, tmp_path, _INSTANCES) == {}
