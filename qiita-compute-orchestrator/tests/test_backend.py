"""Tests for compute backend abstraction."""

from abc import ABC
from pathlib import Path

import pytest


def test_compute_backend_is_abstract():
    """ComputeBackend must be an ABC with abstract methods."""
    from qiita_compute_orchestrator.backend import ComputeBackend

    assert issubclass(ComputeBackend, ABC)
    with pytest.raises(TypeError):
        ComputeBackend()


def test_local_backend_is_concrete():
    """LocalBackend must be a concrete implementation of ComputeBackend."""
    from qiita_compute_orchestrator.backend import ComputeBackend
    from qiita_compute_orchestrator.backends.local import LocalBackend

    assert issubclass(LocalBackend, ComputeBackend)


async def test_slurm_backend_run_step_raises():
    """SlurmBackend.run_step must raise NotImplementedError until production
    SLURM dispatch lands. Asserted regardless of step name — the backend
    is unbuilt across the board, not on a per-step basis."""
    from qiita_compute_orchestrator.backends.slurm import SlurmBackend

    backend = SlurmBackend()
    with pytest.raises(NotImplementedError):
        await backend.run_step("hash", {}, Path("/fake"), reference_idx=1)
    with pytest.raises(NotImplementedError):
        await backend.run_step("load", {}, Path("/fake"), reference_idx=1)


async def test_local_backend_rejects_unknown_step():
    """LocalBackend.run_step raises ValueError for a step it doesn't
    implement — better than a silent no-op when the runner asks for an
    unknown name."""
    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    with pytest.raises(ValueError, match="does not implement step"):
        await backend.run_step("nonexistent", {}, Path("/fake"), reference_idx=1)
