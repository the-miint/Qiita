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


async def test_slurm_backend_hash_job_raises():
    """SlurmBackend.run_hash_job must raise NotImplementedError."""
    from qiita_compute_orchestrator.backends.slurm import SlurmBackend

    backend = SlurmBackend()
    with pytest.raises(NotImplementedError):
        await backend.run_hash_job(Path("/fake"), Path("/fake"), 1)


async def test_slurm_backend_load_job_raises():
    """SlurmBackend.run_load_job must raise NotImplementedError."""
    from qiita_compute_orchestrator.backends.slurm import SlurmBackend

    backend = SlurmBackend()
    with pytest.raises(NotImplementedError):
        await backend.run_load_job(Path("/fake"), Path("/fake"), Path("/fake"), Path("/fake"), 1)
