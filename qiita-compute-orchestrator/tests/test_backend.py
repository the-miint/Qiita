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
    """LocalBackend.run_step raises BackendFailure(CONTRACT_VIOLATION)
    for a step it doesn't implement — better than a silent no-op when
    the runner asks for an unknown name. CONTRACT_VIOLATION (permanent)
    rather than a generic ValueError because retry won't help: same
    YAML against the same backend will always miss."""
    from qiita_common.backend_failure import BackendFailure, FailureKind
    from qiita_common.models import WorkTicketFailureStage

    from qiita_compute_orchestrator.backends.local import LocalBackend

    backend = LocalBackend()
    with pytest.raises(BackendFailure) as ei:
        await backend.run_step("nonexistent", {}, Path("/fake"), reference_idx=1)
    assert ei.value.kind == FailureKind.CONTRACT_VIOLATION
    assert ei.value.stage == WorkTicketFailureStage.STEP_RUN
    assert ei.value.step_name == "nonexistent"
    assert not ei.value.transient
    assert "does not implement step" in ei.value.reason
