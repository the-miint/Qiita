"""SLURM backend internals.

Split into three concerns, each unit-testable in isolation:

- `payload`: builds the slurmrestd job-submit JSON dict from a
  workflow step's metadata. Pure function, no I/O.
- `verify`: walks `$QIITA_OUTPUT_PATH/manifest.json` and validates the
  three container-contract gates (manifest exists, every listed file
  exists at declared size, all output files mode 440). Pure file-system,
  no HTTP.
- `client`: thin httpx wrapper around slurmrestd's submit / get-status
  routes, plus JWT loading and refresh.

`SlurmBackend.run_step` (in backends/slurm.py) wires them together:
write `params.json` => submit via client => poll until terminal =>
verify output => return name => path map.
"""

from .client import (
    DEFAULT_SLURMRESTD_API_VERSION,
    SlurmJobInfo,
    SlurmrestdClient,
    SlurmrestdError,
    TerminalSlurmState,
)
from .launcher_failure import LauncherFailure, parse_launcher_failure
from .payload import build_job_submit_payload
from .verify import VerificationFailure, parse_outputs_map, verify_container_output

__all__ = [
    "DEFAULT_SLURMRESTD_API_VERSION",
    "LauncherFailure",
    "SlurmJobInfo",
    "SlurmrestdClient",
    "SlurmrestdError",
    "TerminalSlurmState",
    "VerificationFailure",
    "build_job_submit_payload",
    "parse_launcher_failure",
    "parse_outputs_map",
    "verify_container_output",
]
