"""Parity guard: the QC thresholds the runner folds into the read-mask identity
must equal the thresholds the real `qc` filter applies.

`runner._QC_RESOLVED_MIN_LENGTH` / `_QC_RESOLVED_FILTER_TAIL` mirror
`qc._MIN_LENGTH` / `_FILTER_READ_TAIL` so the minted `mask_idx`'s params describe
the filter that actually ran. The control-plane venv does NOT depend on the
orchestrator package (and qc.py pulls in heavy bioinformatics deps at import), so
this reads the two constants out of the qc.py source via AST — no import, no
execution — and asserts equality. If the real filter's thresholds drift without
the runner's copies following, the minted mask identity would silently misdescribe
the filter; this test fails first.
"""

import ast
from pathlib import Path

from qiita_control_plane import runner

# qiita-compute-orchestrator lives as a sibling package in the monorepo checkout.
_QC_SOURCE = (
    Path(__file__).resolve().parents[2]
    / "qiita-compute-orchestrator"
    / "src"
    / "qiita_compute_orchestrator"
    / "jobs"
    / "qc.py"
)


def _module_constant(source_path: Path, name: str):
    """Return the literal value assigned to a module-level constant, parsed from
    source via AST (no import — qc.py's runtime deps are not installed here)."""
    tree = ast.parse(source_path.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found as a module-level constant in {source_path}")


def test_qc_resolved_thresholds_match_real_filter():
    assert _QC_SOURCE.exists(), f"expected qc.py at {_QC_SOURCE}"
    assert runner._QC_RESOLVED_MIN_LENGTH == _module_constant(_QC_SOURCE, "_MIN_LENGTH")
    assert runner._QC_RESOLVED_FILTER_TAIL == _module_constant(_QC_SOURCE, "_FILTER_READ_TAIL")
