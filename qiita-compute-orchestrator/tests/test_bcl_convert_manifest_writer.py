"""Unit tests for workflows/bcl-convert/manifest_writer.py.

The script lives inside the Apptainer image at /opt/qiita/manifest_writer.py
and runs there under the container's python3, with stdlib only. Tests load
it via importlib (no installed package home, no entry point) so we exercise
the actual file the container will ship, not a copy.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

_MANIFEST_WRITER_PATH = (
    Path(__file__).resolve().parents[2] / "workflows" / "bcl-convert" / "manifest_writer.py"
)


def _load_manifest_writer():
    spec = importlib.util.spec_from_file_location(
        "bcl_convert_manifest_writer", _MANIFEST_WRITER_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _seed_output_tree(root: Path) -> None:
    """Write a nested ConvertJob output that mimics bcl-convert's real
    layout: per-project subdirs, per-sample FASTQ pairs, plus the auxiliary
    Logs / Reports trees bcl-convert always emits."""
    (root / "ConvertJob" / "Project_X").mkdir(parents=True)
    (root / "ConvertJob" / "Project_X" / "1_S1_L001_R1_001.fastq.gz").write_bytes(b"R1")
    (root / "ConvertJob" / "Project_X" / "1_S1_L001_R2_001.fastq.gz").write_bytes(b"R2!")
    (root / "ConvertJob" / "Project_X" / "2_S2_L001_R1_001.fastq.gz").write_bytes(b"R1R1")
    (root / "ConvertJob" / "Logs").mkdir()
    (root / "ConvertJob" / "Logs" / "Run.log").write_text("ok\n")
    (root / "ConvertJob" / "Reports").mkdir()
    (root / "ConvertJob" / "Reports" / "Demultiplex_Stats.csv").write_text("a,b\n")


def test_manifest_walks_nested_dirs_and_emits_expected_schema(tmp_path: Path) -> None:
    mw = _load_manifest_writer()
    _seed_output_tree(tmp_path)

    mw.main(["manifest_writer.py", str(tmp_path), "convert_dir=ConvertJob"])

    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["outputs"] == {"convert_dir": "ConvertJob"}

    file_paths = {entry["path"] for entry in manifest["files"]}
    expected = {
        "ConvertJob/Project_X/1_S1_L001_R1_001.fastq.gz",
        "ConvertJob/Project_X/1_S1_L001_R2_001.fastq.gz",
        "ConvertJob/Project_X/2_S2_L001_R1_001.fastq.gz",
        "ConvertJob/Logs/Run.log",
        "ConvertJob/Reports/Demultiplex_Stats.csv",
    }
    assert file_paths == expected
    # manifest.json itself must not enumerate itself in `files`.
    assert "manifest.json" not in file_paths

    sizes = {entry["path"]: entry["size_bytes"] for entry in manifest["files"]}
    assert sizes["ConvertJob/Project_X/1_S1_L001_R1_001.fastq.gz"] == 2
    assert sizes["ConvertJob/Project_X/1_S1_L001_R2_001.fastq.gz"] == 3
    assert sizes["ConvertJob/Logs/Run.log"] == 3


def test_manifest_files_sorted_by_path_for_reproducible_diffs(tmp_path: Path) -> None:
    """A second run on the same tree must produce a byte-identical manifest
    so a re-run during recovery doesn't churn the on-disk artifact."""
    mw = _load_manifest_writer()
    _seed_output_tree(tmp_path)
    mw.main(["manifest_writer.py", str(tmp_path), "convert_dir=ConvertJob"])
    first = (tmp_path / "manifest.json").read_bytes()
    mw.main(["manifest_writer.py", str(tmp_path), "convert_dir=ConvertJob"])
    second = (tmp_path / "manifest.json").read_bytes()
    assert first == second


def test_manifest_rejects_malformed_output_arg(tmp_path: Path) -> None:
    """A bare token without `=` is a contract violation; the verifier
    would otherwise fail in a confusing way at the orchestrator boundary."""
    mw = _load_manifest_writer()
    (tmp_path / "x.txt").write_text("y")

    with pytest.raises(SystemExit) as exc:
        mw.main(["manifest_writer.py", str(tmp_path), "convert_dir"])
    assert "<name>=<relpath>" in str(exc.value)


def test_manifest_rejects_duplicate_output_name(tmp_path: Path) -> None:
    """Two args with the same output name would silently drop one;
    surface as a contract violation."""
    mw = _load_manifest_writer()

    with pytest.raises(SystemExit) as exc:
        mw.main(
            [
                "manifest_writer.py",
                str(tmp_path),
                "convert_dir=ConvertJob",
                "convert_dir=Other",
            ]
        )
    assert "duplicate output name" in str(exc.value)


def test_manifest_writer_runs_via_subprocess(tmp_path: Path) -> None:
    """Smoke-test the script the way the container invokes it: via
    `python3 /opt/qiita/manifest_writer.py <output_root> convert_dir=ConvertJob`.
    Catches __main__ guard regressions and Python-version-specific
    syntax issues a direct import would miss."""
    import subprocess

    _seed_output_tree(tmp_path)
    result = subprocess.run(
        [sys.executable, str(_MANIFEST_WRITER_PATH), str(tmp_path), "convert_dir=ConvertJob"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.returncode == 0
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["outputs"] == {"convert_dir": "ConvertJob"}
