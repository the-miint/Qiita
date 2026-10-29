"""Pin the assemble step's myloasm provenance dump.

`workflows/long-read-assembly/myloasm_provenance.py` runs inside the assemble
container, after the splitter, and writes each `read_myloasm_*` table verbatim into
`genomes_dir/<MYLOASM_RAW_DIR>/<table>.parquet`. The assembler's output tree is not a
declared output, so no later step can read it; this dump is how its read placements
reach `assembly_load`.

The behavioural tests run the script against the real miint this component's
conftest stages, over committed myloasm 0.6.0 and 0.7.0 output
(`tests/data/myloasm/`). The static pins cover the wiring that only runs inside the
built image.
"""

from __future__ import annotations

import importlib.util
import os
import re
import sys
from pathlib import Path

import duckdb
import pytest
from myloasm_fixture import (
    MYLOASM_FIXTURE_ROWS,
    MYLOASM_FIXTURE_VERSIONS,
    PROVENANCE_SCRIPT,
    myloasm_output_dir,
    run_provenance_dump,
)
from qiita_common.myloasm_constants import MYLOASM_RAW_DIR, MYLOASM_TABLES

from qiita_compute_orchestrator.miint import open_miint_conn

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW_DIR = _REPO_ROOT / "workflows" / "long-read-assembly"
_ASSEMBLE_SH = _WORKFLOW_DIR / "assemble.sh"
_ASSEMBLE_DEF = _WORKFLOW_DIR / "assemble.def"
_ASSEMBLE_ENV = _WORKFLOW_DIR / "sif-build.d" / "assemble.env"

_EXIT_CONTRACT_VIOLATION = 64


def _script_module():
    spec = importlib.util.spec_from_file_location("myloasm_provenance", PROVENANCE_SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(_WORKFLOW_DIR))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.remove(str(_WORKFLOW_DIR))
    return module


@pytest.mark.parametrize("version", MYLOASM_FIXTURE_VERSIONS)
def test_every_reader_is_dumped_verbatim(tmp_path: Path, version: str) -> None:
    """One Parquet per lake table, holding exactly what miint's reader returns: the
    same columns and the same rows."""
    asm_dir = myloasm_output_dir(tmp_path / "assembler", version)
    out_dir = tmp_path / "genomes" / MYLOASM_RAW_DIR

    result = run_provenance_dump(asm_dir, out_dir)
    assert result.returncode == 0, result.stderr

    assert sorted(p.name for p in out_dir.iterdir()) == sorted(
        f"{table}.parquet" for table in MYLOASM_TABLES
    )
    with open_miint_conn() as con:
        for table, spec in MYLOASM_TABLES.items():
            dumped = out_dir / f"{table}.parquet"
            assert con.execute(f"SELECT count(*) FROM '{dumped}'").fetchone() == (
                MYLOASM_FIXTURE_ROWS[table],
            ), table
            reader_cols = con.execute(f"DESCRIBE FROM {spec.reader}('{asm_dir}')").fetchall()
            dumped_cols = con.execute(f"DESCRIBE FROM '{dumped}'").fetchall()
            assert [c[:2] for c in dumped_cols] == [c[:2] for c in reader_cols], table


@pytest.mark.parametrize("version", MYLOASM_FIXTURE_VERSIONS)
def test_every_kept_column_already_has_its_lake_type(tmp_path: Path, version: str) -> None:
    """Every reader column the lake keeps under its own name arrives with the lake's
    type, so the transform's CAST changes nothing for it. A reader that changes a
    column's type fails here rather than being converted in silence."""
    asm_dir = myloasm_output_dir(tmp_path / "assembler", version)
    with open_miint_conn() as con:
        for table, spec in MYLOASM_TABLES.items():
            raw = dict(
                c[:2] for c in con.execute(f"DESCRIBE FROM {spec.reader}('{asm_dir}')").fetchall()
            )
            kept = {name: sqltype for name, sqltype in spec.columns if name in raw}
            assert kept, table
            assert {name: raw[name] for name in kept} == kept, table


def test_tagless_links_are_null_not_zero_on_0_6_0(tmp_path: Path) -> None:
    """myloasm 0.6.0 writes no overlap evidence on its graph edges; the dump keeps
    that as NULL, never a zero that would read as a measured overlap."""
    asm_dir = myloasm_output_dir(tmp_path / "assembler", "0.6.0")
    out_dir = tmp_path / "out"
    assert run_provenance_dump(asm_dir, out_dir).returncode == 0
    with duckdb.connect() as con:
        assert con.execute(
            "SELECT count(*), count(overlap_length), count(from_read_id) "
            f"FROM '{out_dir}/myloasm_link.parquet'"
        ).fetchone() == (2, 0, 0)


def test_the_dumped_tables_are_the_contracts_tables() -> None:
    """The container cannot import qiita_common, so the script carries its own
    table -> reader map; it must be the contract's."""
    assert _script_module().READERS == {t: spec.reader for t, spec in MYLOASM_TABLES.items()}


def test_a_directory_myloasm_did_not_finish_fails_loud(tmp_path: Path) -> None:
    """A missing member file is miint's error, surfaced as the step's failure."""
    asm_dir = myloasm_output_dir(tmp_path / "assembler", "0.7.0")
    (asm_dir / "3-mapping" / "map_to_unitigs.paf.gz").unlink()
    result = run_provenance_dump(asm_dir, tmp_path / "out")
    assert result.returncode == _EXIT_CONTRACT_VIOLATION
    assert "map_to_unitigs.paf.gz" in result.stderr


def test_missing_extension_directory_fails_loud(tmp_path: Path) -> None:
    asm_dir = myloasm_output_dir(tmp_path / "assembler", "0.7.0")
    env = dict(os.environ)
    env.pop("MIINT_EXTENSION_DIRECTORY", None)
    result = run_provenance_dump(asm_dir, tmp_path / "out", env=env)
    assert result.returncode == _EXIT_CONTRACT_VIOLATION
    assert "MIINT_EXTENSION_DIRECTORY" in result.stderr


def test_assemble_sh_dumps_provenance_only_for_a_non_empty_assembly() -> None:
    """The dump runs in the myloasm arm, inside the same non-empty-primary guard as
    the splitter (miint refuses an empty FASTA), and writes under genomes_dir."""
    src = _ASSEMBLE_SH.read_text()
    myloasm_arm = src[src.index("    myloasm)") : src.index("    *)")]
    guard = myloasm_arm[myloasm_arm.index('if [[ -s "${PRIMARY}" ]]; then') :]
    guard = guard[: guard.index("\n        fi\n")]
    assert re.search(
        r'python3 /opt/qiita/myloasm_provenance\.py\s+\\?\s*"\$\{ASM_DIR\}"\s+'
        rf'"\$\{{OUT\}}/{MYLOASM_RAW_DIR}"',
        guard,
    ), "assemble.sh must dump provenance from ${ASM_DIR} into ${OUT}/" + MYLOASM_RAW_DIR


def test_script_uses_the_staged_miint_and_never_installs() -> None:
    src = PROVENANCE_SCRIPT.read_text()
    assert "from miint_connect import" in src
    assert "duckdb.connect" not in src
    assert not re.search(r"[\"']\s*INSTALL\b", src)


def test_image_copies_and_hashes_the_script() -> None:
    """%files copies it where assemble.sh calls it, %test greps the call, and the
    build spec hashes it so an edit rebuilds the image."""
    definition = _ASSEMBLE_DEF.read_text()
    assert re.search(
        r"^\s*myloasm_provenance\.py /opt/qiita/myloasm_provenance\.py$", definition, re.M
    )
    assert r"python3 /opt/qiita/myloasm_provenance\.py" in definition
    env = _ASSEMBLE_ENV.read_text()
    (hash_inputs,) = re.findall(r'^HASH_INPUTS="([^"]*)"', env, re.M)
    assert "myloasm_provenance.py" in hash_inputs.split()
