"""jobs.krepp_index_register against the small index from conftest: the meta it hands
register-index, and its refusals. The reference's tree, which the job streams from the
data plane, is stubbed by a local table."""

from __future__ import annotations

import json
import shutil
import types
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from helpers import KREPP_TREE_NEWICK, krepp_recorded_version
from qiita_common.krepp import SHA256SUMS_SHA256_PARAM

from qiita_compute_orchestrator.derived_store import krepp_build_relpath
from qiita_compute_orchestrator.jobs import krepp_index_register as job
from qiita_compute_orchestrator.krepp_build import (
    JEST_LABELLED_TREE,
    JEST_NULL,
    KreppBuildInvalid,
)

_JOB = "qiita_compute_orchestrator.jobs.krepp_index_register"
_LABELLED = "((g0:1{0},g1:1{1}):1{2},(g2:1{3},g3:1{4}):1{5}){6};\n"


@pytest.fixture
def derived(krepp_built_index, tmp_path, monkeypatch):
    """PATH_DERIVED holding build `t1` of reference 18, and a hook to set the tree the
    data plane would stream for the reference (default: the build's own, numbered
    edge_id = node_index)."""
    root = tmp_path / "derived"
    folder = root / krepp_build_relpath(18, "t1")
    shutil.copytree(krepp_built_index, folder / "index")
    (folder / "jest").mkdir()
    (folder / JEST_LABELLED_TREE).write_text(_LABELLED)
    (folder / JEST_NULL).write_text("0.1\n")
    monkeypatch.setattr(f"{_JOB}.get_settings", lambda: types.SimpleNamespace(path_derived=root))
    state = {
        "lake_sql": "SELECT * REPLACE (node_index AS edge_id) FROM read_newick('{nwk}')",
    }
    nwk = tmp_path / "lake.nwk"
    nwk.write_text(KREPP_TREE_NEWICK)

    @asynccontextmanager
    async def _stream(conn, *, reference_idx, relation="reference_phylogeny"):
        assert reference_idx == 18
        conn.execute(
            f"CREATE OR REPLACE TEMP VIEW {relation} AS {state['lake_sql'].format(nwk=nwk)}"
        )
        yield relation

    monkeypatch.setattr(f"{_JOB}.open_reference_phylogeny_stream", _stream)
    return types.SimpleNamespace(folder=folder, state=state)


async def _run(tmp_path: Path) -> dict:
    out = await job.execute(
        job.Inputs(reference_idx=18, work_ticket_idx=1, build_label="t1"), tmp_path
    )
    return json.loads(out["krepp_index_meta"].read_text())


async def test_registers_a_build_whose_tree_is_the_references(derived, tmp_path):
    meta = await _run(tmp_path)
    assert meta["index_type"] == "krepp"
    assert meta["fs_path"] == "references/18/krepp/t1"
    assert (meta["state"], meta["fs_path_root"]) == ("registered", "node_local")
    assert meta["params"]["m"] == 4
    assert len(meta["params"][SHA256SUMS_SHA256_PARAM]) == 64
    assert (derived.folder / "SHA256SUMS").is_file()


async def test_refuses_a_tree_numbered_another_way(derived, tmp_path):
    derived.state["lake_sql"] = (
        "SELECT * REPLACE (node_index + 100 AS edge_id) FROM read_newick('{nwk}')"
    )
    with pytest.raises(KreppBuildInvalid, match="is not reference 18's tree") as exc:
        await _run(tmp_path)
    assert exc.value.check == "tree"
    assert not (derived.folder / "SHA256SUMS").exists()


async def test_refuses_a_reference_tree_without_edge_numbers(derived, tmp_path):
    derived.state["lake_sql"] = (
        "SELECT * REPLACE (NULL::BIGINT AS edge_id) FROM read_newick('{nwk}')"
    )
    with pytest.raises(KreppBuildInvalid, match="mint-edge-id"):
        await _run(tmp_path)


async def test_refuses_a_jest_tree_with_other_edges(derived, tmp_path):
    (derived.folder / JEST_LABELLED_TREE).write_text(_LABELLED.replace("{6}", "{9}"))
    with pytest.raises(KreppBuildInvalid, match="JEST's labelled tree"):
        await _run(tmp_path)


async def test_records_the_builds_krepp_version(derived, tmp_path):
    meta = await _run(tmp_path)
    assert meta["params"]["krepp_version"] == krepp_recorded_version(derived.folder / "index")
