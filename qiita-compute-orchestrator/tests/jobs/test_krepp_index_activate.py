"""jobs.krepp_index_activate against the small index from conftest, registered and
copied to a stand-in PATH_NODE_LOCAL. The reference's tree and sequences, which the job
streams from the data plane, are stubbed by local tables; tips g0..g3 carry
feature_idx 10..13."""

from __future__ import annotations

import shutil
import types
from contextlib import asynccontextmanager

import pytest
from helpers import KREPP_TREE_NEWICK, krepp_genomes, krepp_recorded_version
from qiita_common.krepp import KreppActivationReport, KreppBuildRecord

from qiita_compute_orchestrator.derived_store import krepp_build_relpath
from qiita_compute_orchestrator.jobs import krepp_index_activate as job
from qiita_compute_orchestrator.krepp_build import (
    JEST_LABELLED_TREE,
    JEST_NULL,
    KreppBuildInvalid,
    write_build_files,
)

_JOB = "qiita_compute_orchestrator.jobs.krepp_index_activate"


@pytest.fixture
def node(krepp_built_index, tmp_path, monkeypatch):
    rel = krepp_build_relpath(18, "t1")
    master = tmp_path / "derived" / rel
    shutil.copytree(krepp_built_index, master / "index")
    (master / "jest").mkdir()
    (master / JEST_LABELLED_TREE).write_text("(a{0});\n")
    (master / JEST_NULL).write_text("0.1\n")
    sums, _ = write_build_files(master, reference_idx=18, build_label="t1")
    node_root = tmp_path / "node"
    shutil.copytree(master, node_root / rel)
    record = tmp_path / "record.json"
    record.write_text(
        KreppBuildRecord(
            reference_index_idx=5, reference_idx=18, fs_path=rel, sha256sums_sha256=sums
        ).model_dump_json()
    )
    nwk = tmp_path / "lake.nwk"
    nwk.write_text(KREPP_TREE_NEWICK)
    state = {"edge": "node_index"}

    @asynccontextmanager
    async def _tree(conn, *, reference_idx, relation="reference_phylogeny"):
        conn.execute(
            f"CREATE OR REPLACE TEMP VIEW {relation} AS SELECT * REPLACE ({state['edge']} AS"
            " edge_id), CASE WHEN is_tip THEN 10 + CAST(substr(name, 2) AS BIGINT) END"
            f" AS feature_idx FROM read_newick('{nwk}')"
        )
        yield relation

    @asynccontextmanager
    async def _chunks(conn, *, reference_idx, feature_idx, relation="reference_chunks"):
        rows = [(10 + int(k[1:]), 0, v) for k, v in krepp_genomes().items()]
        conn.execute(
            f"CREATE OR REPLACE TEMP TABLE {relation}"
            " (feature_idx BIGINT, chunk_index INTEGER, chunk_data VARCHAR)"
        )
        conn.executemany(f"INSERT INTO {relation} VALUES (?, ?, ?)", rows)
        yield relation

    monkeypatch.setattr(f"{_JOB}.open_reference_phylogeny_stream", _tree)
    monkeypatch.setattr(f"{_JOB}.open_reference_chunk_stream", _chunks)
    monkeypatch.setattr(
        f"{_JOB}.get_settings", lambda: types.SimpleNamespace(path_node_local=node_root)
    )
    built = krepp_recorded_version(master / "index")
    monkeypatch.setattr(f"{_JOB}.linked_krepp_version", lambda _conn: built)
    return types.SimpleNamespace(record=record, state=state, copy=node_root / rel)


async def _run(node, tmp_path, minimum=0.75) -> KreppActivationReport:
    out = await job.execute(
        job.Inputs(
            reference_idx=18,
            work_ticket_idx=1,
            krepp_build_record=node.record,
            min_own_edge_share=minimum,
        ),
        tmp_path,
    )
    return KreppActivationReport.model_validate_json(out["krepp_activation_report"].read_text())


async def test_tips_land_on_their_own_edges(node, tmp_path):
    report = await _run(node, tmp_path)
    assert (report.tips, report.placed, report.own_edge) == (4, 4, 4)
    assert report.passed and report.reference_index_idx == 5


async def test_a_tree_numbered_another_way_fails_the_known_answer_check(node, tmp_path):
    """The control: the same build and tips against the same tree numbered +100."""
    node.state["edge"] = "node_index + 100"
    report = await _run(node, tmp_path)
    assert report.own_edge == 0
    assert not report.passed


async def test_a_bad_node_copy_fails_before_placing(node, tmp_path):
    (node.copy / "index" / "stray").write_text("x")
    with pytest.raises(KreppBuildInvalid) as exc:
        await _run(node, tmp_path)
    assert exc.value.check == "files"
