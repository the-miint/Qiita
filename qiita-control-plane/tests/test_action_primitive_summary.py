"""The receipt `summary` counts the action primitives report through `summarize()`.

The primitive's own return values are covered elsewhere (`test_actions_library`,
`test_register_files_payload`); this pins the wiring that turns them into the step
receipt's summary — so a reader of the ticket log sees `features_new`/`features_known`
for a mint and `tables` for a register, not a silently-empty summary."""

from pathlib import Path

from qiita_common.actions import LibraryPrimitive, WorkflowAction
from qiita_common.step_log import capture_to

from qiita_control_plane.runner import _reconstruct
from qiita_control_plane.testing.run_context import run_context


def _ctx():
    # No primitive below touches the pool (the real LIBRARY call is stubbed), so a
    # sentinel pool is enough — a path that reached it would fail loudly, not stand in.
    return run_context(
        pool=object(),
        work_ticket_idx=7,
        scope_target={"kind": "sequenced_pool", "sequenced_pool_idx": 1},
        signing_key=b"\x00" * 32,
        data_plane_url="grpc://unused:50051",
    )


async def test_mint_features_summarizes_new_and_known(tmp_path, monkeypatch):
    async def _fake_mint(pool, manifest_path, workspace, *, genome_map_path, scope):
        return Path("feature_map.parquet"), 5, 2

    monkeypatch.setitem(_reconstruct.LIBRARY, LibraryPrimitive.MINT_FEATURES, _fake_mint)
    (tmp_path / "manifest.tsv").write_text("")

    with capture_to(tmp_path / "logs", "mint-features") as scope:
        out = await _reconstruct._run_action_primitive(
            _ctx(),
            WorkflowAction(
                kind="action",
                name=LibraryPrimitive.MINT_FEATURES,
                inputs=["manifest"],
                outputs=["feature_map"],
            ),
            {"manifest": str(tmp_path / "manifest.tsv")},
            tmp_path,
        )

    assert out == {"feature_map": Path("feature_map.parquet")}
    assert scope.summary == {"features_new": 5, "features_known": 2}


async def test_register_files_summarizes_one_count_per_table(tmp_path, monkeypatch):
    staging = tmp_path / "staging"
    (staging / "reference_sequence_chunks").mkdir(parents=True)
    (staging / "reference_membership.parquet").write_bytes(b"")
    (staging / "reference_sequence_chunks" / "part_00000.parquet").write_bytes(b"")

    async def _fake_register(*, staging_dir, files, work_ticket_idx, signing_key, data_plane_url):
        return None

    monkeypatch.setitem(_reconstruct.LIBRARY, LibraryPrimitive.REGISTER_FILES, _fake_register)

    with capture_to(tmp_path / "logs", "register-files") as scope:
        out = await _reconstruct._run_action_primitive(
            _ctx(),
            WorkflowAction(
                kind="action", name=LibraryPrimitive.REGISTER_FILES, inputs=["staging"], outputs=[]
            ),
            {"staging": str(staging)},
            tmp_path / "attempt",
        )

    assert out == {}
    # One per staged table, whatever the file count within it.
    assert scope.summary == {"tables": {"reference_membership": 1, "reference_sequence_chunks": 1}}
