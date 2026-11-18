"""`qiita-admin backfill wol-genome-repair`: the handler around the planner and the
apply. Both are replaced (their DB tests pin them); what is pinned here is what the
operator sees, which arguments it insists on, and that a dry run never applies."""

import re
from pathlib import Path

import asyncpg
import pytest

from qiita_control_plane.backfill.wol_genome_repair import (
    RepairCounts,
    RepairPlan,
    RepairPostconditionFailed,
    RepairRefused,
)
from qiita_control_plane.cli.admin import backfill_wol_genome_repair as cli
from qiita_control_plane.cli.admin import main

_COUNTS = RepairCounts(
    already_linked=195943,
    to_link=117,
    to_create=2,
    stale_links=117,
    labels_to_retire=46621,
    derivations_to_add=196062,
    derivations_present=0,
)
_PLAN = RepairPlan(reference_idx=18, parents=(), counts=_COUNTS)


@pytest.fixture
def stubbed(monkeypatch, tmp_path):
    seen: dict = {"applied": None}

    class _Pool:
        async def close(self):
            seen["closed"] = True

    async def _open(url):
        seen["url"] = url
        return _Pool()

    async def _plan(pool, *, reference_idx, metadata_path):
        seen["reference_idx"], seen["metadata_path"] = reference_idx, metadata_path
        return _PLAN

    async def _apply(pool, plan, *, manifest_path):
        seen["applied"] = (plan, manifest_path)
        return _COUNTS

    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    monkeypatch.setattr(cli, "open_admin_pool", _open)
    monkeypatch.setattr(cli, "plan_repair", _plan)
    monkeypatch.setattr(cli, "apply_repair", _apply)
    seen["metadata"] = tmp_path / "metadata.txt"
    return seen


def _args(seen, *extra):
    return [
        "backfill",
        "wol-genome-repair",
        "--reference-idx",
        "18",
        "--metadata",
        str(seen["metadata"]),
        *extra,
    ]


def _shows(out: str, label: str, n: int) -> bool:
    return re.search(rf"^\s*{re.escape(label)}\s*:\s*{n}$", out, re.MULTILINE) is not None


def test_a_dry_run_reports_and_applies_nothing(stubbed, capsys):
    assert main(_args(stubbed)) == 0
    out = capsys.readouterr().out
    assert stubbed["applied"] is None
    assert stubbed["reference_idx"] == 18
    assert stubbed["metadata_path"] == Path(stubbed["metadata"])
    assert stubbed["closed"]
    assert "reference 18: 196062 GCA_G genome(s) to fold" in out
    assert _shows(out, "already linked to G<n>", 195943)
    assert _shows(out, "to link to G<n>", 117)
    assert _shows(out, "G<n> to create", 2)
    assert _shows(out, "stale WoL links to remove", 117)
    assert _shows(out, "labels to retire", 46621)
    assert _shows(out, "derivations to add", 196062)
    assert _shows(out, "derivations present", 0)
    assert "DRY RUN" in out


def test_execute_applies_the_plan_and_names_the_manifest(stubbed, tmp_path, capsys):
    manifest = tmp_path / "manifest.csv"
    assert main(_args(stubbed, "--execute", "--manifest", str(manifest))) == 0
    plan, manifest_path = stubbed["applied"]
    assert plan is _PLAN and manifest_path == manifest
    out = capsys.readouterr().out
    assert "DRY RUN" not in out
    assert f"manifest: {manifest}" in out
    # The report after the run says what was done, not what is to do.
    done = out[out.index("repaired reference 18") :]
    assert "repaired reference 18: 196062 GCA_G genome(s) folded" in done
    assert _shows(done, "linked to G<n>", 117)
    assert _shows(done, "G<n> created", 2)
    assert _shows(done, "stale WoL links removed", 117)
    assert _shows(done, "labels retired", 46621)
    assert _shows(done, "derivations added", 196062)
    assert " to " not in done.replace("linked to G<n>", "")


def test_execute_with_nothing_to_do_applies_nothing(stubbed, monkeypatch, tmp_path, capsys):
    """A repaired reference plans no work; --execute then takes no locks and writes no
    manifest."""
    repaired = RepairCounts(
        already_linked=0,
        to_link=0,
        to_create=0,
        stale_links=0,
        labels_to_retire=0,
        derivations_to_add=0,
        derivations_present=196062,
    )

    async def _plan(pool, *, reference_idx, metadata_path):
        return RepairPlan(reference_idx=reference_idx, parents=(), counts=repaired)

    monkeypatch.setattr(cli, "plan_repair", _plan)
    manifest = tmp_path / "manifest.csv"
    assert main(_args(stubbed, "--execute", "--manifest", str(manifest))) == 0
    assert stubbed["applied"] is None
    assert not manifest.exists()
    assert "nothing to do" in capsys.readouterr().out


def test_execute_without_a_manifest_is_refused(stubbed, capsys):
    assert main(_args(stubbed, "--execute")) == 2
    assert "--execute needs --manifest" in capsys.readouterr().err
    assert "reference_idx" not in stubbed, "refused before planning"


def test_a_manifest_without_execute_is_refused(stubbed, tmp_path, capsys):
    """A dry run writes no manifest, so accepting one would suggest that it had."""
    assert main(_args(stubbed, "--manifest", str(tmp_path / "m.csv"))) == 2
    assert "--manifest is only written with --execute" in capsys.readouterr().err


def test_database_url_is_required(stubbed, monkeypatch, capsys):
    monkeypatch.delenv("DATABASE_URL")
    assert main(_args(stubbed)) == 2
    assert "DATABASE_URL not set" in capsys.readouterr().err


@pytest.mark.parametrize("missing", ["--reference-idx", "--metadata"])
def test_reference_and_metadata_are_required(stubbed, missing):
    args = _args(stubbed)
    i = args.index(missing)
    del args[i : i + 2]
    with pytest.raises(SystemExit) as exc:
        main(args)
    assert exc.value.code == 2


def test_a_refusal_exits_non_zero_and_says_why(stubbed, monkeypatch, capsys):
    async def _refuse(pool, *, reference_idx, metadata_path):
        raise RepairRefused("GCA_G000000001 has 2 features")

    monkeypatch.setattr(cli, "plan_repair", _refuse)
    assert main(_args(stubbed)) == 1
    assert "refused: GCA_G000000001 has 2 features" in capsys.readouterr().err
    assert stubbed["applied"] is None


def test_a_failed_postcondition_exits_non_zero_and_says_rolled_back(
    stubbed, monkeypatch, tmp_path, capsys
):
    async def _fail(pool, plan, *, manifest_path):
        raise RepairPostconditionFailed("feature 7 links to 2 genomes")

    monkeypatch.setattr(cli, "apply_repair", _fail)
    assert main(_args(stubbed, "--execute", "--manifest", str(tmp_path / "m.csv"))) == 1
    err = capsys.readouterr().err
    assert "rolled back" in err and "feature 7 links to 2 genomes" in err


@pytest.mark.parametrize(
    "error",
    [
        asyncpg.exceptions.DeadlockDetectedError("deadlock detected"),
        asyncpg.exceptions.ConnectionDoesNotExistError("connection was closed"),
    ],
    ids=["postgres", "connection"],
)
def test_a_database_error_exits_non_zero_and_says_why(
    stubbed, monkeypatch, tmp_path, capsys, error
):
    async def _fail(pool, plan, *, manifest_path):
        raise error

    monkeypatch.setattr(cli, "apply_repair", _fail)
    assert main(_args(stubbed, "--execute", "--manifest", str(tmp_path / "m.csv"))) == 1
    assert f"error: {error}" in capsys.readouterr().err
