"""`qiita-admin backfill myloasm-provenance`: the handler around the planner and the
submit loop. The planner is replaced (its own DB tests pin it); what is pinned here
is what the operator sees, that a dry run needs no PAT and posts nothing, and that
a refused submission makes the command exit non-zero."""

from pathlib import Path

import httpx
import pytest
from qiita_common.api_paths import PATH_WORK_TICKET_PREFIX, PATH_WORK_TICKET_ROOT

from qiita_control_plane.backfill.myloasm_provenance import (
    BackfillPlan,
    ResidueKind,
    Run,
    RunStatus,
)
from qiita_control_plane.cli.admin import backfill_myloasm_provenance as cli
from qiita_control_plane.cli.admin import main

_PLAN = BackfillPlan(
    runs=[
        Run(1, 10, 5, RunStatus.WRITABLE, source_work_ticket_idx=100, assemble_attempt=2),
        Run(2, 20, 5, RunStatus.WRITABLE, source_work_ticket_idx=200, assemble_attempt=1),
        Run(3, 30, 5, RunStatus.DONE),
        Run(4, 40, 5, RunStatus.SKIPPED, reason="assembly is no_data"),
        Run(
            5,
            50,
            5,
            RunStatus.RESIDUE,
            reason="no manifest at /scratch/ticket/500/x",
            residue_kind=ResidueKind.NO_MANIFEST,
        ),
    ]
)


@pytest.fixture
def planned(monkeypatch, tmp_path):
    seen: dict = {}

    class _Pool:
        async def close(self):
            seen["closed"] = True

    async def _open(url):
        return _Pool()

    async def _plan(pool, *, ticket_root, retry_failed):
        seen["ticket_root"], seen["retry_failed"] = ticket_root, retry_failed
        return _PLAN

    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    monkeypatch.setenv("PATH_SCRATCH", str(tmp_path))
    monkeypatch.setattr(cli, "open_admin_pool", _open)
    monkeypatch.setattr(cli, "plan_backfill", _plan)
    return seen


def test_a_dry_run_reports_and_posts_nothing(monkeypatch, planned, tmp_path, capsys):
    monkeypatch.delenv("QIITA_TOKEN", raising=False)
    monkeypatch.setattr(cli._common, "read_token", lambda: pytest.fail("a dry run needs no PAT"))
    monkeypatch.setattr(cli._common, "call", lambda *a, **k: pytest.fail("a dry run posts"))

    assert main(["backfill", "myloasm-provenance"]) == 0
    out = capsys.readouterr().out
    assert planned["ticket_root"] == Path(tmp_path) / "ticket"
    assert planned["retry_failed"] is False and planned["closed"]
    assert "writable  : 2" in out and "residue   : 1" in out
    assert "prep_sample 5 run 50: no manifest at /scratch/ticket/500/x" in out
    assert "residue, no manifest: 1" in out
    assert "assembly is no_data: 1" in out
    assert "DRY RUN" in out


def test_execute_posts_one_ticket_per_writable_run(monkeypatch, planned, capsys):
    monkeypatch.setenv("QIITA_TOKEN", "pat")
    posted: list[tuple[str, dict]] = []

    def _call(method, base_url, token, path, *, json=None, params=None):
        posted.append((path, json))
        return {"work_ticket_idx": 9000 + json["action_context"]["assembly_processing_idx"]}

    monkeypatch.setattr(cli._common, "call", _call)
    assert main(["backfill", "myloasm-provenance", "--execute", "--retry-failed"]) == 0
    assert planned["retry_failed"] is True
    assert [p for p, _ in posted] == [f"{PATH_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_ROOT}"] * 2
    assert "prep_sample 1 run 10 -> ticket 9010" in capsys.readouterr().out


def test_a_refused_submission_exits_non_zero(monkeypatch, planned, capsys):
    monkeypatch.setenv("QIITA_TOKEN", "pat")

    def _call(method, base_url, token, path, *, json=None, params=None):
        request = httpx.Request(method, f"{base_url}{path}")
        raise httpx.HTTPStatusError(
            "refused", request=request, response=httpx.Response(409, request=request, text="busy")
        )

    monkeypatch.setattr(cli._common, "call", _call)
    assert main(["backfill", "myloasm-provenance", "--execute", "--limit", "1"]) == 1
    out = capsys.readouterr().out
    assert "prep_sample 1 run 10: 409: busy" in out
    # The second writable run is past the limit, and says so.
    assert "not attempted (over --limit): 1" in out
