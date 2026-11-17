"""`qiita-admin backfill assembly-gene-call`: the handler around the planner and the
submit loop. The planner is replaced (its own DB tests pin it); what is pinned here
is what the operator sees, that a dry run needs no PAT and posts nothing, and that
a refused submission makes the command exit non-zero."""

from pathlib import Path

import httpx
import pytest
from qiita_common.api_paths import PATH_WORK_TICKET_PREFIX, PATH_WORK_TICKET_ROOT

from qiita_control_plane.backfill.assembly_gene_call import (
    DONE_BY_SOURCE,
    SOURCE_ENTRIES,
    BackfillPlan,
    ResidueKind,
    Run,
    RunStatus,
)
from qiita_control_plane.cli.admin import backfill_assembly_gene_call as cli
from qiita_control_plane.cli.admin import main

_ATTEMPTS = dict.fromkeys(SOURCE_ENTRIES, 0)
_PLAN = BackfillPlan(
    runs=[
        Run(1, 10, RunStatus.WRITABLE, source_work_ticket_idx=100, attempts=_ATTEMPTS),
        Run(2, 20, RunStatus.WRITABLE, source_work_ticket_idx=200, attempts=_ATTEMPTS),
        Run(3, 30, RunStatus.DONE, reason=DONE_BY_SOURCE),
        Run(4, 40, RunStatus.SKIPPED, reason="assembly is no_data"),
        Run(
            5,
            50,
            RunStatus.RESIDUE,
            reason="3 of 9 assembly_membership rows without a genome",
            residue_kind=ResidueKind.MEMBERSHIP_WITHOUT_GENOME,
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

    assert main(["backfill", "assembly-gene-call"]) == 0
    out = capsys.readouterr().out
    assert planned["ticket_root"] == Path(tmp_path) / "ticket"
    assert planned["retry_failed"] is False and planned["closed"]
    assert "writable  : 2" in out and "residue   : 1" in out
    assert "prep_sample 5 run 50: 3 of 9 assembly_membership rows without a genome" in out
    assert "residue, assembly_membership rows without a genome: 1" in out
    assert f"done, {DONE_BY_SOURCE}: 1" in out
    assert "skipped, assembly is no_data: 1" in out
    assert "DRY RUN" in out


def test_execute_posts_one_ticket_per_writable_run(monkeypatch, planned, capsys):
    monkeypatch.setenv("QIITA_TOKEN", "pat")
    posted: list[tuple[str, dict]] = []

    def _call(method, base_url, token, path, *, json=None, params=None):
        posted.append((path, json))
        return {"work_ticket_idx": 9000 + json["action_context"]["assembly_processing_idx"]}

    monkeypatch.setattr(cli._common, "call", _call)
    assert main(["backfill", "assembly-gene-call", "--execute", "--retry-failed"]) == 0
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
    assert main(["backfill", "assembly-gene-call", "--execute", "--limit", "1"]) == 1
    out = capsys.readouterr().out
    assert "prep_sample 1 run 10: 409: busy" in out
    assert "not attempted (over --limit): 1" in out


def test_a_non_positive_limit_is_refused(planned, capsys):
    assert main(["backfill", "assembly-gene-call", "--limit", "0"]) == 2
    assert "--limit must be positive" in capsys.readouterr().err
