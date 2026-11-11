"""qiita-admin CLI — `alignment rerun-blocks` (one batch of align block re-runs) and
`alignment purge-sample-rows`.

`rerun-blocks` is driven by hand, one batch per invocation: it re-arms each listed
ticket (POST .../rerun-block), waits until every ticket it re-armed is terminal, and
prints a JSON report. It never sends a POST twice and keeps no state between runs.
What decides whether the next batch starts is the operator's comparison of this one
against the lake baseline, which this client cannot read.

Guard: a ticket missing from `--baseline-file` (the snapshot taken before the
rollout), or whose `updated_at` differs from its row there, is not POSTed unless
`--force-rearm`; one that differs was already changed, most likely re-armed.

A POST whose reply is a 5xx without a reason code, a timeout, a dropped connection or
unreadable is not judged here: the ticket may or may not have been re-armed. The
batch stops POSTing, the ticket is reported `unknown` together with the route's bound
(`actions/align_rerun.RERUN_REQUEST_BOUND_S`), and the operator decides from the
lake once that has passed since the request.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import httpx
from qiita_common.actions import ALIGN_ACTION_ID
from qiita_common.api_paths import (
    PATH_ALIGNMENT_DEFINITION_PREFIX,
    PATH_ALIGNMENT_DEFINITION_PURGE_SAMPLE_ROWS,
    PATH_ALIGNMENT_DEFINITION_RERUN_BLOCK,
    PATH_WORK_TICKET_BY_IDX,
    PATH_WORK_TICKET_PREFIX,
)
from qiita_common.models import TERMINAL_WORK_TICKET_STATES, WorkTicketState

from ...actions.align_rerun import NOT_COMMITTED_REASONS, RERUN_REQUEST_BOUND_S
from .. import _common

# At most this many tickets per batch, and so re-runs in flight. It is below the
# control plane's process-wide dispatch cap (`dispatch.py` `_DISPATCH_CONCURRENCY`),
# leaving slots for other work.
MAX_BATCH = 6

# Above the route's own bound so a reply that is still coming is not cut off.
_POST_MARGIN_S = 15.0


# Longest pause between polls while GETs keep failing.
_MAX_BACKOFF_S = 600.0


def _path(template: str, alignment_idx: int) -> str:
    return f"{PATH_ALIGNMENT_DEFINITION_PREFIX}{template}".format(alignment_idx=alignment_idx)


def _ticket_path(idx: int) -> str:
    return f"{PATH_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_BY_IDX}".format(work_ticket_idx=idx)


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace(" ", "T"))


def load_baseline(path: Path) -> dict[int, datetime]:
    """`work_ticket_idx,state,updated_at` rows (the step-3 snapshot) → updated_at."""
    with path.open(newline="") as fh:
        return {int(r["work_ticket_idx"]): _parse_ts(r["updated_at"]) for r in csv.DictReader(fh)}


def _precheck(ticket: dict, alignment_idx: int, baseline: dict[int, datetime]) -> dict:
    idx = ticket["work_ticket_idx"]
    context = ticket.get("action_context") or {}
    base = baseline.get(idx)
    return {
        "action_id": ticket["action_id"],
        "state": ticket["state"],
        "alignment_idx": context.get("alignment_idx"),
        "updated_at": ticket["updated_at"],
        "in_baseline": base is not None,
        "changed_since_baseline": base is not None and _parse_ts(ticket["updated_at"]) != base,
        "matches": ticket["action_id"] == ALIGN_ACTION_ID
        and context.get("alignment_idx") == alignment_idx,
    }


def _local_refusal(pre: dict) -> str | None:
    if not pre["in_baseline"]:
        return "not_in_baseline"
    if pre["changed_since_baseline"]:
        return "changed_since_baseline"
    return None


def _not_committed(exc: httpx.HTTPStatusError) -> bool:
    try:
        detail = exc.response.json().get("detail")
    except ValueError, AttributeError:
        return False
    return isinstance(detail, dict) and detail.get("reason") in NOT_COMMITTED_REASONS


def _http_error(exc: Exception) -> dict:
    if isinstance(exc, httpx.HTTPStatusError):
        try:
            body = exc.response.json()
        except ValueError:
            body = exc.response.text
        return {"status": exc.response.status_code, "body": body}
    return {"error": repr(exc)}


def run_batch(
    args: argparse.Namespace,
    token: str,
    *,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    emit: Callable[[str], None] = lambda line: print(line, file=sys.stderr, flush=True),
) -> tuple[dict, bool]:
    """One batch. Returns the report and whether every ticket was re-armed and
    ended COMPLETED (False also for a dry run that found a problem)."""
    idxs: list[int] = args.work_ticket_idx
    baseline = load_baseline(args.baseline_file)
    get = lambda idx: _common.call("GET", args.base_url, token, _ticket_path(idx))  # noqa: E731
    tickets: dict[int, dict] = {
        idx: {"pre_check": _precheck(get(idx), args.alignment_idx, baseline)} for idx in idxs
    }
    report: dict = {
        "alignment_idx": args.alignment_idx,
        "mode": "wait-only" if args.wait_only else ("execute" if args.execute else "dry-run"),
        "tickets": tickets,
        "unknown": [],
        "unknown_decide_after_s": RERUN_REQUEST_BOUND_S,
        "not_attempted": [],
    }
    if not args.execute and not args.wait_only:
        ok = all(
            t["pre_check"]["matches"]
            and t["pre_check"]["in_baseline"]
            and not t["pre_check"]["changed_since_baseline"]
            for t in tickets.values()
        )
        return report, ok

    armed: list[int] = list(idxs) if args.wait_only else []
    if not args.wait_only:
        for pos, idx in enumerate(idxs):
            refusal = _local_refusal(tickets[idx]["pre_check"])
            if refusal is not None and not args.force_rearm:
                tickets[idx]["post"] = {"refused_locally": refusal}
                emit(json.dumps({"work_ticket_idx": idx, **tickets[idx]["post"]}))
                continue
            try:
                body = _common.call(
                    "POST",
                    args.base_url,
                    token,
                    _path(PATH_ALIGNMENT_DEFINITION_RERUN_BLOCK, args.alignment_idx),
                    json={
                        "work_ticket_idx": idx,
                        "reason": args.reason,
                        "allow_deprecated_mask": args.allow_deprecated_mask,
                    },
                    timeout=RERUN_REQUEST_BOUND_S + _POST_MARGIN_S,
                )
            except httpx.HTTPStatusError as exc:
                tickets[idx]["post"] = _http_error(exc)
                emit(json.dumps({"work_ticket_idx": idx, **tickets[idx]["post"]}))
                if exc.response.status_code < 500 or _not_committed(exc):
                    continue
                report["unknown"].append(idx)
                report["not_attempted"] = idxs[pos + 1 :]
                break
            except (httpx.RequestError, ValueError) as exc:
                tickets[idx]["post"] = _http_error(exc)
                emit(json.dumps({"work_ticket_idx": idx, **tickets[idx]["post"]}))
                report["unknown"].append(idx)
                report["not_attempted"] = idxs[pos + 1 :]
                break
            tickets[idx]["post"] = {"status": 200, "body": body}
            emit(json.dumps({"work_ticket_idx": idx, **tickets[idx]["post"]}))
            armed.append(idx)

    try:
        _wait(args, armed, tickets, get, sleep=sleep, clock=clock, emit=emit)
    except (RuntimeError, KeyboardInterrupt) as exc:
        # The report still says what was POSTed; `--wait-only` resumes the wait.
        report["wait_stopped"] = repr(exc)
        return report, False
    ok = (
        not report["unknown"]
        and not report["not_attempted"]
        and len(armed) == len(idxs)
        and all(tickets[i]["final"]["state"] == WorkTicketState.COMPLETED.value for i in armed)
    )
    if args.wait_only:
        # Unchanged since the baseline: never re-armed, so its terminal state is the
        # original run's, not a re-run's.
        report["not_rearmed"] = [
            i
            for i in idxs
            if tickets[i]["pre_check"]["in_baseline"]
            and not tickets[i]["pre_check"]["changed_since_baseline"]
        ]
        ok = ok and not report["not_rearmed"]
    return report, ok


def _wait(args, armed, tickets, get, *, sleep, clock, emit) -> None:
    """Poll the re-armed tickets and return once each is terminal; Ctrl-C stops the
    wait, and `--wait-only` resumes it. A GET error (e.g. a control plane restart) is
    printed and retried, the pause doubling up to `_MAX_BACKOFF_S`. If GETs keep
    failing for `--wait-deadline-h` with no round in which every GET succeeded, the
    wait raises; a 4xx raises at once (an expired token or a missing ticket does not
    heal).

    A ticket still PENDING after
    `--pending-warn-min` is warned about once per `--pending-warn-min` and still
    waited on."""
    start = clock()
    warn_s = args.pending_warn_min * 60
    last_warned: dict[int, float] = {}
    failing_rounds = 0
    failing_since: float | None = None
    left = set(armed)
    while left:
        errored = False
        for idx in sorted(left):
            try:
                t = get(idx)
            except (httpx.HTTPError, ValueError) as exc:
                if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code < 500:
                    raise RuntimeError(
                        f"GET work_ticket {idx}: {_http_error(exc)}; resume with --wait-only"
                    ) from exc
                errored = True
                now = clock()
                failing_since = now if failing_since is None else failing_since
                if now - failing_since > args.wait_deadline_h * 3600:
                    raise RuntimeError(
                        f"GETs have failed for {args.wait_deadline_h} h, last for work_ticket"
                        f" {idx}: {exc!r}; resume with --wait-only"
                    ) from exc
                emit(f"GET work_ticket {idx} failed: {exc!r}; retrying")
                continue
            if t["state"] in TERMINAL_WORK_TICKET_STATES:
                tickets[idx]["final"] = {
                    k: t.get(k)
                    for k in ("state", "failure_type", "failure_stage", "failure_reason")
                }
                left.discard(idx)
                continue
            now = clock()
            if (
                t["state"] == WorkTicketState.PENDING.value
                and now - start > warn_s
                and now - last_warned.get(idx, start) > warn_s
            ):
                last_warned[idx] = now
                emit(
                    f"work_ticket {idx} is still pending after {(now - start) / 60:.0f} min."
                    " It may be queued for a dispatch slot: check the control-plane log for"
                    " 'queued behind the dispatch cap' before running `qiita ticket run`."
                    " Still waiting."
                )
        if not errored:
            failing_since = None
        if left:
            failing_rounds = failing_rounds + 1 if errored else 0
            sleep(min(args.poll_seconds * 2**failing_rounds, _MAX_BACKOFF_S))


def _handle_rerun_blocks(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    if len(args.work_ticket_idx) > MAX_BATCH:
        parser.error(f"at most {MAX_BATCH} tickets per batch, got {len(args.work_ticket_idx)}")
    if len(set(args.work_ticket_idx)) != len(args.work_ticket_idx):
        parser.error("--work-ticket-idx must not repeat")
    if args.execute and args.wait_only:
        parser.error("--execute and --wait-only are exclusive")
    try:
        token = _common.read_token()
        report, ok = run_batch(args, token)
    except (RuntimeError, httpx.HTTPError, OSError, KeyError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2, default=str))
    return 0 if ok else 1


def _handle_purge_sample_rows(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda token: _common.call(
            "POST",
            args.base_url,
            token,
            _path(PATH_ALIGNMENT_DEFINITION_PURGE_SAMPLE_ROWS, args.alignment_idx),
            json={"prep_sample_idx": args.prep_sample_idx, "reason": args.reason},
            # A whole-table predicate on the data plane; above the gateway's own
            # ceiling, so a slow delete surfaces as the gateway's 504.
            timeout=_common.BULK_READ_HTTP_TIMEOUT_SECONDS,
        )
    )


def add_rerun_parsers(sub, with_idx) -> None:
    sp = with_idx(
        "rerun-blocks",
        f"re-run one batch (at most {MAX_BATCH}) of align block tickets, wait, and report",
        _handle_rerun_blocks,
    )
    sp.add_argument("--work-ticket-idx", type=int, nargs="+", required=True)
    sp.add_argument("--reason", required=True, help="why the blocks are re-run")
    sp.add_argument(
        "--baseline-file",
        type=Path,
        required=True,
        help="CSV work_ticket_idx,state,updated_at taken before the rollout",
    )
    sp.add_argument(
        "--allow-deprecated-mask",
        action="store_true",
        help="run tickets whose mask has since been deprecated under it again",
    )
    sp.add_argument(
        "--force-rearm",
        action="store_true",
        help="POST tickets missing from the baseline or whose updated_at differs from it",
    )
    sp.add_argument(
        "--wait-only",
        action="store_true",
        help="POST nothing; wait on the given tickets and report",
    )
    sp.add_argument("--execute", action="store_true", help="POST (default: dry run)")
    sp.add_argument(
        "--pending-warn-min",
        type=float,
        default=30.0,
        help="minutes a ticket may stay pending before a warning, repeated at this interval",
    )
    sp.add_argument("--poll-seconds", type=float, default=60.0, help="pause between polls")
    sp.add_argument(
        "--wait-deadline-h",
        type=float,
        default=48.0,
        help="give up after GETs have failed without a break for this long",
    )

    sp = with_idx(
        "purge-sample-rows",
        "delete the alignment rows of one prep_sample that has no gate (0 allowed)",
        _handle_purge_sample_rows,
    )
    sp.add_argument("--prep-sample-idx", type=int, required=True)
    sp.add_argument("--reason", required=True, help="why the rows are deleted")
