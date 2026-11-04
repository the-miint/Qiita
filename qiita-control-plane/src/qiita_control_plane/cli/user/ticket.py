"""qiita user CLI — work-ticket subcommands: submit, status, run, list, redrive
and logs, each a thin client over the /work-ticket routes."""

import argparse
import json
import sys
import time

import httpx
from pydantic import ValidationError
from qiita_common.api_paths import (
    PATH_WORK_TICKET_PREFIX,
)
from qiita_common.models import (
    NON_TERMINAL_WORK_TICKET_STATES,
    ScopeTargetKind,
    WorkTicketCreateRequest,
    WorkTicketListResponse,
)

from .. import _common
from ._helpers import _build_body, _follow_on_from_args


def _post_work_ticket(base_url: str, token: str, body: dict) -> dict:
    """POST /api/v1/work-ticket. originator_principal_idx is set server-side
    from the authenticated caller; the body carries action_id, action_version,
    scope_target (discriminated union), and action_context (free-form per the
    action's declared context_schema)."""
    return _common.call("POST", base_url, token, PATH_WORK_TICKET_PREFIX, json=body)


def _get_work_ticket(base_url: str, token: str, work_ticket_idx: int) -> dict:
    """GET /api/v1/work-ticket/{idx}. Returns the full WorkTicket record —
    state, action info, scope_target, action_context, retry accounting,
    failure surface, timestamps. Auth: originator or wet_lab_admin+."""
    return _common.call("GET", base_url, token, f"{PATH_WORK_TICKET_PREFIX}/{work_ticket_idx}")


def _run_work_ticket(base_url: str, token: str, work_ticket_idx: int) -> dict:
    """POST /api/v1/work-ticket/{idx}/run. Operator override and the only retry
    mechanism (there is no auto-retry worker): resets a FAILED ticket to PENDING
    (clears failure_*, retry_count → 0) and re-dispatches it, or re-dispatches a
    PENDING ticket whose create-time dispatch was lost. The runner fast-forwards
    every step already marked COMPLETED and resumes at the first incomplete one,
    so a costly finished step (e.g. stage_local_fasta) is not recomputed. No
    request body. Auth: originator or wet_lab_admin+. Returns the new {idx,
    state}."""
    return _common.call("POST", base_url, token, f"{PATH_WORK_TICKET_PREFIX}/{work_ticket_idx}/run")


def _get_work_ticket_step_logs(
    base_url: str,
    token: str,
    work_ticket_idx: int,
    *,
    step_index: int,
    attempt: int | None,
    tail_lines: int | None,
) -> dict:
    """GET /api/v1/work-ticket/{idx}/step/{step_index}/logs. Returns the step
    attempt's stdout/stderr tail (and per-stream truncation flags). Query
    params are sent only when set so the server's defaults (latest attempt,
    200 lines) apply otherwise. Auth: originator or wet_lab_admin+."""
    params: dict[str, str] = {}
    if attempt is not None:
        params["attempt"] = str(attempt)
    if tail_lines is not None:
        params["tail_lines"] = str(tail_lines)
    return _common.call(
        "GET",
        base_url,
        token,
        f"{PATH_WORK_TICKET_PREFIX}/{work_ticket_idx}/step/{step_index}/logs",
        params=params,
    )


def _list_work_tickets(
    base_url: str,
    token: str,
    *,
    state: str | None,
    active: bool,
    all_tickets: bool,
    sequenced_pool_idx: int | None,
    prep_sample_idx: int | None,
    action_id: str | None,
    limit: int | None,
    failure_type: str | None = None,
) -> dict:
    """GET /api/v1/work-ticket. Returns the WorkTicketListResponse envelope:
    `tickets` (each ticket plus its current step's compute_target /
    slurm_job_id / step_state, and for a prep_sample-scoped ticket its
    sample's read_outcome), `count`, and `truncated` — true when the set
    exceeded `limit`, so a short page is never mistaken for a whole one.
    Scope: the caller's own tickets, or all originators' with `all_tickets`
    (wet_lab_admin+). Query params are sent only when set so the server's
    defaults (own, all states, limit 50) apply otherwise."""
    params: dict[str, str] = {}
    if state is not None:
        params["state"] = state
    if active:
        params["active"] = "true"
    if all_tickets:
        params["all"] = "true"
    if sequenced_pool_idx is not None:
        params["sequenced_pool_idx"] = str(sequenced_pool_idx)
    if prep_sample_idx is not None:
        params["prep_sample_idx"] = str(prep_sample_idx)
    if action_id is not None:
        params["action_id"] = action_id
    if limit is not None:
        params["limit"] = str(limit)
    if failure_type is not None:
        params["failure_type"] = failure_type
    return _common.call("GET", base_url, token, PATH_WORK_TICKET_PREFIX, params=params)


def _render_ticket_logs(body: dict) -> None:
    """Print a step's logs for human reading: a one-line header, then each
    stream raw (so embedded newlines render as newlines, not the JSON-escaped
    `\\n` literals a plain dump would show — the whole point of a logs verb)."""
    print(
        f"# work_ticket {body['work_ticket_idx']} step {body['step_index']}"
        f" attempt {body['attempt']} ({body['step_name']})"
    )
    for stream in ("stdout", "stderr"):
        suffix = " (truncated)" if body.get(f"{stream}_truncated") else ""
        print(f"\n===== {stream}{suffix} =====")
        text = body.get(stream, "")
        if text:
            print(text)


def _handle_ticket_submit(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Submit a work-ticket. Construct the scope_target from the convenience
    --prep-sample-idx flag when supplied; otherwise parse --scope-target-json
    verbatim. action_context comes from --context-json (or stays {} when
    omitted), and the `--then-*` flags chain an optional follow-on. All JSON
    parsing flows through parse_json_arg so a malformed paste lands as a clean
    exit 2.
    """
    if args.prep_sample_idx is not None:
        args.scope_target = {
            "kind": ScopeTargetKind.PREP_SAMPLE.value,
            "prep_sample_idx": args.prep_sample_idx,
        }
    else:
        args.scope_target = _common.parse_json_arg(
            args.scope_target_json, parser, flag="--scope-target-json"
        )
    parsed_context = _common.parse_json_arg(args.context_json, parser, flag="--context-json")
    # action_context defaults to {} server-side via the model; only set it
    # when the user supplied --context-json so unset stays "not set".
    if parsed_context is not None:
        args.action_context = parsed_context

    # Map the convenience --mem-gb flag onto the resource_override model field
    # _build_body picks up. Only set it when supplied so unset stays "not set".
    if args.mem_gb is not None:
        args.resource_override = {"mem_gb": args.mem_gb}

    follow_on = _follow_on_from_args(args, parser)
    if follow_on is not None:
        # exclude_unset drops the follow-on's own `on_success` when the CLI did
        # not set it (the CLI chains a single follow-on), keeping the body stable.
        args.on_success = follow_on.model_dump(mode="json", exclude_unset=True)

    body = _build_body(WorkTicketCreateRequest, args, parser)
    return _common.run_http_subcommand(lambda t: _post_work_ticket(args.base_url, t, body))


def _handle_ticket_status(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """GET a single work-ticket's full record. The CLI prints the full
    server response so a USER polling for state can render every field
    (state, retry_count, failure_*, timestamps) without a second call."""
    return _common.run_http_subcommand(
        lambda t: _get_work_ticket(args.base_url, t, args.work_ticket_idx)
    )


def _handle_ticket_run(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Resume/retry a work-ticket: reset a FAILED ticket and re-dispatch (the
    only retry path — no auto-retry worker). The runner fast-forwards every
    already-COMPLETED step, so an expensive finished step is not re-run; the
    ticket resumes at the first incomplete step. Prints the new {idx, state}."""
    return _common.run_http_subcommand(
        lambda t: _run_work_ticket(args.base_url, t, args.work_ticket_idx)
    )


def _handle_ticket_list(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """List work-tickets (your own by default, or every originator's with
    --all for wet_lab_admin+). Prints the full envelope — `tickets` plus
    `count` / `truncated` — so a poller sees each ticket's state and its
    current step's compute_target / slurm_job_id / step_state in one call,
    and sees when the page was capped. --sequenced-pool-idx narrows to one pool's
    tickets; with --action-id read-mask that is the pool's per-sample read
    outcomes, up to --limit rows. A pool masked through the block path has
    read-mask-block tickets instead, which span many samples and carry no
    read_outcome — read that pool's counts off its sequenced-sample roster."""
    return _common.run_http_subcommand(
        lambda t: _list_work_tickets(
            args.base_url,
            t,
            state=args.state,
            active=args.active,
            all_tickets=args.all_tickets,
            sequenced_pool_idx=args.sequenced_pool_idx,
            prep_sample_idx=args.prep_sample_idx,
            action_id=args.action_id,
            limit=args.limit,
        )
    )


def _handle_ticket_logs(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """GET a step attempt's stdout/stderr tail so an operator can diagnose a
    failure (OOM, bad input, contract violation) without a host shell. Renders
    each stream raw (not JSON-escaped); defaults to the latest recorded attempt
    when --attempt is omitted."""
    return _common.run_http_subcommand(
        lambda t: _get_work_ticket_step_logs(
            args.base_url,
            t,
            args.work_ticket_idx,
            step_index=args.step_index,
            attempt=args.attempt,
            tail_lines=args.tail_lines,
        ),
        render=_render_ticket_logs,
    )


# `/run` answers these when it refuses this one ticket (gone or not visible to
# the caller; not in a redrivable state, or a refusal of its own such as a
# download ticket's). The run records the refusal and goes on to the next ticket.
# Any other answer stops the run, so an outage is not met with one call per
# remaining ticket.
_PER_TICKET_REFUSALS = (404, 409)


def _redrive(args: argparse.Namespace, token: str) -> tuple[dict, bool]:
    """Select, then (with --execute) redrive one at a time. Returns the report
    and whether every selected ticket was redriven."""
    listing = WorkTicketListResponse.model_validate(
        _list_work_tickets(
            args.base_url,
            token,
            state=args.state,
            active=False,
            all_tickets=args.all_tickets,
            sequenced_pool_idx=args.sequenced_pool_idx,
            prep_sample_idx=args.prep_sample_idx,
            action_id=args.action_id,
            limit=args.limit,
            failure_type=args.failure_type,
        )
    )
    if listing.truncated:
        raise RuntimeError(
            f"the selection matches more than the {listing.count} tickets returned;"
            " narrow it or raise --limit, so the run covers the whole selection"
        )
    # The server filters on failure_type; filtering again here keeps a server
    # that predates the parameter, and so ignores it, from widening the selection.
    selected = [
        t
        for t in listing.tickets
        if args.failure_type is None or t.failure_type == args.failure_type
    ]
    report: dict = {
        "mode": "execute" if args.execute else "dry-run",
        "selected": [
            {
                "work_ticket_idx": t.work_ticket_idx,
                "action_id": t.action_id,
                "action_version": t.action_version,
                "state": t.state.value,
                "failure_type": t.failure_type.value if t.failure_type else None,
                "failure_step_name": t.failure_step_name,
            }
            for t in selected
        ],
    }
    if not args.execute:
        return report, True
    redriven: list[dict] = []
    refused: list[dict] = []
    in_flight: list[int] = []
    report.update(redriven=redriven, refused=refused, not_attempted=[])
    for position, ticket in enumerate(selected):
        if position and args.rate_seconds:
            time.sleep(args.rate_seconds)
        try:
            _wait_for_room(args, token, in_flight)
        except (httpx.HTTPStatusError, httpx.RequestError) as exc:
            report["stopped"] = {
                "work_ticket_idx": ticket.work_ticket_idx,
                "error": f"reading the redriven tickets' states failed: {exc!r}",
            }
            report["not_attempted"] = [t.work_ticket_idx for t in selected[position:]]
            return report, False
        try:
            redriven.append(_run_work_ticket(args.base_url, token, ticket.work_ticket_idx))
            in_flight.append(ticket.work_ticket_idx)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code not in _PER_TICKET_REFUSALS:
                report["stopped"] = {
                    "work_ticket_idx": ticket.work_ticket_idx,
                    "status": exc.response.status_code,
                    "detail": exc.response.text,
                }
                report["not_attempted"] = [t.work_ticket_idx for t in selected[position + 1 :]]
                return report, False
            refused.append(
                {
                    "work_ticket_idx": ticket.work_ticket_idx,
                    "status": exc.response.status_code,
                    "detail": exc.response.text,
                }
            )
        except httpx.RequestError as exc:
            report["stopped"] = {"work_ticket_idx": ticket.work_ticket_idx, "error": repr(exc)}
            report["not_attempted"] = [t.work_ticket_idx for t in selected[position + 1 :]]
            return report, False
    return report, not refused


def _wait_for_room(args: argparse.Namespace, token: str, in_flight: list[int]) -> None:
    """Block until fewer than --max-inflight of the tickets this run redrove are
    still pending, queued or processing, dropping the finished ones from
    `in_flight`. A redrive dispatches at once, and what overloads the data
    plane's edge is how many run together, not how fast they are sent."""
    while len(in_flight) >= args.max_inflight:
        time.sleep(args.poll_seconds)
        in_flight[:] = [
            idx
            for idx in in_flight
            if _get_work_ticket(args.base_url, token, idx)["state"]
            in NON_TERMINAL_WORK_TICKET_STATES
        ]


def _handle_ticket_redrive(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Redrive every ticket a selector picks, as `ticket run` does one: select
    FAILED (or CANCELLED) tickets with the `ticket list` filters, print them, and
    with --execute POST /work-ticket/{idx}/run for each in turn, --rate seconds
    apart and with at most --max-inflight of them unfinished at once. Prints a
    JSON report, to stdout on a dry run or when every selected ticket was
    redriven, and to stderr with exit 1 when any was not (refused, or not attempted after the run
    stopped)."""
    if args.rate_seconds < 0:
        parser.error(f"--rate must be >= 0, got {args.rate_seconds}")
    if args.max_inflight < 1:
        parser.error(f"--max-inflight must be >= 1, got {args.max_inflight}")
    if args.poll_seconds <= 0:
        parser.error(f"--poll-seconds must be > 0, got {args.poll_seconds}")
    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    try:
        report, complete = _redrive(args, token)
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except httpx.HTTPStatusError as exc:
        _common.print_http_status_error(exc.response)
        return 1
    except httpx.RequestError as exc:
        _common.print_request_error(exc)
        return 1
    except ValidationError as exc:
        _common.print_validation_error(exc)
        return 1
    print(json.dumps(report, indent=2), file=sys.stdout if complete else sys.stderr)
    return 0 if complete else 1
