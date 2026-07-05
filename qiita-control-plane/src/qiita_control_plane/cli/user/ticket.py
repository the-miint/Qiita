"""qiita user CLI — work-ticket subcommands.

Split out of the former single-file ``cli.user`` module; behavior unchanged.
"""

import argparse

from qiita_common.api_paths import (
    PATH_WORK_TICKET_PREFIX,
)
from qiita_common.models import (
    ScopeTargetKind,
    WorkTicketCreateRequest,
)

from .. import _common
from ._helpers import _build_body


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
    limit: int | None,
) -> list:
    """GET /api/v1/work-ticket. Returns a list of WorkTicketSummary records —
    each ticket plus its current step's compute_target / slurm_job_id /
    step_state. Scope: the caller's own tickets, or all originators' with
    `all_tickets` (wet_lab_admin+). Query params are sent only when set so
    the server's defaults (own, all states, limit 50) apply otherwise."""
    params: dict[str, str] = {}
    if state is not None:
        params["state"] = state
    if active:
        params["active"] = "true"
    if all_tickets:
        params["all"] = "true"
    if limit is not None:
        params["limit"] = str(limit)
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
    omitted). All JSON parsing flows through parse_json_arg so a malformed
    paste lands as a clean exit 2.
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
    --all for wet_lab_admin+). Prints the full summary list so a poller sees
    each ticket's state plus its current step's compute_target / slurm_job_id
    / step_state in one call."""
    return _common.run_http_subcommand(
        lambda t: _list_work_tickets(
            args.base_url,
            t,
            state=args.state,
            active=args.active,
            all_tickets=args.all_tickets,
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
