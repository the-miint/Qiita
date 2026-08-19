"""qiita-admin CLI — ticket cancel subcommand (calls the CP cancel route).

Unlike `force-fail` (direct-DB), cancel goes through POST /work-ticket/cancel so
the CP does the terminal-first flip AND scancels the SLURM job(s) on the operator's
behalf — no crossing into the compute account, no hand-written job-name regex.
Gated server-side on the `work_ticket:cancel` scope (system_admin).
"""

import argparse
import json
import sys

from qiita_common.api_paths import PATH_WORK_TICKET_CANCEL, PATH_WORK_TICKET_PREFIX

from .. import _common

_CANCEL_PATH = f"{PATH_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_CANCEL}"


def _render_cancel(body: dict | list) -> None:
    """Print the full response JSON, then a per-ticket human summary to stderr so an
    operator sees at a glance what flipped, what was already terminal, what wasn't
    found, and any reap that failed."""
    print(json.dumps(body, indent=2))
    if not isinstance(body, dict):
        return
    print(
        f"cancelled {body.get('cancelled')}/{body.get('requested')} ticket(s).",
        file=sys.stderr,
    )
    for r in body.get("results", []):
        idx = r.get("work_ticket_idx")
        if r.get("not_found"):
            print(f"  wt {idx}: not found", file=sys.stderr)
        elif r.get("cancelled"):
            jobs = r.get("cancelled_job_ids") or []
            note = f", scancelled {jobs}" if jobs else ""
            reap = f" — REAP FAILED: {r['reap_error']}" if r.get("reap_error") else ""
            print(
                f"  wt {idx}: {r.get('previous_state')} -> cancelled{note}{reap}", file=sys.stderr
            )
        else:
            print(f"  wt {idx}: already {r.get('state')} (no-op)", file=sys.stderr)


def _handle_ticket_cancel(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    idxs = list(args.work_ticket_idx or [])
    if not idxs and args.action_id is None:
        parser.error(
            "provide one or more work_ticket idxs and/or --action-id to select tickets to cancel"
        )
    if (
        args.sequencing_run_idx is not None or args.sequenced_pool_idx is not None
    ) and args.action_id is None:
        parser.error("--sequencing-run-idx / --sequenced-pool-idx narrow --action-id; set it too")

    body: dict = {"work_ticket_idxs": idxs}
    if args.action_id is not None:
        body["action_id"] = args.action_id
        if args.sequencing_run_idx is not None:
            body["sequencing_run_idx"] = args.sequencing_run_idx
        if args.sequenced_pool_idx is not None:
            body["sequenced_pool_idx"] = args.sequenced_pool_idx

    return _common.run_http_subcommand(
        lambda token: _common.call("POST", args.base_url, token, _CANCEL_PATH, json=body),
        render=_render_cancel,
    )
