"""qiita-admin — operator (admin-only) CLI for principal/role/token management.

Scope: operator/admin tasks only. End-user interactions with qiita (data-plane
operations, study/sample management, etc.) will live in a separate `qiita`
CLI; this module deliberately does not grow user-facing subcommands.

Subcommands:
  set-system-role  — direct DB UPDATE of qiita.principal.system_role.
                     Used for the bootstrap path (first system_admin) and
                     when the operator has DB access but no PAT yet. Refuses
                     to operate on the system principal (idx=1).
  whoami           — calls GET /api/v1/auth/whoami via the configured PAT.
  token revoke-all — calls POST /api/v1/admin/principal/{idx}/revoke-all-tokens.
  login            — drives the AuthRocket LoginRocket Web flow end-to-end.
                     Spawns a localhost loopback HTTP server, opens a
                     browser to /api/v1/auth/login?cli=1&port=N, waits for
                     the handoff to redirect back with a one-time code,
                     exchanges the code at /api/v1/auth/cli-exchange for
                     a PAT, and writes the PAT to ~/.qiita/token (0600).
  actions sync     — read every action YAML under --workflows-dir and upsert
                     YAML-authoritative columns into qiita.action. Direct DB
                     write; reads DATABASE_URL from env. Idempotent: re-runs
                     converge to the YAML state without touching operational
                     columns (enabled / first_seen_at / disabled_*).
  reference load   — drive the reference-add workflow end-to-end: per-file
                     Arrow conversion + DoPut to the data plane + POST
                     /work-ticket with the resulting upload handles, then
                     watch the work_ticket through to terminal. See
                     `cli.reference_load.do_reference_load` for the
                     programmatic entry point integration tests call.
  ticket force-fail — direct-DB transition of a non-terminal work_ticket
                     to state=failed with a captured failure_type /
                     stage / step_name / reason. Replaces the previous
                     "operator writes UPDATE qiita.work_ticket by hand"
                     recovery pattern with a single command that
                     respects the schema's CHECK constraints. Refuses
                     to operate on already-terminal tickets.
  compute-readiness — exercise the path qiita-job needs end-to-end and
                     report per-check status (JWT, CP /healthz,
                     SLURM_NATIVE_PYTHON on host, plus an optional
                     SLURM probe-job that verifies the same env from
                     a compute node). Subprocess-execs into the
                     orchestrator's venv since the diagnostic uses the
                     orchestrator's Settings.from_env() and
                     SlurmrestdClient surfaces.

Authentication for HTTP subcommands: read PAT from QIITA_TOKEN env var or
from ~/.qiita/token (mode 0600 expected). Loopback login flow, token I/O,
and the generic HTTP runner live in `cli._common`.
"""

import argparse
import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import asyncpg
from pydantic import ValidationError
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, SystemRole

from qiita_control_plane.actions import (
    DuplicateActionError,
    load_actions,
    sync_actions,
)

from . import _common

# Direct-DB connection timeout for the bootstrap subcommand. Short because the
# DB is expected to be reachable on the operator's network; a multi-second
# stall here masks misconfiguration.
_DB_CONNECT_TIMEOUT_SECONDS = 5

# Production install location for the orchestrator's venv. Same path
# the deploy script writes to and the systemd unit launches from —
# this constant is a default for the operator-side wrapper, not the
# source of truth; --orchestrator-venv overrides for dev hosts or
# unusual layouts. The wrapper subprocess-execs `<venv>/bin/python -m
# qiita_compute_orchestrator.cli.compute_readiness`.
_DEFAULT_ORCHESTRATOR_VENV = Path("/opt/qiita/compute-orchestrator/.venv")

# Derived from SystemRole so the role list isn't repeated anywhere in this
# file — adding `SystemRole.X` widens validation, error message, and `--help`
# automatically.
_VALID_ROLE_VALUES = tuple(r.value for r in SystemRole)


# ---------------------------------------------------------------------------
# Bootstrap subcommand: set-system-role (direct DB)
# ---------------------------------------------------------------------------


async def _set_system_role(database_url: str, email: str, role: str) -> int:
    """Update the principal's system_role by email lookup.

    Returns the principal_idx that was updated. Refuses to operate on
    idx=1 (the system principal). Raises with a clear message if the
    email is not found (the operator probably hasn't logged in via OIDC
    yet, which is what creates the principal+user pair).
    """
    if role not in _VALID_ROLE_VALUES:
        raise ValueError(f"role must be one of {' / '.join(_VALID_ROLE_VALUES)} (got {role!r})")
    try:
        conn = await asyncpg.connect(database_url, timeout=_DB_CONNECT_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 — show full reason, including OS errors
        raise RuntimeError(
            f"could not connect to DATABASE_URL: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        idx = await conn.fetchval(
            "SELECT u.principal_idx FROM qiita.user u WHERE u.email = $1",
            email,
        )
        if idx is None:
            raise RuntimeError(
                f"no user with email {email!r} — has this user logged in"
                " via OIDC at least once? First login creates the principal+user"
                " rows; only then can their role be set."
            )
        if idx == SYSTEM_PRINCIPAL_IDX:
            raise RuntimeError(
                f"refusing to modify the system principal (idx={SYSTEM_PRINCIPAL_IDX})"
            )
        await conn.execute(
            "UPDATE qiita.principal SET system_role = $1 WHERE idx = $2",
            role,
            idx,
        )
        return idx
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# actions sync — direct-DB upsert of YAML-authoritative columns
# ---------------------------------------------------------------------------


async def _sync_actions(database_url: str, workflows_dir: Path) -> dict:
    """Load every action YAML under workflows_dir, then upsert into
    qiita.action inside one transaction. Returns a dict with counts of
    inserted, updated, and total actions found."""
    actions = load_actions(workflows_dir)
    if not actions:
        return {"found": 0, "inserted": 0, "updated": 0}
    try:
        conn = await asyncpg.connect(database_url, timeout=_DB_CONNECT_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 — show full reason, including OS errors
        raise RuntimeError(
            f"could not connect to DATABASE_URL: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        result = await sync_actions(conn, actions)
    finally:
        await conn.close()
    return {"found": len(actions), **result}


# ---------------------------------------------------------------------------
# ticket force-fail — direct-DB transition of a non-terminal work_ticket
# ---------------------------------------------------------------------------

# work_ticket_failure_step_name_consistent in db/migrations/20260504000001
# requires failure_step_name IS NOT NULL iff failure_stage='step_run'.
# Mirrored here so the CLI fails before the DB does, with a clearer message.
_FAILURE_STAGES_REQUIRING_STEP_NAME = ("step_run",)
_FAILURE_STAGES_REJECTING_STEP_NAME = ("submission", "finalize")
_FAILURE_STAGE_CHOICES = _FAILURE_STAGES_REQUIRING_STEP_NAME + _FAILURE_STAGES_REJECTING_STEP_NAME

# Tickets in these states are eligible for force-fail; anything terminal
# (failed / completed) is rejected so the CLI doesn't silently overwrite
# a captured failure or convert a real success into a fake failure.
_FORCE_FAIL_ELIGIBLE_STATES = ("pending", "queued", "processing")


def _validate_force_fail_args(stage: str, step_name: str | None) -> None:
    """Surface CHECK violations before sending UPDATE so the error
    message names the constraint directly. Stage / step-name
    interlock matches work_ticket_failure_step_name_consistent."""
    if stage in _FAILURE_STAGES_REQUIRING_STEP_NAME and not step_name:
        raise ValueError(
            f"--step-name is required when --stage={stage} (mirrors the"
            " work_ticket_failure_step_name_consistent CHECK constraint)"
        )
    if stage in _FAILURE_STAGES_REJECTING_STEP_NAME and step_name:
        raise ValueError(
            f"--step-name must not be set when --stage={stage} (mirrors the"
            " work_ticket_failure_step_name_consistent CHECK constraint)"
        )


async def _force_fail_ticket(
    database_url: str,
    *,
    work_ticket_idx: int,
    stage: str,
    step_name: str | None,
    reason: str,
) -> dict:
    """Transition a non-terminal work_ticket to state=failed with the
    captured failure_* columns set. Refuses to overwrite an already-
    terminal ticket so a real success or a captured prior failure isn't
    lost.

    The CHECK constraint shape (work_ticket_failure_consistent +
    work_ticket_failure_step_name_consistent) is enforced by the DB;
    we validate stage / step-name compatibility client-side first
    (_validate_force_fail_args) so the error message is more direct than
    asyncpg's CheckViolationError surface.

    failure_type is always 'permanent' for the force-fail path: an
    operator hand-failing a stuck ticket has already concluded retries
    won't help. Sites that need a retriable force-fail (rare —
    PROCESSING tickets already get retry semantics from the runner)
    can extend this later.
    """
    _validate_force_fail_args(stage, step_name)
    try:
        conn = await asyncpg.connect(database_url, timeout=_DB_CONNECT_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 — show full reason, including OS errors
        raise RuntimeError(
            f"could not connect to DATABASE_URL: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        async with conn.transaction():
            current_state = await conn.fetchval(
                "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1 FOR UPDATE",
                work_ticket_idx,
            )
            if current_state is None:
                raise RuntimeError(f"no work_ticket with idx={work_ticket_idx}")
            if current_state not in _FORCE_FAIL_ELIGIBLE_STATES:
                raise RuntimeError(
                    f"work_ticket idx={work_ticket_idx} is in terminal state"
                    f" {current_state!r}; refusing to overwrite. Eligible states:"
                    f" {', '.join(_FORCE_FAIL_ELIGIBLE_STATES)}."
                )
            await conn.execute(
                """
                UPDATE qiita.work_ticket
                SET state             = 'failed',
                    failure_type      = 'permanent',
                    failure_stage     = $2,
                    failure_step_name = $3,
                    failure_reason    = $4
                WHERE work_ticket_idx  = $1
                """,
                work_ticket_idx,
                stage,
                step_name,
                reason,
            )
        return {
            "work_ticket_idx": work_ticket_idx,
            "previous_state": current_state,
            "state": "failed",
            "failure_type": "permanent",
            "failure_stage": stage,
            "failure_step_name": step_name,
            "failure_reason": reason,
        }
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# HTTP subcommand helpers
# ---------------------------------------------------------------------------


def _token_revoke_all(base_url: str, token: str, principal_idx: int) -> dict:
    return _common.call(
        "POST",
        base_url,
        token,
        f"/admin/principal/{principal_idx}/revoke-all-tokens",
    )


# ---------------------------------------------------------------------------
# argparse entry point
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qiita-admin", description="Qiita admin CLI")
    _common.add_base_url_arg(parser)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_role = sub.add_parser(
        "set-system-role",
        help="Direct-DB role update (bootstrap path)",
    )
    p_role.add_argument("--email", required=True)
    p_role.add_argument(
        "--role",
        required=True,
        choices=list(_VALID_ROLE_VALUES),
    )
    p_role.set_defaults(handler=_handle_set_system_role)

    p_whoami = sub.add_parser("whoami", help="Print the authenticated principal")
    p_whoami.set_defaults(handler=_handle_whoami)

    p_token = sub.add_parser("token", help="Token operations")
    p_token_sub = p_token.add_subparsers(dest="token_cmd", required=True)
    p_revoke = p_token_sub.add_parser("revoke-all", help="Bulk-revoke all of a principal's tokens")
    p_revoke.add_argument("--principal-idx", required=True, type=int)
    p_revoke.set_defaults(handler=_handle_token_revoke_all)

    p_login = sub.add_parser(
        "login",
        help="AuthRocket LoginRocket Web flow with localhost loopback",
    )
    _common.add_token_file_arg(p_login)
    p_login.set_defaults(handler=_handle_login)

    p_ticket = sub.add_parser("ticket", help="Work-ticket operations")
    p_ticket_sub = p_ticket.add_subparsers(dest="ticket_cmd", required=True)
    p_force_fail = p_ticket_sub.add_parser(
        "force-fail",
        help=(
            "Direct-DB transition of a non-terminal work_ticket to state=failed."
            " Replaces the previous 'operator runs UPDATE qiita.work_ticket by"
            " hand' recovery pattern."
        ),
    )
    p_force_fail.add_argument(
        "--idx", required=True, type=int, dest="work_ticket_idx", help="work_ticket_idx"
    )
    p_force_fail.add_argument("--reason", required=True, help="Operator-supplied failure_reason")
    p_force_fail.add_argument(
        "--stage",
        required=True,
        choices=list(_FAILURE_STAGE_CHOICES),
        help=(
            "failure_stage: submission / step_run / finalize."
            " --step-name is required when --stage=step_run and rejected otherwise."
        ),
    )
    p_force_fail.add_argument(
        "--step-name",
        dest="step_name",
        default=None,
        help="failure_step_name (required iff --stage=step_run)",
    )
    p_force_fail.set_defaults(handler=_handle_ticket_force_fail)

    p_actions = sub.add_parser("actions", help="Action registry operations")
    p_actions_sub = p_actions.add_subparsers(dest="actions_cmd", required=True)
    p_actions_sync = p_actions_sub.add_parser(
        "sync",
        help="Upsert workflows YAMLs into qiita.action (YAML-authoritative columns only)",
    )
    p_actions_sync.add_argument(
        "--workflows-dir",
        type=Path,
        default=Path("workflows"),
        help="Directory to scan for action YAMLs (default: ./workflows)",
    )
    p_actions_sync.set_defaults(handler=_handle_actions_sync)

    p_readiness = sub.add_parser(
        "compute-readiness",
        help=(
            "Exercise the path qiita-job needs and report per-check status."
            " Local checks (JWT, CP /healthz, SLURM_NATIVE_PYTHON on host)"
            " plus an optional SLURM probe-job."
        ),
    )
    p_readiness.add_argument(
        "--orchestrator-venv",
        type=Path,
        default=_DEFAULT_ORCHESTRATOR_VENV,
        help=(
            "Path to the orchestrator's venv; the wrapper invokes"
            f" `<venv>/bin/python -m qiita_compute_orchestrator.cli.compute_readiness`."
            f" Default: {_DEFAULT_ORCHESTRATOR_VENV}"
        ),
    )
    p_readiness.add_argument(
        "--no-slurm-probe",
        action="store_true",
        dest="no_slurm_probe",
        help="Skip the SLURM submit phase; run local checks only.",
    )
    p_readiness.add_argument(
        "--json",
        action="store_true",
        dest="emit_json",
        help="Emit JSON instead of the human-readable report.",
    )
    p_readiness.add_argument(
        "--probe-timeout-seconds",
        type=float,
        default=None,
        help=(
            "Override the orchestrator-side wait for the SLURM probe-job"
            " (the probe itself also has a SLURM time_limit). Default: rely"
            " on the orchestrator-side default."
        ),
    )
    p_readiness.set_defaults(handler=_handle_compute_readiness)

    p_reference = sub.add_parser("reference", help="Reference-data lifecycle operations")
    p_reference_sub = p_reference.add_subparsers(dest="reference_cmd", required=True)
    p_reference_load = p_reference_sub.add_parser(
        "load",
        help=("Upload FASTA + optional inputs and run the reference-add workflow end-to-end"),
    )
    # Reference selection — XOR enforced inside the handler so the help
    # output reads cleanly; argparse's mutually_exclusive_group can't
    # express "either A+B together, or C alone."
    p_reference_load.add_argument("--name", help="New reference name (paired with --version)")
    p_reference_load.add_argument("--version", help="New reference version (paired with --name)")
    p_reference_load.add_argument(
        "--kind",
        default="sequence_reference",
        choices=("sequence_reference", "taxonomy_authority"),
        help="Reference kind for newly-created references (default: sequence_reference)",
    )
    p_reference_load.add_argument(
        "--reference-idx",
        type=int,
        help="Bind to an existing reference instead of creating one",
    )
    p_reference_load.add_argument("--fasta", required=True, type=Path)
    p_reference_load.add_argument("--taxonomy", type=Path)
    p_reference_load.add_argument("--tree", type=Path)
    p_reference_load.add_argument("--jplace", type=Path)
    p_reference_load.add_argument("--genome-map", type=Path, dest="genome_map")
    p_reference_load.add_argument(
        "--data-plane-url",
        required=True,
        help="gRPC URL of the data plane (e.g. grpc://qiita-data.example.com:50051)",
    )
    p_reference_load.add_argument(
        "--no-watch",
        action="store_true",
        help="Submit the work_ticket and exit without polling. Default polls until terminal.",
    )
    p_reference_load.add_argument(
        "--poll-interval",
        type=float,
        default=2.0,
        help="Seconds between work_ticket polls under --watch (default: 2.0)",
    )
    p_reference_load.add_argument(
        "--timeout",
        type=float,
        default=24 * 3600,
        help="Max seconds to wait for the work_ticket under --watch (default: 86400)",
    )
    p_reference_load.set_defaults(handler=_handle_reference_load)

    return parser


# ---------------------------------------------------------------------------
# Subcommand handlers (registered via parser.set_defaults(handler=...))
# ---------------------------------------------------------------------------


def _handle_set_system_role(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("error: DATABASE_URL not set", file=sys.stderr)
        return 2
    try:
        idx = asyncio.run(_set_system_role(database_url, args.email, args.role))
    except (RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(f"updated principal idx={idx} system_role={args.role}")
    return 0


def _handle_whoami(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(lambda t: _common.whoami(args.base_url, t))


def _handle_token_revoke_all(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.run_http_subcommand(
        lambda t: _token_revoke_all(args.base_url, t, args.principal_idx)
    )


def _handle_login(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    return _common.do_login(
        base_url=args.base_url,
        token_file=args.token_file,
        cli_command="qiita-admin login",
    )


def _handle_actions_sync(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("error: DATABASE_URL not set", file=sys.stderr)
        return 2
    try:
        result = asyncio.run(_sync_actions(database_url, args.workflows_dir))
    except (FileNotFoundError, DuplicateActionError, ValidationError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


def _handle_compute_readiness(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Subprocess into the orchestrator's venv to run the compute-readiness
    diagnostic. The orchestrator owns the actual checks (it has the
    Settings.from_env() + SlurmrestdClient surface); this wrapper is a
    thin pass-through so operators have a single `qiita-admin` UX
    surface for cluster-side problems too.

    Returns the subprocess's exit code verbatim so non-zero from any
    check failure propagates up through `qiita-admin` cleanly.
    """
    venv: Path = args.orchestrator_venv
    python = venv / "bin" / "python"
    if not python.exists():
        print(
            f"error: orchestrator python not found at {python}."
            " Pass --orchestrator-venv if the venv is installed elsewhere.",
            file=sys.stderr,
        )
        return 2
    cmd = [str(python), "-m", "qiita_compute_orchestrator.cli.compute_readiness"]
    if args.no_slurm_probe:
        cmd.append("--no-slurm-probe")
    if args.emit_json:
        cmd.append("--json")
    if args.probe_timeout_seconds is not None:
        cmd += ["--probe-timeout-seconds", str(args.probe_timeout_seconds)]
    return subprocess.call(cmd)


def _handle_ticket_force_fail(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("error: DATABASE_URL not set", file=sys.stderr)
        return 2
    try:
        result = asyncio.run(
            _force_fail_ticket(
                database_url,
                work_ticket_idx=args.work_ticket_idx,
                stage=args.stage,
                step_name=args.step_name,
                reason=args.reason,
            )
        )
    except (RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


async def _run_reference_load(
    *,
    base_url: str,
    token: str,
    data_plane_url: str,
    args: argparse.Namespace,
) -> dict:
    """Construct real httpx + pyarrow.flight clients and drive
    `do_reference_load`. Lives next to the CLI handler so the handler
    stays a thin argparse → entry-point shim; the entry point itself
    (in cli.reference_load) takes injected clients so tests bypass this
    function entirely."""
    import httpx as _httpx
    import pyarrow.flight as flight

    from .reference_load import do_reference_load

    flight_client = flight.FlightClient(data_plane_url)
    try:
        async with _httpx.AsyncClient(
            base_url=base_url, timeout=_common.CLI_HTTP_TIMEOUT_SECONDS
        ) as http:
            return await do_reference_load(
                http=http,
                token=token,
                flight_client=flight_client,
                fasta_path=args.fasta,
                name=args.name,
                version=args.version,
                kind=args.kind,
                reference_idx=args.reference_idx,
                taxonomy_path=args.taxonomy,
                tree_path=args.tree,
                jplace_path=args.jplace,
                genome_map_path=args.genome_map,
                watch=not args.no_watch,
                poll_interval_seconds=args.poll_interval,
                timeout_seconds=args.timeout,
            )
    finally:
        flight_client.close()


def _handle_reference_load(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    """Entry point for `qiita-admin reference load`. Reads the PAT, builds
    a real httpx + flight client, and calls `do_reference_load`. Maps
    every known failure shape to exit 1 with a one-line stderr message —
    no silent retry, no buried traceback. Terminal work_ticket=failed
    also exits 1 so callers wrapping this in a Makefile / CI step get
    the build break."""
    import httpx as _httpx
    import pyarrow.flight as _flight

    try:
        token = _common.read_token()
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    try:
        result = asyncio.run(
            _run_reference_load(
                base_url=args.base_url,
                token=token,
                data_plane_url=args.data_plane_url,
                args=args,
            )
        )
    except _httpx.HTTPStatusError as exc:
        print(
            f"http error {exc.response.status_code}: {exc.response.text}",
            file=sys.stderr,
        )
        return 1
    except _flight.FlightError as exc:
        # Catch the gRPC-level error explicitly so the operator sees a
        # formatted error line instead of a raw traceback. FlightError is
        # NOT a RuntimeError subclass, so the catch-all below would miss
        # it. Common shapes: network refused, expired ticket, DP
        # rejected the stream mid-write.
        print(f"flight error: {exc}", file=sys.stderr)
        return 1
    except (RuntimeError, ValueError, TimeoutError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    # Terminal work_ticket=failed under --watch surfaces as exit 1, not
    # exit 0 — a CI step wrapping this CLI must distinguish a successful
    # reference build from a failed one. The JSON body still goes to
    # stdout so the caller can see the failure_reason.
    work_ticket = result.get("work_ticket") or {}
    final_state = work_ticket.get("state")
    print(json.dumps(_serializable(result), indent=2))
    if final_state == "failed":
        return 1
    return 0


def _serializable(obj):
    """Recursively replace Pydantic / Path values with their JSON form so
    `json.dumps` succeeds on the result dict (which carries upload-idx
    metadata + the final work_ticket body)."""
    from pathlib import Path as _Path

    if isinstance(obj, dict):
        return {k: _serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serializable(v) for v in obj]
    if isinstance(obj, _Path):
        return str(obj)
    return obj


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    _common.validate_base_url(args, parser)
    return args.handler(args, parser)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
