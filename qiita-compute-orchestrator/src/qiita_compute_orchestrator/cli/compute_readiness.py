"""qiita compute-readiness — operator-facing diagnostic.

Exercises the path qiita-job needs end-to-end so the next deploy site
doesn't rediscover gaps interactively. The first end-to-end
fastq-to-parquet smoke (May 2026) surfaced 9 distinct gaps, several
of which only manifested when SLURM submitted a job to a compute
node: SLURM_NATIVE_PYTHON pointing at a non-visible venv, HOME unset
breaking DuckDB's extension cache, /etc/qiita/*.token not readable
from compute nodes, a stale JWT silently authenticating as the prior
tester. Discovery was iterative — submit a workflow, watch it fail,
ssh to the node to probe. This command compresses that into one run.

Two phases:

1. **Local checks** (always run on the orchestrator host):
   - JWT readable, well-shaped, `sun` matches `SLURMRESTD_USER_NAME`,
     not expired (`exp` claim in the future).
   - SLURM_NATIVE_PYTHON path exists and is executable from the
     orchestrator host. Visibility from a compute node is verified
     by the probe phase.
   - QIITA_CP_URL/healthz reachable with the CO→CP token. Catches
     misconfigured URL, dead nginx, wrong token before any SLURM job
     burns walltime.

2. **SLURM probe** (skippable with --no-slurm-probe):
   Submit a 1-cpu, 1-min bash job through the configured slurmrestd
   that emits structured `key=value` lines to stdout covering:
     - hostname / running user (so the operator sees who the job
       actually ran as — surfaces a JWT sun-vs-actual-user mismatch
       that doesn't show up in the local JWT check)
     - SLURM_NATIVE_PYTHON exists on the compute node + can import
       `qiita_compute_orchestrator.jobs` (the launcher's entry point)
     - SHARED_FILESYSTEM_ROOT visible + writable
     - QIITA_CP_URL reachable *from the compute node* with the
       CO→CP token (catches network partitions between cluster and
       deploy host that don't show up in the local CP check)
   Polls until terminal (default 5 min cap), reads the probe job's
   stdout, parses, reports.

Output is human-readable by default (✓ pass / ✗ fail / · skip) with
an aggregate exit code (0 if all-pass, 1 if any fail). `--json`
switches to machine-readable JSON for scripted consumption.

Invoked by `qiita-admin compute-readiness` on the control plane side
via subprocess into the orchestrator's venv — see
`qiita_control_plane.cli.admin._handle_compute_readiness`.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import shlex
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import httpx

from ..config import Settings

# Probe job constants. Conservative: the probe is purely diagnostic, no
# need to fight for big allocations.
_PROBE_JOB_NAME = "qiita-compute-readiness-probe"
_PROBE_CPU = 1
_PROBE_MEM_MB = 256
_PROBE_TIME_LIMIT_MINUTES = 5
_DEFAULT_POLL_INTERVAL_SECONDS = 5
_DEFAULT_PROBE_TIMEOUT_SECONDS = 5 * 60

# The probe script's structured-output marker. Lines on stdout that
# start with this prefix are parsed back into CheckResults by the
# parent; everything else is echoed as context. Same idea as
# `slurm/launcher_failure.py` uses for the launcher-failure line.
_PROBE_LINE_PREFIX = "compute-readiness:"


@dataclass(frozen=True)
class CheckResult:
    """One row in the report. `status` is "pass" | "fail" | "skip"; the
    aggregate exit code is 1 iff any row is "fail" (skip is non-fatal —
    a skipped CP check on a host with no CP_URL is informational, not
    broken)."""

    name: str
    status: str
    detail: str

    def render(self) -> str:
        glyph = {"pass": "✓", "fail": "✗", "skip": "·"}[self.status]
        return f"  {glyph} {self.name}: {self.detail}"


# ---------------------------------------------------------------------------
# Local checks
# ---------------------------------------------------------------------------


def _decode_jwt_payload(token: str) -> dict[str, Any]:
    """Decode the payload segment of a JWT to a Python dict. Stdlib
    only — signature verification is slurmrestd's job; we only need
    the claims to be readable.

    Mirrors the decode in `slurm/client._verify_jwt_sun_matches`;
    duplicated here because that helper raises specific RuntimeErrors
    suited to boot-time fail-fast, whereas this CLI wants to surface
    each subclaim as a separate CheckResult.
    """
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError(f"expected 3 segments (header.payload.signature), got {len(parts)}")
    payload_segment = parts[1]
    padding = "=" * (-len(payload_segment) % 4)
    payload_bytes = base64.urlsafe_b64decode(payload_segment + padding)
    payload = json.loads(payload_bytes)
    if not isinstance(payload, dict):
        raise ValueError(f"payload is not a JSON object: {payload!r}")
    return payload


def check_jwt(jwt_path: Path, expected_user: str) -> list[CheckResult]:
    """Read SLURM JWT, decode, validate `sun` matches the configured
    user and `exp` is in the future. Each subclaim is its own check
    row so the operator sees which specific claim is wrong rather
    than a single opaque "JWT bad"."""
    results: list[CheckResult] = []
    try:
        token = jwt_path.read_text().strip()
    except OSError as exc:
        results.append(CheckResult("jwt-readable", "fail", f"{jwt_path}: {exc}"))
        return results
    if not token:
        results.append(CheckResult("jwt-readable", "fail", f"{jwt_path} is empty"))
        return results
    results.append(CheckResult("jwt-readable", "pass", str(jwt_path)))

    try:
        payload = _decode_jwt_payload(token)
    except (ValueError, json.JSONDecodeError) as exc:
        results.append(CheckResult("jwt-shape", "fail", str(exc)))
        return results
    results.append(
        CheckResult("jwt-shape", "pass", "header.payload.signature, payload JSON-decoded")
    )

    sun = payload.get("sun")
    if sun == expected_user:
        results.append(CheckResult("jwt-sun-match", "pass", f"sun={sun!r}"))
    else:
        results.append(
            CheckResult(
                "jwt-sun-match",
                "fail",
                f"sun={sun!r} != SLURMRESTD_USER_NAME={expected_user!r}"
                " (was this JWT minted by the wrong user, or has the refresh"
                " timer paged a stale token into the orchestrator?)",
            )
        )

    exp = payload.get("exp")
    if not isinstance(exp, (int, float)):
        results.append(CheckResult("jwt-exp", "fail", f"`exp` missing or non-numeric: {exp!r}"))
    else:
        now = time.time()
        if exp < now:
            results.append(
                CheckResult("jwt-exp", "fail", f"expired {int(now - exp)}s ago (exp={int(exp)})")
            )
        else:
            results.append(CheckResult("jwt-exp", "pass", f"valid for {int(exp - now)}s"))
    return results


def check_native_python_on_host(native_python: str) -> CheckResult:
    """The SLURM_NATIVE_PYTHON path must exist on the orchestrator host
    (it's on a shared filesystem in production, so host-side absence is
    diagnostic for "this won't work on compute nodes either"). The
    probe phase verifies compute-node visibility separately."""
    if native_python == "python":
        return CheckResult(
            "native-python-on-host",
            "skip",
            "SLURM_NATIVE_PYTHON=python (assumes compute-node PATH; visibility is probe-only)",
        )
    p = Path(native_python)
    if not p.exists():
        return CheckResult(
            "native-python-on-host",
            "fail",
            f"{native_python} does not exist on the orchestrator host",
        )
    if not os.access(p, os.X_OK):
        return CheckResult("native-python-on-host", "fail", f"{native_python} is not executable")
    return CheckResult("native-python-on-host", "pass", native_python)


async def check_cp_healthz(cp_url: str, co_to_cp_token: str) -> CheckResult:
    """GET {cp_url}/healthz with the CO→CP bearer. Catches a wrong
    URL, a misnamed env, an expired PAT, or a CP that's down before
    we burn a SLURM submission on it."""
    if not cp_url:
        return CheckResult("cp-healthz", "skip", "QIITA_CP_URL not set")
    if not co_to_cp_token:
        return CheckResult("cp-healthz", "skip", "CO_TO_CP_TOKEN not resolvable on the host")
    url = f"{cp_url.rstrip('/')}/healthz"
    headers = {"Authorization": f"Bearer {co_to_cp_token}"}
    try:
        async with httpx.AsyncClient(timeout=10) as http:
            resp = await http.get(url, headers=headers)
    except httpx.HTTPError as exc:
        return CheckResult("cp-healthz", "fail", f"{url}: {type(exc).__name__}: {exc}")
    if resp.status_code == 200:
        return CheckResult("cp-healthz", "pass", f"GET {url} → 200")
    return CheckResult("cp-healthz", "fail", f"GET {url} → {resp.status_code}")


# ---------------------------------------------------------------------------
# SLURM probe
# ---------------------------------------------------------------------------


def build_probe_script(*, shared_filesystem_root: str) -> str:
    """Bash script the probe SLURM job runs on a compute node. Every
    informational line is prefixed with `compute-readiness:` so the
    parent CLI can pick it out of arbitrary stdout/stderr noise; the
    suffix is `key=value` so parsing stays one-line-per-check.

    Script always exits 0 so we can distinguish "probe ran and
    reported failures" from "probe didn't run at all" (SLURM-side
    timeout or transport failure → no probe lines arrive).
    """
    # `${VAR:-default}` shape handles env vars the orchestrator may or
    # may not have set. Defaults match the Settings field defaults.
    # `set -u` is intentionally off so a missing env var isn't a hard
    # script error — we want the report line, not a non-zero exit.
    sf_default = shlex.quote(shared_filesystem_root)
    return f"""#!/bin/bash
# Stay forgiving: a missing env var should produce a `*-set=no` line,
# not abort the probe and lose all subsequent diagnostics.
set +u
echo "{_PROBE_LINE_PREFIX} hostname=$(hostname)"
echo "{_PROBE_LINE_PREFIX} uid=$(id -u)"
echo "{_PROBE_LINE_PREFIX} user=$(id -un)"

PYTHON="${{SLURM_NATIVE_PYTHON:-python}}"
if [ -x "$PYTHON" ]; then
    echo "{_PROBE_LINE_PREFIX} native-python-on-compute=ok path=$PYTHON"
else
    echo "{_PROBE_LINE_PREFIX} native-python-on-compute=missing path=$PYTHON"
fi

if "$PYTHON" -c 'import qiita_compute_orchestrator.jobs' 2>/dev/null; then
    echo "{_PROBE_LINE_PREFIX} native-import=ok"
else
    echo "{_PROBE_LINE_PREFIX} native-import=fail"
fi

SF="${{SHARED_FILESYSTEM_ROOT:-{sf_default}}}"
if [ -d "$SF" ]; then
    echo "{_PROBE_LINE_PREFIX} shared-fs-visible=yes path=$SF"
    PROBE_DIR="$SF/.compute-readiness.$$"
    if mkdir "$PROBE_DIR" 2>/dev/null && rmdir "$PROBE_DIR"; then
        echo "{_PROBE_LINE_PREFIX} shared-fs-writable=yes"
    else
        echo "{_PROBE_LINE_PREFIX} shared-fs-writable=no"
    fi
else
    echo "{_PROBE_LINE_PREFIX} shared-fs-visible=no path=$SF"
fi

CP_URL="${{QIITA_CP_URL:-}}"
TOK="${{CO_TO_CP_TOKEN:-}}"
if [ -n "$CP_URL" ] && [ -n "$TOK" ]; then
    if curl -fsS -m 10 -H "Authorization: Bearer $TOK" "$CP_URL/healthz" >/dev/null 2>&1; then
        echo "{_PROBE_LINE_PREFIX} cp-from-compute=ok cp_url=$CP_URL"
    else
        echo "{_PROBE_LINE_PREFIX} cp-from-compute=fail cp_url=$CP_URL"
    fi
else
    CPSET=$([ -n "$CP_URL" ] && echo y || echo n)
    TOKSET=$([ -n "$TOK" ] && echo y || echo n)
    echo "{_PROBE_LINE_PREFIX} cp-from-compute=skip cp_url_set=$CPSET token_set=$TOKSET"
fi
exit 0
"""


def build_probe_submit_payload(
    *,
    script: str,
    settings: Settings,
    log_path: Path,
) -> dict[str, Any]:
    """Minimal slurmrestd `POST /job/submit` body for the probe. Does
    NOT route through `build_job_submit_payload` because that helper
    encodes qiita-step contract (params.json, mounts, native vs
    container dispatch) the probe doesn't need."""
    assert settings.slurm is not None, "build_probe_submit_payload requires slurm settings"
    env: dict[str, str] = {
        "SLURM_NATIVE_PYTHON": settings.slurm.native_python,
        "SHARED_FILESYSTEM_ROOT": settings.shared_filesystem_root,
    }
    if settings.cp_url:
        env["QIITA_CP_URL"] = settings.cp_url
    if settings.co_to_cp_token:
        env["CO_TO_CP_TOKEN"] = settings.co_to_cp_token
    job: dict[str, Any] = {
        "name": _PROBE_JOB_NAME,
        "account": settings.slurm.account,
        "partition": settings.slurm.partition,
        # Stay out of the per-ticket workspace tree — this is a one-off
        # probe; /tmp is fine for cwd. The bash script doesn't write
        # there anyway.
        "current_working_directory": "/tmp",
        "environment": [f"{k}={v}" for k, v in sorted(env.items())],
        # slurmrestd wants the typed-numeric envelope here.
        "memory_per_node": {"number": _PROBE_MEM_MB, "set": True, "infinite": False},
        "tasks": 1,
        "cpus_per_task": _PROBE_CPU,
        "time_limit": {"number": _PROBE_TIME_LIMIT_MINUTES, "set": True, "infinite": False},
        "standard_output": str(log_path),
        "standard_error": str(log_path),
    }
    if settings.slurm.qos:
        job["qos"] = settings.slurm.qos
    return {"script": script, "job": job}


_TERMINAL_STATES = {
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "NODE_FAIL",
    "BOOT_FAIL",
    "DEADLINE",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "SPECIAL_EXIT",
}


async def submit_probe_and_collect(
    settings: Settings,
    *,
    poll_interval_seconds: float = _DEFAULT_POLL_INTERVAL_SECONDS,
    probe_timeout_seconds: float = _DEFAULT_PROBE_TIMEOUT_SECONDS,
    log_dir: Path | None = None,
) -> list[CheckResult]:
    """End-to-end probe: build script + payload, submit via the
    configured slurmrestd, poll until terminal or timeout, read the
    log file, parse `compute-readiness:` lines into CheckResults."""
    # Lazy import so this module is loadable in tests that don't have
    # an httpx-backed slurmrestd available.
    from ..slurm.client import SlurmrestdClient

    assert settings.slurm is not None, "compute-readiness probe requires COMPUTE_BACKEND=slurm"
    log_dir = log_dir or Path("/tmp")
    log_path = log_dir / f"qiita-compute-readiness.{os.getpid()}.log"
    script = build_probe_script(shared_filesystem_root=settings.shared_filesystem_root)
    payload = build_probe_submit_payload(script=script, settings=settings, log_path=log_path)

    async with SlurmrestdClient(
        base_url=settings.slurm.base_url,
        jwt_path=settings.slurm.jwt_path,
        user_name=settings.slurm.user_name,
        api_version=settings.slurm.api_version,
    ) as client:
        try:
            job_id = await client.submit_job(payload)
        except Exception as exc:  # noqa: BLE001 — surface any transport / 5xx as one fail row
            return [CheckResult("slurm-submit", "fail", f"{type(exc).__name__}: {exc}")]

        results: list[CheckResult] = [
            CheckResult("slurm-submit", "pass", f"job_id={job_id}, polling for terminal state"),
        ]
        deadline = time.monotonic() + probe_timeout_seconds
        terminal_state: str | None = None
        while time.monotonic() < deadline:
            try:
                info = await client.get_job(job_id)
            except Exception as exc:  # noqa: BLE001
                results.append(CheckResult("slurm-poll", "fail", f"{type(exc).__name__}: {exc}"))
                return results
            if info.state in _TERMINAL_STATES:
                terminal_state = info.state
                break
            await asyncio.sleep(poll_interval_seconds)
        if terminal_state is None:
            results.append(
                CheckResult(
                    "slurm-probe-timeout",
                    "fail",
                    (
                        f"job_id={job_id} did not reach terminal state within"
                        f" {probe_timeout_seconds:.0f}s"
                    ),
                )
            )
            return results
        if terminal_state == "COMPLETED":
            results.append(CheckResult("slurm-probe-completed", "pass", f"state={terminal_state}"))
        else:
            results.append(
                CheckResult(
                    "slurm-probe-completed",
                    "fail",
                    f"state={terminal_state} (probe script exit was non-zero or SLURM killed it)",
                )
            )

    try:
        log_text = log_path.read_text()
    except OSError as exc:
        results.append(
            CheckResult(
                "slurm-probe-log",
                "fail",
                f"could not read probe log at {log_path}: {exc} (job may have run on a"
                " compute node without shared-FS visibility to this path)",
            )
        )
        return results
    results.extend(_parse_probe_log(log_text))
    # Best-effort cleanup; if it fails the file is in /tmp and SLURM
    # rotation will handle it.
    try:
        log_path.unlink()
    except OSError:
        pass
    return results


def _parse_probe_log(log_text: str) -> list[CheckResult]:
    """Pick `compute-readiness: <key>=<value> [...]` lines out of the
    probe job's stdout and turn each into a CheckResult.

    The probe script's contract: exactly one key=value pair per line
    determines the check's name + status; any trailing key=value pairs
    on the same line become the detail string.

    Status mapping:
      - native-python-on-compute=ok        → pass
      - native-python-on-compute=missing   → fail
      - native-import=ok                   → pass
      - native-import=fail                 → fail
      - shared-fs-visible=yes/no           → pass / fail
      - shared-fs-writable=yes/no          → pass / fail
      - cp-from-compute=ok/fail/skip       → pass / fail / skip
      - hostname / uid / user              → informational (pass with detail)
    """
    results: list[CheckResult] = []
    for raw_line in log_text.splitlines():
        line = raw_line.strip()
        if not line.startswith(_PROBE_LINE_PREFIX):
            continue
        payload = line[len(_PROBE_LINE_PREFIX) :].strip()
        if not payload:
            continue
        # Split into the primary key=value and the rest.
        primary, _, extra = payload.partition(" ")
        if "=" not in primary:
            continue
        key, _, value = primary.partition("=")
        detail = value
        if extra:
            detail = f"{value} ({extra})"
        status = _classify_probe_pair(key, value)
        results.append(CheckResult(name=f"probe/{key}", status=status, detail=detail))
    return results


_PROBE_PASS_VALUES = {"ok", "yes"}
_PROBE_FAIL_VALUES = {"fail", "no", "missing"}
_PROBE_SKIP_VALUES = {"skip"}
# `hostname`, `uid`, `user` are informational — never a fail condition;
# the operator reads the actual value to confirm the job ran as the
# expected SLURM user.
_INFORMATIONAL_KEYS = {"hostname", "uid", "user"}


def _classify_probe_pair(key: str, value: str) -> str:
    if key in _INFORMATIONAL_KEYS:
        return "pass"
    if value in _PROBE_PASS_VALUES:
        return "pass"
    if value in _PROBE_FAIL_VALUES:
        return "fail"
    if value in _PROBE_SKIP_VALUES:
        return "skip"
    # Unknown values default to "fail" rather than silently passing —
    # an unrecognized probe line is a contract drift and the operator
    # should see it.
    return "fail"


# ---------------------------------------------------------------------------
# Top-level runner
# ---------------------------------------------------------------------------


async def _run_all_checks(
    *,
    skip_slurm_probe: bool,
    poll_interval_seconds: float,
    probe_timeout_seconds: float,
) -> list[CheckResult]:
    results: list[CheckResult] = []
    # Use require_cp_to_co_token=False because the diagnostic doesn't
    # serve /step/run; we don't want a partial install (missing inbound
    # bearer) to abort all checks before reporting anything actionable.
    try:
        settings = Settings.from_env(require_cp_to_co_token=False)
    except RuntimeError as exc:
        results.append(CheckResult("settings-resolvable", "fail", str(exc)))
        return results
    results.append(CheckResult("settings-resolvable", "pass", f"backend={settings.backend_type}"))

    if settings.backend_type != "slurm":
        results.append(
            CheckResult(
                "backend-is-slurm",
                "skip",
                f"COMPUTE_BACKEND={settings.backend_type!r} — compute-readiness is SLURM-specific",
            )
        )
        return results
    assert settings.slurm is not None  # narrowed by backend_type check

    results.extend(check_jwt(settings.slurm.jwt_path, settings.slurm.user_name))
    results.append(check_native_python_on_host(settings.slurm.native_python))
    results.append(await check_cp_healthz(settings.cp_url, settings.co_to_cp_token))

    if skip_slurm_probe:
        results.append(CheckResult("slurm-probe", "skip", "--no-slurm-probe set"))
        return results

    results.extend(
        await submit_probe_and_collect(
            settings,
            poll_interval_seconds=poll_interval_seconds,
            probe_timeout_seconds=probe_timeout_seconds,
        )
    )
    return results


def _render_human(results: list[CheckResult]) -> str:
    lines = ["compute-readiness report:"]
    for r in results:
        lines.append(r.render())
    n_fail = sum(1 for r in results if r.status == "fail")
    n_pass = sum(1 for r in results if r.status == "pass")
    n_skip = sum(1 for r in results if r.status == "skip")
    lines.append(f"summary: {n_pass} pass, {n_fail} fail, {n_skip} skip")
    return "\n".join(lines)


def _render_json(results: list[CheckResult]) -> str:
    return json.dumps(
        {
            "results": [asdict(r) for r in results],
            "summary": {
                "pass": sum(1 for r in results if r.status == "pass"),
                "fail": sum(1 for r in results if r.status == "fail"),
                "skip": sum(1 for r in results if r.status == "skip"),
            },
        },
        indent=2,
    )


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m qiita_compute_orchestrator.cli.compute_readiness",
        description="Exercise the path qiita-job needs and report per-check status.",
    )
    p.add_argument(
        "--no-slurm-probe",
        dest="skip_slurm_probe",
        action="store_true",
        help=(
            "Skip the SLURM submit phase. Local checks (JWT, CP /healthz) still"
            " run. Useful when the cluster is known-unreachable and you want"
            " to triage host-side state."
        ),
    )
    p.add_argument(
        "--json",
        dest="emit_json",
        action="store_true",
        help="Emit machine-readable JSON instead of the human-readable report.",
    )
    p.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=_DEFAULT_POLL_INTERVAL_SECONDS,
        help=f"Seconds between probe-job polls (default {_DEFAULT_POLL_INTERVAL_SECONDS}).",
    )
    p.add_argument(
        "--probe-timeout-seconds",
        type=float,
        default=_DEFAULT_PROBE_TIMEOUT_SECONDS,
        help=(
            f"Cap on probe-job wait (default {_DEFAULT_PROBE_TIMEOUT_SECONDS}s)."
            " The probe itself has a SLURM time_limit; this is the orchestrator-"
            "side wait before declaring a timeout."
        ),
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    results = asyncio.run(
        _run_all_checks(
            skip_slurm_probe=args.skip_slurm_probe,
            poll_interval_seconds=args.poll_interval_seconds,
            probe_timeout_seconds=args.probe_timeout_seconds,
        )
    )
    output = _render_json(results) if args.emit_json else _render_human(results)
    print(output)
    return 1 if any(r.status == "fail" for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
