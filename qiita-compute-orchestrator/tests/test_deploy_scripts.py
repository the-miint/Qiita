"""Smoke-check the deploy shell scripts (deploy/*.sh) and the workflow entrypoints.

These run on the Linux deploy host, not in CI's Python env, so they have no
unit-test harness of their own. This pure-unit guard (under `make test`) catches
the cheap-but-real failures — syntax errors and shellcheck warnings — before they
ship to a host where a broken deploy script is expensive. Mirrors the `bash -n`
precedent in test_compute_readiness.py::test_probe_script_is_valid_bash and the
repo-root reach in test_sif_build_spec.py.

The workflow entrypoints get the `bash -n` half for the same reason and a sharper
one: they run inside a SIF, so a syntax error surfaces as a container step dying
on a real ticket, after the image has been rebuilt and staged. Several embed a
long single-quoted awk program, where an apostrophe in a comment closes the quote
and breaks the script — the shape this gate is here to stop.

shellcheck is optional: when it isn't installed the shellcheck assertion skips
gracefully (same posture as the apptainer-optional workflow tests).
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import time
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEPLOY = _REPO_ROOT / "deploy"
_COMMON = _DEPLOY / "_common.sh"
_BUILD_SIF = _REPO_ROOT / "scripts" / "build-sif.sh"
_LAKE_SHELL = _REPO_ROOT / "scripts" / "lake-shell.sh"
_LAKE_GC = _REPO_ROOT / "scripts" / "lake-gc.sh"

# The scripts introduced/maintained for the deploy-ease work. Kept
# explicit (not a glob) so a new deploy script is a deliberate add here.
# build-sifs.sh is the deploy-time SIF auto-builder (wraps scripts/build-sif.sh).
_SCRIPTS = ("preflight.sh", "verify.sh", "redeploy.sh", "build-sifs.sh")
# Sourced-only fragments (no shebang-as-entrypoint, not executable). _common.sh
# carries real logic (qiita_native_checkout_from_python etc.) the executable
# scripts rely on, so it gets the same bash -n + shellcheck gate — but NOT the
# executable-bit check below, since it's never run directly.
_SOURCED = ("_common.sh",)

# The per-step entrypoints and the shared helper they source. A glob, not a list:
# a new workflow step ships a new .sh, and it wants this gate by default rather
# than by remembering to register it.
_WORKFLOW_SCRIPTS = sorted((_REPO_ROOT / "workflows").rglob("*.sh"))


@pytest.mark.parametrize("name", _SCRIPTS)
def test_deploy_script_exists_and_executable(name: str) -> None:
    path = _DEPLOY / name
    assert path.is_file(), f"{path} missing"
    assert path.stat().st_mode & 0o111, f"{path} is not executable"


@pytest.mark.parametrize("name", _SCRIPTS + _SOURCED)
def test_deploy_script_is_valid_bash(name: str) -> None:
    """`bash -n` parses the script without executing it — catches the unmatched
    quote / stray fi class of bug that broke deploys before."""
    path = _DEPLOY / name
    result = subprocess.run(
        ["bash", "-n", str(path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"bash -n failed for {name}:\n{result.stderr}"


@pytest.mark.parametrize("name", _SCRIPTS + _SOURCED)
def test_deploy_script_passes_shellcheck(name: str) -> None:
    # Gate on warnings+ (`-S warning`), not the default info/style level, so the
    # check is deterministic across shellcheck versions — info checks like SC2015
    # ("A && B || C is not if-then-else") are enabled/disabled differently between
    # releases (CI's apt build flags some that a newer local build doesn't), which
    # would otherwise flake CI on a stylistic note. Mirrors the repo's
    # `cargo clippy -- -D warnings` posture: catch the substantive issues
    # (unquoted expansions, real logic bugs), not the version-unstable nits.
    if shutil.which("shellcheck") is None:
        pytest.skip("shellcheck not installed")
    path = _DEPLOY / name
    result = subprocess.run(
        ["shellcheck", "-S", "warning", str(path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"shellcheck flagged {name}:\n{result.stdout}\n{result.stderr}"


def _call_native_checkout(native_python: str) -> subprocess.CompletedProcess[str]:
    """Source _common.sh and invoke qiita_native_checkout_from_python with one arg.
    Returns the CompletedProcess so a test can assert on returncode + stdout."""
    return subprocess.run(
        [
            "bash",
            "-c",
            f'source "{_COMMON}"; qiita_native_checkout_from_python "$1"',
            "_",
            native_python,
        ],
        capture_output=True,
        text=True,
    )


def _fake_native_checkout(tmp_path: Path) -> Path:
    """Build a `<repo>/qiita-compute-orchestrator/.venv/bin/python` layout under a
    git clone, matching what SLURM_NATIVE_PYTHON points at in production."""
    checkout = tmp_path / "qiita-compute-orchestrator"
    (checkout / ".venv" / "bin").mkdir(parents=True)
    (tmp_path / ".git").mkdir()  # the repo root the checkout sits under
    (checkout / "pyproject.toml").write_text("[project]\nname='qiita-compute-orchestrator'\n")
    py = checkout / ".venv" / "bin" / "python"
    py.write_text("#!/bin/sh\n")
    py.chmod(0o755)
    return py


# The data-plane.env keys the deploy reads. Removed from every topology test's
# environment so the caller's shell cannot reach the helpers under test.
_TOPOLOGY_VARS = (
    "QIITA_DATA_PLANE_PORTS",
    "QIITA_DATA_PLANE_PEERS",
    "QIITA_DATA_PLANE_BIND_HOST",
    "QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX",
    "QIITA_DATA_PLANE_LOCAL_INSTANCES",
    "DUCKDB_MEMORY_LIMIT",
    "DUCKLAKE_POOL_SIZE",
)
_ABSENT_DP_ENV = "/nonexistent/qiita/data-plane.env"


def _run_common(
    snippet: str,
    *args: str,
    dp_env: Path | None = None,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Source _common.sh and run `snippet`, with `args` as $1…

    The topology variables are removed from the environment and DP_ENV points at
    `dp_env`, or at a path that does not exist, so neither the caller's shell nor a
    real /etc/qiita/data-plane.env reaches the test. `extra_env` is applied last."""
    env = {k: v for k, v in os.environ.items() if k not in _TOPOLOGY_VARS}
    env["DP_ENV"] = str(dp_env) if dp_env is not None else _ABSENT_DP_ENV
    env.update(extra_env or {})
    return subprocess.run(
        ["bash", "-c", f'source "{_COMMON}"; {snippet}', "_", *args],
        capture_output=True,
        text=True,
        env=env,
    )


def _dp_env(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "data-plane.env"
    path.write_text(body)
    return path


def _lb_port() -> str:
    """QIITA_DATA_PLANE_LB_PORT as _common.sh defines it."""
    result = _run_common('printf %s "$QIITA_DATA_PLANE_LB_PORT"')
    assert result.returncode == 0 and result.stdout, result.stderr
    return result.stdout


def test_data_plane_ports_defaults_to_the_single_instance() -> None:
    """Set neither in the environment nor in data-plane.env ⇒ the one instance every
    host has run, so a host that sets nothing deploys as before."""
    result = _run_common("qiita_data_plane_ports")
    assert result.returncode == 0, result.stderr
    assert result.stdout == "50051"


def test_data_plane_units_of_an_unconfigured_host_are_the_single_instance() -> None:
    """The units the deploy waits on and restarts, from the default port list: the
    one `qiita-data-plane@50051` a single-machine host has always run."""
    result = _run_common('qiita_data_plane_units "$(qiita_data_plane_ports)"')
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["qiita-data-plane@50051"]


def test_data_plane_units_names_one_unit_per_port() -> None:
    result = _run_common('qiita_data_plane_units "50051 50052"')
    assert result.stdout.split() == ["qiita-data-plane@50051", "qiita-data-plane@50052"]


# (active, enabled) per instance, as `systemctl list-units --all` and `is-enabled`
# report them on systemd 257: an instance neither running nor enabled is not listed.
_UNIT_STATES = {
    "50051": ("active", "enabled"),  # configured
    "50052": ("active", "disabled"),  # started by hand, outside the list
    "50053": ("inactive", "enabled"),  # stopped, but starts at boot
    "50054": ("failed", "disabled"),  # not serving and not starting at boot
    "50055": ("activating", "disabled"),  # restarting after a crash
}


def _run_stray_units(
    tmp_path: Path,
    ports: str,
    list_rc: int = 0,
    snippet: str = "qiita_data_plane_stray_units",
    extra_env: dict[str, str] | None = None,
):
    stub = tmp_path / "stub"
    stub.mkdir()
    listing = "".join(
        f"qiita-data-plane@{port}.service loaded {active} running x\n"
        for port, (active, _) in _UNIT_STATES.items()
    )
    enabled = "".join(
        f"  *@{port}.service) echo {state}; [ {state} = enabled ] ;;\n"
        for port, (_, state) in _UNIT_STATES.items()
    )
    (stub / "systemctl").write_text(
        "#!/bin/sh\n"
        'case "$1" in\n'
        f"  list-units) printf '{listing}'; exit {list_rc} ;;\n"
        '  is-enabled) case "$2" in\n'
        f"{enabled}"
        "  esac ;;\n"
        "esac\n"
    )
    (stub / "systemctl").chmod(0o755)
    return _run_common(
        f'{snippet} "$1"',
        ports,
        extra_env={"PATH": f"{stub}:{os.environ['PATH']}", **(extra_env or {})},
    )


def test_stray_units_reports_running_or_enabled_instances_outside_the_list(
    tmp_path: Path,
) -> None:
    """A port taken out of QIITA_DATA_PLANE_PORTS leaves its unit on the previous
    build, outside the upstream, until someone disables it; verify-deploy fails on
    it. A failed, disabled unit serves nothing and starts at no boot."""
    result = _run_stray_units(tmp_path, "50051")
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "qiita-data-plane@50052.service active/disabled",
        "qiita-data-plane@50053.service inactive/enabled",
        "qiita-data-plane@50055.service activating/disabled",
    ]


def test_stray_units_is_empty_when_every_instance_is_configured(tmp_path: Path) -> None:
    result = _run_stray_units(tmp_path, "50051\t50052 50053\n50054 50055")
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


def test_stray_units_fails_when_systemctl_cannot_list(tmp_path: Path) -> None:
    result = _run_stray_units(tmp_path, "50051", list_rc=1)
    assert result.returncode != 0
    assert "could not list" in result.stderr


def test_units_to_disable_stops_the_deploy_without_the_flag(tmp_path: Path) -> None:
    """Without QIITA_DP_DISABLE_REMOVED=1 a stray instance stops the deploy, naming
    it and the ways out, so no deploy leaves an instance on the previous build."""
    result = _run_stray_units(tmp_path, "50051", snippet="qiita_data_plane_units_to_disable")
    assert result.returncode != 0
    assert result.stdout == ""
    assert "qiita-data-plane@50052.service active/disabled" in result.stderr
    assert "QIITA_DP_DISABLE_REMOVED=1" in result.stderr


def test_units_to_disable_names_the_strays_with_the_flag(tmp_path: Path) -> None:
    result = _run_stray_units(
        tmp_path,
        "50051",
        snippet="qiita_data_plane_units_to_disable",
        extra_env={"QIITA_DP_DISABLE_REMOVED": "1"},
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == [
        "qiita-data-plane@50052.service",
        "qiita-data-plane@50053.service",
        "qiita-data-plane@50055.service",
    ]


@pytest.mark.parametrize("flag", ["", "1"])
def test_units_to_disable_is_empty_without_strays(tmp_path: Path, flag: str) -> None:
    """A host running only its listed instances deploys as before, flag or not."""
    result = _run_stray_units(
        tmp_path,
        "50051 50052 50053 50054 50055",
        snippet="qiita_data_plane_units_to_disable",
        extra_env={"QIITA_DP_DISABLE_REMOVED": flag},
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


@pytest.mark.parametrize("flag", ["yes", "true", "0"])
def test_units_to_disable_rejects_a_flag_other_than_1(tmp_path: Path, flag: str) -> None:
    result = _run_stray_units(
        tmp_path,
        "50051 50052 50053 50054 50055",
        snippet="qiita_data_plane_units_to_disable",
        extra_env={"QIITA_DP_DISABLE_REMOVED": flag},
    )
    assert result.returncode != 0
    assert "must be 1 or unset" in result.stderr


def test_activate_stops_removed_instances_alongside_the_restart() -> None:
    """The strays are resolved before anything is installed; each is disabled and its
    stop queued before the restart, so its drain overlaps the listed instances', and
    a blocking stop after the nginx reload waits for it, so nginx drops the removed
    member without waiting on its drain."""
    src = (_DEPLOY / "activate.sh").read_text()
    resolve = src.index('DP_DISABLE_UNITS=$(qiita_data_plane_units_to_disable "$DP_PORTS")')
    assert resolve < src.index("assert_migrations_applied()")
    disable = src.index('    systemctl disable "${dp_disable_units[@]}" >/dev/null')
    queue = src.index('    systemctl stop --no-block "${dp_disable_units[@]}"')
    restart = src.index('    systemctl restart "${dp_units[@]}"')
    reload = src.index("    systemctl reload nginx")
    wait = src.index('    systemctl stop "${dp_disable_units[@]}"')
    assert disable < queue < restart < reload < wait


def test_data_plane_ports_reads_the_host_file(tmp_path: Path) -> None:
    """The list lives in data-plane.env, so a routine redeploy — which passes no
    scaling variables — renders the instances the host was given."""
    env_file = _dp_env(tmp_path, 'QIITA_DATA_PLANE_PORTS="50051 50052 50053"\n')
    result = _run_common("qiita_data_plane_ports", dp_env=env_file)
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["50051", "50052", "50053"]


def test_data_plane_ports_defaults_when_the_host_file_omits_it(tmp_path: Path) -> None:
    env_file = _dp_env(tmp_path, "PATH_SCRATCH=/scratch\n")
    result = _run_common("qiita_data_plane_ports", dp_env=env_file)
    assert result.returncode == 0, result.stderr
    assert result.stdout == "50051"


@pytest.mark.parametrize(
    ("snippet", "var", "file_value", "env_value", "expected"),
    [
        ("qiita_data_plane_ports", "QIITA_DATA_PLANE_PORTS", "50051 50052", "50053", "50051 50052"),
        ("qiita_data_plane_ports", "QIITA_DATA_PLANE_PORTS", None, "50053", "50051"),
        (
            "qiita_data_plane_peers",
            "QIITA_DATA_PLANE_PEERS",
            "10.0.0.9:50051",
            "",
            "10.0.0.9:50051",
        ),
        ("qiita_data_plane_peers", "QIITA_DATA_PLANE_PEERS", None, "10.0.0.9:50051", ""),
        ("qiita_data_plane_bind_host", "QIITA_DATA_PLANE_BIND_HOST", None, "10.0.0.9", "127.0.0.1"),
    ],
)
def test_topology_ignores_the_deploy_environment(
    tmp_path: Path, snippet: str, var: str, file_value: str | None, env_value: str, expected: str
) -> None:
    """Only the host file counts: a value exported by whoever runs the deploy, set or
    blank, with or without the key in the file, changes nothing, so what a deploy
    renders and enables is what the file says."""
    body = "PATH_SCRATCH=/scratch\n" if file_value is None else f'{var}="{file_value}"\n'
    result = _run_common(snippet, dp_env=_dp_env(tmp_path, body), extra_env={var: env_value})
    assert result.returncode == 0, result.stderr
    assert result.stdout == expected


@pytest.mark.parametrize(
    "value",
    [
        "50051 abc",  # non-numeric entry
        "0",  # not a valid TCP port
        "99999",  # out of range
        "-1",  # negative
        "050051",  # a second unit name for port 50051
        "50051 50052 50051",  # one member rendered twice
        "50051; rm -rf /",  # the value reaches a systemd unit name + nginx config
        "   ",  # blank
    ],
)
def test_data_plane_ports_rejects_malformed_values(tmp_path: Path, value: str) -> None:
    """Each port becomes a systemd unit name, an nginx `server` line and a
    health-check target, so a bad entry fails here instead of rendering broken
    config."""
    env_file = _dp_env(tmp_path, f'QIITA_DATA_PLANE_PORTS="{value}"\n')
    result = _run_common("qiita_data_plane_ports", dp_env=env_file)
    assert result.returncode != 0, f"{value!r} should be rejected, got {result.stdout!r}"
    assert result.stdout == ""


def test_data_plane_ports_rejects_the_loopback_listener_port(tmp_path: Path) -> None:
    """nginx binds QIITA_DATA_PLANE_LB_PORT itself; an instance on it would fight
    nginx for the port."""
    env_file = _dp_env(tmp_path, f'QIITA_DATA_PLANE_PORTS="50051 {_lb_port()}"\n')
    result = _run_common("qiita_data_plane_ports", dp_env=env_file)
    assert result.returncode != 0, result.stdout
    assert "loopback listener" in result.stderr


def test_data_plane_peers_defaults_to_empty() -> None:
    """Unset everywhere ⇒ no remote members; a single-host deploy is unchanged."""
    result = _run_common("qiita_data_plane_peers")
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


def test_data_plane_peers_reads_the_host_file(tmp_path: Path) -> None:
    env_file = _dp_env(tmp_path, 'QIITA_DATA_PLANE_PEERS="10.0.0.9:50051 10.0.0.9:50052"\n')
    result = _run_common("qiita_data_plane_peers", dp_env=env_file)
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["10.0.0.9:50051", "10.0.0.9:50052"]


@pytest.mark.parametrize(
    "value",
    [
        "dp2.example.org:50051",
        "dp2.example.org:50051 dp3.example.org:50051",
        "10.0.0.7:50051",
        "[2001:db8::1]:50051",  # bracketed IPv6 literal
    ],
)
def test_data_plane_peers_accepts_host_port_forms(tmp_path: Path, value: str) -> None:
    env_file = _dp_env(tmp_path, f'QIITA_DATA_PLANE_PEERS="{value}"\n')
    result = _run_common("qiita_data_plane_peers", dp_env=env_file)
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == value.split()


@pytest.mark.parametrize(
    "value",
    [
        "dp2.example.org",  # no port
        "dp2.example.org:",  # empty port
        ":50051",  # empty host
        "dp2.example.org:abc",  # non-numeric port
        "dp2.example.org:0",  # not a valid TCP port
        "dp2.example.org:99999",  # out of range
        "2001:db8::1:50051",  # unbracketed IPv6 — ambiguous
        "dp2.example.org:50051; rm -rf /",  # reaches an nginx `server` directive
        "dp2.example.org:50051;",  # a bare `;` would inject config
        "dp2.example.org:50051 }",  # would close the upstream block early
        "999.999.999.999:50051",  # digits and dots, not an address
        "10.0.0.256:50051",  # one octet out of range
        "1.2.3:50051",  # nginx and getent read this as 1.2.0.3
        "1.2.3.4.5:50051",  # too many octets
        "010.0.0.1:50051",  # inet_aton reads 010 as octal 8
        "1.2.3.4.:50051",  # trailing dot; nginx fails to resolve it
    ],
)
def test_data_plane_peers_rejects_malformed_values(tmp_path: Path, value: str) -> None:
    """Each entry is written into an nginx `server` directive verbatim, so shape
    validation is what stands between the setting and an injected directive or a
    member at an address nobody meant."""
    env_file = _dp_env(tmp_path, f'QIITA_DATA_PLANE_PEERS="{value}"\n')
    result = _run_common("qiita_data_plane_peers", dp_env=env_file)
    assert result.returncode != 0, f"{value!r} should be rejected, got {result.stdout!r}"
    assert result.stdout == ""


@pytest.mark.parametrize("blank", ["   ", "\t", " \t "])
def test_data_plane_peers_treats_whitespace_only_as_unset(tmp_path: Path, blank: str) -> None:
    """Whitespace-only ⇒ no peers, not an error (the ports list rejects blank because
    a host always runs at least one instance). Tabs count: a tab-only value must not
    be echoed back as a peer."""
    env_file = _dp_env(tmp_path, f'QIITA_DATA_PLANE_PEERS="{blank}"\n')
    result = _run_common("qiita_data_plane_peers", dp_env=env_file)
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


def test_bind_host_defaults_to_loopback() -> None:
    result = _run_common("qiita_data_plane_bind_host")
    assert result.returncode == 0, result.stderr
    assert result.stdout == "127.0.0.1"


def test_bind_host_defaults_to_loopback_when_the_host_file_omits_it(tmp_path: Path) -> None:
    env_file = _dp_env(tmp_path, "PATH_SCRATCH=/scratch\n")
    result = _run_common("qiita_data_plane_bind_host", dp_env=env_file)
    assert result.returncode == 0, result.stderr
    assert result.stdout == "127.0.0.1"


@pytest.mark.parametrize(
    "line",
    ["QIITA_DATA_PLANE_BIND_HOST=10.0.0.5", 'QIITA_DATA_PLANE_BIND_HOST="10.0.0.5"'],
)
def test_bind_host_reads_the_host_file(tmp_path: Path, line: str) -> None:
    result = _run_common("qiita_data_plane_bind_host", dp_env=_dp_env(tmp_path, line + "\n"))
    assert result.returncode == 0, result.stderr
    assert result.stdout == "10.0.0.5"


@pytest.mark.parametrize(
    "value",
    [
        "",  # set but empty: the unit would build LISTEN_ADDR=":<port>"
        "0.0.0.0",  # every interface
        "dp1.internal",  # a name, not an address
        "10.0.0.256",
        "010.0.0.1",
        "10.0.0.5.",
        "::1",
        "169.228.46.111",  # public
        "8.8.8.8",
        "172.32.0.1",  # outside 172.16.0.0/12
    ],
)
def test_bind_host_rejects_values_that_are_not_one_ipv4_interface(
    tmp_path: Path, value: str
) -> None:
    env_file = _dp_env(tmp_path, f"QIITA_DATA_PLANE_BIND_HOST={value}\n")
    result = _run_common("qiita_data_plane_bind_host", dp_env=env_file)
    assert result.returncode != 0, f"{value!r} should be rejected, got {result.stdout!r}"
    assert result.stdout == ""


@pytest.mark.parametrize("value", ["127.0.0.1", "127.0.1.1", "172.25.212.111", "192.168.0.10"])
def test_bind_host_accepts_loopback_and_private_addresses(tmp_path: Path, value: str) -> None:
    env_file = _dp_env(tmp_path, f"QIITA_DATA_PLANE_BIND_HOST={value}\n")
    result = _run_common("qiita_data_plane_bind_host", dp_env=env_file)
    assert result.returncode == 0, result.stderr
    assert result.stdout == value


def test_ipv4_literal_refuses_an_octet_past_int64_without_a_shell_error() -> None:
    result = _run_common('qiita_is_ipv4_literal "$1"', "1.2.3.99999999999999999999")
    assert result.returncode != 0
    assert result.stderr == ""


@pytest.mark.parametrize(
    "helper", ["qiita_data_plane_ports", "qiita_data_plane_peers", "qiita_data_plane_bind_host"]
)
def test_topology_refuses_a_host_file_that_does_not_parse(tmp_path: Path, helper: str) -> None:
    """An unclosed quote would otherwise read every key as unset and deploy the
    default topology on a host configured for more."""
    env_file = _dp_env(tmp_path, 'QIITA_DATA_PLANE_PORTS="50051 50052\nPATH_SCRATCH=/scratch\n')
    result = _run_common(helper, dp_env=env_file)
    assert result.returncode != 0, result.stdout
    assert result.stdout == ""
    assert "does not parse" in result.stderr


def test_data_plane_unit_sets_listen_addr_on_the_command_line() -> None:
    """The unit's comment carries why an `Environment=LISTEN_ADDR` line does not
    hold against data-plane.env. This pins the form that does: LISTEN_ADDR on the
    ExecStart command line from the bind host and the instance port, with the bind
    host defaulting to loopback."""
    unit = (_DEPLOY / "systemd" / "qiita-data-plane@.service").read_text().splitlines()
    assert not [line for line in unit if line.startswith("Environment=LISTEN_ADDR")]
    assert "Environment=QIITA_DATA_PLANE_BIND_HOST=127.0.0.1" in unit
    assert [line for line in unit if line.startswith("ExecStart=")] == [
        "ExecStart=/usr/bin/env LISTEN_ADDR=${QIITA_DATA_PLANE_BIND_HOST}:%i "
        "/opt/qiita/data-plane/qiita-data-plane"
    ]


def _render(
    tmp_path: Path,
    *,
    bind_host: str = "127.0.0.1",
    ports: str = "50051",
    peers: str = "",
    template: Path | None = None,
) -> tuple[subprocess.CompletedProcess[str], Path]:
    """Render deploy/nginx/qiita.conf (or `template`) with the function activate.sh
    calls, into tmp_path/qiita.conf."""
    dest = tmp_path / "qiita.conf"
    result = _run_common(
        'qiita_render_nginx_conf "$1" "$2" "$3" "$4" "$5" "$6"',
        str(template or _DEPLOY / "nginx" / "qiita.conf"),
        str(dest),
        "qiita.example.org",
        bind_host,
        ports,
        peers,
    )
    return result, dest


def _call_rendered_members(conf: str) -> subprocess.CompletedProcess[str]:
    return _run_common('qiita_data_plane_rendered_members "$1"', conf)


def test_render_fills_every_placeholder(tmp_path: Path) -> None:
    result, dest = _render(tmp_path)
    assert result.returncode == 0, result.stderr
    text = dest.read_text()
    assert "__QIITA_" not in text
    assert "server_name qiita.example.org;" in text
    assert f"listen 127.0.0.1:{_lb_port()};" in text


def test_render_places_local_members_at_the_bind_host_then_peers(tmp_path: Path) -> None:
    """Local instances at the bind host, then peers verbatim, read back through the
    same parser verify.sh uses."""
    result, dest = _render(
        tmp_path,
        bind_host="10.0.0.5",
        ports="50051 50052",
        peers="dp2.internal:50051 [2001:db8::1]:50052",
    )
    assert result.returncode == 0, result.stderr
    members = _call_rendered_members(str(dest))
    assert members.returncode == 0, members.stderr
    assert members.stdout.split() == [
        "10.0.0.5:50051",
        "10.0.0.5:50052",
        "dp2.internal:50051",
        "[2001:db8::1]:50052",
    ]


def test_render_from_an_unconfigured_host_is_one_loopback_member(tmp_path: Path) -> None:
    """The validators' defaults fed to the render, as activate.sh chains them: one
    member, 127.0.0.1:50051."""
    dest = tmp_path / "qiita.conf"
    result = _run_common(
        'qiita_render_nginx_conf "$1" "$2" example.org "$(qiita_data_plane_bind_host)" '
        '"$(qiita_data_plane_ports)" "$(qiita_data_plane_peers)"',
        str(_DEPLOY / "nginx" / "qiita.conf"),
        str(dest),
    )
    assert result.returncode == 0, result.stderr
    assert _call_rendered_members(str(dest)).stdout.split() == ["127.0.0.1:50051"]


def _template_with_unfilled_placeholder(tmp_path: Path) -> Path:
    """deploy/nginx/qiita.conf plus a placeholder the render has no substitution for."""
    template = tmp_path / "template.conf"
    template.write_text((_DEPLOY / "nginx" / "qiita.conf").read_text() + "# __QIITA_NEW__\n")
    return template


def test_render_refuses_a_placeholder_it_does_not_fill(tmp_path: Path) -> None:
    """A placeholder added to the template without a matching substitution fails the
    render and leaves no config behind, instead of reaching `nginx -t`."""
    template = _template_with_unfilled_placeholder(tmp_path)
    result, dest = _render(tmp_path, template=template)
    assert result.returncode != 0
    assert "__QIITA_NEW__" in result.stderr
    assert not dest.exists()


def test_rendered_members_ignores_the_sibling_control_plane_upstream(tmp_path: Path) -> None:
    """`upstream qiita_control_plane { server 127.0.0.1:8080; }` is in the same file;
    picking it up would health-check the control plane as a data plane."""
    result, dest = _render(tmp_path)
    assert result.returncode == 0, result.stderr
    assert "qiita_control_plane" in dest.read_text(), "fixture no longer has the sibling"
    assert _call_rendered_members(str(dest)).stdout.split() == ["127.0.0.1:50051"]


def test_rendered_members_signals_fallback_when_config_is_absent(tmp_path: Path) -> None:
    """rc 1 = no config ⇒ verify.sh falls back to the data-plane.env lists, the real
    state on a first deploy."""
    result = _call_rendered_members(str(tmp_path / "does-not-exist.conf"))
    assert result.returncode == 1
    assert result.stdout == ""


def test_rendered_members_distinguishes_unparseable_from_absent(tmp_path: Path) -> None:
    """rc 2 = config present, nothing parsed. The fallback would yield the single
    default port whatever the host runs, so verify.sh fails instead of falling back."""
    conf = tmp_path / "qiita.conf"
    conf.write_text("upstream qiita_control_plane {\n    server 127.0.0.1:8080;\n}\n")
    result = _call_rendered_members(str(conf))
    assert result.returncode == 2
    assert result.stdout == ""
    assert "no 'upstream qiita_data_plane' members" in result.stderr


def test_rendered_members_ignores_upstream_server_parameters(tmp_path: Path) -> None:
    """A `server` line may carry parameters; only the address is a health-check
    target."""
    conf = tmp_path / "qiita.conf"
    conf.write_text(
        "upstream qiita_data_plane {\n    server 127.0.0.1:50051 max_fails=3 fail_timeout=30s;\n}\n"
    )
    result = _call_rendered_members(str(conf))
    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["127.0.0.1:50051"]


@pytest.mark.parametrize(
    ("member", "bind_host", "label"),
    [
        ("127.0.0.1:50051", "127.0.0.1", "health/data-plane@50051"),
        ("10.0.0.5:50052", "10.0.0.5", "health/data-plane@50052"),
        ("10.0.0.50:50051", "10.0.0.5", "health/data-plane-peer@10.0.0.50:50051"),
        ("[2001:db8::1]:50052", "127.0.0.1", "health/data-plane-peer@[2001:db8::1]:50052"),
    ],
)
def test_member_label_names_local_instances_by_port(
    member: str, bind_host: str, label: str
) -> None:
    result = _run_common('qiita_data_plane_member_label "$1" "$2"', member, bind_host)
    assert result.returncode == 0, result.stderr
    assert result.stdout == label


def _first_command(src: str, pattern: str) -> int:
    """Offset of the first non-comment line of a shell script matching `pattern`, so
    an ordering assertion cannot be satisfied by a comment that mentions the step."""
    match = re.search(rf"^(?![ \t]*#)[^\n]*{pattern}", src, re.M)
    assert match, f"no command matching {pattern!r}"
    return match.start()


def test_activate_tests_the_nginx_config_before_restarting_services() -> None:
    """`nginx -t` rejects what the shell checks do not (a peer such as
    `[1::2::3]:50051`, whose characters pass the IPv6 check but which is not an
    address); run after the restarts, it would fail the deploy with the services
    already on new code."""
    src = (_DEPLOY / "activate.sh").read_text()
    tested = _first_command(src, r"qiita_render_and_test_nginx_conf ")
    first_restart = _first_command(src, r"restart_if_env_present qiita-control-plane")
    assert tested < first_restart


def test_activate_waits_on_and_restarts_every_local_instance_together() -> None:
    """The idle wait and the restart take the same unit list, and the restart is one
    `systemctl restart` for all of them, so they stop accepting new requests together
    and their drains overlap."""
    src = (_DEPLOY / "activate.sh").read_text()
    assert re.search(
        r'(?m)^read -r -a dp_units <<< "\$\(qiita_data_plane_units "\$DP_PORTS"\)"$', src
    )
    assert re.search(
        r'(?m)^wait_for_data_plane_idle "\$DP_ENV" "\$\{dp_units\[@\]\}" '
        r'"\$\{dp_disable_units\[@\]\}"$',
        src,
    )
    assert re.search(r'(?m)^    systemctl restart "\$\{dp_units\[@\]\}"$', src)
    assert not re.search(r"(?m)^\s*restart_if_env_present\s+\S*qiita-data-plane", src)


def _render_and_test(
    tmp_path: Path,
    *,
    nginx_accepts: bool,
    previous: str | None,
    template: Path | None = None,
    failing: tuple[str, ...] = (),
) -> tuple[subprocess.CompletedProcess[str], Path]:
    """Run qiita_render_and_test_nginx_conf with a stub `nginx` whose `-t` accepts or
    rejects, over an existing deployed file holding `previous` (or none). Each command
    in `failing` is stubbed to exit 1."""
    stub = tmp_path / "stub"
    stub.mkdir()
    stubs = {"nginx": 0 if nginx_accepts else 1, **dict.fromkeys(failing, 1)}
    for command, rc in stubs.items():
        (stub / command).write_text(f"#!/bin/sh\nexit {rc}\n")
        (stub / command).chmod(0o755)
    dest = tmp_path / "qiita.conf"
    if previous is not None:
        dest.write_text(previous)
    result = _run_common(
        'qiita_render_and_test_nginx_conf "$1" "$2" example.org 127.0.0.1 50051 ""',
        str(template or _DEPLOY / "nginx" / "qiita.conf"),
        str(dest),
        extra_env={"PATH": f"{stub}:{os.environ['PATH']}"},
    )
    return result, dest


def test_a_rejected_render_puts_the_previous_config_back(tmp_path: Path) -> None:
    """nginx refuses to start on a config `nginx -t` rejects, so the rejected render
    must not stay in conf.d for the next nginx start."""
    result, dest = _render_and_test(tmp_path, nginx_accepts=False, previous="# previous\n")
    assert result.returncode != 0
    assert dest.read_text() == "# previous\n"
    assert not (tmp_path / "qiita.conf.previous").exists()


def test_a_rejected_first_render_leaves_no_config(tmp_path: Path) -> None:
    result, dest = _render_and_test(tmp_path, nginx_accepts=False, previous=None)
    assert result.returncode != 0
    assert not dest.exists()


def test_an_accepted_render_replaces_the_config(tmp_path: Path) -> None:
    result, dest = _render_and_test(tmp_path, nginx_accepts=True, previous="# previous\n")
    assert result.returncode == 0, result.stderr
    assert "upstream qiita_data_plane" in dest.read_text()
    assert not (tmp_path / "qiita.conf.previous").exists()


def test_a_render_failure_leaves_the_config_untouched(tmp_path: Path) -> None:
    template = _template_with_unfilled_placeholder(tmp_path)
    result, dest = _render_and_test(
        tmp_path, nginx_accepts=True, previous="# previous\n", template=template
    )
    assert result.returncode != 0
    assert dest.read_text() == "# previous\n"
    assert not (tmp_path / "qiita.conf.previous").exists()


@pytest.mark.parametrize("failure", ["missing template", "mktemp fails"])
def test_a_failed_render_step_fails_the_tested_render(tmp_path: Path, failure: str) -> None:
    """A failed step inside the render fails the wrapper, with `nginx -t` accepting,
    and leaves the deployed file as it was. Why each step checks its own status: the
    comment above qiita_render_nginx_conf."""
    result, dest = _render_and_test(
        tmp_path,
        nginx_accepts=True,
        previous="# previous\n",
        template=tmp_path / "missing.conf" if failure == "missing template" else None,
        failing=("mktemp",) if failure == "mktemp fails" else (),
    )
    assert result.returncode != 0
    assert dest.read_text() == "# previous\n"
    assert not (tmp_path / "qiita.conf.previous").exists()
    assert not (tmp_path / "qiita.conf.tmp").exists()


def test_a_member_list_write_error_fails_the_render(tmp_path: Path) -> None:
    """With no peers, the default, a failed write of the member list fails the render
    instead of rendering an empty upstream. `printf` is shadowed by a function that
    fails, standing in for a write error such as ENOSPC on $TMPDIR."""
    dest = tmp_path / "qiita.conf"
    dest.write_text("# previous\n")
    result = _run_common(
        'printf() { return 1; }; qiita_render_nginx_conf "$1" "$2" example.org 127.0.0.1 50051 ""',
        str(_DEPLOY / "nginx" / "qiita.conf"),
        str(dest),
    )
    assert result.returncode != 0
    assert dest.read_text() == "# previous\n"
    assert not (tmp_path / "qiita.conf.tmp").exists()


def _flight_locations(conf: str) -> list[dict[str, str]]:
    """The directives of each `location /arrow.flight.protocol.FlightService/` block."""
    blocks = re.findall(
        r"location /arrow\.flight\.protocol\.FlightService/ \{(.*?)\n    \}", conf, re.S
    )
    return [
        dict(
            line.strip().rstrip(";").split(None, 1)
            for line in block.splitlines()
            if line.strip() and not line.strip().startswith("#")
        )
        for block in blocks
    ]


def test_data_plane_upstream_shares_its_balancing_state_across_workers() -> None:
    """Without a shared `zone`, each nginx worker round-robins on its own from the
    first member, and one-call clients all land on that member."""
    conf = (_DEPLOY / "nginx" / "qiita.conf").read_text()
    upstream = re.search(r"(?ms)^upstream qiita_data_plane \{\n(.*?)^\}", conf)
    assert upstream, "no qiita_data_plane upstream block"
    assert re.search(r"(?m)^\s*zone qiita_data_plane \d+k;$", upstream.group(1))


def test_nginx_flight_locations_agree_on_body_cap_and_timeouts() -> None:
    """The loopback listener copies the 443 edge's Flight location; a change to the
    edge's body cap or timeouts has to reach the copy too. limit_conn is the one
    directive only the public edge carries."""
    locations = _flight_locations((_DEPLOY / "nginx" / "qiita.conf").read_text())
    assert len(locations) == 2, "expected the 443 edge and the loopback listener"
    edge, loopback = locations
    for directive in (
        "grpc_pass",
        "client_max_body_size",
        "grpc_read_timeout",
        "grpc_send_timeout",
    ):
        assert edge[directive] == loopback[directive], directive
    assert set(edge) - set(loopback) == {"limit_conn"}


def test_activate_validates_data_plane_settings_before_installing_anything() -> None:
    """A malformed topology, a public or unresolvable peer, or a QIITA_BUILD_SHA the
    data planes would not boot on has to fail with the running deploy untouched:
    before the first rsync, the binary install, the nginx render and any restart.
    Asserted on source order, since the order is the property."""
    src = (_DEPLOY / "activate.sh").read_text()
    resolved = max(
        _first_command(src, re.escape("DP_PORTS=$(qiita_data_plane_ports)")),
        _first_command(src, re.escape("DP_PEERS=$(qiita_data_plane_peers)")),
        _first_command(src, re.escape("DP_BIND_HOST=$(qiita_data_plane_bind_host)")),
        _first_command(src, re.escape("DP_BUILD_COMMIT=$(qiita_data_plane_build_commit)")),
        _first_command(src, re.escape('qiita_assert_peer_private "$peer"')),
        _first_command(src, re.escape("DP_MEMORY_MAX=$(qiita_data_plane_instance_memory_max)")),
        _first_command(
            src, re.escape('qiita_assert_instance_memory_fits "$DP_PORTS" "$DP_MEMORY_MAX"')
        ),
        _first_command(src, re.escape('qiita_assert_pool_memory_fits "$DP_MEMORY_MAX"')),
        _first_command(src, re.escape("qiita_dp_env_omits_local_instances")),
    )
    for step in (
        r"rsync -a",
        r"install -m 0755",
        r"qiita_render_(?:and_test_)?nginx_conf ",
        r"qiita_write_data_plane_dropins ",
        r"systemctl restart",
    ):
        assert resolved < _first_command(src, step), (
            f"data-plane settings must be checked before {step!r}"
        )


# --- Per-instance limits ------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("4096", "4096"),
        ("1K", "1024"),
        ("3M", str(3 * 1024**2)),
        ("16G", str(16 * 1024**3)),
        ("2T", str(2 * 1024**4)),
    ],
)
def test_size_bytes_reads_systemd_suffixes_as_powers_of_1024(value: str, expected: str) -> None:
    result = _run_common('qiita_size_bytes "$1"', value)
    assert result.returncode == 0, result.stderr
    assert result.stdout == expected


@pytest.mark.parametrize(
    "value",
    ["", "0", "0G", "016G", "1.5G", "16g", "16GB", "16 G", "-1G", "infinity", "50%", "4194305T"],
)
def test_size_bytes_rejects_anything_but_a_positive_integer_and_suffix(value: str) -> None:
    result = _run_common('qiita_size_bytes "$1"', value)
    assert result.returncode != 0
    assert result.stdout == ""


def test_instance_memory_max_defaults_to_none() -> None:
    result = _run_common("qiita_data_plane_instance_memory_max")
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


def test_instance_memory_max_reads_the_host_file_only(tmp_path: Path) -> None:
    """Like the topology keys, only the file counts: the units read the file, so a
    value the deploy saw but the file lacks would check one limit and apply another."""
    env_file = _dp_env(tmp_path, "QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX=48G\n")
    result = _run_common(
        "qiita_data_plane_instance_memory_max",
        dp_env=env_file,
        extra_env={"QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX": "1T"},
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "48G"

    absent = _dp_env(tmp_path, "PATH_SCRATCH=/scratch\n")
    result = _run_common(
        "qiita_data_plane_instance_memory_max",
        dp_env=absent,
        extra_env={"QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX": "1T"},
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""


@pytest.mark.parametrize("value", ["", "48GB", "lots", "0"])
def test_instance_memory_max_rejects_a_blank_or_malformed_value(tmp_path: Path, value: str) -> None:
    env_file = _dp_env(tmp_path, f'QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX="{value}"\n')
    result = _run_common("qiita_data_plane_instance_memory_max", dp_env=env_file)
    assert result.returncode != 0, result.stdout
    assert result.stdout == ""
    assert "QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX" in result.stderr


def test_host_file_may_not_set_the_local_instance_count(tmp_path: Path) -> None:
    """The drop-in sets it with Environment=, which the shared file's
    EnvironmentFile= would override."""
    env_file = _dp_env(tmp_path, "QIITA_DATA_PLANE_LOCAL_INSTANCES=1\n")
    result = _run_common("qiita_dp_env_omits_local_instances", dp_env=env_file)
    assert result.returncode != 0
    assert "QIITA_DATA_PLANE_LOCAL_INSTANCES" in result.stderr

    clean = _dp_env(tmp_path, "PATH_SCRATCH=/scratch\n")
    assert _run_common("qiita_dp_env_omits_local_instances", dp_env=clean).returncode == 0
    assert _run_common("qiita_dp_env_omits_local_instances").returncode == 0


def _meminfo(tmp_path: Path, mem_total_kib: int) -> Path:
    path = tmp_path / "meminfo"
    path.write_text(
        f"MemTotal:       {mem_total_kib} kB\n"
        "MemFree:         1000 kB\n"
        f"MemAvailable:   {mem_total_kib // 2} kB\n"
    )
    return path


@pytest.mark.parametrize(
    ("ports", "value", "fits"),
    [
        ("50051 50052", "32G", True),  # exactly MemTotal
        ("50051 50052", "33G", False),
        ("50051 50052 50053", "22G", False),
        ("50051", "64G", True),
        ("50051", "65G", False),
        ("50051", "4194303T", False),  # past MemTotal without overflowing
    ],
)
def test_instance_memory_fits_compares_count_times_value_with_mem_total(
    tmp_path: Path, ports: str, value: str, fits: bool
) -> None:
    meminfo = _meminfo(tmp_path, 64 * 1024 * 1024)  # 64 GiB
    result = _run_common(
        'qiita_assert_instance_memory_fits "$1" "$2" "$3"', ports, value, str(meminfo)
    )
    assert (result.returncode == 0) is fits, result.stderr
    if not fits:
        assert "MemTotal" in result.stderr


def test_instance_memory_fits_has_nothing_to_check_when_unset(tmp_path: Path) -> None:
    result = _run_common(
        'qiita_assert_instance_memory_fits "$1" "" "$2"', "50051", str(tmp_path / "absent")
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("body", [None, "MemFree: 1000 kB\n"])
def test_instance_memory_fits_fails_without_mem_total(tmp_path: Path, body: str | None) -> None:
    meminfo = tmp_path / "meminfo"
    if body is not None:
        meminfo.write_text(body)
    result = _run_common(
        'qiita_assert_instance_memory_fits "$1" "$2" "$3"', "50051", "1G", str(meminfo)
    )
    assert result.returncode != 0
    assert "MemTotal" in result.stderr


_CONFIG_RS = _REPO_ROOT / "qiita-data-plane" / "src" / "config.rs"


def test_default_pool_size_matches_the_data_plane() -> None:
    """The deploy's pool size when DUCKLAKE_POOL_SIZE is unset is the data plane's."""
    match = re.search(
        r"^pub\(crate\) const DEFAULT_POOL_SIZE: usize = (\d+);$",
        _CONFIG_RS.read_text(),
        re.MULTILINE,
    )
    assert match, f"DEFAULT_POOL_SIZE not found in {_CONFIG_RS}"
    result = _run_common('printf %s "$QIITA_DATA_PLANE_DEFAULT_POOL_SIZE"')
    assert result.returncode == 0, result.stderr
    assert result.stdout == match.group(1)


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (None, "8"),
        ("PATH_SCRATCH=/scratch\n", "8"),
        ("DUCKLAKE_POOL_SIZE=\n", "8"),
        ('DUCKLAKE_POOL_SIZE="  "\n', "8"),
        ("DUCKLAKE_POOL_SIZE=3\n", "3"),
        ('DUCKLAKE_POOL_SIZE=" 12 "\n', "12"),
    ],
)
def test_pool_size_reads_the_host_file_else_the_default(
    tmp_path: Path, body: str | None, expected: str
) -> None:
    env_file = _dp_env(tmp_path, body) if body is not None else None
    result = _run_common(
        "qiita_data_plane_pool_size",
        dp_env=env_file,
        extra_env={"DUCKLAKE_POOL_SIZE": "99"},
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == expected


@pytest.mark.parametrize("value", ["0", "-1", "eight", "1.5", "4 4"])
def test_pool_size_rejects_anything_but_a_positive_integer(tmp_path: Path, value: str) -> None:
    env_file = _dp_env(tmp_path, f'DUCKLAKE_POOL_SIZE="{value}"\n')
    result = _run_common("qiita_data_plane_pool_size", dp_env=env_file)
    assert result.returncode != 0, result.stdout
    assert result.stdout == ""
    assert "DUCKLAKE_POOL_SIZE" in result.stderr


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("3MiB", str(3 * 1024**2)),
        ("4GiB", str(4 * 1024**3)),
    ],
)
def test_duckdb_size_bytes_reads_mib_and_gib(value: str, expected: str) -> None:
    result = _run_common('qiita_duckdb_size_bytes "$1"', value)
    assert result.returncode == 0, result.stderr
    assert result.stdout == expected


@pytest.mark.parametrize(
    "value",
    [
        "",
        "4096",
        "1GB",
        "1gib",
        "4 GiB",
        "04GiB",
        "0GiB",
        "4294967297GiB",  # past 2^62 bytes
    ],
)
def test_duckdb_size_bytes_rejects_other_spellings(value: str) -> None:
    result = _run_common('qiita_duckdb_size_bytes "$1"', value)
    assert result.returncode != 0
    assert result.stdout == ""


def _pool_memory_fits(env_file: Path | None, value: str) -> subprocess.CompletedProcess[str]:
    return _run_common('qiita_assert_pool_memory_fits "$1"', value, dp_env=env_file)


@pytest.mark.parametrize(
    ("body", "fits"),
    [
        ("DUCKDB_MEMORY_LIMIT=1GiB\n", True),  # 8 × 1GiB is exactly 8G
        ("DUCKDB_MEMORY_LIMIT=1025MiB\n", False),
        ("DUCKDB_MEMORY_LIMIT=1024MiB\n", True),
        ("DUCKDB_MEMORY_LIMIT=2GiB\nDUCKLAKE_POOL_SIZE=4\n", True),
        ("DUCKDB_MEMORY_LIMIT=2GiB\nDUCKLAKE_POOL_SIZE=5\n", False),
        ("DUCKDB_MEMORY_LIMIT=4294967296GiB\n", False),  # 2^62 bytes: past the limit, no overflow
    ],
)
def test_pool_memory_fits_compares_pool_times_limit_with_memory_max(
    tmp_path: Path, body: str, fits: bool
) -> None:
    result = _pool_memory_fits(_dp_env(tmp_path, body), "8G")
    assert (result.returncode == 0) is fits, result.stderr
    if not fits:
        assert "Unset DUCKDB_MEMORY_LIMIT" in result.stderr
        assert "QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX=8G" in result.stderr


@pytest.mark.parametrize(
    ("body", "value"),
    [
        ("DUCKDB_MEMORY_LIMIT=64GiB\n", ""),  # no MemoryMax
        ("PATH_SCRATCH=/scratch\n", "8G"),  # no DUCKDB_MEMORY_LIMIT
        ("DUCKDB_MEMORY_LIMIT=\n", "8G"),
        ('DUCKDB_MEMORY_LIMIT="  "\n', "8G"),
    ],
)
def test_pool_memory_fits_has_nothing_to_check_unless_both_are_set(
    tmp_path: Path, body: str, value: str
) -> None:
    result = _pool_memory_fits(_dp_env(tmp_path, body), value)
    assert result.returncode == 0, result.stderr


def test_pool_memory_fits_reads_the_limit_from_the_host_file_only(tmp_path: Path) -> None:
    """The units read the file, so a limit in the deploy's environment is not the one
    the data plane gets."""
    result = _run_common(
        'qiita_assert_pool_memory_fits "$1"',
        "8G",
        dp_env=_dp_env(tmp_path, "PATH_SCRATCH=/scratch\n"),
        extra_env={"DUCKDB_MEMORY_LIMIT": "64GiB"},
    )
    assert result.returncode == 0, result.stderr
    no_file = _run_common(
        'qiita_assert_pool_memory_fits "$1"', "8G", extra_env={"DUCKDB_MEMORY_LIMIT": "64GiB"}
    )
    assert no_file.returncode == 0, no_file.stderr


@pytest.mark.parametrize("limit", ["4G", "1GB", "4096"])
def test_pool_memory_fits_refuses_a_limit_it_cannot_compare(tmp_path: Path, limit: str) -> None:
    result = _pool_memory_fits(_dp_env(tmp_path, f'DUCKDB_MEMORY_LIMIT="{limit}"\n'), "1T")
    assert result.returncode != 0
    assert f"DUCKDB_MEMORY_LIMIT '{limit}'" in result.stderr
    assert "write DUCKDB_MEMORY_LIMIT in MiB or GiB, or unset it" in result.stderr


def test_pool_memory_fits_refuses_an_invalid_pool_size(tmp_path: Path) -> None:
    body = "DUCKDB_MEMORY_LIMIT=1GiB\nDUCKLAKE_POOL_SIZE=0\n"
    result = _pool_memory_fits(_dp_env(tmp_path, body), "8G")
    assert result.returncode != 0
    assert "DUCKLAKE_POOL_SIZE" in result.stderr


def _preflight_row(tmp_path: Path, body: str, row_name: str) -> str:
    """The `row_name` row of a preflight run whose only env file is data-plane.env,
    holding `body`."""
    env = {k: v for k, v in os.environ.items() if k not in _TOPOLOGY_VARS}
    env.update(
        CP_ENV=str(tmp_path / "absent-cp.env"),
        CO_ENV=str(tmp_path / "absent-co.env"),
        DP_ENV=str(_dp_env(tmp_path, body)),
    )
    env.pop("SKIP_PREFLIGHT", None)
    result = subprocess.run(
        ["bash", str(_DEPLOY / "preflight.sh")], capture_output=True, text=True, env=env
    )
    rows = [line for line in result.stdout.splitlines() if f" {row_name}: " in line]
    assert len(rows) == 1, result.stdout + result.stderr
    return rows[0]


def test_preflight_fails_the_memory_row_when_the_pool_exceeds_memory_max(tmp_path: Path) -> None:
    body = "QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX=1G\nDUCKDB_MEMORY_LIMIT=129MiB\n"
    row = _preflight_row(tmp_path, body, "dp-memory-max")
    assert row.lstrip().startswith("\u2717"), row
    assert "Unset DUCKDB_MEMORY_LIMIT" in row


def test_preflight_memory_row_accepts_a_pool_within_memory_max(tmp_path: Path) -> None:
    """8 × 128MiB is exactly 1G. The row then reaches the MemTotal check, which reads
    /proc/meminfo, so it passes only where the host has one."""
    body = "QIITA_DATA_PLANE_INSTANCE_MEMORY_MAX=1G\nDUCKDB_MEMORY_LIMIT=128MiB\n"
    row = _preflight_row(tmp_path, body, "dp-memory-max")
    assert "DUCKDB_MEMORY_LIMIT" not in row
    if Path("/proc/meminfo").is_file():
        assert row.lstrip().startswith("\u2713"), row


def _write_dropins(unitdir: Path, ports: str, value: str) -> subprocess.CompletedProcess[str]:
    return _run_common('qiita_write_data_plane_dropins "$1" "$2" "$3"', ports, value, str(unitdir))


def _dropin_name() -> str:
    result = _run_common('printf %s "$QIITA_DATA_PLANE_DROPIN_NAME"')
    assert result.returncode == 0 and result.stdout, result.stderr
    return result.stdout


def test_dropins_carry_the_instance_count_and_the_memory_max(tmp_path: Path) -> None:
    result = _write_dropins(tmp_path, "50051 50052", "24G")
    assert result.returncode == 0, result.stderr
    for port in ("50051", "50052"):
        conf = tmp_path / f"qiita-data-plane@{port}.service.d" / _dropin_name()
        assert conf.read_text() == (
            "[Service]\nEnvironment=QIITA_DATA_PLANE_LOCAL_INSTANCES=2\nMemoryMax=24G\n"
        )


def test_dropins_without_a_memory_max_set_only_the_count(tmp_path: Path) -> None:
    """A host that sets nothing gets no MemoryMax= line, so its instances' limits
    are what they were before the drop-in existed."""
    result = _write_dropins(tmp_path, "50051", "")
    assert result.returncode == 0, result.stderr
    conf = tmp_path / "qiita-data-plane@50051.service.d" / _dropin_name()
    assert conf.read_text() == "[Service]\nEnvironment=QIITA_DATA_PLANE_LOCAL_INSTANCES=1\n"


def test_dropins_are_removed_from_instances_outside_the_list(tmp_path: Path) -> None:
    """A port that left the list keeps neither the old count nor the old limit, and
    another file in its drop-in directory is left alone."""
    assert _write_dropins(tmp_path, "50051 50052 50053", "8G").returncode == 0
    other = tmp_path / "qiita-data-plane@50053.service.d" / "operator.conf"
    other.write_text("[Service]\n")

    result = _write_dropins(tmp_path, "50051", "8G")

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "qiita-data-plane@50051.service.d" / _dropin_name()).is_file()
    assert not (tmp_path / "qiita-data-plane@50052.service.d").exists()
    assert not (tmp_path / "qiita-data-plane@50053.service.d" / _dropin_name()).exists()
    assert other.is_file()
    assert (
        "QIITA_DATA_PLANE_LOCAL_INSTANCES=1"
        in (tmp_path / "qiita-data-plane@50051.service.d" / _dropin_name()).read_text()
    )


def test_activate_loads_the_dropins_before_restarting_the_instances() -> None:
    """The data plane reads its limits only at boot, so the drop-ins have to be
    written and loaded before the restart that applies them."""
    src = (_DEPLOY / "activate.sh").read_text()
    written = _first_command(
        src, re.escape('qiita_write_data_plane_dropins "$DP_PORTS" "$DP_MEMORY_MAX"')
    )
    reload = src.index("systemctl daemon-reload", written)
    restart = _first_command(src, re.escape('systemctl restart "${dp_units[@]}"'))
    assert written < reload < restart


def _call_peer_private(peer: str, stub_dir: str | None) -> subprocess.CompletedProcess[str]:
    env = {**os.environ}
    if stub_dir is not None:
        env["PATH"] = f"{stub_dir}:{env['PATH']}"
    return subprocess.run(
        ["bash", "-c", f'source "{_COMMON}"; qiita_assert_peer_private "$1"', "_", peer],
        capture_output=True,
        text=True,
        env=env,
    )


# `getent ahosts` output for each name the stub resolves, in the layout glibc 2.39
# prints on Rocky 10: one STREAM/DGRAM/RAW row per address.
_AHOSTS = {
    "private.internal": ["10.1.2.3"],
    "public.internal": ["169.228.46.5"],
    "mixed.internal": ["172.25.212.5", "169.228.46.5"],
}


@pytest.fixture
def getent_stub(tmp_path: Path) -> str:
    """A stand-in for glibc's getent that answers `ahosts` for the names in _AHOSTS.

    getent does not exist on macOS, so without a stub every hostname case would
    take the tool-absent path and fail the same way.
    """
    d = tmp_path / "stub"
    d.mkdir()
    cases = "".join(
        f"    {name}) printf '"
        + "".join(f"{a}    STREAM {name}\\n{a}    DGRAM\\n{a}    RAW\\n" for a in addrs)
        + "'; exit 0 ;;\n"
        for name, addrs in _AHOSTS.items()
    )
    (d / "getent").write_text(
        f'#!/bin/sh\n[ "$1" = ahosts ] || exit 2\ncase "$2" in\n{cases}esac\nexit 2\n'
    )
    (d / "getent").chmod(0o755)
    return str(d)


def test_peer_private_accepts_a_name_resolving_to_private_addresses(getent_stub: str) -> None:
    result = _call_peer_private("private.internal:50051", getent_stub)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("peer", ["public.internal:50051", "mixed.internal:50051"])
def test_peer_private_rejects_a_name_with_any_public_address(peer: str, getent_stub: str) -> None:
    result = _call_peer_private(peer, getent_stub)
    assert result.returncode != 0
    assert "169.228.46.5, not a private address" in result.stderr


def test_peer_private_rejects_an_unresolvable_host(getent_stub: str) -> None:
    result = _call_peer_private("bad.internal:50051", getent_stub)
    assert result.returncode != 0
    assert "does not resolve" in result.stderr


@pytest.mark.parametrize(
    "peer", ["10.0.0.7:50051", "172.16.0.1:50051", "172.31.255.1:50051", "192.168.1.1:50051"]
)
def test_peer_private_accepts_private_ipv4_literals_without_lookup(
    peer: str, getent_stub: str
) -> None:
    """The stub resolves none of these, so passing means no lookup ran."""
    result = _call_peer_private(peer, getent_stub)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "peer",
    [
        "169.228.46.111:50051",
        "8.8.8.8:50051",
        "172.15.255.1:50051",  # just below 172.16.0.0/12
        "172.32.0.1:50051",  # just above it
        "192.169.0.1:50051",
        "11.0.0.1:50051",
        "127.0.0.1:50051",  # a peer is another host
        "[2001:db8::1]:50051",
        "[fd00::1]:50051",
    ],
)
def test_peer_private_rejects_other_literals(peer: str, getent_stub: str) -> None:
    result = _call_peer_private(peer, getent_stub)
    assert result.returncode != 0, result.stdout
    assert "private" in result.stderr


def test_peer_private_fails_a_name_when_getent_is_unavailable(tmp_path: Path) -> None:
    """No getent ⇒ the name's addresses cannot be checked, so the peer fails."""
    empty = tmp_path / "empty-path"
    empty.mkdir()
    bash = shutil.which("bash")
    assert bash, "bash not found"
    result = subprocess.run(
        # bash by ABSOLUTE path: the empty PATH is meant to hide getent from the
        # script, not to hide the interpreter from subprocess.
        [
            bash,
            "-c",
            f'source "{_COMMON}"; qiita_assert_peer_private "$1"',
            "_",
            "private.internal:1",
        ],
        capture_output=True,
        text=True,
        env={**os.environ, "PATH": str(empty)},
    )
    assert result.returncode != 0
    assert "getent unavailable" in result.stderr


def test_native_checkout_resolves_valid_layout(tmp_path: Path) -> None:
    py = _fake_native_checkout(tmp_path)
    result = _call_native_checkout(str(py))
    assert result.returncode == 0, result.stderr
    # Realpath both sides — macOS /tmp is a /private symlink, and the helper cd's.
    assert os.path.realpath(result.stdout) == os.path.realpath(str(py.parents[2]))


@pytest.mark.parametrize("arg", ["", "python"])
def test_native_checkout_skips_when_unset_or_path_based(arg: str) -> None:
    """Empty / bare `python` (local backend) is a SKIP signal (rc=1), not a FAIL —
    redeploy.sh degrades like the miint stage rather than aborting the deploy."""
    result = _call_native_checkout(arg)
    assert result.returncode == 1
    assert result.stdout == ""


def test_native_checkout_fails_on_wrong_basename(tmp_path: Path) -> None:
    """A python whose grandparent dir isn't qiita-compute-orchestrator is a hard
    FAIL (rc=2) so redeploy.sh refuses to `uv sync` a wrong path."""
    bad = tmp_path / "some-other-dir" / ".venv" / "bin"
    bad.mkdir(parents=True)
    (tmp_path / ".git").mkdir()
    py = bad / "python"
    py.write_text("#!/bin/sh\n")
    result = _call_native_checkout(str(py))
    assert result.returncode == 2
    assert "qiita-compute-orchestrator" in result.stderr


def test_native_checkout_fails_outside_git_clone(tmp_path: Path) -> None:
    """Right shape but no ../.git → not a checkout → hard FAIL (rc=2)."""
    checkout = tmp_path / "qiita-compute-orchestrator"
    (checkout / ".venv" / "bin").mkdir(parents=True)
    (checkout / "pyproject.toml").write_text("[project]\n")
    py = checkout / ".venv" / "bin" / "python"
    py.write_text("#!/bin/sh\n")
    result = _call_native_checkout(str(py))
    assert result.returncode == 2
    assert "git clone" in result.stderr


# --- qiita_deploy_self_fingerprint / qiita_deploy_reexec_if_changed ----------
# redeploy.sh pulls the clone it lives in, so the pull can replace redeploy.sh and
# _common.sh under the running shell. The fingerprint is how the script notices;
# the re-exec is what it does about it. Both are exercised end to end below, on a
# fixture that copies the real _common.sh so the helpers under test are the
# shipped ones. -----------------------------------------------------------------

_REDEPLOY = _DEPLOY / "redeploy.sh"


def test_grpc_connect_timeout_is_set_once_for_every_data_plane_location() -> None:
    """At http level, so a new grpc_pass location cannot miss it, and nowhere else."""
    conf = (_DEPLOY / "nginx" / "qiita.conf").read_text()
    assert re.findall(r"^\s*grpc_connect_timeout\b.*$", conf, re.M) == ["grpc_connect_timeout 5s;"]


def test_build_service_name_matches_the_data_plane() -> None:
    """verify.sh asks each member for the health service qiita_data_plane_build_service
    names; the data plane registers BUILD_SERVICE_PREFIX + BUILD_COMMIT, read from the
    file activate.sh writes. Nothing else ties the two sides together, and a drift
    fails every member's commit row."""
    sha = "0123456789abcdef0123456789abcdef01234567"
    result = _run_common('qiita_data_plane_build_service "$1"', sha)
    assert result.returncode == 0, result.stderr
    assert result.stdout == f"qiita-data-plane/build/{sha}"
    main_rs = (_REPO_ROOT / "qiita-data-plane" / "src" / "main.rs").read_text()
    assert 'const BUILD_SERVICE_PREFIX: &str = "qiita-data-plane/build/";' in main_rs
    # The key the deploy writes, the data plane reads and verify reads back.
    assert 'std::env::var("BUILD_COMMIT")' in main_rs
    assert "printf 'BUILD_COMMIT=%s\\n'" in (_DEPLOY / "activate.sh").read_text()
    assert 'BUILD_ENV" BUILD_COMMIT)' in (_DEPLOY / "verify.sh").read_text()
    build_env = _run_common('printf %s "$QIITA_DATA_PLANE_BUILD_ENV"').stdout
    assert build_env == "/opt/qiita/data-plane/build.env"
    unit = (_DEPLOY / "systemd" / "qiita-data-plane@.service").read_text()
    assert f"EnvironmentFile=-{build_env}" in unit


@pytest.mark.parametrize("sha", ["", "0123456789abcdef0123456789abcdef01234567"])
def test_build_commit_accepts_unset_or_a_full_commit(sha: str) -> None:
    result = _run_common("qiita_data_plane_build_commit", extra_env={"QIITA_BUILD_SHA": sha})
    assert result.returncode == 0, result.stderr
    assert result.stdout == sha


@pytest.mark.parametrize(
    "sha",
    [
        "0123456",
        "0123456789ABCDEF0123456789ABCDEF01234567",
        "0123456789abcdef0123456789abcdef012345678",
        "0123456789abcdef0123456789abcdef0123456g",
    ],
)
def test_build_commit_rejects_what_the_data_plane_would_not_boot_on(sha: str) -> None:
    result = _run_common("qiita_data_plane_build_commit", extra_env={"QIITA_BUILD_SHA": sha})
    assert result.returncode != 0
    assert "QIITA_BUILD_SHA" in result.stderr


def test_verify_dp_pool_row_greps_what_the_data_plane_actually_logs() -> None:
    """`verify.sh`'s dp-pool row reads the data plane's journal by exact string, and
    nothing in Rust knows that. A reword on either side would turn the row green (or
    permanently red) without failing anything, so the three strings are pinned against
    the source that writes them.

    This pins the strings, not the row's logic — `bash -n` and shellcheck cover the
    shell, and the behaviour needs a host with systemd."""
    verify = (_DEPLOY / "verify.sh").read_text()
    dp_src = _REPO_ROOT / "qiita-data-plane" / "src"
    for string, writer in (
        ("qiita-data-plane pool:", "main.rs"),
        ("discarding a pooled DuckLake connection", "pool.rs"),
        ("fallback default", "resources.rs"),
    ):
        assert string in verify, f"verify.sh no longer greps for {string!r}"
        assert string in (dp_src / writer).read_text(), (
            f"the data plane's {writer} no longer writes {string!r}, so verify.sh's "
            "dp-pool row can never match it again"
        )

    # One hatch per row, documented in the header like its siblings.
    assert verify.count("SKIP_DP_POOL") == 3, (
        "the dp-pool hatch is missing from the row, its skip message, or the header"
    )


def _fake_deploy_dir(tmp_path: Path, *, script: str, common_suffix: str = "") -> Path:
    """A `deploy/`-shaped dir: a script beside a copy of the real `_common.sh`.
    The copy is what `QIITA_COMMON_SH` resolves to, so a test can edit or delete
    it. `common_suffix` appends bytes, standing in for a pull that touched only
    `_common.sh`."""
    d = tmp_path / "deploy"
    d.mkdir(parents=True)
    (d / "_common.sh").write_text(_COMMON.read_text() + common_suffix)
    path = d / "redeploy.sh"
    path.write_text(script)
    return path


def _call_self_fingerprint(script: Path, *, prelude: str = "") -> subprocess.CompletedProcess[str]:
    """Source the fixture's `_common.sh`, then fingerprint the fixture script."""
    common = script.parent / "_common.sh"
    return subprocess.run(
        [
            "bash",
            "-c",
            f'source "{common}"; {prelude} qiita_deploy_self_fingerprint "$1"',
            "_",
            str(script),
        ],
        capture_output=True,
        text=True,
    )


def test_self_fingerprint_changes_when_the_script_is_replaced_by_rename(tmp_path: Path) -> None:
    """The production event, exactly: `git checkout` writes a temp file and
    renames it over the tracked path. Same path, new inode, new bytes."""
    script = _fake_deploy_dir(tmp_path, script="# old\n")
    before = _call_self_fingerprint(script)
    assert before.returncode == 0, before.stderr
    assert before.stdout.strip() != ""

    replacement = script.parent / "redeploy.sh.pulled"
    replacement.write_text("# new\n")
    os.replace(replacement, script)

    after = _call_self_fingerprint(script)
    assert after.returncode == 0, after.stderr
    assert after.stdout != before.stdout


def test_self_fingerprint_covers_the_sourced_common(tmp_path: Path) -> None:
    """`_common.sh` is read into the same process, so a pull that changes only it
    leaves the running script just as stale. Anchoring on the script alone would
    miss that."""
    script = _fake_deploy_dir(tmp_path, script="# script\n")
    before = _call_self_fingerprint(script).stdout
    common = script.parent / "_common.sh"
    common.write_text(common.read_text() + "\n# pulled\n")
    assert _call_self_fingerprint(script).stdout != before


def test_self_fingerprint_separates_the_two_files(tmp_path: Path) -> None:
    """Digesting each file under its name, rather than hashing the two
    concatenated, is what keeps bytes moving from one to the other visible."""
    moved = "# moved line\n"
    in_script = _fake_deploy_dir(tmp_path / "a", script=f"# script\n{moved}")
    in_common = _fake_deploy_dir(tmp_path / "b", script="# script\n", common_suffix=moved)
    assert _call_self_fingerprint(in_script).stdout != _call_self_fingerprint(in_common).stdout


def test_self_fingerprint_fails_loudly_when_the_script_is_unreadable(tmp_path: Path) -> None:
    """rc=1 + a stderr reason, not an empty digest. An empty digest compares
    unequal to the pre-pull one, which would re-exec a file that just failed to
    read."""
    script = _fake_deploy_dir(tmp_path, script="# script\n")
    script.unlink()
    result = _call_self_fingerprint(script)
    assert result.returncode == 1
    assert result.stdout.strip() == ""
    assert "redeploy.sh" in result.stderr


def test_self_fingerprint_fails_loudly_when_common_is_unreadable(tmp_path: Path) -> None:
    """Same, for the sourced half: the file is gone from disk after this process
    read it, which is what an interrupted pull can leave behind."""
    script = _fake_deploy_dir(tmp_path, script="# script\n")
    common = script.parent / "_common.sh"
    result = _call_self_fingerprint(script, prelude=f'rm "{common}";')
    assert result.returncode == 1
    assert result.stdout.strip() == ""
    assert "_common.sh" in result.stderr


# A script that mimics redeploy.sh step 1 against the fixture's own _common.sh:
# fingerprint, optionally let a "pull" replace it by rename, then hand off to the
# helper. v1 prints `original`; the replacement prints `pulled`, so stdout says
# which body finished the run.
_PULLED_DEFAULT = "printf 'pulled\\n'"

_STEP1_V1 = """#!/usr/bin/env bash
set -euo pipefail
source "{common}"
SELF="{self}"
printf 'original\\n'
before=$(qiita_deploy_self_fingerprint "$SELF")
if [ -n "{replace}" ]; then
    cat > "$SELF.pulled" <<'PULLED'
#!/usr/bin/env bash
{pulled}
PULLED
    mv "$SELF.pulled" "$SELF"
fi
qiita_deploy_reexec_if_changed "$SELF" "$before"
printf 'original-continued\\n'
"""


def _run_step1(
    tmp_path: Path,
    *,
    replace: bool,
    env: dict[str, str] | None = None,
    pulled: str = _PULLED_DEFAULT,
) -> subprocess.CompletedProcess[str]:
    d = tmp_path / "deploy"
    d.mkdir(parents=True)
    (d / "_common.sh").write_text(_COMMON.read_text())
    script = d / "redeploy.sh"
    script.write_text(
        _STEP1_V1.format(
            common=d / "_common.sh",
            self=script,
            replace="1" if replace else "",
            pulled=pulled,
        )
    )
    return subprocess.run(
        ["bash", str(script)],
        capture_output=True,
        text=True,
        env={**os.environ, **(env or {})},
    )


def test_reexec_runs_the_pulled_body_and_abandons_the_original(tmp_path: Path) -> None:
    """The whole point, end to end: the helper `exec`s, so the run finishes on the
    replacement and never returns to the line after the call."""
    run = _run_step1(tmp_path, replace=True)
    assert run.returncode == 0, run.stderr
    assert run.stdout.splitlines()[0] == "original"
    assert run.stdout.splitlines()[-1] == "pulled"
    assert "original-continued" not in run.stdout


def test_reexec_is_a_no_op_when_nothing_changed(tmp_path: Path) -> None:
    """Control for the test above: same fixture, no replacement. The helper must
    return so the caller carries on — a deploy that re-execs every time would
    never leave step 1."""
    run = _run_step1(tmp_path, replace=False)
    assert run.returncode == 0, run.stderr
    assert run.stdout.splitlines() == ["original", "original-continued"]


def test_reexec_refuses_a_second_time_rather_than_looping(tmp_path: Path) -> None:
    """With the sentinel already set, a further change aborts. Nothing is deployed
    at step 1, so the caller can be stopped for the cost of a re-run; carrying on
    would be running code the process knows is not the pulled code."""
    run = _run_step1(tmp_path, replace=True, env={"QIITA_REDEPLOY_REEXECED": "1"})
    assert run.returncode != 0
    assert "changed again after the re-exec" in run.stderr
    assert "pulled" not in run.stdout
    assert "original-continued" not in run.stdout


def test_reexec_sentinel_reaches_the_replacement(tmp_path: Path) -> None:
    """The sentinel is exported, so the re-exec'd process sees it. Were it a plain
    shell variable the replacement would start with it unset, and a clone that
    kept changing could re-exec without end."""
    run = _run_step1(
        tmp_path,
        replace=True,
        pulled="printf 'pulled sentinel=%s\\n' \"$QIITA_REDEPLOY_REEXECED\"",
    )
    assert run.returncode == 0, run.stderr
    assert "pulled sentinel=1" in run.stdout


def test_bash_keeps_executing_a_script_replaced_by_rename(tmp_path: Path) -> None:
    """The premise the re-exec rests on, asserted rather than read off the docs.

    bash holds the script's fd, so a rename over the path swaps the inode without
    touching what the running shell reads: the process finishes on the original
    body. That is why steps 2-8 of a redeploy run pre-pull code, and it is a
    property of bash, not something the deploy arranges. The body is padded well
    past a single read so the case is not decided by read-ahead alone; either way
    what is asserted is the observable. The control below shows the swap landed."""
    script = tmp_path / "self-replacing.sh"
    padding = "\n".join(f"# pad {i:05d}" * 4 for i in range(4096))
    body = "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "printf 'start\\n'",
            padding,
            # The replacement is a script in its own right, so the control below
            # can run it and show the rename landed.
            "cat > \"$0.new\" <<'NEW'",
            "printf 'REPLACEMENT\\n'",
            "NEW",
            'mv "$0.new" "$0"',
            "printf 'end-of-original\\n'",
            "",
        ]
    )
    script.write_text(body)
    assert len(body) > 65536, "keep the body well past a single read"

    run = subprocess.run(["bash", str(script)], capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    assert run.stdout.splitlines() == ["start", "end-of-original"], (
        "bash executed something other than the original body after the rename; "
        f"stdout was {run.stdout!r}"
    )

    # Control: the swap landed, so a FRESH bash on the same path runs the new body.
    fresh = subprocess.run(["bash", str(script)], capture_output=True, text=True)
    assert fresh.stdout.splitlines() == ["REPLACEMENT"], (
        "the rename did not replace the script, so the assertion above proved "
        f"nothing (stdout={fresh.stdout!r}, stderr={fresh.stderr!r})"
    )


def test_redeploy_refuses_a_self_outside_the_clone(tmp_path: Path) -> None:
    """The re-exec check can only see a pull that lands in $QIITA_CLONE, so
    redeploy.sh aborts when it is not running from that clone — otherwise the
    check is green for a reason unrelated to freshness."""
    text = _REDEPLOY.read_text()
    guard = text.find('case "$SELF" in')
    pull = text.find('git -C "$QIITA_CLONE" pull --ff-only')
    assert 0 <= guard < pull, "the containment guard must run before the pull"
    assert '"$QIITA_CLONE"/*' in text


# --- qiita_buckets_12: the "skip the bucket 1 & 2 ack when there's nothing to
# apply" predicate redeploy.sh uses. rc 0 = empty (skip prompt), 1 = has steps
# (prompt), 2 = unreadable/markers-absent (fail safe → prompt). -----------------

# Empty Pending-deploy buckets 1 & 2 — only headers + the "_None yet._"
# placeholder. The "### 3. Migrations" header bounds the range.
_EMPTY_BUCKETS = """\
## Pending deploy

### 1. Env vars — set BEFORE the deploy (each is `from_env()` fail-fast)

_None yet._

### 2. One-time host setup

_None yet._

### 3. Migrations

_None yet._
"""

# A real step in bucket 1 — the operator must apply it, so the ack must NOT skip.
_NONEMPTY_BUCKETS = """\
## Pending deploy

### 1. Env vars — set BEFORE the deploy

- (#123) sudo bash -c 'echo "FOO=bar" >> /etc/qiita/compute-orchestrator.env'

### 2. One-time host setup

_None yet._

### 3. Migrations

_None yet._
"""


def _call_buckets_12(checklist: Path) -> subprocess.CompletedProcess[str]:
    """Source _common.sh and invoke qiita_buckets_12 with a checklist path."""
    return subprocess.run(
        ["bash", "-c", f'source "{_COMMON}"; qiita_buckets_12 "$1"', "_", str(checklist)],
        capture_output=True,
        text=True,
    )


def test_buckets_12_empty_returns_zero(tmp_path: Path) -> None:
    """Placeholder-only buckets → rc 0 so redeploy.sh skips the prompt; the text
    is still echoed so the caller could print it."""
    f = tmp_path / "DEPLOY_CHECKLIST.md"
    f.write_text(_EMPTY_BUCKETS)
    result = _call_buckets_12(f)
    assert result.returncode == 0, result.stdout
    assert "_None yet._" in result.stdout
    # The bounding "### 3. Migrations" header is dropped, not part of buckets 1+2.
    assert "Migrations" not in result.stdout


def test_buckets_12_nonempty_returns_one(tmp_path: Path) -> None:
    """A real step present → rc 1 so the operator is prompted, and the step text
    is echoed for them to read."""
    f = tmp_path / "DEPLOY_CHECKLIST.md"
    f.write_text(_NONEMPTY_BUCKETS)
    result = _call_buckets_12(f)
    assert result.returncode == 1
    assert "FOO=bar" in result.stdout


def test_buckets_12_unreadable_returns_two(tmp_path: Path) -> None:
    """Missing/unreadable checklist → rc 2 so the caller falls back to prompting
    (fail safe) rather than silently skipping the ack."""
    result = _call_buckets_12(tmp_path / "does-not-exist.md")
    assert result.returncode == 2
    assert result.stdout == ""


def test_buckets_12_markers_absent_returns_two(tmp_path: Path) -> None:
    """Readable file but no bucket markers → can't judge → rc 2 (prompt)."""
    f = tmp_path / "DEPLOY_CHECKLIST.md"
    f.write_text("# Some unrelated file\n\nNo bucket headers here.\n")
    result = _call_buckets_12(f)
    assert result.returncode == 2


def test_buckets_12_pins_real_checklist_headers() -> None:
    """qiita_buckets_12's bucket-1/bucket-3 markers are the contract with the
    live DEPLOY_CHECKLIST.md. Run it against the REAL file (not a fixture copy):
    it must return 0 (empty) or 1 (has steps) — never 2, which would mean the
    headers it keys on no longer match the file and every deploy quietly fell
    back to prompting. A bucket rename in DEPLOY_CHECKLIST.md fails here."""
    result = _call_buckets_12(_REPO_ROOT / "DEPLOY_CHECKLIST.md")
    assert result.returncode in (0, 1), (
        "qiita_buckets_12 could not locate buckets 1 & 2 in the real "
        f"DEPLOY_CHECKLIST.md (rc={result.returncode}); the '### 1. Env vars' / "
        "'### 3. Migrations' markers it keys on have drifted from the file."
    )


def test_deployed_history_heading_pins_the_live_section_boundary() -> None:
    """`## Deployed history` terminates the sed range that prints the live section.

    Two consumers slice DEPLOY_CHECKLIST.md with
    `sed -n '/^## Pending deploy/,/^## Deployed history/p'` — the operator, in
    redeploy.md §1, and the agent, in /deploy-note. Since the archived deploys
    moved out to docs/deploy-archive/, the heading is a short pointer stub with no
    content under it, which makes it look like dead weight a tidy-up would delete.
    It isn't: delete it and both ranges run to EOF. Pin it."""
    text = (_REPO_ROOT / "DEPLOY_CHECKLIST.md").read_text()
    assert "\n## Deployed history\n" in text, (
        "DEPLOY_CHECKLIST.md lost its '## Deployed history' heading. It is the "
        "terminator of the `## Pending deploy` sed range used by redeploy.md §1 "
        "and /deploy-note; without it both print the rest of the file."
    )
    assert text.index("\n## Pending deploy\n") < text.index("\n## Deployed history\n"), (
        "'## Deployed history' must come after '## Pending deploy' — the sed range "
        "between them is empty otherwise."
    )


def test_deploy_archive_index_covers_every_archived_deploy() -> None:
    """`docs/deploy-archive/README.md` indexes exactly the files beside it.

    The index is hand-maintained (by `/deploy-archive`, which adds a line when it
    writes a file), so it drifts the moment someone writes one and forgets the
    other. A missing line hides a deploy from the only listing anyone reads; a
    stale line is a dead link. Both are silent."""
    archive = _REPO_ROOT / "docs" / "deploy-archive"
    index = (archive / "README.md").read_text()

    on_disk = {p.name for p in archive.glob("*.md")} - {"README.md"}
    linked = set(re.findall(r"\]\((\d{4}-\d{2}-\d{2}-[^)]+\.md)\)", index))

    assert on_disk == linked, (
        "docs/deploy-archive/ and its README index disagree. Missing from the "
        f"index: {sorted(on_disk - linked)}. Indexed but absent from disk (dead "
        f"links): {sorted(linked - on_disk)}."
    )


# --- scripts/build-sif.sh: now sources deploy/_common.sh for the build-inputs
# hash, so it gets the same bash -n + shellcheck gate as the deploy scripts. ------


def test_build_sif_exists_and_executable() -> None:
    assert _BUILD_SIF.is_file(), f"{_BUILD_SIF} missing"
    assert _BUILD_SIF.stat().st_mode & 0o111, f"{_BUILD_SIF} is not executable"


def test_build_sif_is_valid_bash() -> None:
    result = subprocess.run(["bash", "-n", str(_BUILD_SIF)], capture_output=True, text=True)
    assert result.returncode == 0, f"bash -n failed for build-sif.sh:\n{result.stderr}"


def test_build_sif_passes_shellcheck() -> None:
    if shutil.which("shellcheck") is None:
        pytest.skip("shellcheck not installed")
    # -S warning to match the deploy-script gate above; the `# shellcheck source=`
    # directive in build-sif.sh keeps the cross-dir _common.sh source from flagging.
    result = subprocess.run(
        ["shellcheck", "-S", "warning", str(_BUILD_SIF)], capture_output=True, text=True
    )
    assert result.returncode == 0, (
        f"shellcheck flagged build-sif.sh:\n{result.stdout}\n{result.stderr}"
    )


# --- qiita_sif_build_inputs_hash: the content stamp build-sif.sh uses to detect a
# changed def/entrypoint/manifest (the trap VERIFY_MATCH can't see). --------------


def _make_workflow_tree(root: Path) -> tuple[Path, Path]:
    """A minimal repo layout: workflows/<wf>/ + workflows/_shared/. Returns
    (workflow_dir, shared_dir). Includes files the hash must IGNORE (the spec, a
    .gitignore, a vendored *.rpm) so a test can prove they don't affect the digest."""
    wf = root / "workflows" / "demo"
    shared = root / "workflows" / "_shared"
    wf.mkdir(parents=True)
    shared.mkdir(parents=True)
    (wf / "Apptainer.def").write_text("Bootstrap: docker\nFrom: oraclelinux:8\n")
    (wf / "entrypoint.sh").write_text("#!/bin/sh\necho hi\n")
    (wf / "sif-build.env").write_text('SIF_FILENAME="demo.sif"\n')  # must be ignored
    (wf / ".gitignore").write_text("*.rpm\n")  # must be ignored
    (wf / "demo-1.0.rpm").write_text("binary-ish")  # vendored SOURCE — must be ignored
    (shared / "manifest_writer.py").write_text("x = 1\n")
    return wf, shared


def _call_build_inputs_hash(repo_root: Path, wf: Path, shared: Path) -> str:
    result = subprocess.run(
        [
            "bash",
            "-c",
            f'source "{_COMMON}"; qiita_sif_build_inputs_hash "$1" "$2" "$3"',
            "_",
            str(repo_root),
            str(wf),
            str(shared),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_build_inputs_hash_is_deterministic(tmp_path: Path) -> None:
    wf, shared = _make_workflow_tree(tmp_path)
    assert _call_build_inputs_hash(tmp_path, wf, shared) == _call_build_inputs_hash(
        tmp_path, wf, shared
    )


def test_build_inputs_hash_changes_on_entrypoint_edit(tmp_path: Path) -> None:
    """A def/entrypoint/manifest edit MUST change the hash — that's the whole point
    (it triggers the rebuild VERIFY_MATCH would have skipped)."""
    wf, shared = _make_workflow_tree(tmp_path)
    before = _call_build_inputs_hash(tmp_path, wf, shared)
    (wf / "entrypoint.sh").write_text("#!/bin/sh\necho changed\n")
    assert _call_build_inputs_hash(tmp_path, wf, shared) != before


def test_build_inputs_hash_ignores_spec_gitignore_and_sources(tmp_path: Path) -> None:
    """Changing the spec, .gitignore, or a vendored *.rpm must NOT change the hash —
    re-vendoring 4.5.4-1 → 4.5.4-2 must not force a rebuild (VERIFY_MATCH's loose
    patch component), and the spec/gitignore aren't baked into the image."""
    wf, shared = _make_workflow_tree(tmp_path)
    before = _call_build_inputs_hash(tmp_path, wf, shared)
    (wf / "sif-build.env").write_text('SIF_FILENAME="demo.sif"\nSOURCES="demo-2.0.rpm"\n')
    (wf / ".gitignore").write_text("*.rpm\n*.sif\n")
    (wf / "demo-1.0.rpm").write_text("re-vendored bytes")
    assert _call_build_inputs_hash(tmp_path, wf, shared) == before


def test_build_inputs_hash_is_location_independent(tmp_path: Path) -> None:
    """Same content under a different repo root → same digest (the keys are
    repo-RELATIVE paths). Matters: activate.sh runs from the clone, the CI path
    from /opt/qiita/incoming — both must agree on 'unchanged'."""
    wf_a, shared_a = _make_workflow_tree(tmp_path / "clone")
    wf_b, shared_b = _make_workflow_tree(tmp_path / "incoming")
    assert _call_build_inputs_hash(tmp_path / "clone", wf_a, shared_a) == _call_build_inputs_hash(
        tmp_path / "incoming", wf_b, shared_b
    )


def test_build_inputs_hash_survives_unreadable_cwd(tmp_path: Path) -> None:
    """Regression: a manual `sudo -u qiita-orch build-sif.sh` launched from an
    admin's 0700 home left `find` unable to restore that cwd, so it exited
    non-zero and aborted the build under `set -o pipefail`. The helper now cd's to
    / in a subshell, so it doesn't depend on (or need to restore) the caller cwd.

    Reproduce deterministically: run from a directory, strip its traversal bit
    (chmod 000) for the duration of the call so a naive `find` could not chdir
    back, then restore it for cleanup. Skipped under root, which ignores the
    permission and so can't exercise the failure."""
    if os.geteuid() == 0:
        pytest.skip("root ignores the dir-traversal bit; can't reproduce the failure")
    wf, shared = _make_workflow_tree(tmp_path)
    expected = _call_build_inputs_hash(tmp_path, wf, shared)  # baseline from a normal cwd
    locked = tmp_path / "locked"
    locked.mkdir()
    # Spawn with cwd=locked (still traversable), then drop its traversal bit from
    # inside so a naive `find` could not chdir back to it. Restore perms in Python
    # via the ABSOLUTE path in finally — the parents stay traversable, so cleanup
    # never depends on the now-unreadable cwd (chmod'ing "." from a 000 cwd is
    # unreliable). With the fix the helper cd's to / and so doesn't care.
    try:
        result = subprocess.run(
            [
                "bash",
                "-c",
                f'chmod 000 .; source "{_COMMON}"; qiita_sif_build_inputs_hash "$1" "$2" "$3"',
                "_",
                str(tmp_path),
                str(wf),
                str(shared),
            ],
            cwd=str(locked),
            capture_output=True,
            text=True,
        )
    finally:
        os.chmod(locked, 0o755)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == expected


# --- qiita_sif_build_inputs_hash_scoped: the per-tool-image variant. Hashes an
# EXPLICIT file list + _shared instead of the whole workflow dir, so an edit to a
# sibling tool's def/entrypoint leaves this image's stamp unchanged (the rebuild
# granularity the multi-image split delivers). ------------------------------------


def _call_scoped_hash(repo_root: Path, shared: Path, files: list[Path]) -> str:
    result = subprocess.run(
        [
            "bash",
            "-c",
            f'source "{_COMMON}"; qiita_sif_build_inputs_hash_scoped "$@"',
            "_",
            str(repo_root),
            str(shared),
            *[str(f) for f in files],
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_scoped_hash_is_deterministic(tmp_path: Path) -> None:
    wf, shared = _make_workflow_tree(tmp_path)
    files = [wf / "Apptainer.def", wf / "entrypoint.sh"]
    assert _call_scoped_hash(tmp_path, shared, files) == _call_scoped_hash(tmp_path, shared, files)


def test_scoped_hash_changes_when_a_listed_input_edits(tmp_path: Path) -> None:
    wf, shared = _make_workflow_tree(tmp_path)
    files = [wf / "Apptainer.def", wf / "entrypoint.sh"]
    before = _call_scoped_hash(tmp_path, shared, files)
    (wf / "entrypoint.sh").write_text("#!/bin/sh\necho changed\n")
    assert _call_scoped_hash(tmp_path, shared, files) != before


def test_scoped_hash_ignores_files_not_in_its_input_set(tmp_path: Path) -> None:
    """The whole point of scoping: a sibling tool's file (present in the workflow
    dir but NOT in this image's declared input list) must NOT change the digest —
    that is what lets one tool's image rebuild independently of the others."""
    wf, shared = _make_workflow_tree(tmp_path)
    files = [wf / "Apptainer.def", wf / "entrypoint.sh"]
    before = _call_scoped_hash(tmp_path, shared, files)
    (wf / "sibling-tool.def").write_text("Bootstrap: docker\nFrom: oraclelinux:9\n")
    (wf / "sibling.sh").write_text("#!/bin/sh\necho sibling\n")
    assert _call_scoped_hash(tmp_path, shared, files) == before


def test_scoped_hash_changes_on_shared_edit(tmp_path: Path) -> None:
    """_shared/ is always in scope (every image %files-copies manifest_writer.py),
    so a change there rebuilds every image — the intended fan-out."""
    wf, shared = _make_workflow_tree(tmp_path)
    files = [wf / "Apptainer.def", wf / "entrypoint.sh"]
    before = _call_scoped_hash(tmp_path, shared, files)
    (shared / "manifest_writer.py").write_text("x = 2\n")
    assert _call_scoped_hash(tmp_path, shared, files) != before


# --- qiita_sif_missing_sources: gates whether build-sifs.sh SKIPS an image whose
# licensed artifact isn't staged. rc 0 = all present, 1 = some missing (echoed). --


def _call_missing_sources(sources_dir: Path, sources: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "bash",
            "-c",
            f'source "{_COMMON}"; qiita_sif_missing_sources "$1" "$2"',
            "_",
            str(sources_dir),
            sources,
        ],
        capture_output=True,
        text=True,
    )


def test_missing_sources_all_present_returns_zero(tmp_path: Path) -> None:
    (tmp_path / "a.rpm").write_text("x")
    (tmp_path / "b.rpm").write_text("y")
    result = _call_missing_sources(tmp_path, "a.rpm b.rpm")
    assert result.returncode == 0
    assert result.stdout == ""


def test_missing_sources_empty_list_returns_zero(tmp_path: Path) -> None:
    """A workflow that vendors nothing from sources/ (empty SOURCES) → nothing
    missing → rc 0, so build-sifs.sh proceeds to build it."""
    result = _call_missing_sources(tmp_path, "")
    assert result.returncode == 0
    assert result.stdout == ""


def test_missing_sources_some_missing_returns_one_and_lists_them(tmp_path: Path) -> None:
    (tmp_path / "present.rpm").write_text("x")
    result = _call_missing_sources(tmp_path, "present.rpm gone.rpm also-gone.rpm")
    assert result.returncode == 1
    missing = set(result.stdout.split())
    assert missing == {"gone.rpm", "also-gone.rpm"}
    assert "present.rpm" not in missing


# --- scripts/lake-shell.sh: the read-only DuckLake/CP shell. Sources
# deploy/_common.sh for read_env_var + qiita_split_conn_password, so it gets the
# same bash -n + shellcheck gate as the deploy scripts. ------------------------


def test_lake_data_path_helper_derives_exactly_like_the_data_plane() -> None:
    """DuckLake pins DATA_PATH into the catalog at creation and rejects an attach
    whose DATA_PATH differs by even a slash, so the derivation must reproduce
    config.rs's bare `format!("{path_persistent_raw}/ducklake")` — no trailing-slash
    normalization. A `${PERSISTENT%/}` anywhere breaks every host whose
    PATH_PERSISTENT ends in `/`. Exercised through the helper rather than grepped
    for, and both callers are pinned to it."""

    def derive(persistent: str) -> str:
        result = subprocess.run(
            ["bash", "-c", f'source "{_COMMON}"; qiita_lake_data_path "$1"', "_", persistent],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout

    assert derive("/data") == "/data/ducklake"
    assert derive("/data/") == "/data//ducklake", "a trailing slash must survive verbatim"
    for script in (_LAKE_SHELL, _LAKE_GC):
        body = script.read_text()
        assert "PERSISTENT%/" not in body, f"{script.name} normalizes the trailing slash"
        assert "qiita_lake_data_path" in body, f"{script.name} must use the shared helper"


def _call_split_conn_password(connstr: str) -> list[str]:
    """Returns [sanitized, user, password] as the helper echoes them."""
    result = subprocess.run(
        ["bash", "-c", f'source "{_COMMON}"; qiita_split_conn_password "$1"', "_", connstr],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.rstrip("\n").split("\t")


@pytest.mark.parametrize(
    ("connstr", "expected"),
    [
        # libpq key=value: password lifted out, remaining whitespace collapsed.
        (
            "dbname=lake host=db port=5432 user=lake_rw password=s3cr3t sslmode=prefer",
            ["dbname=lake host=db port=5432 user=lake_rw sslmode=prefer", "lake_rw", "s3cr3t"],
        ),
        # libpq with no password at all (peer auth): untouched.
        (
            "dbname=lake host=db user=lake_rw",
            ["dbname=lake host=db user=lake_rw", "lake_rw", ""],
        ),
        # postgres:// URI: credentials come out of the authority section.
        (
            "postgresql://cp_rw:pw@db:5432/qiita",
            ["postgresql://cp_rw@db:5432/qiita", "cp_rw", "pw"],
        ),
        # URI with a user but no password.
        (
            "postgresql://cp_rw@db:5432/qiita",
            ["postgresql://cp_rw@db:5432/qiita", "cp_rw", ""],
        ),
        # A ':' inside the password is legal unencoded and must not truncate it.
        (
            "postgresql://cp_rw:pa:ss@db/qiita",
            ["postgresql://cp_rw@db/qiita", "cp_rw", "pa:ss"],
        ),
    ],
)
def test_split_conn_password_extracts(connstr: str, expected: list[str]) -> None:
    assert _call_split_conn_password(connstr) == expected


@pytest.mark.parametrize(
    "connstr",
    [
        # Percent-encoded URI password: pgpass wants the DECODED value, so the
        # helper must decline rather than hand libpq the still-encoded string.
        "postgresql://cp_rw:pa%40ss@db/qiita",
        # No username to key a pgpass entry on.
        "dbname=lake host=db password=s3cr3t",
    ],
)
def test_split_conn_password_declines_when_it_cannot_key_a_pgpass_entry(connstr: str) -> None:
    sanitized, _user, password = _call_split_conn_password(connstr)
    assert password == "", "password must not be lifted out when it cannot be keyed"
    assert sanitized == connstr, "connstr must be left intact for the caller to use as-is"


def _required_duckdb_cli_version() -> str:
    """The CLI version the lake scripts require, as they read it: by sourcing _common.sh."""
    version = subprocess.run(
        ["bash", "-c", f'source "{_COMMON}"; printf %s "$QIITA_DUCKDB_VERSION"'],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    assert version, "deploy/_common.sh no longer sets QIITA_DUCKDB_VERSION"
    return version


def _write_duckdb_stub(path: Path, body: str = "exit 0\n") -> Path:
    """A `duckdb` stand-in that reports the required version to `--version` (the
    resolver checks it) and otherwise runs `body`."""
    version = _required_duckdb_cli_version()
    path.write_text(
        f'#!/bin/sh\nif [ "$1" = --version ]; then echo "v{version} (stub) 0"; exit 0; fi\n' + body
    )
    path.chmod(0o755)
    return path


def test_lake_shell_refuses_to_open_without_the_staged_miint_extension(tmp_path: Path) -> None:
    """miint is a core dependency, so the shell must fail LOUD rather than open a
    session whose bioinformatics functions differ from production's. Everything
    else it needs is present here — only MIINT_EXTENSION_DIRECTORY is missing."""
    persistent = tmp_path / "persistent"
    (persistent / "ducklake").mkdir(parents=True)
    dp_env = tmp_path / "data-plane.env"
    dp_env.write_text(
        "DUCKLAKE_CATALOG_CONNSTR='dbname=lake host=localhost user=lake_rw'\n"
        f"PATH_PERSISTENT={persistent}\n"
    )
    cp_env = tmp_path / "control-plane.env"
    cp_env.write_text("DATABASE_URL='postgresql://cp_rw@localhost:5432/qiita'\n")

    env = {
        **os.environ,
        "DP_ENV": str(dp_env),
        "CP_ENV": str(cp_env),
        "CO_ENV": str(tmp_path / "absent.env"),
        "QIITA_DUCKDB_BIN": str(_write_duckdb_stub(tmp_path / "duckdb-stub")),
    }
    env.pop("MIINT_EXTENSION_DIRECTORY", None)

    result = subprocess.run(
        ["bash", str(_LAKE_SHELL), "-c", "SELECT 1"], capture_output=True, text=True, env=env
    )
    assert result.returncode == 1, f"expected a hard failure, got:\n{result.stdout}"
    assert "MIINT_EXTENSION_DIRECTORY" in result.stderr


# --- scripts/lake-gc.sh: DuckLake reclamation. Reports by default; --reclaim
# expires snapshots and unlinks files. Sources deploy/_common.sh like
# lake-shell.sh, so it gets the same bash -n + shellcheck gate. ---------------


_LAKE_SCRIPTS = (_LAKE_SHELL, _LAKE_GC)


@pytest.mark.parametrize("script", _LAKE_SCRIPTS, ids=lambda p: p.name)
def test_lake_script_exists_and_executable(script: Path) -> None:
    assert script.is_file(), f"{script} missing"
    assert script.stat().st_mode & 0o111, f"{script} is not executable"


@pytest.mark.parametrize("script", _LAKE_SCRIPTS, ids=lambda p: p.name)
def test_lake_script_is_valid_bash(script: Path) -> None:
    result = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
    assert result.returncode == 0, f"bash -n failed for {script.name}:\n{result.stderr}"


@pytest.mark.parametrize("script", _LAKE_SCRIPTS, ids=lambda p: p.name)
def test_lake_script_passes_shellcheck(script: Path) -> None:
    if shutil.which("shellcheck") is None:
        pytest.skip("shellcheck not installed")
    result = subprocess.run(
        ["shellcheck", "-S", "warning", str(script)], capture_output=True, text=True
    )
    assert result.returncode == 0, (
        f"shellcheck flagged {script.name}:\n{result.stdout}\n{result.stderr}"
    )


def _lake_gc_code() -> str:
    """The script with comment-only lines stripped, so an invariant test can
    assert about what the script *runs* without matching the header prose that
    explains the same thing."""
    return "\n".join(
        line for line in _LAKE_GC.read_text().splitlines() if not line.lstrip().startswith("#")
    )


def test_lake_gc_help_exits_zero_and_names_reclaim() -> None:
    result = subprocess.run(["bash", str(_LAKE_GC), "--help"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "--reclaim" in result.stdout


@pytest.mark.parametrize(
    "bad",
    [
        "7; DROP TABLE x",  # injection shape — the value lands in an INTERVAL literal
        "7 FORTNIGHTS",  # not an INTERVAL unit
        "DAYS",  # no quantity
        "-1 DAYS",  # negative retention would expire everything
    ],
)
def test_lake_gc_rejects_malformed_retention(bad: str) -> None:
    """--older-than is interpolated into `now() - INTERVAL <value>`, so it is
    constrained to the shape an INTERVAL takes rather than screened afterwards."""
    result = subprocess.run(
        ["bash", str(_LAKE_GC), "--older-than", bad], capture_output=True, text=True
    )
    assert result.returncode == 1, f"expected refusal for {bad!r}:\n{result.stdout}"
    assert "--older-than must be" in result.stderr


def test_lake_gc_rejects_unknown_argument() -> None:
    result = subprocess.run(["bash", str(_LAKE_GC), "--wat"], capture_output=True, text=True)
    assert result.returncode == 1
    assert "unknown argument" in result.stderr


def _lake_gc_env(tmp_path, *, writable: bool = True) -> dict:
    """A data-plane env plus a lake dir, with duckdb stubbed so the script's own
    logic runs without a catalog. The stub reports the required version, saves the
    SQL it is handed, and prints nothing — the same shape as a maintenance call that
    reclaims nothing."""
    persistent = tmp_path / "persistent"
    (persistent / "ducklake").mkdir(parents=True)
    if not writable:
        (persistent / "ducklake").chmod(0o500)
    dp_env = tmp_path / "data-plane.env"
    dp_env.write_text(
        "DUCKLAKE_CATALOG_CONNSTR='dbname=lake host=localhost user=lake_rw'\n"
        f"PATH_PERSISTENT={persistent}\n"
    )
    # Stub duckdb with a shim that SAVES the SQL it is handed. `true` would
    # discard it, leaving the thing these tests are about — dry_run, the
    # transaction split — unasserted: a hardcoded `dry_run := false` would pass.
    captured = tmp_path / "captured.sql"
    stub = _write_duckdb_stub(
        tmp_path / "duckdb-stub",
        "while [ $# -gt 0 ]; do\n"
        f'  if [ "$1" = -f ]; then cat "$2" >> "{captured}"; fi\n'
        "  shift\n"
        "done\n"
        "exit 0\n",
    )
    return {**os.environ, "DP_ENV": str(dp_env), "QIITA_DUCKDB_BIN": str(stub)}


@pytest.mark.parametrize("script", _LAKE_SCRIPTS, ids=lambda p: p.name)
@pytest.mark.parametrize(
    ("stub", "shown"),
    [
        ("echo 'v1.0.0 (stub) 0'", "printed 'v1.0.0 (stub) 0'"),
        (":", "printed 'nothing'"),
        ("echo 'cannot execute binary file' >&2; exit 126", "printed 'cannot execute binary file'"),
    ],
    ids=["other-version", "silent", "unrunnable"],
)
def test_lake_script_refuses_a_duckdb_cli_of_another_version(
    tmp_path: Path, script: Path, stub: str, shown: str
) -> None:
    """The CLI must be the DuckDB the data plane links (deploy/_common.sh says why). A
    binary that cannot say which version it is gets no benefit of the doubt, and the
    refusal shows what it did print — stderr included, so an unrunnable one says why."""
    env = _lake_gc_env(tmp_path)
    Path(env["QIITA_DUCKDB_BIN"]).write_text(f"#!/bin/sh\n{stub}\n")
    result = subprocess.run(["bash", str(script)], capture_output=True, text=True, env=env)
    assert result.returncode != 0, f"a mismatched CLI was accepted:\n{result.stdout}"
    assert shown in result.stderr, result.stderr
    assert f"v{_required_duckdb_cli_version()}" in result.stderr, result.stderr
    assert not (tmp_path / "captured.sql").exists(), "SQL ran on the wrong CLI"


def test_lake_gc_defaults_to_report_only(tmp_path) -> None:
    """No flag must never reclaim: the whole point of the default run is that an
    operator can look before anything is unlinked."""
    result = subprocess.run(
        ["bash", str(_LAKE_GC)], capture_output=True, text=True, env=_lake_gc_env(tmp_path)
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert "report only" in result.stdout
    assert "Nothing was removed" in result.stdout
    sql = (tmp_path / "captured.sql").read_text()
    assert "dry_run := true" in sql, "report mode must ask the database for a dry run"
    assert "dry_run := false" not in sql


def test_lake_gc_reclaim_mode_announces_it_acts(tmp_path) -> None:
    result = subprocess.run(
        ["bash", str(_LAKE_GC), "--reclaim"],
        capture_output=True,
        text=True,
        env={**_lake_gc_env(tmp_path), "ASSUME_YES": "1"},
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert "Nothing was removed" not in result.stdout
    # The banner alone is a weak signal — report mode's banner also contains the
    # string "--reclaim" ("pass --reclaim to act"). The SQL is what discriminates,
    # and --reclaim is deliberately mixed: steps 1-2 act, step 3 stays dry.
    sql = (tmp_path / "captured.sql").read_text()
    assert "ducklake_expire_snapshots('qiita_lake', dry_run := false" in sql
    assert "ducklake_cleanup_old_files('qiita_lake', dry_run := false" in sql
    assert "ducklake_delete_orphaned_files('qiita_lake', dry_run := true" in sql, (
        "--reclaim must NOT delete orphans: a registration in flight has its file "
        "on disk with no catalog row, which is what that step deletes"
    )


def test_lake_gc_reclaim_orphans_is_the_only_way_to_delete_orphans(tmp_path) -> None:
    """The orphan step is the only one that can reach a file belonging to a
    registration in flight (register_files moves the file before opening the
    catalog transaction). It gets its own flag so the safe bulk reclaim needs no
    quiescing, and only this one carries that precondition."""
    result = subprocess.run(
        ["bash", str(_LAKE_GC), "--reclaim-orphans"],
        capture_output=True,
        text=True,
        env={**_lake_gc_env(tmp_path), "ASSUME_YES": "1"},
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    sql = (tmp_path / "captured.sql").read_text()
    assert "ducklake_delete_orphaned_files('qiita_lake', dry_run := false" in sql
    assert "dry_run := true" not in sql, "--reclaim-orphans acts on all three steps"


def test_lake_gc_reclaim_requires_typed_confirmation(tmp_path) -> None:
    """--reclaim expires snapshot history irreversibly, so it must not proceed on
    the flag alone. Anything other than the typed word aborts before the first
    maintenance call."""
    env = _lake_gc_env(tmp_path)
    for reply in ("", "yes\n", "y\n", "RECLAIM\n"):
        result = subprocess.run(
            ["bash", str(_LAKE_GC), "--reclaim"],
            capture_output=True,
            text=True,
            input=reply,
            env=env,
        )
        assert result.returncode == 1, f"expected an abort for {reply!r}:\n{result.stdout}"
        assert "Aborted" in result.stderr
    assert not (tmp_path / "captured.sql").exists(), "aborting must not reach the database"


def test_lake_gc_refuses_unwritable_data_path(tmp_path) -> None:
    """The gate is unconditional, so it fires in report mode too — which is what
    this exercises. DuckLake needs to write the data path even for the dry-run
    orphan scan (it fails read-only there), so a group-read operator cannot run
    even the report. Failing here names the right run-as instead of surfacing as
    a permission error from inside duckdb."""
    if os.geteuid() == 0:
        pytest.skip("root ignores the write bit")
    result = subprocess.run(
        ["bash", str(_LAKE_GC)],
        capture_output=True,
        text=True,
        env=_lake_gc_env(tmp_path, writable=False),
    )
    assert result.returncode == 1
    assert "not writable" in result.stderr
    assert "qiita-data" in result.stderr


def test_lake_gc_never_passes_cleanup_all() -> None:
    """`cleanup_all := true` drops the mtime filter outright, so a reclaim would
    also sweep files produced inside the cutoff. The script header carries the
    rationale, including why that filter is not sufficient on its own."""
    assert "cleanup_all" not in _lake_gc_code()


def test_lake_gc_uses_the_documented_call_form() -> None:
    """DuckLake documents these table functions as `CALL f(...)`. `SELECT * FROM
    f(...)` behaves identically (measured on 1.5.4 — same rows, same deletions),
    but a reader checking this against the docs should find the documented form.
    https://ducklake.select/docs/stable/duckdb/maintenance/expire_snapshots"""
    code = _lake_gc_code()
    assert "SELECT * FROM ducklake_" not in code
    for fn in ("expire_snapshots", "cleanup_old_files", "delete_orphaned_files"):
        assert f"CALL ducklake_{fn}(" in code, f"{fn} must be invoked with CALL"


def test_lake_gc_scopes_the_orphan_scan_to_its_own_transaction() -> None:
    """Steps 1-2 share a transaction (cleanup only sees the expiry from inside
    it). Step 3 must open a NEW one: measured on 1.5.4, an orphan scan inside a
    transaction that opened earlier reports a file another session registered and
    COMMITTED in the meantime, so acting on it would unlink a live file whose
    catalog row survives. Asserting positions, not mere presence — a CALL that
    drifts across a COMMIT is exactly the regression this guards."""
    code = _lake_gc_code()
    assert code.count("BEGIN TRANSACTION;") == 2, "expected the 1-2 / 3 split"
    assert code.count("COMMIT;") == 2
    begin1 = code.index("BEGIN TRANSACTION;")
    commit1 = code.index("COMMIT;")
    begin2 = code.index("BEGIN TRANSACTION;", commit1)
    commit2 = code.index("COMMIT;", begin2)
    expire = code.index("CALL ducklake_expire_snapshots(")
    cleanup = code.index("CALL ducklake_cleanup_old_files(")
    orphans = code.index("CALL ducklake_delete_orphaned_files(")
    assert begin1 < expire < cleanup < commit1, "expire+cleanup belong to the first transaction"
    assert begin2 < orphans < commit2, "the orphan scan belongs to its own, later transaction"


def test_lake_gc_always_passes_older_than_explicitly() -> None:
    """Relying on the extension's default retention would let an upstream change
    silently move how much history this script destroys."""
    code = _lake_gc_code()
    pattern = r"ducklake_(?:expire_snapshots|cleanup_old_files|delete_orphaned_files)\([^;]*"
    calls = re.findall(pattern, code)
    assert len(calls) == 3, f"expected the three maintenance calls, found {len(calls)}"
    for call in calls:
        assert "older_than :=" in call, f"call without an explicit older_than: {call}"
        assert "dry_run :=" in call, f"call without an explicit dry_run: {call}"


def test_workflow_scripts_were_found() -> None:
    """Anti-vacuity guard: the parametrization below is a glob, so an empty or
    moved `workflows/` tree would leave it silently exercising nothing.

    Named rather than counted — a floor would still pass if the glob stopped
    reaching a whole workflow's directory. These four are the shared helper and
    one entrypoint from each workflow that ships them.
    """
    found = {p.name for p in _WORKFLOW_SCRIPTS}
    assert {"_lib.sh", "assemble.sh", "entrypoint.sh", "lima.sh"} <= found, found


@pytest.mark.parametrize("path", _WORKFLOW_SCRIPTS, ids=lambda p: p.name)
def test_workflow_script_is_valid_bash(path: Path) -> None:
    """`bash -n` parses without executing — the container never gets that chance
    until a ticket is already running inside it."""
    result = subprocess.run(["bash", "-n", str(path)], capture_output=True, text=True)
    assert result.returncode == 0, f"bash -n failed for {path}:\n{result.stderr}"


# --- wait_for_data_plane_idle ----------------------------------------------------


def _run_idle_wait(
    tmp_path: Path,
    counts: list[int | str],
    *,
    wait_s: str = "60",
    env_file=True,
    pids: dict[str, int] | None = None,
):
    """Run `wait_for_data_plane_idle` with `systemctl` and `psql` stubbed: systemctl
    names each unit's main process from `pids` (unit name -> pid; default one unit,
    `qiita-data-plane@50051`), and psql answers the in-flight catalog-session count
    from `counts`, one per call, then the last one again, and fails on a `"fail"`
    entry.
    Returns the completed process and the argv of every psql call."""
    pids = pids or {"qiita-data-plane@50051": 4242}
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    cases = "".join(f"    {unit}) echo {pid} ;;\n" for unit, pid in pids.items())
    (bin_dir / "systemctl").write_text(
        f'#!/bin/sh\neval "unit=\\${{$#}}"\ncase "$unit" in\n{cases}    *) echo 0 ;;\nesac\n'
    )
    queue = tmp_path / "counts"
    queue.write_text("".join(f"{n}\n" for n in counts))
    calls = tmp_path / "psql-calls"
    (bin_dir / "psql").write_text(
        "#!/bin/sh\n"
        'argv=$(printf %s "$*" | tr "\\n" " ")\n'
        f'printf "%s|PGPASSFILE=%s\\n" "$argv" "$PGPASSFILE" >> "{calls}"\n'
        f'n=$(head -n1 "{queue}")\n'
        f'[ "$(wc -l < "{queue}")" -gt 1 ] && sed -i.bak 1d "{queue}"\n'
        '[ "$n" = fail ] && exit 2\n'
        'echo "$n"\n'
    )
    for stub in ("systemctl", "psql"):
        (bin_dir / stub).chmod(0o755)
    env_path = tmp_path / "data-plane.env"
    if env_file:
        env_path.write_text(
            "DUCKLAKE_CATALOG_CONNSTR='dbname=lake host=db user=lake_rw password=s3cr3t'\n"
        )
    proc = subprocess.run(
        [
            "bash",
            "-c",
            f'set -euo pipefail; source "{_COMMON}"; '
            f'wait_for_data_plane_idle "{env_path}" {" ".join(pids)}',
        ],
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "QIITA_DP_IDLE_POLL_S": "0",
            "QIITA_DP_IDLE_WAIT_S": wait_s,
        },
        capture_output=True,
        text=True,
        timeout=60,
    )
    lines = calls.read_text().splitlines() if calls.exists() else []
    return proc, lines


def test_idle_wait_returns_once_no_catalog_session_is_in_flight(tmp_path: Path) -> None:
    proc, calls = _run_idle_wait(tmp_path, [2, 1, 0])
    assert proc.returncode == 0, proc.stderr
    assert len(calls) == 3
    assert "waiting for qiita-data-plane@50051 to go idle (catalog sessions: 2" in proc.stderr


def test_idle_wait_goes_ahead_when_the_wait_runs_out(tmp_path: Path) -> None:
    proc, calls = _run_idle_wait(tmp_path, [1], wait_s="1")
    assert proc.returncode == 0, proc.stderr
    assert "still busy after 1s" in proc.stderr
    assert calls


def test_idle_wait_keeps_the_catalog_password_off_psql_argv(tmp_path: Path) -> None:
    """The password reaches libpq through a PGPASSFILE (argv is readable by every user
    on the host through /proc)."""
    _proc, calls = _run_idle_wait(tmp_path, [0])
    argv, pgpassfile = calls[0].split("|PGPASSFILE=")
    assert "s3cr3t" not in argv
    assert "user=lake_rw" in argv
    assert pgpassfile, "psql ran without a PGPASSFILE"


def test_idle_wait_counts_only_the_data_plane_roles_sessions(tmp_path: Path) -> None:
    """With pg_read_all_stats the role sees the state of every role's sessions; the
    count is limited to its own, so another role's open transaction on the catalog
    does not hold the restart."""
    _proc, calls = _run_idle_wait(tmp_path, [0])
    argv, _pgpassfile = calls[0].split("|PGPASSFILE=")
    assert "AND usename = current_user" in argv


@pytest.mark.parametrize(("wait_s", "env_file"), [("0", True), ("60", False)])
def test_idle_wait_skips_when_disabled_or_on_a_first_deploy(
    tmp_path: Path, wait_s: str, env_file: bool
) -> None:
    proc, calls = _run_idle_wait(tmp_path, [1], wait_s=wait_s, env_file=env_file)
    assert proc.returncode == 0, proc.stderr
    assert calls == []


def test_idle_wait_refuses_a_wait_that_is_not_whole_seconds(tmp_path: Path) -> None:
    proc, calls = _run_idle_wait(tmp_path, [1], wait_s="1h")
    assert proc.returncode != 0
    assert "QIITA_DP_IDLE_WAIT_S must be a whole number of seconds, got '1h'" in proc.stderr
    assert calls == []


def test_idle_wait_reads_a_leading_zero_as_decimal(tmp_path: Path) -> None:
    """Read as octal, `09` is an arithmetic error that ends the wait non-zero."""
    proc, _calls = _run_idle_wait(tmp_path, [1, 0], wait_s="09")
    assert proc.returncode == 0, proc.stderr


def test_idle_wait_stops_the_deploy_when_the_catalog_query_fails(tmp_path: Path) -> None:
    """A wait that cannot tell whether the data plane is idle stops the deploy before
    any restart, and names the override."""
    proc, _calls = _run_idle_wait(tmp_path, ["fail"])
    assert proc.returncode != 0
    assert "set QIITA_DP_IDLE_WAIT_S=0 to restart without waiting" in proc.stderr


@pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="needs /proc (Linux)")
@pytest.mark.parametrize(
    ("name", "busy"), [("upload.parquet.partial", True), ("upload.parquet", False)]
)
def test_idle_wait_counts_an_open_partial_upload(tmp_path: Path, name: str, busy: bool) -> None:
    """A DoPut holds its `upload.parquet.partial` open, and the wait finds it through
    /proc/<pid>/fd; a published `upload.parquet` held open is not an upload in flight."""
    held = tmp_path / name
    held.touch()
    holder = subprocess.Popen(
        ["python3", "-c", "import sys; f = open(sys.argv[1]); sys.stdin.read()", str(held)],
        stdin=subprocess.PIPE,
    )
    try:
        fd_dir = Path(f"/proc/{holder.pid}/fd")
        for _ in range(100):
            if any(os.readlink(fd) == str(held) for fd in fd_dir.iterdir()):
                break
            time.sleep(0.05)
        proc, _calls = _run_idle_wait(
            tmp_path, [0], wait_s="1", pids={"qiita-data-plane@50051": holder.pid}
        )
    finally:
        holder.communicate(b"")
    assert proc.returncode == 0, proc.stderr
    assert (
        "still busy after 1s (catalog sessions: 0, open partial files: 1)" in proc.stderr
    ) is busy


@pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="needs /proc (Linux)")
def test_idle_wait_counts_a_partial_upload_on_any_local_instance(tmp_path: Path) -> None:
    """With several instances on the host, an upload in flight on the second one
    keeps the wait going, as one on the first does."""
    held = tmp_path / "upload.parquet.partial"
    held.touch()
    holder = subprocess.Popen(
        ["python3", "-c", "import sys; f = open(sys.argv[1]); sys.stdin.read()", str(held)],
        stdin=subprocess.PIPE,
    )
    try:
        fd_dir = Path(f"/proc/{holder.pid}/fd")
        for _ in range(100):
            if any(os.readlink(fd) == str(held) for fd in fd_dir.iterdir()):
                break
            time.sleep(0.05)
        proc, _calls = _run_idle_wait(
            tmp_path,
            [0],
            wait_s="1",
            pids={"qiita-data-plane@50051": os.getpid(), "qiita-data-plane@50052": holder.pid},
        )
    finally:
        holder.communicate(b"")
    assert proc.returncode == 0, proc.stderr
    assert (
        "qiita-data-plane@50051 qiita-data-plane@50052 still busy after 1s "
        "(catalog sessions: 0, open partial files: 1)"
    ) in proc.stderr


def test_idle_wait_skips_units_with_no_main_process(tmp_path: Path) -> None:
    """A listed instance that is not running is not waited on; with none running
    the wait returns without querying the catalog."""
    proc, calls = _run_idle_wait(
        tmp_path, [1], pids={"qiita-data-plane@50051": 0, "qiita-data-plane@50052": 0}
    )
    assert proc.returncode == 0, proc.stderr
    assert calls == []


def test_data_plane_unit_stop_timeout_covers_the_idle_wait() -> None:
    """The data plane drains on SIGTERM for up to the unit's TimeoutStopSec, and the
    deploy's idle wait defaults to the same bound."""
    unit = (_DEPLOY / "systemd" / "qiita-data-plane@.service").read_text()
    assert re.search(r"(?m)^TimeoutStopSec=3900$", unit)
    assert "QIITA_DP_IDLE_WAIT_S:-3900}" in _COMMON.read_text()


# --- catalog connection budget ---------------------------------------------------

_DUCKLAKE_RS = _REPO_ROOT / "qiita-data-plane" / "src" / "ducklake.rs"


def _budget(settings: str, rows: str) -> tuple[list[str], list[str]]:
    """Run qiita_catalog_connection_budget on a settings row and backend rows, as
    psql prints them. Returns the pass/fail rows and the detail lines under them."""
    result = _run_common(
        'n_pass=0 n_fail=0; qiita_catalog_connection_budget "$1" <<<"$2"; '
        'echo "counts $n_pass $n_fail"',
        settings,
        rows,
    )
    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert lines[-1] in ("counts 1 0", "counts 0 1"), result.stdout
    rows_out = [line for line in lines[:-1] if " catalog-connections: " in line]
    assert len(rows_out) == 1, result.stdout
    details = [line.strip() for line in lines[:-1] if line not in rows_out]
    return rows_out, details


def _passed(row: str) -> bool:
    return row.lstrip().startswith("✓")


def test_budget_counts_each_instance_once_at_its_pool_size() -> None:
    """Two backends of one instance, under two roles, are one instance; a Unix-socket
    session is `local`; an IPv6 address is bracketed; other clients count as they are."""
    rows = "\n".join(
        [
            "10.0.0.5|3|dp|qiita-data-plane:50051:pool=8",
            "10.0.0.5|1|dp_other|qiita-data-plane:50051:pool=8",
            "10.0.0.5|1|dp|qiita-data-plane:50052:pool=8",
            "10.0.0.6|2|dp|qiita-data-plane:50051:pool=6",
            "local|1|dp|qiita-data-plane:50051:pool=4",
            "::1|1|dp|qiita-data-plane:50053:pool=2",
            "10.0.0.9|3|admin|psql",
            "10.0.0.9|2|dp|",
        ]
    )
    (row,), details = _budget("100|3|0|t|dp", rows)
    assert _passed(row), row
    assert "5 data-plane instance(s), pools 28 + 5 other client backend(s) = 33 of 97" in row
    assert details == [
        "10.0.0.5:50051 pool=8",
        "10.0.0.5:50052 pool=8",
        "10.0.0.6:50051 pool=6",
        "local:50051 pool=4",
        "[::1]:50053 pool=2",
        "other: 10.0.0.9 user=admin application_name=psql x3",
        "other: 10.0.0.9 user=dp application_name=(none) x2",
    ]


@pytest.mark.parametrize(("other", "passes"), [(5, True), (6, False)])
def test_budget_fails_past_max_connections_less_the_reserved(other: int, passes: bool) -> None:
    """20 − 3 superuser-reserved − 2 reserved leaves 15: pools 10 plus 5 others fit,
    plus 6 do not."""
    rows = f"10.0.0.5|1|dp|qiita-data-plane:50051:pool=10\n10.0.0.9|{other}|admin|psql"
    (row,), _ = _budget("20|3|2|t|dp", rows)
    assert _passed(row) is passes, row
    assert f"= {10 + other} of 15 (max_connections 20" in row


def test_budget_fails_without_pg_read_all_stats() -> None:
    (row,), details = _budget(
        "100|3|0|f|lake_rw", "10.0.0.5|1|lake_rw|qiita-data-plane:50051:pool=8"
    )
    assert not _passed(row), row
    assert "GRANT pg_read_all_stats TO lake_rw;" in row
    assert details == []


def test_budget_fails_on_a_data_plane_session_without_a_client_address() -> None:
    (row,), _ = _budget("100|3|0|t|lake_rw", "|1|lake_rw|qiita-data-plane:50051:pool=8")
    assert not _passed(row), row
    assert "grant pg_read_all_stats to role 'lake_rw'" in row


@pytest.mark.parametrize(
    "name", ["qiita-data-plane:50051", "qiita-data-plane:x:pool=8", "qiita-data-plane:50051:pool="]
)
def test_budget_fails_on_a_name_it_cannot_read(name: str) -> None:
    (row,), _ = _budget("100|3|0|t|dp", f"10.0.0.5|1|dp|{name}")
    assert not _passed(row), row
    assert f"application_name '{name}'" in row


def test_budget_fails_when_no_instance_is_connected() -> None:
    (row,), details = _budget("100|3|0|t|dp", "10.0.0.9|2|admin|psql")
    assert not _passed(row), row
    assert "no catalog session is named" in row
    assert details == ["other: 10.0.0.9 user=admin application_name=psql x2"]


@pytest.mark.parametrize("settings", ["", "100|3||t|dp", "lots|3|0|t|dp"])
def test_budget_fails_on_a_settings_row_it_cannot_read(settings: str) -> None:
    (row,), _ = _budget(settings, "10.0.0.5|1|dp|qiita-data-plane:50051:pool=8")
    assert not _passed(row), row
    assert "unexpected settings row" in row


def test_budget_reads_the_name_the_data_plane_writes() -> None:
    """catalog_application_name's format, filled in, is a name the budget counts."""
    match = re.search(
        r"fn catalog_application_name\(port: u16, pool_size: usize\) -> String \{\s*"
        r'format!\("([^"]+)"\)',
        _DUCKLAKE_RS.read_text(),
    )
    assert match, f"catalog_application_name not found in {_DUCKLAKE_RS}"
    name = match.group(1).replace("{port}", "50057").replace("{pool_size}", "13")
    (row,), details = _budget("100|3|0|t|dp", f"10.0.0.5|1|dp|{name}")
    assert _passed(row), row
    assert details == ["10.0.0.5:50057 pool=13"]


def _run_budget_check(
    tmp_path: Path, *, fail_on: str = ""
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    """Run qiita_check_catalog_connection_budget with `psql` stubbed: it answers the
    settings query and the backends query with one row each, records its argv and
    PGPASSFILE, and exits 2 when its argv contains `fail_on`."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "psql-calls"
    (bin_dir / "psql").write_text(
        "#!/bin/sh\n"
        'argv=$(printf %s "$*" | tr "\\n" " ")\n'
        f'printf "%s|PGPASSFILE=%s\\n" "$argv" "$PGPASSFILE" >> "{calls}"\n'
        f'if [ -n "{fail_on}" ]; then case "$argv" in *"{fail_on}"*)\n'
        '  echo "psql: boom" >&2; exit 2 ;; esac; fi\n'
        'case "$argv" in\n'
        "  *max_connections*) echo '100|3|0|t|lake_rw' ;;\n"
        "  *) echo '10.0.0.5|2|lake_rw|qiita-data-plane:50051:pool=8' ;;\n"
        "esac\n"
    )
    (bin_dir / "psql").chmod(0o755)
    env_path = tmp_path / "data-plane.env"
    env_path.write_text(
        "DUCKLAKE_CATALOG_CONNSTR='dbname=lake host=db user=lake_rw password=s3cr3t'\n"
    )
    proc = subprocess.run(
        [
            "bash",
            "-c",
            f'set -euo pipefail; source "{_COMMON}"; n_pass=0 n_fail=0; '
            f'qiita_check_catalog_connection_budget "{env_path}"; echo "counts $n_pass $n_fail"',
        ],
        env={**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}"},
        capture_output=True,
        text=True,
        timeout=60,
    )
    lines = calls.read_text().splitlines() if calls.exists() else []
    return proc, lines


def test_budget_check_queries_the_catalog_with_the_password_off_argv(tmp_path: Path) -> None:
    proc, calls = _run_budget_check(tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert "counts 1 0" in proc.stdout, proc.stdout
    assert "10.0.0.5:50051 pool=8" in proc.stdout
    assert len(calls) == 2
    for call in calls:
        argv, pgpassfile = call.split("|PGPASSFILE=")
        assert "s3cr3t" not in argv
        assert "user=lake_rw" in argv
        assert pgpassfile, "psql ran without a PGPASSFILE"


@pytest.mark.parametrize("fail_on", ["max_connections", "pg_stat_activity"])
def test_budget_check_fails_the_row_when_a_query_fails(tmp_path: Path, fail_on: str) -> None:
    proc, _calls = _run_budget_check(tmp_path, fail_on=fail_on)
    assert proc.returncode == 0, proc.stderr
    assert "counts 0 1" in proc.stdout, proc.stdout
    assert "cannot query the catalog server: psql: boom" in proc.stdout


def test_verify_runs_the_budget_against_the_data_plane_env() -> None:
    verify = (_DEPLOY / "verify.sh").read_text()
    assert re.search(r'(?m)^    qiita_check_catalog_connection_budget "\$DP_ENV"$', verify)


@pytest.mark.parametrize(
    "connstr",
    [
        "dbname=lake host=db application_name=mine",
        "dbname=lake host=db application_name = mine",
    ],
)
def test_preflight_refuses_a_catalog_connstr_that_names_the_session(
    tmp_path: Path, connstr: str
) -> None:
    row = _preflight_row(tmp_path, f"DUCKLAKE_CATALOG_CONNSTR='{connstr}'\n", "connstr/ducklake")
    assert not _passed(row), row
    assert "sets application_name" in row
