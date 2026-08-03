#!/bin/bash
# ADMIN DEBUGGING TOOL — READ-ONLY, NOT A DATA-ACCESS PATH.
#
# For an operator diagnosing the live system. It is not an export tool, not a
# user-facing query interface, and not a substitute for the REST API: it bypasses
# every authorization check the control plane enforces, so whoever runs it sees
# ALL studies' data regardless of who owns it. Read-only is not permission —
# treat what you see as confidential, take nothing out of the session that a
# scoped API call would not have given you, and prefer the narrowest query that
# answers your question. Handle with care even though it cannot write.
#
# READ_ONLY is a DuckDB-level guard, not a database grant: the credentials it
# reads are the services' read-write ones. It performs no catalog writes at all
# (verified against a SELECT-only role with the data dir read-only), so a site
# wanting defence in depth can point it at a SELECT-only role.
#
# Two catalogs, so one query can join lake data to control-plane metadata:
#   qiita_lake   DuckLake   qiita_lake.read, .alignment, .reference_* …
#   qiita_cp     Postgres   qiita_cp.qiita.study, .work_ticket, .action …
#                           (CP tables are in the `qiita` schema — three-part
#                           name; skipped by --no-cp or an unreadable CP env)
#
# miint is loaded from the deploy-staged MIINT_EXTENSION_DIRECTORY — the same
# build the services and SLURM jobs run, never your ~/.duckdb cache — plus core
# httpfs, so a query here behaves like the same query in a job. Read from
# control-plane.env, else compute-orchestrator.env (preflight keeps them
# identical); export the var to override. It is a core dependency: failing to
# load it is a hard ERROR and the shell does not open.
#
# Needs no root. Group-granted reads, all established at first deploy:
#   * /etc/qiita/data-plane.env         0440 root:qiita-data   (required)
#   * PATH_PERSISTENT/ducklake          0750 qiita-data        (required)
#   * /etc/qiita/control-plane.env      0440 root:qiita-api    (qiita_cp + miint)
#   * /etc/qiita/compute-orchestrator.env  0440 root:qiita-orch (miint fallback)
#
# Catalog passwords never reach the SQL file or argv (world-readable via /proc):
# they go to libpq through a 0600 PGPASSFILE deleted on exit.
#
# Usage:
#   bash scripts/lake-shell.sh                      # interactive shell
#   bash scripts/lake-shell.sh -c "SELECT ..."      # one-shot query
#   bash scripts/lake-shell.sh -c "..." -json       # duckdb CLI flags pass through
#   bash scripts/lake-shell.sh --no-cp              # lake only
#
# Starts at 4 threads / 32GB — modest, since this shares a host with the
# services. Raise with `SET threads = 8;` / `SET memory_limit = '64GB';`.
#
# Env overrides:
#   DP_ENV                   data-plane env file (default /etc/qiita/data-plane.env)
#   CP_ENV                   control-plane env file (default /etc/qiita/control-plane.env)
#   QIITA_DUCKDB_BIN         duckdb CLI to run (default: `duckdb` on PATH)
#   QIITA_LAKE_THREADS       starting thread count (default 4)
#   QIITA_LAKE_MEMORY_LIMIT  starting memory limit (default 32GB)
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/.." && pwd )"

# Reach over to the deploy/ shared helpers for DP_ENV / CP_ENV and read_env_var
# (pure; no side effects on source — see its header), so this script parses the
# env files exactly the way preflight.sh/verify.sh do rather than growing a
# second, subtly-different parser.
# shellcheck source=../deploy/_common.sh
source "${REPO_ROOT}/deploy/_common.sh"

WANT_CP=1
DUCKDB_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            # Print the header comment block (everything after the shebang, up
            # to the first non-comment line) as the usage text — one copy, not two.
            awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "${BASH_SOURCE[0]}"
            exit 0
            ;;
        --no-cp) WANT_CP=0 ;;
        *) DUCKDB_ARGS+=("$1") ;;
    esac
    shift
done

# The DuckDB CLI should match the version the data plane links (duckdb crate
# 1.10504.0 == DuckDB 1.5.4): the ducklake extension is versioned with DuckDB,
# and a newer one may want to migrate the catalog schema it opens.
DUCKDB_VERSION="1.5.4"
DUCKDB_BIN="${QIITA_DUCKDB_BIN:-$(command -v duckdb || true)}"
if [[ -z "${DUCKDB_BIN}" ]]; then
    cat >&2 <<EOF
ERROR: no duckdb CLI on PATH.

Install one into your own account (no root needed):

  mkdir -p ~/.local/bin && cd "\$(mktemp -d)" \\
    && curl -sSfL -O https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip \\
    && unzip -q duckdb_cli-linux-amd64.zip \\
    && install -m 0755 duckdb ~/.local/bin/duckdb

Then re-run this script (or point QIITA_DUCKDB_BIN at the binary).
EOF
    exit 1
fi

# ATTACH takes no bind parameters, so connection strings and paths are
# interpolated into SQL. Reject the same characters
# qiita-data-plane/src/ducklake.rs validate_sql_literal does — this is input
# validation, not sanitization.
reject_sql_metacharacters() {
    local value="$1" label="$2"
    case "${value}" in
        *\'*|*\;*)
            echo "ERROR: ${label} contains a quote or semicolon; refusing to interpolate it into SQL." >&2
            exit 1
            ;;
    esac
}

# The password-splitting itself lives in deploy/_common.sh (qiita_split_conn_password,
# unit-tested in test_deploy_scripts.py). These three hold its last result.
CONN_SANITIZED=""
CONN_USER=""
CONN_PASSWORD=""
split_conn_password() {
    IFS=$'\t' read -r CONN_SANITIZED CONN_USER CONN_PASSWORD \
        < <(qiita_split_conn_password "$1")
}

# Every duckdb invocation goes through here so the PGPASSFILE handling exists in
# exactly one place. The file is only handed over when it actually holds an
# entry — libpq ignores an empty one anyway, but not setting the variable keeps
# a password-less deployment obviously password-less.
run_duckdb() {
    if [[ -s "${PGPASS_FILE}" ]]; then
        PGPASSFILE="${PGPASS_FILE}" "${DUCKDB_BIN}" "$@"
    else
        "${DUCKDB_BIN}" "$@"
    fi
}

# Both temp files can hold a credential, so create them at 0600 BEFORE anything
# is written — a redirection would otherwise create them at the umask's mode and
# only be narrowed afterwards. The 0700 parent already blocks other users; this
# is belt-and-braces. Trap the signals a dropped session actually sends, not just
# EXIT, so a pgpass file never outlives the shell.
TMPROOT="$(mktemp -d "${TMPDIR:-/tmp}/qiita-lake-shell.XXXXXX")"
chmod 700 "${TMPROOT}"
trap 'rm -rf "${TMPROOT}"' EXIT INT TERM HUP
INIT_SQL="${TMPROOT}/init.sql"
PGPASS_FILE="${TMPROOT}/pgpass"
: > "${INIT_SQL}"; chmod 600 "${INIT_SQL}"
: > "${PGPASS_FILE}"; chmod 600 "${PGPASS_FILE}"

# pgpass lines are `host:port:database:user:password` with `:` and `\` escaped.
# Both connections are keyed on the username alone (wildcard host/port/db),
# which is unambiguous as long as they do not share one — and if they do share a
# username with different passwords, error rather than let the first line win.
#
# The seen-set is two parallel indexed arrays and a linear scan rather than an
# associative array, because macOS ships bash 3.2 — which has none, and which
# `make test` exercises this script under on the mac CI runner. The `_COUNT`
# scalar bounds the scan instead of `${#array[@]}`: under `set -u`, bash 3.2
# treats an empty array as unset. Passwords may hold any byte, so a single
# delimited-string map would need escaping that a scan does not.
PGPASS_SEEN_COUNT=0
PGPASS_SEEN_USERS=()
PGPASS_SEEN_PASSWORDS=()
add_pgpass_entry() {
    local user="$1" password="$2" escaped_user escaped_password i
    for ((i = 0; i < PGPASS_SEEN_COUNT; i++)); do
        [[ "${PGPASS_SEEN_USERS[i]}" == "${user}" ]] || continue
        if [[ "${PGPASS_SEEN_PASSWORDS[i]}" != "${password}" ]]; then
            echo "ERROR: the lake catalog and the control-plane database both connect as '${user}'" >&2
            echo "  with different passwords, so they cannot be keyed apart in a pgpass file." >&2
            echo "  Re-run with --no-cp, or give one of them its own role." >&2
            exit 1
        fi
        return 0
    done
    PGPASS_SEEN_USERS[PGPASS_SEEN_COUNT]="${user}"
    PGPASS_SEEN_PASSWORDS[PGPASS_SEEN_COUNT]="${password}"
    PGPASS_SEEN_COUNT=$((PGPASS_SEEN_COUNT + 1))
    escaped_user="${user//\\/\\\\}"; escaped_user="${escaped_user//:/\\:}"
    escaped_password="${password//\\/\\\\}"; escaped_password="${escaped_password//:/\\:}"
    printf '*:*:*:%s:%s\n' "${escaped_user}" "${escaped_password}" >> "${PGPASS_FILE}"
}

# ---- the lake (required) ----------------------------------------------------

if [[ ! -r "${DP_ENV}" ]]; then
    echo "ERROR: cannot read ${DP_ENV}" >&2
    echo "  It is mode 0440 root:qiita-data — you must be in the owning group." >&2
    echo "  Check with: ls -l ${DP_ENV} && id" >&2
    exit 1
fi

LAKE_CONNSTR="$(read_env_var "${DP_ENV}" DUCKLAKE_CATALOG_CONNSTR)"
PERSISTENT="$(read_env_var "${DP_ENV}" PATH_PERSISTENT)"
[[ -n "${LAKE_CONNSTR}" ]] || { echo "ERROR: DUCKLAKE_CATALOG_CONNSTR is unset in ${DP_ENV}" >&2; exit 1; }
[[ -n "${PERSISTENT}" ]]   || { echo "ERROR: PATH_PERSISTENT is unset in ${DP_ENV}" >&2; exit 1; }

# Byte-identical to the data plane's derivation — config.rs does a bare
# format!("{path_persistent_raw}/ducklake") with NO normalization, and DuckLake
# pins that exact string into the catalog at creation. Do not "tidy" a trailing
# slash off PERSISTENT here: on a host with PATH_PERSISTENT=/data/ the catalog
# holds "/data//ducklake", and the tidied "/data/ducklake" is rejected outright
# with `DATA_PATH parameter ... does not match existing data path in the catalog`.
DATA_PATH="${PERSISTENT}/ducklake"
if [[ ! -d "${DATA_PATH}" ]]; then
    echo "ERROR: lake data path ${DATA_PATH} is not a directory" >&2
    exit 1
fi
if [[ ! -r "${DATA_PATH}" || ! -x "${DATA_PATH}" ]]; then
    echo "ERROR: cannot read ${DATA_PATH}" >&2
    echo "  It is mode 0750 qiita-data:qiita-data — you must be in the owning group." >&2
    echo "  Check with: ls -ld ${DATA_PATH} && id" >&2
    exit 1
fi

split_conn_password "${LAKE_CONNSTR}"
LAKE_CONNSTR="${CONN_SANITIZED}"
[[ -z "${CONN_PASSWORD}" ]] || add_pgpass_entry "${CONN_USER}" "${CONN_PASSWORD}"
reject_sql_metacharacters "${LAKE_CONNSTR}" "the lake catalog connection string"
reject_sql_metacharacters "${DATA_PATH}" "the lake data path"

# ---- the control-plane database (optional) ----------------------------------

CP_URL=""
if [[ "${WANT_CP}" -eq 1 ]]; then
    if [[ -r "${CP_ENV}" ]]; then
        CP_URL="$(read_env_var "${CP_ENV}" DATABASE_URL)"
        if [[ -z "${CP_URL}" ]]; then
            echo "NOTE: DATABASE_URL is unset in ${CP_ENV}; attaching the lake only." >&2
        else
            split_conn_password "${CP_URL}"
            CP_URL="${CONN_SANITIZED}"
            [[ -z "${CONN_PASSWORD}" ]] || add_pgpass_entry "${CONN_USER}" "${CONN_PASSWORD}"
            reject_sql_metacharacters "${CP_URL}" "the control-plane connection string"
        fi
    else
        echo "NOTE: ${CP_ENV} is not readable, so qiita_cp is not attached." >&2
        echo "      It is 0440 root:qiita-api — a different group than the lake's." >&2
        echo "      Pass --no-cp to silence this." >&2
    fi
fi

# ---- miint (the deploy-staged build, same file production runs) -------------

# miint is a CORE dependency, so the shell loads the SAME staged extension the
# services and SLURM jobs run rather than whatever happens to be in the caller's
# ~/.duckdb cache — a query here has to behave exactly like the same query in a
# job. The directory is byte-identical across control-plane.env and
# compute-orchestrator.env (preflight compares them), so either file answers; the
# caller's own environment wins if it already has one.
#
# Fail LOUD, never fail-soft: a shell that silently came up without miint would
# answer bioinformatics queries differently from production, which is worse than
# not opening at all. Same posture as the cluster runtime — an unresolvable or
# unreadable directory aborts here, and a directory that resolves but fails to
# LOAD aborts inside DuckDB under `.bail on`.
MIINT_EXT_DIR="${MIINT_EXTENSION_DIRECTORY:-}"
if [[ -z "${MIINT_EXT_DIR}" ]]; then
    for miint_env in "${CP_ENV}" "${CO_ENV}"; do
        [[ -r "${miint_env}" ]] || continue
        MIINT_EXT_DIR="$(read_env_var "${miint_env}" MIINT_EXTENSION_DIRECTORY)"
        [[ -z "${MIINT_EXT_DIR}" ]] || break
    done
fi
if [[ -z "${MIINT_EXT_DIR}" ]]; then
    echo "ERROR: MIINT_EXTENSION_DIRECTORY is not set, and could not be read from" >&2
    echo "  ${CP_ENV} (0440 root:qiita-api)" >&2
    echo "  ${CO_ENV} (0440 root:qiita-orch)" >&2
    echo "miint is a core dependency — this shell will not open without the staged" >&2
    echo "build the services run. Join one of those groups, or export" >&2
    echo "MIINT_EXTENSION_DIRECTORY yourself (it is the deploy-staged dir, typically" >&2
    echo "PATH_DERIVED/duckdb-ext)." >&2
    exit 1
fi
if [[ ! -d "${MIINT_EXT_DIR}" || ! -r "${MIINT_EXT_DIR}" ]]; then
    echo "ERROR: MIINT_EXTENSION_DIRECTORY=${MIINT_EXT_DIR} is not a readable directory." >&2
    echo "  It is the deploy-staged extension dir (typically 0755 qiita-orch:qiita-orch)." >&2
    echo "  Check with: ls -ld ${MIINT_EXT_DIR}" >&2
    exit 1
fi
reject_sql_metacharacters "${MIINT_EXT_DIR}" "MIINT_EXTENSION_DIRECTORY"

# The duckdb CLI aborts the ENTIRE init file on the first error — `.bail off`
# does not cover that, and neither does moving the statement to `-cmd` — so an
# unreachable control-plane database would cost you the lake shell too. Since
# qiita_cp is the optional extra (already skipped when its env file is
# unreadable) and a CP outage is exactly when someone wants to poke at the lake,
# probe it in a throwaway process first and drop it if it does not answer.
if [[ -n "${CP_URL}" ]]; then
    if ! CP_PROBE_ERROR="$(run_duckdb -bail -c \
        "INSTALL postgres; LOAD postgres; ATTACH '${CP_URL}' AS probe (TYPE postgres, READ_ONLY);" \
        2>&1 >/dev/null)"; then
        echo "NOTE: the control-plane database did not answer, so qiita_cp is not attached." >&2
        printf '%s\n' "${CP_PROBE_ERROR}" | head -3 | sed 's/^/      /' >&2
        CP_URL=""
    fi
fi

# ---- build the session ------------------------------------------------------

# TMPDIR is interpolated into the SET below, so it gets the same validation the
# connection strings and the data path get — no carve-out just because it is the
# caller's own variable.
SESSION_TMPDIR="${TMPDIR:-/tmp}"
reject_sql_metacharacters "${SESSION_TMPDIR}" "TMPDIR"

# Resource defaults. DuckDB otherwise takes every core and ~80% of system RAM,
# which is antisocial on a shared deploy host where the services are the point.
# These are only the session's STARTING values — `SET threads = 8;` /
# `SET memory_limit = '64GB';` at the prompt overrides either one at any time.
# (`max_memory` and `worker_threads` are aliases of these two, not separate
# knobs. Note DuckDB reads GB as decimal, so the 32GB default caps at 29.8 GiB;
# write GiB if you want a binary ceiling.)
LAKE_THREADS="${QIITA_LAKE_THREADS:-4}"
LAKE_MEMORY_LIMIT="${QIITA_LAKE_MEMORY_LIMIT:-32GB}"
if [[ ! "${LAKE_THREADS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: QIITA_LAKE_THREADS must be a positive integer, got '${LAKE_THREADS}'" >&2
    exit 1
fi
reject_sql_metacharacters "${LAKE_MEMORY_LIMIT}" "QIITA_LAKE_MEMORY_LIMIT"

# `.bail on` so an attach failure exits instead of silently dropping the user
# into a shell with nothing attached. It stays on for non-interactive runs (a
# one-shot `-c` query must set a non-zero exit status on error) and is turned
# back off for an interactive session, where a typo should not kill the shell.
{
    echo ".bail on"
    # Remember where DuckDB looks for extensions BEFORE the miint switch below,
    # so it can be put back afterwards.
    echo "SET VARIABLE qiita_default_ext_dir = current_setting('extension_directory');"
    echo "INSTALL ducklake; LOAD ducklake;"
    echo "INSTALL postgres; LOAD postgres;"
    echo "INSTALL httpfs; LOAD httpfs;"
    # extension_directory redirects lookups for EVERY extension, and the staged
    # directory holds only miint and is not writable by us — so the core three
    # above must already be loaded before the switch, and the original directory
    # must be restored after it. Without the restore, the first extension DuckDB
    # tries to autoload later in the session looks in the staged directory, does
    # not find it, and cannot install it there either. LOAD, never INSTALL: the
    # build is staged at deploy, exactly as the services load it.
    echo "SET extension_directory='${MIINT_EXT_DIR}';"
    echo "LOAD miint;"
    echo "SET extension_directory=getvariable('qiita_default_ext_dir');"
    echo "SET threads = ${LAKE_THREADS};"
    echo "SET memory_limit = '${LAKE_MEMORY_LIMIT}';"
    # An in-memory DB spills to ./.tmp by default, which fails from a read-only cwd.
    echo "SET temp_directory='${SESSION_TMPDIR}';"
    echo "ATTACH 'ducklake:postgres:${LAKE_CONNSTR}' AS qiita_lake (DATA_PATH '${DATA_PATH}', READ_ONLY);"
    [[ -z "${CP_URL}" ]] || echo "ATTACH '${CP_URL}' AS qiita_cp (TYPE postgres, READ_ONLY);"
    echo "USE qiita_lake;"
} > "${INIT_SQL}"

# The duckdb CLI flags that run something and exit (`-cmd` is deliberately NOT
# here: it runs a command *before* reading stdin and stays interactive).
INTERACTIVE=1
[[ -t 0 ]] || INTERACTIVE=0
for arg in "${DUCKDB_ARGS[@]+"${DUCKDB_ARGS[@]}"}"; do
    case "${arg}" in
        -c|-s|-f|-batch|-no-stdin) INTERACTIVE=0 ;;
    esac
done
if [[ "${INTERACTIVE}" -eq 1 ]]; then
    # Report what is ACTUALLY attached, from the catalog itself, and only once
    # the attaches above have run — a banner echoed from the shell beforehand
    # would announce success and then be followed by the attach error.
    {
        echo ".print"
        echo ".print ADMIN DEBUGGING SESSION — read-only, and NOT authorization-scoped:"
        echo ".print this sees every study's data. Handle it as confidential."
        echo ".print"
        echo ".print Current catalog: qiita_lake. miint from ${MIINT_EXT_DIR}"
        [[ -z "${CP_URL}" ]] || echo ".print Control-plane tables are qiita_cp.qiita.<table>."
        # duckdb_extensions().install_path resolves against the CURRENT
        # extension_directory, not where the extension was actually loaded from,
        # so it would misreport miint's provenance after the restore above.
        # Report `loaded` only; the staged path is printed from the shell.
        echo "SELECT database_name, type, readonly FROM duckdb_databases()"
        echo "  WHERE database_name IN ('qiita_lake', 'qiita_cp') ORDER BY database_name;"
        echo ".bail off"
    } >> "${INIT_SQL}"
fi

# -unsigned is required to load miint at all: it ships through the team's mirror,
# whose signing chain is not DuckDB's. This is the CLI equivalent of the
# `allow_unsigned_extensions` the services set in miint_connect_config().
run_duckdb -unsigned -init "${INIT_SQL}" "${DUCKDB_ARGS[@]+"${DUCKDB_ARGS[@]}"}"
