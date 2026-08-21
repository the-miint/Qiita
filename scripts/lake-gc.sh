#!/bin/bash
# Report — and, with --reclaim, remove — lake data files nothing references.
#
# DuckLake never reclaims a data file on its own. Deleting rows leaves the file
# on disk and still referenced by the snapshots that predate the delete, so
# every superseded load accumulates:
#
#   * `register_files` replace-by-key (REPLACE_KEY_TABLES) supersedes rows in
#     assembled_sequence / *_chunks / reference_sequences / *_chunks and leaves
#     the previous load's Parquet behind;
#   * delete_reference / delete_mask / delete_pool_reads / delete_alignment
#     reclaim nothing from disk either;
#   * a registration whose catalog transaction rolls back leaves the moved file
#     with no catalog entry at all (see flight_service.rs, register_files).
#
# The first two become reclaimable only after their snapshots expire; the third
# is an orphan the catalog never knew about. This drives DuckLake's own
# maintenance functions for both, in the order they must run:
#
#   1. ducklake_expire_snapshots      drop snapshots older than the retention
#   2. ducklake_cleanup_old_files     remove files no surviving snapshot holds
#   3. ducklake_delete_orphaned_files remove files the catalog never references
#
# Step 1 gates step 2: measured on DuckDB/DuckLake 1.5.4, cleanup_old_files
# reports nothing for a file whose rows were all deleted until the snapshots
# referencing it are expired. Running 2 without 1 is a no-op, not a shortcut.
#
# REPORTS BY DEFAULT — nothing is removed unless you pass --reclaim.
#
# EXPIRING SNAPSHOTS IS NOT REVERSIBLE: it drops the catalog history that
# time-travel queries read. --older-than sets how much history is kept
# (default 7 days).
#
# Why not READ_ONLY for the report: ducklake_delete_orphaned_files fails with
# "Cannot append to a readonly database" even at dry_run := true (measured,
# 1.5.4), while the other two work read-only. Attaching read-only would silently
# drop orphan reporting, so the attach is writable and `dry_run` is what makes
# the default run inert. Do not add READ_ONLY without re-checking step 3.
#
# `cleanup_all := true` is deliberately NOT offered. It bypasses the mtime
# filter, and register_files MOVES a file into the lake dir before its catalog
# transaction commits — so a concurrent load's just-placed file is
# indistinguishable from an orphan while that window is open. The mtime filter
# is what keeps this from deleting a load in flight. `older_than` is always
# passed explicitly rather than relying on the extension's default, so the
# retention this script applies does not move under an upstream default change.
#
# Usage:
#   bash scripts/lake-gc.sh                          # report only
#   bash scripts/lake-gc.sh --reclaim                # remove what it reports
#   bash scripts/lake-gc.sh --older-than '30 DAYS'   # keep 30 days of history
#
# Run as the account that owns the lake data path (qiita-data on the deploy
# host); it is a non-login account with no PATH and no home, so:
#   sudo -u qiita-data /usr/local/bin/duckdb --version   # confirm reachable
#   sudo -u qiita-data bash scripts/lake-gc.sh
#
# Needs, all established at first deploy:
#   * /etc/qiita/data-plane.env   0440 root:qiita-data
#   * PATH_PERSISTENT/ducklake    0750 qiita-data   (WRITE access, unlike lake-shell.sh)
#
# The catalog password never reaches the SQL file or argv (both readable via
# /proc): it goes to libpq through a 0600 PGPASSFILE deleted on exit.
#
# Env overrides:
#   DP_ENV                   data-plane env file (default /etc/qiita/data-plane.env)
#   QIITA_DUCKDB_BIN         duckdb CLI to run (default: `duckdb` on PATH)
#   QIITA_LAKE_THREADS       thread count (default 4)
#   QIITA_LAKE_MEMORY_LIMIT  memory limit (default 32GB)
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/.." && pwd )"

# Reach over to the deploy/ shared helpers for DP_ENV, read_env_var and
# qiita_split_conn_password (pure; no side effects on source) so this parses the
# env file exactly the way preflight.sh / verify.sh / lake-shell.sh do rather
# than growing a second, subtly-different parser.
# shellcheck source=../deploy/_common.sh
source "${REPO_ROOT}/deploy/_common.sh"

DRY_RUN=true
RETENTION="7 DAYS"
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            # Print the header comment block (everything after the shebang, up
            # to the first non-comment line) as the usage text — one copy, not two.
            awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "${BASH_SOURCE[0]}"
            exit 0
            ;;
        --reclaim) DRY_RUN=false ;;
        --older-than)
            shift
            [[ $# -gt 0 ]] || { echo "ERROR: --older-than needs a value, e.g. '30 DAYS'" >&2; exit 1; }
            RETENTION="$1"
            ;;
        *) echo "ERROR: unknown argument '$1' (see --help)" >&2; exit 1 ;;
    esac
    shift
done

# The retention is interpolated into an INTERVAL literal, so constrain it to the
# shape an INTERVAL takes rather than screening for metacharacters afterwards.
if [[ ! "${RETENTION}" =~ ^[1-9][0-9]*\ (MINUTE|HOUR|DAY|WEEK|MONTH)S?$ ]]; then
    echo "ERROR: --older-than must be '<n> <MINUTES|HOURS|DAYS|WEEKS|MONTHS>', got '${RETENTION}'" >&2
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

DUCKDB_BIN="${QIITA_DUCKDB_BIN:-$(command -v duckdb || true)}"
if [[ -z "${DUCKDB_BIN}" ]]; then
    cat >&2 <<'EOF'
ERROR: no duckdb CLI on PATH.

Install one (version 1.5.4, matching what the data plane links):

  mkdir -p ~/.local/bin && cd "$(mktemp -d)" \
    && curl -sSfL -O https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-linux-amd64.zip \
    && unzip -q duckdb_cli-linux-amd64.zip \
    && install -m 0755 duckdb ~/.local/bin/duckdb

Then re-run this script (or point QIITA_DUCKDB_BIN at the binary).
EOF
    exit 1
fi

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

# Byte-identical to the data plane's derivation — do NOT strip a trailing slash
# off PERSISTENT: DuckLake pins DATA_PATH into the catalog at creation and
# rejects an attach that differs by one.
DATA_PATH="${PERSISTENT}/ducklake"
[[ -d "${DATA_PATH}" ]] || { echo "ERROR: lake data path ${DATA_PATH} is not a directory" >&2; exit 1; }
if [[ ! -w "${DATA_PATH}" ]]; then
    echo "ERROR: ${DATA_PATH} is not writable by $(id -un)." >&2
    echo "  It is 0750 qiita-data; reclamation unlinks files there, so group read" >&2
    echo "  is not enough. Re-run as: sudo -u qiita-data bash scripts/lake-gc.sh" >&2
    exit 1
fi

SESSION_TMPDIR="${TMPDIR:-/tmp}"
LAKE_THREADS="${QIITA_LAKE_THREADS:-4}"
LAKE_MEMORY_LIMIT="${QIITA_LAKE_MEMORY_LIMIT:-32GB}"
if [[ ! "${LAKE_THREADS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: QIITA_LAKE_THREADS must be a positive integer, got '${LAKE_THREADS}'" >&2
    exit 1
fi
reject_sql_metacharacters "${DATA_PATH}" "the lake data path"
reject_sql_metacharacters "${SESSION_TMPDIR}" "TMPDIR"
reject_sql_metacharacters "${LAKE_MEMORY_LIMIT}" "QIITA_LAKE_MEMORY_LIMIT"

# Split the password out so it reaches libpq through PGPASSFILE instead of the
# SQL file or argv. Created at 0600 BEFORE anything is written — a redirection
# would otherwise create it at the umask's mode and only narrow it afterwards.
# Trap the signals a dropped session actually sends, not just EXIT.
CONN_SANITIZED=""; CONN_USER=""; CONN_PASSWORD=""
IFS=$'\t' read -r CONN_SANITIZED CONN_USER CONN_PASSWORD \
    < <(qiita_split_conn_password "${LAKE_CONNSTR}")
reject_sql_metacharacters "${CONN_SANITIZED}" "the lake catalog connection string"

TMPROOT="$(mktemp -d "${SESSION_TMPDIR%/}/qiita-lake-gc.XXXXXX")"
chmod 700 "${TMPROOT}"
trap 'rm -rf "${TMPROOT}"' EXIT INT TERM HUP
PGPASS_FILE="${TMPROOT}/pgpass"
: > "${PGPASS_FILE}"; chmod 600 "${PGPASS_FILE}"
if [[ -n "${CONN_PASSWORD}" ]]; then
    escaped_user="${CONN_USER//\\/\\\\}";     escaped_user="${escaped_user//:/\\:}"
    escaped_password="${CONN_PASSWORD//\\/\\\\}"; escaped_password="${escaped_password//:/\\:}"
    printf '*:*:*:%s:%s\n' "${escaped_user}" "${escaped_password}" >> "${PGPASS_FILE}"
fi

# Every duckdb invocation goes through here so the PGPASSFILE handling exists in
# exactly one place, and so each query gets the identical attach preamble.
# `-noheader -list` makes the output one bare path per line for the shell to
# count and size; `.bail on` turns an attach failure into a non-zero exit
# instead of an empty result that reads like "nothing to reclaim".
run_lake_sql() {
    local query="$1" sql="${TMPROOT}/q.sql"
    {
        echo ".bail on"
        echo "INSTALL ducklake; LOAD ducklake;"
        echo "INSTALL postgres; LOAD postgres;"
        # qiita-data's home is /dev/null, so INSTALL would resolve
        # $HOME/.duckdb/extensions and die with "Can't find the home directory".
        echo "SET home_directory='${SESSION_TMPDIR}';"
        echo "SET threads = ${LAKE_THREADS};"
        echo "SET memory_limit = '${LAKE_MEMORY_LIMIT}';"
        echo "SET temp_directory='${SESSION_TMPDIR}';"
        echo "ATTACH 'ducklake:postgres:${CONN_SANITIZED}' AS qiita_lake (DATA_PATH '${DATA_PATH}');"
        echo "${query}"
    } > "${sql}"
    if [[ -s "${PGPASS_FILE}" ]]; then
        PGPASSFILE="${PGPASS_FILE}" "${DUCKDB_BIN}" -noheader -list -f "${sql}"
    else
        "${DUCKDB_BIN}" -noheader -list -f "${sql}"
    fi
}

# Size a newline-separated file list. `stat` differs between GNU and BSD, so try
# both rather than assuming the deploy host's coreutils.
file_size() { stat -c%s "$1" 2>/dev/null || stat -f%z "$1" 2>/dev/null || echo 0; }

# Print one summary line for a newline-separated file list. In --reclaim mode the
# files are already unlinked by the time this runs, so only the count is exact;
# the byte total covers whatever still exists (all of it in report mode, none of
# it after a reclaim). Stated in the header line rather than silently differing.
report_paths() {
    local label="$1" paths="$2" n=0 bytes=0 sz
    while IFS= read -r p; do
        [[ -n "${p}" ]] || continue
        n=$((n + 1))
        if [[ -e "${p}" ]]; then sz="$(file_size "${p}")"; bytes=$((bytes + sz)); fi
    done <<< "${paths}"
    if [[ "${DRY_RUN}" == true ]]; then
        printf '  %-34s %6d file(s)  %s\n' "${label}" "${n}" \
            "$(awk -v b="${bytes}" 'BEGIN { printf "%.2f GB", b/1073741824 }')"
    else
        printf '  %-34s %6d file(s)  removed\n' "${label}" "${n}"
    fi
}

CUTOFF="now() - INTERVAL ${RETENTION}"
echo "lake-gc: ${DATA_PATH}"
echo "         catalog ${CONN_SANITIZED}"
echo "         retention: keeping snapshots newer than ${RETENTION}"
if [[ "${DRY_RUN}" == true ]]; then
    echo "         MODE: report only — nothing will be removed (pass --reclaim to act)"
else
    echo "         MODE: --reclaim — snapshots will be expired and files unlinked"
fi
echo

# 1. Snapshots. In report mode this is the set that WOULD be expired; because it
#    has not happened, step 2's report below can only see files already
#    unreferenced by a previous run, and so understates what a --reclaim would
#    free. That gap is inherent to reporting without mutating, not a defect.
snapshots="$(run_lake_sql \
    "SELECT * FROM ducklake_expire_snapshots('qiita_lake', dry_run := ${DRY_RUN}, older_than := ${CUTOFF});")"
snapshot_count="$(printf '%s' "${snapshots}" | grep -c . || true)"
printf '  %-34s %6d snapshot(s)\n' "snapshots older than retention" "${snapshot_count}"

# 2. Files no surviving snapshot references. Only meaningful after step 1 has
#    actually run, which is why --reclaim does both in one invocation.
old_files="$(run_lake_sql \
    "SELECT * FROM ducklake_cleanup_old_files('qiita_lake', dry_run := ${DRY_RUN}, older_than := ${CUTOFF});")"
report_paths "superseded data files" "${old_files}"

# 3. Files under the data path the catalog never references — a registration
#    whose catalog transaction rolled back after its file was already moved.
orphans="$(run_lake_sql \
    "SELECT * FROM ducklake_delete_orphaned_files('qiita_lake', dry_run := ${DRY_RUN}, older_than := ${CUTOFF});")"
report_paths "unreferenced orphan files" "${orphans}"

echo
if [[ "${DRY_RUN}" == true ]]; then
    echo "Nothing was removed. Re-run with --reclaim to expire and unlink the above."
    echo "The superseded-file count understates the result of a --reclaim run: those"
    echo "files stay referenced until the snapshots in step 1 are actually expired."
else
    echo "Reclaimed. Snapshots older than ${RETENTION} are gone; time-travel queries"
    echo "cannot reach them."
fi
