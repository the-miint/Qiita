#!/bin/bash
# Report — and, with --reclaim, remove — lake data files nothing references.
#
# DuckLake never reclaims a data file on its own. Deleting rows leaves the file
# on disk and still referenced by the snapshots that predate the delete, so
# every superseded load accumulates:
#
#   * `register_files` replace-by-key supersedes rows in every table listed in
#     REPLACE_KEY_TABLES (flight_service.rs) and leaves the previous load's
#     Parquet behind;
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
# For an operator reclaiming space on the deploy host. Reports by default;
# nothing is removed unless you pass --reclaim, which then prompts for a typed
# confirmation. Running it is an operator decision, not a deploy step.
#
# EXPIRING SNAPSHOTS IS NOT REVERSIBLE: it drops the catalog history that
# time-travel queries read. --older-than sets how much history is kept (default
# 7 days) and is the same cutoff the two file-deleting steps filter on.
#
# Why not READ_ONLY for the report: ducklake_delete_orphaned_files fails with
# "Cannot append to a readonly database" even at dry_run := true (measured,
# 1.5.4), while the other two work read-only. Attaching read-only would silently
# drop orphan reporting, so the attach is writable and `dry_run` is what makes
# the default run inert. Do not add READ_ONLY without re-checking step 3.
#
# QUIESCE REGISTRATIONS BEFORE --reclaim; the cutoff does not do it for you.
# `older_than` filters on filesystem mtime (measured, 1.5.4: of two
# byte-identical unreferenced files, only the one stamped 30 days back was listed
# under a 7-day cutoff). register_files places a file with std::fs::rename, which
# carries over the mtime the producing job gave it in staging — so a lake file's
# mtime is when it was PRODUCED, not when it entered the lake. A Parquet that sat
# in staging longer than the cutoff is eligible the instant it is moved, during
# the window before its catalog transaction commits, when it has no catalog entry
# and reads as an orphan. That window is common on a redrive, whose staging files
# can be weeks old. No `older_than` value closes it: run this when nothing is
# registering.
#
# `cleanup_all := true` is not offered: it drops the mtime filter outright, so a
# reclaim would sweep files produced inside the cutoff too. `older_than` is always
# passed explicitly rather than relying on the extension's default, so what this
# script destroys does not move under an upstream default change.
#
# It reclaims; it does not COMPACT. Merging many small Parquets into fewer
# (ducklake_merge_adjacent_files) is a separate operation this does not perform.
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

DUCKDB_BIN="${QIITA_DUCKDB_BIN:-$(command -v duckdb || true)}"
if [[ -z "${DUCKDB_BIN}" ]]; then
    cat >&2 <<'EOF'
ERROR: no duckdb CLI on PATH.

Install one, matching what the data plane links. The run-as account has no home,
so install it somewhere that account can reach:

  cd "$(mktemp -d)" \
    && curl -sSfL -O https://github.com/duckdb/duckdb/releases/download/v1.5.4/duckdb_cli-linux-amd64.zip \
    && unzip -q duckdb_cli-linux-amd64.zip \
    && sudo install -m 0755 duckdb /usr/local/bin/duckdb

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

DATA_PATH="$(qiita_lake_data_path "${PERSISTENT}")"
[[ -d "${DATA_PATH}" ]] || { echo "ERROR: lake data path ${DATA_PATH} is not a directory" >&2; exit 1; }
if [[ ! -r "${DATA_PATH}" || ! -x "${DATA_PATH}" ]]; then
    echo "ERROR: cannot read ${DATA_PATH}" >&2
    echo "  It is 0750 qiita-data — you must be in the owning group." >&2
    echo "  Check with: ls -ld ${DATA_PATH} && id" >&2
    exit 1
fi
if [[ ! -w "${DATA_PATH}" ]]; then
    echo "ERROR: ${DATA_PATH} is not writable by $(id -un)." >&2
    echo "  It is 0750 qiita-data. Group read is not enough even to REPORT: the" >&2
    echo "  dry-run orphan scan fails on a read-only attach, so DuckLake needs to" >&2
    echo "  write here in both modes." >&2
    echo "  Re-run as: sudo -u qiita-data bash scripts/lake-gc.sh" >&2
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

# Separates the three result sets in one invocation's output. `.print` is a
# client-side dot command, so it emits between result sets without joining the
# transaction.
MARK="@@lake-gc@@"

# All three steps in one duckdb process, across TWO transactions: steps 1-2
# together, step 3 on its own. Measured on 1.5.4 for both halves — cleanup_old_files
# sees step 1's expiry from inside the same transaction, and step 3's catalog read
# is the snapshot from its BEGIN, so it must open after 1-2 finish. See the
# per-step comments below.
#
# What a transaction does NOT cover: the unlinking. A file removed by step 2 or 3
# stays removed even if the transaction then rolls back (measured — ROLLBACK left
# the file gone). A transaction bounds the CATALOG changes only.
#
# `CALL` is the documented invocation form for these table functions
# (https://ducklake.select/docs/stable/duckdb/maintenance/expire_snapshots).
# `SELECT * FROM` behaves identically here (measured, same rows, same deletions),
# but the docs are what the next reader will check this against.
#
# `.bail on` turns an attach failure into a non-zero exit instead of an empty
# result that reads like "nothing to reclaim".
#
# Measured output shapes under `-noheader -list` (1.5.4): cleanup_old_files and
# delete_orphaned_files return one bare path per line, which report_paths counts
# and sizes; expire_snapshots returns a 7-column snapshot row, which is only
# line-counted here. A shape change in the first two would surface as a 0.00 GB
# total, so they are what to re-check on a version bump.
run_maintenance() {
    local sql="${TMPROOT}/gc.sql"
    {
        echo ".bail on"
        # FIRST: INSTALL resolves against $HOME/.duckdb, and this runs as
        # qiita-data, whose home is /dev/null ("Can't find the home directory").
        # Redirect before anything can install.
        echo "SET home_directory='${SESSION_TMPDIR}';"
        # ducklake, plus postgres for the catalog backend — `ducklake:postgres:`
        # below needs it. It would autoload, but autoINSTALL is the part that
        # would reach for a home this account does not have, so both are named.
        # No miint: this never reads a sequence (unlike lake-shell.sh), so it
        # also never switches extension_directory to the staged build.
        echo "INSTALL ducklake; LOAD ducklake;"
        echo "INSTALL postgres; LOAD postgres;"
        echo "SET threads = ${LAKE_THREADS};"
        echo "SET memory_limit = '${LAKE_MEMORY_LIMIT}';"
        echo "SET temp_directory='${SESSION_TMPDIR}';"
        echo "ATTACH 'ducklake:postgres:${CONN_SANITIZED}' AS qiita_lake (DATA_PATH '${DATA_PATH}');"
        # Steps 1-2 share a transaction: cleanup only sees the expiry from inside
        # it, and the two are one catalog outcome.
        echo "BEGIN TRANSACTION;"
        echo ".print ${MARK}snapshots"
        echo "CALL ducklake_expire_snapshots('qiita_lake', dry_run := ${DRY_RUN}, older_than := ${CUTOFF});"
        echo ".print ${MARK}old_files"
        echo "CALL ducklake_cleanup_old_files('qiita_lake', dry_run := ${DRY_RUN}, older_than := ${CUTOFF});"
        echo "COMMIT;"
        # Step 3 takes its OWN transaction, and must. Measured on 1.5.4: an orphan
        # scan run inside a transaction that opened earlier reports a file another
        # session registered and COMMITTED in the meantime — its catalog read is
        # the snapshot from that BEGIN. At dry_run := false that unlinks a live
        # file whose catalog row survives. Re-opening takes the read after the
        # work above, which the same probe showed reports nothing. Keep this
        # COMMIT/BEGIN pair; folding step 3 upward reintroduces the window.
        echo "BEGIN TRANSACTION;"
        echo ".print ${MARK}orphans"
        echo "CALL ducklake_delete_orphaned_files('qiita_lake', dry_run := ${DRY_RUN}, older_than := ${CUTOFF});"
        echo "COMMIT;"
    } > "${sql}"
    if [[ -s "${PGPASS_FILE}" ]]; then
        PGPASSFILE="${PGPASS_FILE}" "${DUCKDB_BIN}" -noheader -list -f "${sql}"
    else
        "${DUCKDB_BIN}" -noheader -list -f "${sql}"
    fi
}

# One section of run_maintenance's output, by marker. $1 = section name.
section() {
    printf '%s\n' "${MAINTENANCE_OUT}" |
        awk -v start="${MARK}$1" -v mark="${MARK}" '
            $0 == start { on = 1; next }
            on && index($0, mark) == 1 { on = 0 }
            on { print }'
}

# `stat` differs between GNU and BSD, so try both. Echoes nothing when neither
# works; the caller counts that rather than adding a zero, because the byte total
# is what the report is for and a silent undercount reads as "less to reclaim".
file_size() { stat -c%s "$1" 2>/dev/null || stat -f%z "$1" 2>/dev/null; }

# One summary line per file list. Sizes are only taken in report mode — after a
# --reclaim the files are already unlinked, so the count is the only exact figure.
report_paths() {
    local label="$1" paths="$2" n=0 bytes=0 unmeasured=0 sz
    while IFS= read -r p; do
        [[ -n "${p}" ]] || continue
        n=$((n + 1))
        [[ "${DRY_RUN}" == true ]] || continue
        if sz="$(file_size "${p}")" && [[ -n "${sz}" ]]; then
            bytes=$((bytes + sz))
        else
            unmeasured=$((unmeasured + 1))
        fi
    done <<< "${paths}"
    if [[ "${DRY_RUN}" != true ]]; then
        printf '  %-34s %6d file(s)  removed\n' "${label}" "${n}"
        return
    fi
    local human
    human="$(awk -v b="${bytes}" 'BEGIN { printf "%.2f GB", b/1073741824 }')"
    if [[ "${unmeasured}" -gt 0 ]]; then
        printf '  %-34s %6d file(s)  %s (+%d unmeasured)\n' "${label}" "${n}" "${human}" "${unmeasured}"
    else
        printf '  %-34s %6d file(s)  %s\n' "${label}" "${n}" "${human}"
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

# Typed confirmation, matching deploy/redeploy.sh's RUN_MIGRATE gate: the only
# other place in this repo where one keystroke starts something that cannot be
# undone. ASSUME_YES=1 skips it for automation.
if [[ "${DRY_RUN}" != true && -z "${ASSUME_YES:-}" ]]; then
    echo "Snapshots older than ${RETENTION} will be dropped; time-travel queries cannot"
    echo "reach them afterwards. A registration in flight whose staging file predates"
    echo "the cutoff can be swept with them — see the header. Quiesce first."
    # `read` returns non-zero at EOF, which under `set -e` would kill the script
    # with a bare exit 1 and no reason — so the EOF case is handled here rather
    # than left to the `[[ ]]` below, which would never run.
    if ! read -r -p "Type 'reclaim' to proceed: " reply || [[ "${reply}" != "reclaim" ]]; then
        echo "Aborted — nothing was expired or unlinked." >&2
        exit 1
    fi
    echo
fi

MAINTENANCE_OUT="$(run_maintenance)"

# 1. Snapshots. In report mode this is the set that WOULD be expired; because it
#    has not happened, step 2 below can only see files already unreferenced by an
#    earlier run, and so understates what a --reclaim would free. That gap is
#    inherent to reporting without mutating, not a defect.
snapshot_count="$(section snapshots | grep -c . || true)"
printf '  %-34s %6d snapshot(s)\n' "snapshots older than retention" "${snapshot_count}"

# 2. Files no surviving snapshot references.
report_paths "superseded data files" "$(section old_files)"

# 3. Files under the data path the catalog never references — a registration
#    whose catalog transaction rolled back after its file was already moved.
report_paths "unreferenced orphan files" "$(section orphans)"

echo
if [[ "${DRY_RUN}" == true ]]; then
    echo "Nothing was removed. Re-run with --reclaim to expire and unlink the above."
    echo "The superseded-file count understates the result of a --reclaim run: those"
    echo "files stay referenced until the snapshots in step 1 are actually expired."
else
    echo "Reclaimed. Snapshots older than ${RETENTION} are gone; time-travel queries"
    echo "cannot reach them."
fi
