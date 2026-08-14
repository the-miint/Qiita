#!/bin/bash
# Lake integrity report for the content-addressed sequence tables, and the
# collapse that repairs what it finds.
#
# `register_files` replaces these tables on `feature_idx` rather than appending
# (`REPLACE_KEY_TABLES` in qiita-data-plane/src/flight_service.rs, which carries
# why). Rows written BEFORE that can hold N copies of one feature, in which case
# `string_agg(chunk_data, '' ORDER BY chunk_index)` returns the sequence
# concatenated with itself while `sequence_length_bp` still describes one copy.
# The two table pairs it covers, and this script with them:
#   assembled_sequence   / assembled_sequence_chunks
#   reference_sequences  / reference_sequence_chunks
#
# REPORT BY DEFAULT — it attaches READ_ONLY and changes nothing. `APPLY=1`
# collapses. Report mode is re-runnable at any time, so this stays useful after
# the one-off repair: a non-zero count is then a signal that something regressed.
#
# It collapses only features whose copies are BYTE-IDENTICAL — `SELECT DISTINCT`
# over the duplicated features, so no copy is picked over another. A feature whose
# copies DIFFER is reported and left alone: the canonical hash keeps a sequence
# and its reverse complement (and its case variants) on one `feature_idx`, so two
# copies can legitimately hold different bytes, and no column records which chunk
# came from which load — a per-`chunk_index` pick could therefore splice two
# strands into one sequence. Resolve those by re-running the producing load (it
# now replaces on the key) or by deleting the feature's rows deliberately.
#
# APPLY validates before it commits: the post-collapse duplicate and
# reassembled-length counts run inside the transaction and `error()` out of it,
# so a collapse that did not converge rolls back rather than reporting a lake
# that is already wrong. Each table pair is its own transaction — a failure on
# the second leaves the first collapsed — and every mode is safe to re-run.
#
# Writes, so unlike scripts/lake-shell.sh it needs an account that can write the
# lake data path (DuckLake writes new Parquet for the delete + insert):
#
#   sudo -u qiita-data bash scripts/dedup-lake-sequence-tables.sh           # report
#   sudo -u qiita-data APPLY=1 bash scripts/dedup-lake-sequence-tables.sh   # collapse
#
# Reads, all established at first deploy:
#   * /etc/qiita/data-plane.env   0440 root:qiita-data
#   * PATH_PERSISTENT/ducklake    0750 qiita-data  (read-write in APPLY mode)
#
# The catalog password never reaches the SQL file or argv (world-readable via
# /proc): it goes to libpq through a 0600 PGPASSFILE deleted on exit.
#
# Env overrides:
#   APPLY                    1 to collapse; anything else reports only
#   DP_ENV                   data-plane env file (default /etc/qiita/data-plane.env)
#   QIITA_DUCKDB_BIN         duckdb CLI to run (default: `duckdb` on PATH)
#   QIITA_LAKE_THREADS       thread count (default 4)
#   QIITA_LAKE_MEMORY_LIMIT  memory limit (default 32GB)
#   TMPDIR                   where the run's 0700 workdir goes; DuckDB spills its
#                            sort/DISTINCT there, so point it at a filesystem with
#                            room for the duplicated features' chunk bytes
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/.." && pwd )"

# deploy/_common.sh for DP_ENV, read_env_var, qiita_split_conn_password and the
# DuckLake session helpers (pure; no side effects on source), so this script
# resolves the CLI, derives DATA_PATH and validates SQL literals exactly the way
# scripts/lake-shell.sh does.
# shellcheck source=../deploy/_common.sh
source "${REPO_ROOT}/deploy/_common.sh"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            # Print the header comment block (everything after the shebang, up
            # to the first non-comment line) as the usage text — one copy, not two.
            awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "${BASH_SOURCE[0]}"
            exit 0
            ;;
        *)
            echo "ERROR: unrecognized argument '$1'. See --help." >&2
            exit 1
            ;;
    esac
done

APPLY="${APPLY:-0}"

DUCKDB_BIN="$(qiita_duckdb_bin "${QIITA_DUCKDB_BIN:-}")" || exit 1

if [[ ! -r "${DP_ENV}" ]]; then
    echo "ERROR: cannot read ${DP_ENV}" >&2
    echo "  It is mode 0440 root:qiita-data — you must be in the owning group." >&2
    exit 1
fi

LAKE_CONNSTR="$(read_env_var "${DP_ENV}" DUCKLAKE_CATALOG_CONNSTR)"
[[ -n "${LAKE_CONNSTR}" ]] || { echo "ERROR: DUCKLAKE_CATALOG_CONNSTR is unset in ${DP_ENV}" >&2; exit 1; }
DATA_PATH="$(qiita_lake_data_path "${DP_ENV}")" || exit 1

if [[ "${APPLY}" == "1" ]]; then
    # Root passes any -w test, and is the one account that must not do this: the
    # Parquet DuckLake writes would land root-owned in a dir the data plane owns.
    if [[ "${EUID}" -eq 0 ]]; then
        echo "ERROR: refusing to collapse as root — the new Parquet would be root-owned" >&2
        echo "  in a ${DATA_PATH} the data plane owns. Run: sudo -u qiita-data APPLY=1 bash $0" >&2
        exit 1
    fi
    if [[ ! -w "${DATA_PATH}" ]]; then
        echo "ERROR: ${DATA_PATH} is not writable by $(id -un)." >&2
        echo "  DuckLake writes new Parquet for the delete + insert, so APPLY=1 needs the" >&2
        echo "  owning account: sudo -u qiita-data APPLY=1 bash $0" >&2
        exit 1
    fi
fi

CONN_SANITIZED=""
CONN_USER=""
CONN_PASSWORD=""
IFS=$'\t' read -r CONN_SANITIZED CONN_USER CONN_PASSWORD \
    < <(qiita_split_conn_password "${LAKE_CONNSTR}")
LAKE_CONNSTR="${CONN_SANITIZED}"
qiita_reject_sql_metacharacters "${LAKE_CONNSTR}" "the lake catalog connection string" || exit 1
qiita_reject_sql_metacharacters "${DATA_PATH}" "the lake data path" || exit 1

# Both temp files can hold a credential, so create them at 0600 BEFORE anything
# is written — a redirection would otherwise create them at the umask's mode and
# only be narrowed afterwards. Trap the signals a dropped session actually sends,
# not just EXIT, so a pgpass file never outlives the run.
TMPROOT="$(mktemp -d "${TMPDIR:-/tmp}/qiita-dedup-lake.XXXXXX")"
chmod 700 "${TMPROOT}"
trap 'rm -rf "${TMPROOT}"' EXIT INT TERM HUP
WORK_SQL="${TMPROOT}/dedup.sql"
PGPASS_FILE="${TMPROOT}/pgpass"
: > "${WORK_SQL}"; chmod 600 "${WORK_SQL}"
: > "${PGPASS_FILE}"; chmod 600 "${PGPASS_FILE}"
[[ -z "${CONN_PASSWORD}" ]] || qiita_pgpass_line "${CONN_USER}" "${CONN_PASSWORD}" >> "${PGPASS_FILE}"

SESSION_LIMITS="$(qiita_duckdb_session_limits \
    "${QIITA_LAKE_THREADS:-4}" "${QIITA_LAKE_MEMORY_LIMIT:-32GB}" "${TMPROOT}")" || exit 1

# READ_ONLY in report mode is a DuckDB-level guard against a typo in this script
# mutating the lake on a run the operator asked only to look.
ATTACH_OPTS="DATA_PATH '${DATA_PATH}'"
[[ "${APPLY}" == "1" ]] || ATTACH_OPTS="${ATTACH_OPTS}, READ_ONLY"

# Emit the report — and, under APPLY=1, the collapse — for one table pair.
# `seq_table` / `chunk_table` are this script's own literals, never operator
# input, and must stay in step with REPLACE_KEY_TABLES (pinned by
# test_deploy_scripts.py::test_dedup_lake_covers_every_replace_key_table).
emit_pair_sql() {
    local seq_table="$1" chunk_table="$2"
    cat <<SQL

.print
.print ===== ${seq_table} / ${chunk_table} =====

-- Every feature the lake holds more than one copy of: more than one sequence
-- row, or more than one row at some chunk_index.
CREATE OR REPLACE TEMP TABLE dup_feature AS
SELECT feature_idx FROM (
    SELECT feature_idx FROM qiita_lake.${seq_table}
     GROUP BY feature_idx HAVING count(*) > 1
    UNION
    SELECT feature_idx FROM qiita_lake.${chunk_table}
     GROUP BY feature_idx, chunk_index HAVING count(*) > 1
);

-- Of those, the ones whose copies are not byte-identical (see the header).
CREATE OR REPLACE TEMP TABLE ambiguous_feature AS
SELECT feature_idx FROM qiita_lake.${seq_table}
 SEMI JOIN dup_feature USING (feature_idx)
 GROUP BY feature_idx
HAVING count(DISTINCT sequence_hash) > 1 OR count(DISTINCT sequence_length_bp) > 1
UNION
SELECT feature_idx FROM qiita_lake.${chunk_table}
 SEMI JOIN dup_feature USING (feature_idx)
 GROUP BY feature_idx, chunk_index
HAVING count(DISTINCT chunk_data) > 1;

DELETE FROM dup_feature WHERE feature_idx IN (SELECT feature_idx FROM ambiguous_feature);

SELECT
    (SELECT count(*) FROM qiita_lake.${seq_table})            AS sequence_rows,
    (SELECT count(DISTINCT feature_idx) FROM qiita_lake.${seq_table}) AS distinct_features,
    (SELECT count(*) FROM dup_feature)                        AS collapsible_features,
    (SELECT count(*) FROM ambiguous_feature)                  AS ambiguous_features;

SELECT feature_idx AS ambiguous_feature_idx FROM ambiguous_feature ORDER BY 1 LIMIT 50;
SQL

    [[ "${APPLY}" == "1" ]] || return 0

    cat <<SQL

BEGIN TRANSACTION;
-- DISTINCT, not DISTINCT ON: every copy of a collapsible feature is byte-identical
-- (that is what put it in dup_feature rather than ambiguous_feature), so this
-- picks nothing — it deduplicates, and cannot splice if that classification is
-- ever wrong. It hashes chunk_data, but only for the duplicated features, and
-- spills to the session temp_directory under pressure.
CREATE OR REPLACE TEMP TABLE keep_sequence AS
  SELECT DISTINCT * FROM qiita_lake.${seq_table} SEMI JOIN dup_feature USING (feature_idx);
CREATE OR REPLACE TEMP TABLE keep_chunk AS
  SELECT DISTINCT * FROM qiita_lake.${chunk_table} SEMI JOIN dup_feature USING (feature_idx);
DELETE FROM qiita_lake.${seq_table} WHERE feature_idx IN (SELECT feature_idx FROM dup_feature);
-- ORDER BY on the way back in: the load path sorts its parts by (feature_idx,
-- chunk_index) so a feature_idx DoGet prunes row groups within a file and whole
-- files across the table. An unordered re-insert would land the collapsed
-- features in one file spanning the entire key range, costing that pruning for
-- every later point query, not just theirs.
INSERT INTO qiita_lake.${seq_table} SELECT * FROM keep_sequence ORDER BY feature_idx;
DELETE FROM qiita_lake.${chunk_table} WHERE feature_idx IN (SELECT feature_idx FROM dup_feature);
INSERT INTO qiita_lake.${chunk_table}
  SELECT * FROM keep_chunk ORDER BY feature_idx, chunk_index;

-- Validate BEFORE the commit, and throw rather than print: a collapse that did
-- not converge must roll back, not report a lake that is already wrong. Scoped
-- to the collapsed set, so the reassembly is over those features only.
SELECT CASE WHEN duplicated_sequences + duplicated_chunks + length_mismatches > 0
            THEN error(format(
                '${seq_table}/${chunk_table} collapse did not converge: '
                || '{} duplicated sequence rows, {} duplicated chunk rows, '
                || '{} features whose reassembly disagrees with sequence_length_bp',
                duplicated_sequences, duplicated_chunks, length_mismatches))
            ELSE 'converged' END AS collapse
FROM (SELECT
    (SELECT count(*) FROM (
        SELECT feature_idx FROM qiita_lake.${seq_table}
         SEMI JOIN dup_feature USING (feature_idx)
         GROUP BY feature_idx HAVING count(*) > 1))                AS duplicated_sequences,
    (SELECT count(*) FROM (
        SELECT feature_idx FROM qiita_lake.${chunk_table}
         SEMI JOIN dup_feature USING (feature_idx)
         GROUP BY feature_idx, chunk_index HAVING count(*) > 1))   AS duplicated_chunks,
    -- Length, not the canonical hash: re-deriving that needs miint
    -- (qiita_common.chunking.canonical_sequence_hash_expr), which this script
    -- does not load.
    (SELECT count(*) FROM (
        SELECT s.feature_idx FROM qiita_lake.${seq_table} s
          JOIN qiita_lake.${chunk_table} c USING (feature_idx)
         SEMI JOIN dup_feature d ON d.feature_idx = s.feature_idx
         GROUP BY s.feature_idx, s.sequence_length_bp
        HAVING length(string_agg(c.chunk_data, '' ORDER BY c.chunk_index))
               <> s.sequence_length_bp))                           AS length_mismatches);
COMMIT;
SQL
}

{
    # .bail on so an attach failure, or the convergence error() above, exits
    # non-zero instead of running the remaining statements against a half-done
    # lake. (Verified: `duckdb -f` with a leading `.bail on` stops at the first
    # error and exits 1.)
    echo ".bail on"
    # DuckDB resolves INSTALL against $HOME/.duckdb, and the service accounts are
    # non-login with HOME=/dev/null — which fails with `Can't find the home
    # directory`. Point it at this run's own 0700 tmproot so the script works
    # under whichever account owns the lake.
    echo "SET home_directory='${TMPROOT}';"
    echo "INSTALL ducklake; LOAD ducklake;"
    echo "INSTALL postgres; LOAD postgres;"
    echo "${SESSION_LIMITS}"
    echo "ATTACH 'ducklake:postgres:${LAKE_CONNSTR}' AS qiita_lake (${ATTACH_OPTS});"
    emit_pair_sql assembled_sequence assembled_sequence_chunks
    emit_pair_sql reference_sequences reference_sequence_chunks
} > "${WORK_SQL}"

if [[ "${APPLY}" == "1" ]]; then
    echo "APPLY=1 — collapsing duplicated features in ${DATA_PATH}" >&2
else
    echo "Report only (no writes). Re-run with APPLY=1 to collapse." >&2
fi

if [[ -s "${PGPASS_FILE}" ]]; then
    PGPASSFILE="${PGPASS_FILE}" "${DUCKDB_BIN}" -f "${WORK_SQL}"
else
    "${DUCKDB_BIN}" -f "${WORK_SQL}"
fi
