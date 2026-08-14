#!/bin/bash
# ONE-OFF MAINTENANCE — collapse duplicated rows in the content-addressed
# DuckLake sequence tables.
#
# Rows written BEFORE register-files started replacing these tables on
# `feature_idx` can hold N copies of one feature, in which case
# `string_agg(chunk_data, '' ORDER BY chunk_index)` returns the sequence
# concatenated with itself while `sequence_length_bp` still describes one copy.
# Why they duplicate, and which tables: `REPLACE_KEY_TABLES` in
# qiita-data-plane/src/flight_service.rs. The two pairs it covers, and this
# script with them, are:
#   assembled_sequence   / assembled_sequence_chunks
#   reference_sequences  / reference_sequence_chunks
#
# REPORT BY DEFAULT. It prints what it would collapse and changes nothing.
# Pass APPLY=1 to perform the collapse.
#
# It collapses only features whose copies are BYTE-IDENTICAL — `SELECT DISTINCT`
# over the duplicated features, so no copy is picked over another. A feature whose
# copies DIFFER is reported and left alone: the canonical hash keeps a sequence
# and its reverse complement on one `feature_idx`, so two copies can legitimately
# hold different bytes, and no column records which chunk came from which load —
# so a per-chunk_index pick could splice two strands into one sequence. Resolve
# those by re-running the producing load (it now replaces on the key) or by
# deleting the feature's rows deliberately.
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
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/.." && pwd )"

# deploy/_common.sh for DP_ENV, read_env_var and qiita_split_conn_password (pure;
# no side effects on source), so this parses the env file and lifts the password
# exactly the way preflight.sh / verify.sh / lake-shell.sh do.
# shellcheck source=../deploy/_common.sh
source "${REPO_ROOT}/deploy/_common.sh"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    # Print the header comment block as the usage text — one copy, not two.
    awk 'NR>1 && /^#/ { sub(/^# ?/, ""); print; next } NR>1 { exit }' "${BASH_SOURCE[0]}"
    exit 0
fi

APPLY="${APPLY:-0}"

DUCKDB_BIN="${QIITA_DUCKDB_BIN:-$(command -v duckdb || true)}"
if [[ -z "${DUCKDB_BIN}" ]]; then
    echo "ERROR: no duckdb CLI on PATH. See scripts/lake-shell.sh for install steps," >&2
    echo "  or point QIITA_DUCKDB_BIN at a binary." >&2
    exit 1
fi

# ATTACH takes no bind parameters, so the connection string and data path are
# interpolated into SQL. Reject the same characters
# qiita-data-plane/src/ducklake.rs validate_sql_literal does — input validation,
# not sanitization.
reject_sql_metacharacters() {
    local value="$1" label="$2"
    case "${value}" in
        *\'*|*\;*)
            echo "ERROR: ${label} contains a quote or semicolon; refusing to interpolate it into SQL." >&2
            exit 1
            ;;
    esac
}

if [[ ! -r "${DP_ENV}" ]]; then
    echo "ERROR: cannot read ${DP_ENV}" >&2
    echo "  It is mode 0440 root:qiita-data — you must be in the owning group." >&2
    exit 1
fi

LAKE_CONNSTR="$(read_env_var "${DP_ENV}" DUCKLAKE_CATALOG_CONNSTR)"
PERSISTENT="$(read_env_var "${DP_ENV}" PATH_PERSISTENT)"
[[ -n "${LAKE_CONNSTR}" ]] || { echo "ERROR: DUCKLAKE_CATALOG_CONNSTR is unset in ${DP_ENV}" >&2; exit 1; }
[[ -n "${PERSISTENT}" ]]   || { echo "ERROR: PATH_PERSISTENT is unset in ${DP_ENV}" >&2; exit 1; }

# Byte-identical to the data plane's derivation — config.rs does a bare
# format!("{path_persistent_raw}/ducklake") with NO normalization, and DuckLake
# pins that exact string into the catalog at creation, rejecting an attach whose
# DATA_PATH differs by even a slash.
DATA_PATH="${PERSISTENT}/ducklake"
if [[ ! -d "${DATA_PATH}" ]]; then
    echo "ERROR: lake data path ${DATA_PATH} is not a directory" >&2
    exit 1
fi
if [[ "${APPLY}" == "1" && ! -w "${DATA_PATH}" ]]; then
    echo "ERROR: ${DATA_PATH} is not writable by $(id -un)." >&2
    echo "  DuckLake writes new Parquet for the delete + insert, so APPLY=1 needs the" >&2
    echo "  owning account: sudo -u qiita-data APPLY=1 bash $0" >&2
    exit 1
fi

CONN_SANITIZED=""
CONN_USER=""
CONN_PASSWORD=""
IFS=$'\t' read -r CONN_SANITIZED CONN_USER CONN_PASSWORD \
    < <(qiita_split_conn_password "${LAKE_CONNSTR}")
LAKE_CONNSTR="${CONN_SANITIZED}"
reject_sql_metacharacters "${LAKE_CONNSTR}" "the lake catalog connection string"
reject_sql_metacharacters "${DATA_PATH}" "the lake data path"

TMPROOT="$(mktemp -d "${TMPDIR:-/tmp}/qiita-dedup-lake.XXXXXX")"
chmod 700 "${TMPROOT}"
trap 'rm -rf "${TMPROOT}"' EXIT INT TERM HUP
WORK_SQL="${TMPROOT}/dedup.sql"
PGPASS_FILE="${TMPROOT}/pgpass"
: > "${WORK_SQL}"; chmod 600 "${WORK_SQL}"
: > "${PGPASS_FILE}"; chmod 600 "${PGPASS_FILE}"
if [[ -n "${CONN_PASSWORD}" ]]; then
    escaped_user="${CONN_USER//\\/\\\\}"; escaped_user="${escaped_user//:/\\:}"
    escaped_password="${CONN_PASSWORD//\\/\\\\}"; escaped_password="${escaped_password//:/\\:}"
    printf '*:*:*:%s:%s\n' "${escaped_user}" "${escaped_password}" >> "${PGPASS_FILE}"
fi

SESSION_TMPDIR="${TMPDIR:-/tmp}"
reject_sql_metacharacters "${SESSION_TMPDIR}" "TMPDIR"
LAKE_THREADS="${QIITA_LAKE_THREADS:-4}"
LAKE_MEMORY_LIMIT="${QIITA_LAKE_MEMORY_LIMIT:-32GB}"
if [[ ! "${LAKE_THREADS}" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: QIITA_LAKE_THREADS must be a positive integer, got '${LAKE_THREADS}'" >&2
    exit 1
fi
reject_sql_metacharacters "${LAKE_MEMORY_LIMIT}" "QIITA_LAKE_MEMORY_LIMIT"

# READ_ONLY in report mode is a DuckDB-level guard against a typo in this script
# mutating the lake on a run the operator asked only to look.
ATTACH_OPTS="DATA_PATH '${DATA_PATH}'"
[[ "${APPLY}" == "1" ]] || ATTACH_OPTS="${ATTACH_OPTS}, READ_ONLY"

# Emit the per-pair report (and, under APPLY=1, the collapse) for one table pair.
# `seq_table` / `chunk_table` are this script's own literals, never operator input.
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

-- Of those, the ones whose copies are NOT byte-identical. Collapsing these would
-- have to pick a copy, and picking per chunk_index could splice two strands into
-- one sequence, so they are excluded and reported instead.
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
-- picks nothing — it deduplicates.
CREATE OR REPLACE TEMP TABLE keep_sequence AS
  SELECT DISTINCT * FROM qiita_lake.${seq_table} SEMI JOIN dup_feature USING (feature_idx);
CREATE OR REPLACE TEMP TABLE keep_chunk AS
  SELECT DISTINCT * FROM qiita_lake.${chunk_table} SEMI JOIN dup_feature USING (feature_idx);
DELETE FROM qiita_lake.${seq_table} WHERE feature_idx IN (SELECT feature_idx FROM dup_feature);
INSERT INTO qiita_lake.${seq_table} SELECT * FROM keep_sequence;
DELETE FROM qiita_lake.${chunk_table} WHERE feature_idx IN (SELECT feature_idx FROM dup_feature);
INSERT INTO qiita_lake.${chunk_table} SELECT * FROM keep_chunk;
COMMIT;

.print -- after the collapse (all three counts must be 0 for the collapsed set)
-- Length, not the canonical hash. Re-deriving the hash needs miint's
-- sequence_dna_reverse_complement (qiita_common.chunking.canonical_sequence_hash_expr),
-- and MIINT_EXTENSION_DIRECTORY is readable only from control-plane.env /
-- compute-orchestrator.env — 0440 to groups qiita-data (the account that can
-- write the lake, and so the account that runs this) is not in.
SELECT
    (SELECT count(*) FROM (
        SELECT feature_idx FROM qiita_lake.${seq_table}
         SEMI JOIN dup_feature USING (feature_idx)
         GROUP BY feature_idx HAVING count(*) > 1))              AS still_duplicated_sequences,
    (SELECT count(*) FROM (
        SELECT feature_idx FROM qiita_lake.${chunk_table}
         SEMI JOIN dup_feature USING (feature_idx)
         GROUP BY feature_idx, chunk_index HAVING count(*) > 1))  AS still_duplicated_chunks,
    (SELECT count(*) FROM (
        SELECT s.feature_idx FROM qiita_lake.${seq_table} s
          JOIN qiita_lake.${chunk_table} c USING (feature_idx)
         SEMI JOIN dup_feature d ON d.feature_idx = s.feature_idx
         GROUP BY s.feature_idx, s.sequence_length_bp
        HAVING length(string_agg(c.chunk_data, '' ORDER BY c.chunk_index))
               <> s.sequence_length_bp))                          AS length_mismatches;
SQL
}

{
    # .bail on so an attach failure or a mid-collapse error exits non-zero
    # instead of running the remaining statements against a half-done lake.
    echo ".bail on"
    echo "INSTALL ducklake; LOAD ducklake;"
    echo "INSTALL postgres; LOAD postgres;"
    echo "SET threads = ${LAKE_THREADS};"
    echo "SET memory_limit = '${LAKE_MEMORY_LIMIT}';"
    # An in-memory DB spills to ./.tmp by default, which fails from a read-only cwd.
    echo "SET temp_directory='${SESSION_TMPDIR}';"
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
