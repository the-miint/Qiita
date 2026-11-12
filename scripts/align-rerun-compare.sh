#!/bin/bash
# Check one batch of re-run align blocks against the lake snapshot taken before the
# rollout (docs/runbooks/align-block-rerun.md, step 5). Read-only: it runs one SQL
# batch through scripts/lake-shell.sh and prints one CSV row per ticket.
#
# Usage:
#   bash scripts/align-rerun-compare.sh <alignment_idx> <snapshot_id> <work_ticket_idx>...
#
# Columns, each per ticket over its block's footprint (the block_member ranges):
#   only_before, only_now  rows in one side and not the other (EXCEPT ALL), leaving
#                          out every read that had a `\0` in cigar or tag_md at the
#                          snapshot. Both must be 0.
#   now_rows               rows in the footprint now; must be > 0 (also 0 for a
#                          ticket that is not an align ticket of the alignment).
#   now_rows_new_file      of those, rows in a wt<idx>- file the snapshot did not
#                          have; must equal now_rows.
#   old_file_live_rows     rows left now in the ticket's files at the snapshot,
#                          inside the footprint or not; must be 0.
#   zeroed_at_snapshot     rows with prep_sample_idx = 0 in those files at the
#                          snapshot; when > 0 the runbook's zeroed-rows step applies.
#   gates_not_completed    the block's prep_samples whose gate is not `completed`;
#                          must be 0.
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [[ $# -lt 3 ]]; then
    echo "usage: $0 <alignment_idx> <snapshot_id> <work_ticket_idx>..." >&2
    exit 2
fi
for arg in "$@"; do
    # Every argument is inlined into the SQL below; only integers get there.
    if [[ ! "${arg}" =~ ^[0-9]+$ ]]; then
        echo "ERROR: arguments must be non-negative integers, got '${arg}'" >&2
        exit 2
    fi
done
ALIGNMENT_IDX="$1"
SNAPSHOT_ID="$2"
shift 2
TICKETS="$(IFS=,; echo "$*")"

# The snapshot id is inlined: `AT (VERSION => …)` takes no subquery (DuckDB 2.0.0).
# The wt<idx>-<hex>-alignment.parquet pattern is the lake filename that
# `lake_dest_filename` (qiita-data-plane registration.rs) gives a registered file.
bash "${SCRIPT_DIR}/lake-shell.sh" -csv <<SQL
CREATE TEMP TABLE fp AS
SELECT wt.work_ticket_idx, bm.block_idx, bm.prep_sample_idx,
       bm.min_sequence_idx AS lo, bm.max_sequence_idx AS hi
  FROM qiita_cp.qiita.work_ticket wt
  JOIN qiita_cp.qiita.block_member bm ON bm.block_idx = wt.block_idx
 WHERE wt.work_ticket_idx IN (${TICKETS}) AND wt.alignment_idx = ${ALIGNMENT_IDX};

CREATE TEMP TABLE before AS
SELECT fp.work_ticket_idx, a.*
  FROM qiita_lake.alignment AS a AT (VERSION => ${SNAPSHOT_ID})
  JOIN fp ON a.prep_sample_idx = fp.prep_sample_idx AND a.sequence_idx BETWEEN fp.lo AND fp.hi
 WHERE a.alignment_idx = ${ALIGNMENT_IDX};

CREATE TEMP TABLE now AS
SELECT fp.work_ticket_idx, a.*, a.filename AS lake_file
  FROM qiita_lake.alignment a
  JOIN fp ON a.prep_sample_idx = fp.prep_sample_idx AND a.sequence_idx BETWEEN fp.lo AND fp.hi
 WHERE a.alignment_idx = ${ALIGNMENT_IDX};

CREATE TEMP TABLE damaged AS
SELECT DISTINCT work_ticket_idx, sequence_idx FROM before
 WHERE contains(cigar, chr(0)) OR contains(tag_md, chr(0));

CREATE TEMP TABLE old_files AS
SELECT t.work_ticket_idx, a.filename AS lake_file,
       count(*) FILTER (WHERE a.prep_sample_idx = 0) AS zeroed
  FROM qiita_lake.alignment AS a AT (VERSION => ${SNAPSHOT_ID})
  JOIN (SELECT unnest([${TICKETS}]) AS work_ticket_idx) t
    ON regexp_matches(a.filename, '(^|/)wt' || t.work_ticket_idx || '-[0-9a-f]+-alignment\.parquet$')
 WHERE a.alignment_idx = ${ALIGNMENT_IDX}
 GROUP BY ALL;

CREATE TEMP TABLE diff AS
SELECT work_ticket_idx, 'only_before' AS side FROM (
  SELECT * FROM before EXCEPT ALL SELECT * EXCLUDE (lake_file) FROM now) d
 WHERE NOT EXISTS (SELECT 1 FROM damaged x
                    WHERE x.work_ticket_idx = d.work_ticket_idx AND x.sequence_idx = d.sequence_idx)
UNION ALL
SELECT work_ticket_idx, 'only_now' FROM (
  SELECT * EXCLUDE (lake_file) FROM now EXCEPT ALL SELECT * FROM before) d
 WHERE NOT EXISTS (SELECT 1 FROM damaged x
                    WHERE x.work_ticket_idx = d.work_ticket_idx AND x.sequence_idx = d.sequence_idx);

SELECT t.work_ticket_idx,
       (SELECT count(*) FROM diff d WHERE d.work_ticket_idx = t.work_ticket_idx
                                      AND d.side = 'only_before') AS only_before,
       (SELECT count(*) FROM diff d WHERE d.work_ticket_idx = t.work_ticket_idx
                                      AND d.side = 'only_now') AS only_now,
       (SELECT count(*) FROM now n WHERE n.work_ticket_idx = t.work_ticket_idx) AS now_rows,
       (SELECT count(*) FROM now n
         WHERE n.work_ticket_idx = t.work_ticket_idx
           AND regexp_matches(n.lake_file, '(^|/)wt' || t.work_ticket_idx || '-[0-9a-f]+-alignment\.parquet$')
           AND n.lake_file NOT IN (SELECT lake_file FROM old_files o
                                    WHERE o.work_ticket_idx = t.work_ticket_idx))
         AS now_rows_new_file,
       (SELECT count(*) FROM qiita_lake.alignment a
         WHERE a.alignment_idx = ${ALIGNMENT_IDX}
           AND a.filename IN (SELECT lake_file FROM old_files o
                               WHERE o.work_ticket_idx = t.work_ticket_idx))
         AS old_file_live_rows,
       (SELECT coalesce(sum(zeroed), 0) FROM old_files o
         WHERE o.work_ticket_idx = t.work_ticket_idx) AS zeroed_at_snapshot,
       (SELECT count(*) FROM fp
          LEFT JOIN qiita_cp.qiita.alignment_sample s
            ON s.alignment_idx = ${ALIGNMENT_IDX} AND s.prep_sample_idx = fp.prep_sample_idx
         WHERE fp.work_ticket_idx = t.work_ticket_idx
           AND s.state IS DISTINCT FROM 'completed') AS gates_not_completed
  FROM (SELECT unnest([${TICKETS}]) AS work_ticket_idx) t
 ORDER BY t.work_ticket_idx;
SQL
