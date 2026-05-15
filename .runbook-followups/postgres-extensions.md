# Followup: runbook step 0.2 should list required Postgres extensions

Current state: step 0.2 mentions the qiita_miint database + qiita_miint_rw
role need to exist, but doesn't enumerate Postgres extensions migrations
require. First deploy hit:
    Error: pq: extension "citext" is not available (0A000)

Required (as of this session):
- citext

Action items for the runbook:
1. Add an "Extensions" subsection under 0.2 listing required extensions
   (currently just citext; grep the migrations dir to keep this list
   current: `grep -irh '^CREATE EXTENSION' qiita-control-plane/db/migrations/`)
2. Add the DBA-superuser CREATE EXTENSION command to the sysadmin
   request template
3. Add a step-0 verification: `psql ... -c "SELECT 'a'::citext = 'A'::citext"`
   to confirm extensions are enabled before make migrate

Apply on branch feat/local-deploy-and-runbook once the in-progress
merge on feat/fastq-to-parquet is resolved.
