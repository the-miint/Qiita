# Followup: DATABASE_URL needs sslmode= for dbmate compatibility

dbmate uses Go's lib/pq driver, which defaults to sslmode=require. If
Postgres doesn't have SSL enabled, `make migrate` fails with:
    Error: pq: SSL is not enabled on the server

asyncpg (used by the CP service) defaults to sslmode=prefer and works
without the explicit setting, but every future operator running
`make migrate` would hit this same wall.

Fix: append `?sslmode=prefer` to the DATABASE_URL template in the
runbook's sed-substitute in step 1, plus matching note in the env
file example (.env.control-plane.example).

Update on branch feat/local-deploy-and-runbook once the in-progress
merge on feat/fastq-to-parquet is resolved.
