# Followup: shred needs sudo in step 1 (and 8b, 9a)

The runbook says `shred -u /tmp/<svc>.env` under [admin], but the file
is qiita-owned mode 0600 (qiita created it). admin can't write to it
directly, so shred fails:
    shred: /tmp/control-plane.env: failed to open for writing: Permission denied

Fix: `sudo shred -u /tmp/<svc>.env`. Apply to all three env-file
install blocks (steps 1, 8b, 9a).

Apply on branch feat/local-deploy-and-runbook once the in-progress
merge on feat/fastq-to-parquet is resolved.
