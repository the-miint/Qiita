# krepp placement index: register, activate, remove

For the operator who brings a krepp index built outside Qiita into service on a
reference. What the states mean and what each check covers: [reference-data](../architecture/reference-data.md#krepp-placement-index-builds).

Throughout, reference `18` and build label `w43_r0-25` are the example. A build label is
one path segment (letters, digits, `.`, `_`, `-`); a rebuild gets a new label, never the
same folder overwritten.

## Before you start

- The reference is `active` and its phylogeny is in the lake, fully numbered. A tree
  loaded before the loader numbered trees needs
  `POST /api/v1/reference/18/phylogeny/mint-edge-id` first (registration refuses an
  unnumbered tree and says so).
- Optional: `PATH_NODE_LOCAL` in `compute-orchestrator.env` (e.g. `/scratch/qiita-miint`,
  created by the cluster admins on every node, writable by the job user): the node
  cache ([storage](../architecture/storage.md)). The deploy's readiness check fails
  when it is set but not a writable directory on the node the probe ran on. No workflow reads a krepp build
  through it.

## 1. Place the build at its master path

```
PATH_DERIVED/references/18/krepp/w43_r0-25/
  index/   the krepp index directory as krepp_index_create wrote it, all partitions
           of one index, nothing else (registration hashes whatever is there)
  jest/    labelled.nwk (the backbone tree with JEST's clade labels) and null.txt
           (its null distribution)
```

The SLURM job user must be able to write into the top-level folder (registration writes
two files there) and read everything.

## 2. Register

```
qiita ticket submit --action-id reference-index-register --action-version 1.0.0 \
  --scope-target-json '{"kind": "reference", "reference_idx": 18}' \
  --context-json '{"build_label": "w43_r0-25"}'
```

The job checks the folder and the trees, then hashes every file and
writes `manifest.json` and `SHA256SUMS`. The new row is `registered`, and is not served
until it is activated. Its row, from `GET /api/v1/reference/18/index` (`reference:read`),
has the `reference_index_idx` and, in `params.sha256sums_sha256`, the hash every copy is
checked against. Do not change the folder after this: a job that reads it refuses a
changed file list or size, and a job that copies it to a node hashes every file.

## 3. Activate

```
qiita ticket submit --action-id reference-index-activate --action-version 1.0.0 \
  --scope-target-json '{"kind": "reference", "reference_idx": 18}' \
  --context-json '{"reference_index_idx": <idx>}'
```

The build becomes `active`; the reference's previous active build, if any, becomes
`retired`; a ticket fixed to a retired build may still read it. Nothing is copied
to the nodes.

## 4. Remove a build no longer needed

```
qiita-admin krepp-index remove --reference-idx 18 --reference-index-idx <idx>
```

Only a `registered` or `retired` build, and only when no ticket fixed to it is running
or still has a cancel's reap pending. Then delete its master folder if it will not be
reused. Node copies of a removed build are not deleted (#942). A finished ticket
fixed to a removed build cannot be restarted with `/run`; submit it again. A reference
cannot be deleted while it has a krepp build that is not removed.
