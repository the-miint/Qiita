# krepp placement index: register, copy, activate, remove

For the operator who brings a krepp index built outside Qiita into service on a
reference, and for the cluster admins who copy it to the compute nodes. What the
states mean and what each check covers: [reference-data](../architecture/reference-data.md#krepp-placement-index-builds).

Throughout, reference `18` and build label `w43_r0-25` are the example. A build label is
one path segment (letters, digits, `.`, `_`, `-`); a rebuild gets a new label, never the
same folder overwritten.

## Before you start

- The reference is `active` and its phylogeny is in the lake, fully numbered. A tree
  loaded before the loader numbered trees needs
  `POST /api/v1/reference/18/phylogeny/mint-edge-id` first (registration refuses an
  unnumbered tree and says so).
- The build was written by the krepp the staged miint links. Registration compares the
  `krepp version` in the build's `metadata-*.txt` with the `krepp-version` row of
  `miint_versions()` and refuses a mismatch.
- `PATH_NODE_LOCAL` is set in `compute-orchestrator.env` (e.g. `/scratch/qiita-miint`);
  the orchestrator does not start on SLURM without it.

## 1. Place the build at its master path

```
PATH_DERIVED/references/18/krepp/w43_r0-25/
  index/   the krepp index directory as krepp_index_create wrote it, all partitions
           of one index, nothing else (registration hashes whatever is there)
  jest/    labelled.nwk (the backbone tree with JEST's clade labels) and null.txt
           (its null distribution)
```

The SLURM job user must be able to write into the top-level folder (registration writes
four files there), and the cluster admins' copy must be able to read everything.

## 2. Register

```
qiita ticket submit --action-id reference-index-register --action-version 1.0.0 \
  --scope-target-json '{"kind": "reference", "reference_idx": 18}' \
  --context-json '{"build_label": "w43_r0-25"}'
```

The job checks the folder, the krepp version and the trees, then hashes every file and
writes `README.md`, `manifest.json`, `verify.sh` and `SHA256SUMS`. The new row is
`registered`, and is not served until it is activated. Its row, from `GET /api/v1/reference/18/index`
(`reference:read`), has the `reference_index_idx` and, in `params.sha256sums_sha256`,
the hash to give the cluster admins.

## 3. Copy to every node (cluster admins)

Copy the whole folder, as it is after registration, to
`PATH_NODE_LOCAL/references/18/krepp/w43_r0-25/` on every compute node, then on each
node run, in that folder:

```
./verify.sh <params.sha256sums_sha256>
```

It prints `verify.sh: ok`, or names the file that does not match or should not be
there. Report back when every node is done. Copy into a new folder only; never over a
build folder that is in use.

A node added or reimaged later gets the same copy and `verify.sh`. Until it does, an
activation job that lands there fails, and the readiness probe's `krepp-index` row
warns when it runs there.

## 4. Activate

First measure, without activating, what share of backbone tips land on their own edge
with this build:

```
qiita ticket submit --action-id reference-index-activate --action-version 1.0.0 \
  --scope-target-json '{"kind": "reference", "reference_idx": 18}' \
  --context-json '{"reference_index_idx": <idx>, "min_own_edge_share": 0, "activate": false}'
qiita ticket logs <ticket> --step-index 1      # krepp_index_activate: {... "own_edge_share": ...}
```

Then activate with a `min_own_edge_share` below that share:

```
qiita ticket submit --action-id reference-index-activate --action-version 1.0.0 \
  --scope-target-json '{"kind": "reference", "reference_idx": 18}' \
  --context-json '{"reference_index_idx": <idx>, "min_own_edge_share": <share>}'
```

The build becomes `active`; the reference's previous active build, if any, becomes
`retired` and is still served to the tickets already fixed to it. The result is kept in
the build's `params.activation`.

## 5. Remove a build no longer needed

```
qiita-admin krepp-index remove --reference-idx 18 --reference-index-idx <idx>
```

Only a `registered` or `retired` build, and only when no ticket fixed to it is running.
Then delete its master folder and ask the cluster admins to delete the node copies. A
finished ticket fixed to a removed build cannot be restarted with `/run`; submit it
again. A reference cannot be deleted while it has a krepp build that is not removed.
