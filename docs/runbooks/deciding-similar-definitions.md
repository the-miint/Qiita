# Deciding similar definitions

For a system_admin asked whether two masks, two alignments or two processings should
be read as one. A `similar` decision puts the two definitions in one group, and the
masked-read export, the SynDNA count export, `qiita feature-table build` and the
alignment cohort DoGet then read each sample from the group member it resolves to
(below). A `not_similar` decision keeps them apart. Nothing is re-run or copied.

Requires a PAT carrying the lifecycle scope of the kind you decide:
`mask_definition:lifecycle`, `alignment_definition:lifecycle` or
`processing:lifecycle` (system_admin). Every command below is `qiita-admin pair …`;
each prints the pair as JSON on stdout and a one-line summary on stderr.

The group rules and every refusal code are in the module docstring of
`qiita_control_plane/definition_similarity.py`; the per-sample states are in
`qiita_control_plane/definition_resolver.py`. This runbook says how to use them.

## 1. Open a pair

```bash
qiita-admin pair open mask 5 12        # or: alignment A B, processing P Q
```

Order does not matter. Opening a pair that already exists returns it. The response's
`change` maps each param path that differs to `[low value, high value]`; that is what
you are deciding on. A pair is refused (409, `reason`) when the two definitions are
in different comparison scopes (`cross_scope`: for masks, another filter workflow;
for block alignments, another reference or aligner) or have the same params
(`no_difference`).

`derived_only: true` means every changed path is an upstream reference (a `mask_idx`,
or a de novo alignment's `processing_idx`). Such a pair is grouped by derivation once
its upstreams are grouped (section 3), so it can only be decided `not_similar`.

## 2. Decide

```bash
qiita-admin pair list --verdict pending          # oldest first; --kind to narrow
qiita-admin pair decide 7 --verdict similar --reason "rype 0.05 vs 0.06; same host filter otherwise"
```

`--reason` is required and kept in the pair's history (`qiita-admin pair show 7`).
Re-deciding a decided pair replaces its verdict; the earlier one stays in the
history. One admin's decision is final.

`similar` is refused when:
- either definition is deprecated (`deprecated_side`). `not_similar` is still allowed;
- the change includes the reference shard set (`shard_ids_change`);
- the pair is `derived_only`;
- for an alignment or processing, the upstreams it changes are not in one group yet
  (`upstream_not_grouped`): decide the mask pair first.

Any decision is refused when it would put both sides of an existing `not_similar`
pair into one group (`contradiction`). The 409 names the pairs involved.

A 503 with `Retry-After` means another decision held the group lock; nothing changed,
so run the same command again.

## 3. What a group contains

- **Masks**: masks joined by `similar` pairs, transitively (5 ~ 12 and 12 ~ 19 puts
  5, 12 and 19 in one group).
- **Processings, then alignments**: two definitions whose params differ only in
  upstream references that are in one group are grouped without a decision (an
  alignment on mask 5 and the same reference and aligner on mask 12, once 5 ~ 12),
  plus explicit `similar` pairs. A `not_similar` pair takes both of its sides out of
  those derived links.

Deprecating a definition does not take it out of its group.

## 4. What each sample resolves to

For a sample, the group member used is the highest-id member with a run for that
sample. Its state is one of:

- **processed**: the run completed, its definition and its upstreams are active,
  and its upstream runs are the ones the sample resolves to. Its results are used,
  labelled with that member.
- **incomplete**: a ticket for that member is waiting, running or failed
  (`in_progress`, `failed`), or the sample's upstream now resolves to another member
  (`upstream_moved`: the sample needs re-running on that upstream) or is itself
  incomplete. Cancelled tickets are ignored. Nothing re-runs a sample on its own.
- **not processed**: no member has a run (`no_run`), the run was withdrawn
  (`withdrawn`), or the definition or an upstream is deprecated or withdrawn
  (`deprecated`, `upstream_*`). An older member's run is not used instead.

The exports list incomplete and not-processed samples and leave them out;
`qiita feature-table build` and the cohort DoGet refuse them.

## 5. Revoke, or deprecate a decided definition

```bash
qiita-admin pair revoke 7 --reason "lima version differs after all"
```

Revoke returns the pair to `pending` and records the reason; a pending pair cannot
be revoked (`not_decided`).

Reconsider downstream first. A revoke or a `not_similar` re-decision is refused
(`relied_on`) while an alignment or processing `similar` pair relies on the two
upstreams being grouped, chains included (mask → processing → de novo alignment).
The 409 names those pairs: revoke them first, then this one.

Deprecating a mask, alignment or processing (`qiita-admin alignment deprecate`,
`PATCH /mask-definition/{idx}/status`, `PATCH /processing/{idx}/status`) is refused
(`similar_pairs`) while it is a side of a `similar` pair. Revoke the pair or
re-decide it `not_similar`, then deprecate. A definition grouped only by derivation
is not refused.

A definition deleted by hand in the database leaves its pairs `detached`: they keep
their ids and history, link nothing, and can no longer be decided or revoked.
