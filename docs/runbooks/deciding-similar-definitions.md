# Deciding similar definitions

For a system_admin asked whether two masks, two alignments or two processings should
be read as one. A `similar` decision puts the two definitions in one group. The
masked-read export, the SynDNA count export, `qiita feature-table build` and the
alignment cohort DoGet then read each prep_sample from the group member that it
resolves to (section 4). A `not_similar` decision keeps them apart. Nothing is re-run
or copied.

Requires a PAT carrying the lifecycle scope of the kind you decide:
`mask_definition:lifecycle`, `alignment_definition:lifecycle` or
`processing:lifecycle` (system_admin). Every command below is `qiita-admin pair …`;
each prints the pair as JSON on stdout and a one-line summary on stderr.

The group rules and every refusal code, with what each one means, are in the module
docstring of `qiita_control_plane/definition_similarity.py`; the per-prep_sample
states are in `qiita_control_plane/definition_resolver.py`. A refusal is a 409 whose
`reason` is one of those codes and whose `definition_pair_idx` lists the pairs
involved.

## 1. Open a pair

```bash
qiita-admin pair open mask 5 12        # or: alignment A B, processing P Q
```

Order does not matter. Opening a pair that already exists returns it. The response's
`change` maps each param path that differs to `[low value, high value]`; that is what
you are deciding on. Two definitions in different comparison scopes (`cross_scope`)
or with the same params (`no_difference`) cannot be paired.

A pair with `derived_only: true` changes only upstream references; it can be decided
`not_similar` only.

## 2. Decide

```bash
qiita-admin pair list --verdict pending          # oldest first; --kind to narrow
qiita-admin pair decide 7 --verdict similar --reason "rype 0.05 vs 0.06; same host filter otherwise"
```

`--reason` is required and kept in the pair's history (`qiita-admin pair show 7`).
Re-deciding a decided pair replaces its verdict, and the earlier one stays in the
history. No second approval is asked for.

For an alignment or processing pair whose change swaps upstream definitions, decide
the upstream pair first: `similar` is refused until those upstreams are in one group
(`upstream_not_grouped`).

A 503 with `Retry-After` means another decision or a deprecation was in progress;
nothing changed, so run the same command again.

## 3. What a group contains

- **Masks**: masks joined by `similar` pairs, transitively (5 ~ 12 and 12 ~ 19 puts
  5, 12 and 19 in one group).
- **Processings, then alignments**: definitions whose params differ only in upstream
  references that are in one group are grouped without a decision (an alignment on
  mask 5 and the same reference and aligner on mask 12, once 5 ~ 12), plus explicit
  `similar` pairs. A `not_similar` pair takes both of its sides out of those derived
  links.

Deprecating a definition does not take it out of its group.

## 4. What each prep_sample resolves to

For each prep_sample, the group member used is the highest-id member that has a run
for that prep_sample. Its state is one of:

- **processed**: the run completed, its definition and its upstreams are active, and
  each upstream run is the one that the prep_sample resolves to in the upstream's
  group. Its results are used, labeled with that member.
- **incomplete**: a ticket for that member is waiting, running or failed, or the
  prep_sample's upstream now resolves to another member or is itself incomplete.
  Cancelled tickets are ignored. Nothing re-runs a prep_sample automatically.
- **not processed**: no member has a run, or the run was withdrawn, or the
  definition or an upstream is deprecated or withdrawn. An older member's run is not
  used instead.

The exports list incomplete and not-processed prep_samples and leave them out;
`qiita feature-table build` and the cohort DoGet refuse them.

## 5. Revoke, or deprecate a decided definition

```bash
qiita-admin pair revoke 7 --reason "lima version differs after all"
```

Revoke returns the pair to `pending` and records the reason.

Reconsider downstream pairs first. A revoke or a `not_similar` re-decision is refused
(`relied_on`) while an alignment or processing `similar` pair relies on the two
upstreams being grouped, chains included (mask → processing → de novo alignment):
revoke the pairs that the refusal names, then this one.

Deprecating a mask, alignment or processing (`qiita-admin alignment deprecate`,
`PATCH /mask-definition/{idx}/status`, `PATCH /processing/{idx}/status`) is refused
(`similar_pairs`) while it is a side of a `similar` pair. Revoke the pair or
re-decide it `not_similar`, then deprecate.

A definition deleted by hand in the database leaves its pairs `detached`: they keep
their ids and history, link nothing, and can no longer be decided or revoked.
