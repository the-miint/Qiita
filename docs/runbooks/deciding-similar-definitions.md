# Deciding similar definitions

For a system_admin asked whether two masks, two alignments or two processings should
be read as one. A `similar` decision puts the two definitions in one group. The
masked-read export, the SynDNA count export, `qiita feature-table build` and the
alignment cohort DoGet then read each prep_sample from the group member that it
resolves to (section 4). A `not_similar` decision keeps them apart. Nothing is re-run
or copied. Section 6 covers a separate task on the same definitions: correcting a
definition's settings after its mint.

Requires a PAT carrying the lifecycle scope of the kind you decide:
`mask_definition:lifecycle`, `alignment_definition:lifecycle` or
`processing:lifecycle` (system_admin). Every command in sections 1 to 5 is
`qiita-admin pair …`; each prints the pair as JSON on stdout and a one-line summary on
stderr.

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
`change` maps each param path that differs (a JSON Pointer, such as `/rype` or
`/qc/min_length`) to `[low value, high value]`; that is what
you are deciding on. Two definitions in different comparison scopes (`cross_scope`)
or with the same params (`no_difference`) cannot be paired.

A pair with `derived_only: true` changes only upstream references; it can be decided
`not_similar` only. So can two masks that count SynDNA against different references,
or of which only one counts it (`syndna_reference_change`): a group's SynDNA count
table holds counts against one reference.

## 2. Decide

```bash
qiita-admin pair list --verdict pending --all    # newest first; --kind to narrow
qiita-admin pair decide 7 --verdict similar --reason "rype 0.05 vs 0.06; same host filter otherwise"
```

`--reason` is required and kept in the pair's history (`qiita-admin pair show 7`).
Re-deciding a decided pair replaces its verdict, and the earlier one stays in the
history. No second approval is asked for.

For an alignment or processing pair whose change swaps upstream definitions, decide
the upstream pair first: `similar` is refused until those upstreams are in one group
(`upstream_not_grouped`).

A 503 with `Retry-After` means another decision, revoke or deprecation was in
progress; nothing changed, so run the same command again.

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
  Cancelled tickets are ignored. A failed ticket is also ignored when the
  prep_sample's gate under that member is completed (for an assembly, also
  `no_data`) or withdrawn and was changed
  after the ticket was created: withdrawing a run, re-running it (which fails,
  because a withdrawn gate does not reopen) and restoring the gate leaves the
  restored run processed. Nothing re-runs a prep_sample automatically.

  A failed ticket that still makes a prep_sample incomplete, over a run you have
  checked and want used, can be cancelled by a system_admin; the resolver then
  ignores it:

  ```bash
  qiita-admin ticket cancel <work_ticket_idx>
  ```

  If its output shows a `reap_error`, the scancel failed, or if the command was
  interrupted, the ticket stays marked: `/run` refuses it until the same command, run
  again, reaps without error.
- **not processed**: no member has a run, or the run was withdrawn, or the
  definition or an upstream is deprecated or withdrawn. An older member's run is not
  used instead.

A deprecated mask named by the masked-read export or the SynDNA count export is
refused unless `--include-deprecated` is passed; with it, each prep_sample is read
from that mask alone (its own runs, never another member's; a withdrawn run stays
unreadable), and the output is marked deprecated. The same holds for a deprecated
alignment named by `qiita feature-table build` (`--alignment-idx` or
`--denovo-alignment-idx`) or the alignment cohort DoGet: with the flag it is read
alone, and a prep_sample whose mask run is withdrawn is still refused when named; over
the build's default cohort it is left out instead, with any whose run there failed or
is running, and the manifest records each by biosample accession. The flag also reads
a de novo arm whose assembly run is deprecated. An active alignment whose mask (or
assembly run) is deprecated is refused unless `--include-deprecated-upstream` is
passed; with it the alignment's group is read as usual, and the manifest names each
deprecated upstream by `params_hash` and reason.

The exports list incomplete and not-processed prep_samples and leave them out, and
so does `qiita feature-table build` over its default cohort (it also records them in
the manifest under `table.left_out`); a build over prep_samples named with
`--prep-sample-idx`, and the cohort DoGet, refuse them.

Each output records the decisions it was grouped under (`decisions` in the
feature-table manifest, in each masked-read export manifest entry and in the SynDNA
`<output>.metadata.json`). To rebuild it after a decision has changed, pass its
`decisions.decision_set` back:

```bash
qiita feature-table build ... --as-of-event sha256:<hex>
```

Only the pair decisions are taken from then: runs, gates, tickets and deprecation are
read as they are now, so a run withdrawn since is still left out. A token that names no
decision set is refused (422). `decision_set.py` states what the token hashes, and
which pairs `decisions.pairs_used` counts (the `similar` pairs that shaped the groups
read).

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

No route deletes a mask, alignment or processing. A hand `DELETE` in the database is
refused (SQLSTATE 55006, naming the pair idxs) while a `similar` pair names the
definition: revoke the pair first. Its other pairs (`pending`, `not_similar`, or
revoked to `pending`) are left `detached`: they keep their ids and history, link
nothing, and can no longer be decided or revoked.

## 6. Correct a definition's settings

A mask's, alignment's or processing's params (and params_hash, and a mask's or a
processing's workflow and version columns) are locked once it is minted: a hand
`UPDATE` of them is refused (SQLSTATE 55000). A system_admin with the kind's lifecycle
scope corrects them with:

```bash
qiita-admin definition edit-settings mask 12 --params-file mask-12.json \
    --reason "rype threshold was minted as 0.5, the run used 0.05"
```

`mask-12.json` holds the whole new params; params_hash is recomputed from it. The edit
is recorded in `qiita.definition_settings_edit` with the old and new values, who made it
and the reason. It is refused while a `similar` pair names the definition (revoke it
first), when another definition already has those params, and when it changes nothing.
The `change` of each `pending` or `not_similar` pair naming the definition is recomputed
from the new params; their verdicts stay. A manifest or export built before the edit
cites the old params_hash.
