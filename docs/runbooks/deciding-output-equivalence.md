# Deciding output equivalence

For a system_admin asked whether two masks, two alignments or two processings give
equivalent outputs and should be read as one. An output equivalence records one verdict
on two definitions of one kind:

| Verdict | Meaning | Joins the group |
|---|---|---|
| `pending` | No decision yet: wait for one before reading the two as one. Set on open and on revoke. | no |
| `same` | The outputs are the same. | yes |
| `similar` | The outputs are similar. | yes |
| `different` | The outputs are not equivalent. | no |
| `not_benchmarked` | Treated as `different`: not benchmarked because reprocessing is easier, and the older one is outdated. Nothing else happens; the older one is not deprecated. | no |

`same` and `similar` behave identically everywhere (grouping, every refusal, what the
exports read); only the stored verdict tells them apart. `different` and
`not_benchmarked` behave identically too. Below, "joining" means `same` or `similar`
and "non-joining" means `different` or `not_benchmarked`.

A joining verdict puts the two definitions in one group. The masked-read export, the
SynDNA count export, `qiita feature-table build` and the alignment cohort DoGet then
read each prep_sample from the group member that it resolves to (section 4). A
non-joining verdict keeps them apart. Nothing is re-run or copied. Section 6 covers a
separate task on the same definitions: correcting a definition's settings after its
mint.

Requires a PAT carrying the lifecycle scope of the kind you decide:
`mask_definition:lifecycle`, `alignment_definition:lifecycle` or
`processing:lifecycle` (system_admin). Every command in sections 1 to 5 is
`qiita-admin output-equivalence …`; each prints the output equivalence as JSON on
stdout and a one-line summary on stderr.

The group rules and every refusal code, with what each one means, are in the module
docstring of `qiita_control_plane/definition_similarity.py`; the per-prep_sample
states are in `qiita_control_plane/definition_resolver.py`. A refusal is a 409 whose
`reason` is one of those codes and whose `output_equivalence_idx` lists the output
equivalences involved.

## 1. Open an output equivalence

```bash
qiita-admin output-equivalence open mask 5 12   # or: alignment A B, processing P Q
```

Order does not matter. Opening one that already exists returns it. The response's
`change` maps each param path that differs (a JSON Pointer, such as `/rype` or
`/qc/min_length`) to `[low value, high value]`; that is what you are deciding on. Two
definitions in different comparison scopes (`cross_scope`) or with the same params
(`no_difference`) cannot be compared.

An output equivalence with `derived_only: true` changes only upstream references; it
can be decided non-joining only. So can two masks that count SynDNA against different
references, or of which only one counts it (`syndna_reference_change`): a group's
SynDNA count table holds counts against one reference.

## 2. Decide

```bash
qiita-admin output-equivalence list --verdict pending --all   # newest first; --kind to narrow
qiita-admin output-equivalence decide 7 --verdict similar \
    --reason "rype 0.05 vs 0.06; same host filter otherwise"
```

`--verdict` is one of `same`, `similar`, `different` or `not_benchmarked`. `--reason`
is required and kept in the history (`qiita-admin output-equivalence show 7`).
Re-deciding a decided output equivalence replaces its verdict, and the earlier one
stays in the history. No second approval is asked for.

For an alignment or processing output equivalence whose change swaps upstream
definitions, decide the upstream one first: a joining verdict is refused until those
upstreams are in one group (`upstream_not_grouped`).

A 503 with `Retry-After` means another decision, revoke or deprecation was in
progress; nothing changed, so run the same command again.

## 3. What a group contains

- **Masks**: masks joined by joining output equivalences, transitively (5 ~ 12 and
  12 ~ 19 puts 5, 12 and 19 in one group).
- **Processings, then alignments**: definitions whose params differ only in upstream
  references that are in one group are grouped without a decision (an alignment on
  mask 5 and the same reference and aligner on mask 12, once 5 ~ 12), plus explicit
  joining output equivalences. A non-joining one takes both of its sides out of those
  derived links.

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
deprecated upstream by `params_hash` and reason. Over the build's default cohort a
prep_sample whose mask run is withdrawn is then left out and recorded, as under
`--include-deprecated`; a named one is refused. A deprecated alignment read alone
under `--include-deprecated` names its deprecated upstreams in the manifest too.

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

Only the output-equivalence verdicts are taken from then: runs, gates, tickets and
deprecation are read as they are now, so a run withdrawn since is still left out. A
token that names no decision set is refused (422). `decision_set.py` states what the
token hashes (each verdict as stored, so a `same` and a `similar` decision give
different tokens), and which output equivalences `decisions.equivalences_used` counts
(the joining ones that shaped the groups read).

## 5. Revoke, or deprecate a decided definition

```bash
qiita-admin output-equivalence revoke 7 --reason "lima version differs after all"
```

Revoke returns the output equivalence to `pending` and records the reason.

Reconsider downstream output equivalences first. A revoke or a non-joining
re-decision is refused (`relied_on`) while a joining alignment or processing output
equivalence relies on the two upstreams being grouped, chains included (mask →
processing → de novo alignment): revoke the ones that the refusal names, then this one.

Deprecating a mask, alignment or processing (`qiita-admin alignment deprecate`,
`PATCH /mask-definition/{idx}/status`, `PATCH /processing/{idx}/status`) is refused
(`joining_equivalences`) while it is a side of a joining output equivalence. Revoke it
or re-decide it non-joining, then deprecate. `not_benchmarked` does not deprecate
anything itself.

No route deletes a mask, alignment or processing. A hand `DELETE` in the database is
refused (SQLSTATE 55006, naming the output-equivalence idxs) while a joining output
equivalence names the definition: revoke it first. Its other output equivalences
(`pending`, non-joining, or revoked to `pending`) are left `detached`: they keep their
ids and history, link nothing, and can no longer be decided or revoked.

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
and the reason. It is refused while a joining output equivalence names the definition
(revoke it first), while unfinished work tickets run under it (wait, or cancel them),
while a public handle that is not retired cites it (a `QP<n>` naming it, or a `QM<n>`
minted under it: a published output names the settings it was built under), when
another definition already has those params, and when it changes nothing.
The `change` of each `pending` or non-joining output equivalence naming the definition
is recomputed from the new params; their verdicts stay.
