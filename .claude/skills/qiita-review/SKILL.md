---
name: qiita-review
description: Run the qiita-reviewer agent to convergence on the current branch — review, fix, re-review — with a disposition ledger and a stop rule. Use whenever a review of pending changes is asked for: "qiita-reviewer", "qiita-style review", "review this branch", "review my changes", "review before I open the PR". Owns the loop only; the rules live in the agent.
---

# Reviewing a branch to convergence

One review pass is not a review. The first pass finds what reading the diff finds;
the fixes then change the diff, and the next pass reads what the fixes wrote. This
skill owns that loop. The rules, the trigger map, and the report format live in
`.claude/agents/qiita-reviewer.md` — do not restate them here or in your prompts.

**You are the caller.** The reviewer is read-only by design (no `Edit`/`Write` in its
toolset) and cannot run tests or probes. Everything it hands back that requires
*doing* — applying a fix, running `make test`, building a conda env for a
`[needs-probe]` — is yours.

## Every round gets its own reviewer

**Spawn a fresh `qiita-reviewer` for each round.** A round is a verdict on the code as
it stands now, and a reviewer that already concluded something about a span does not
re-derive it — it remembers. Remembering is the failure mode: it clears code it
cleared before the fix rewrote it, and it defends its earlier findings instead of
re-testing them against what is there. The check that a fix worked has to come from a
reader that did not watch it being made.

Independence is the default; you steer through the **prompt**, not through the agent's
memory. What you carry across rounds is the ledger, not the reviewer.

**`SendMessage` is for steering a reviewer within its own round**, where its context is
the point — it continues the same agent, which still holds the diff it read and the
spans it opened. Use it to:

- hand back probe results it asked for, so it re-tags `[needs-probe]` → `[verified]`
  or drops the finding;
- ask what a finding means, or which of two readings of a rule it applied;
- narrow it mid-flight ("skip R2 on the vendored file, it isn't ours").

Never use it to start a new round. If the code changed, that is a new reviewer.

## The loop

**Round 1.**

```
Agent(subagent_type: "qiita-reviewer",
      prompt: "Review the current branch against <base>. <anything repo-specific>")
```

**Between rounds — disposition, then fix.**

Give every finding exactly one disposition, and write it into the ledger:

| Disposition | Means | Next |
|---|---|---|
| `fixed` | You changed the code | Name the commit or `path:line` |
| `declined` | Correct rule, wrong here | Record the **reason** — it goes into the next prompt |
| `deferred` | Real, but wider than this PR (the reviewer's Scope discipline) | File or draft the issue, then drop it from the loop |
| `probed` | Was `[needs-probe]`; you ran it | Record the result — confirmed or refuted |

Then apply the fixes and run the tests the change actually touches: always the
pure-unit tier (`make test`), plus `make test-integration` when the diff crosses a
process boundary. A round that ships unfixed test failures into the next review
wastes it.

**Round N+1 — a new agent, carrying the ledger in its prompt.** Three things, and
nothing else:

1. **Scope** — the paths that changed since the last round, and any rule family
   already settled. This is what keeps a fresh spawn from costing a full-branch read.
2. **The standing ledger** — every `declined` and `deferred` finding with its reason.
   This is what stops round 3 from re-raising what round 2 settled, and it is a token
   cut as much as a convergence one.
3. **The ask** — review the current state of the named paths, confirm each `fixed`
   finding is actually fixed, and flag anything the fixes introduced.

State in the prompt that the ledger is the *caller's* disposition, not a finding the
reviewer made — it did not make these calls and should not treat them as its own.

## When to stop

**There is no round cap. Stopping is your judgment, and you make the call.** Stop when
any of these holds, and say which one in your summary:

- **Nothing new.** Every finding in this round is one already in the ledger.
- **Nothing actionable.** The new findings are all ones you decline, on reasons already
  in the ledger — the reviewer is re-litigating a class you have settled.
- **Churn.** The round's findings are mostly about code the previous round's fixes
  wrote, and the fixes were right. Two passes disagreeing about a rewrite is a signal
  to stop and let a human read it, not to write a third version.
- **What's left isn't a fix.** Everything open is `deferred` (an issue) or
  `needs-probe` (run the probe, or file it — do not spend a round on it).
- **Diminishing returns.** The findings are real but minor, and each round is
  returning less than the last. Say so plainly and stop; you do not need a rule's
  permission.

A stop is not a pass. End with what remains open and why, so the human reviewer
inherits the ledger rather than re-deriving it.

## Cost discipline

The reviewer has its own, in the agent file. Yours:

- **Scope the prompt — this is the lever.** A fresh reviewer is only expensive if you
  let it re-read the branch. Round 2+ names the changed paths and the rules still in
  play, so it reads a slice. Independence costs one load of the agent file; it does not
  have to cost a second full-branch pass.
- **The ledger is a token cut, not just bookkeeping.** A decline the reviewer can see
  is a decline it does not re-derive.
- **Keep each reviewer short-lived.** A reviewer carried across rounds accumulates every
  diff and span it has read. Per-round spawns keep each one's context to a single round.
- **Don't fan out from here.** The reviewer decides its own fan-out. A second layer of
  subagents on top duplicates its work.

## Report each round

Keep it to a table plus the stop decision. The user is watching convergence, not
re-reading findings:

```
Round 2 — 7 findings: 3 fixed, 2 declined (carve-out, scope), 1 deferred (#N), 1 probed (refuted)
Round 3 — 2 findings: both repeats of declined. Stopping: nothing actionable.
Open: 1 deferred (#N), 0 needs-probe.
```

## Always end with the deferred list, in prose

**When you stop, the last thing you write is a plain-language list of everything left
open** — every `deferred` finding, every unrun `needs-probe`, and anything you
declined that a human might reasonably re-decide. This is not optional and it is not
the per-round table above: the table tracks convergence, this hands over the work.

The reader is deciding what to do next, so write it for someone who has not read the
diff:

- **Group by what the reader would do about it** — cleanups that are somebody's next
  PR, probes that need an environment you lacked, and things only they can settle (an
  assay call, a product decision, filing an issue you have no access to file).
- **One line each: what it is, where, and why it is still open.** Name the file, not
  the finding number — `R9-3` means nothing tomorrow. "Wider than this PR" and
  "needs the deploy host" are reasons; "deferred" is not.
- **Say what a probe would settle**, not just that it is unrun.
- **Separate what is pre-existing from what this branch introduced.** A reviewer
  inheriting the list needs to know which items the PR is answerable for.

The reviewer owes you the raw material for all three — a reason and pre-existing-or-not
are on its **Scope discipline**, a refutation target on its **Verification handoff**. What is yours is the shape:
grouping, brevity, and writing it for someone who has not read the diff.

Keep it short — a few grouped lines, not a report. If nothing is open, say that in one
sentence rather than omitting the section, so the reader can tell the difference
between a clean stop and a forgotten one.

## Then write the PR-body block

A PR records that this loop ran and what it left open. `review-loop-check` in CI fails
a PR whose description lacks the block (the `no-agent-review` label opts out; see
`CLAUDE.md`, "Reviewing a branch"). After the prose list, print the block filled in from
the ledger, ready to paste under the PR template's heading:

```markdown
## Reviewer loop

- Reviewed at: <short sha of the last commit a round read> · rounds: <N> · stopped: <which stop rule>
- Fixed: <count>
- Declined:
  - <what, `path`> — <why>
- Deferred:
  - <what, `path`> — #<issue>
- Not probed:
  - <the question> — <why it was not run>
```

- `Reviewed at` is a commit on the branch. If commits land after the last round, say so
  on that line ("plus 1 later commit: changelog tag") instead of moving the sha.
- Write `none` under a heading with no entries; do not drop the heading.
- A deferred item names its issue. File it first, or write `issue to file` and why.
- Do not edit the PR description yourself unless asked; hand the block to the author.
