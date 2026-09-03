"""Work-ticket constants shared across the control plane, the CLI and the wire
models.

Dependency-free on purpose: `qiita_common.actions` imports the models, so a
model cannot import from it. Mirrors `assembly_constants` — the contract layer
both Python services depend on.
"""

# The command that clears a prep_sample's read numbering, named wherever a step
# refuses because that numbering is already spent. A noun phrase: each site
# supplies its own lead, because the pool is "this pool" to a pool-scoped 409
# and "the prep_sample's pool" to a per-prep_sample failure.
#
# There is no per-prep_sample delete — the control plane exposes four DELETE
# routes and none of them is one, and `PATCH /prep-sample/{idx}/retired` is
# reversible and leaves the numbering — so the pool is the unit. `--force` is
# required because a terminal ticket on the pool blocks the delete by itself,
# which is exactly the state anything reading this message is in.
POOL_REMOVAL_RECOVERY = (
    "`qiita delete-sequenced-pool --force` — it needs system_admin, its `--force`"
    " is for the terminal ticket that would otherwise block the delete, and it"
    " takes every prep_sample in the pool, their stored reads and their study"
    " links with it"
)

# What forcing a re-submit does to the pool's data. Consumed by
# `submit-bcl-convert --force`, by the 409 the route returns when it refuses,
# and by the wire model's `force` field; each prepends its own lead sentence.
#
# `submit-pacbio-ingest --force` deliberately carries its own text instead: the
# gate this waives is scoped to sequenced_pool actions and bcl-convert is the
# only one (`workflows/bcl-convert/1.0.0.yaml` is the sole `target_kind:
# sequenced_pool`), so on PacBio's prep_sample-scoped tickets neither the
# refusal nor the duplication below applies.
#
# The wire semantics that are not operator-facing (force is privileged for every
# action, a no-op outside the sequenced_pool COMPLETED gate, and never relaxes
# the in-flight gate) stay on WorkTicketCreateRequest's docstring.
FORCE_RESUBMIT_EXPLANATION = (
    "The re-run stores the pool's reads a second time rather than replacing"
    " them: the read-storage step finds each prep_sample's staged copy from the"
    " first run already in place and registers it again, and nothing removes"
    " the duplicate. Loading this pool's reads afresh instead means removing the"
    f" pool first: {POOL_REMOVAL_RECOVERY}. Then submit again. Passing force here"
    " requires wet_lab_admin or system_admin."
)
