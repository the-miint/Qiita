"""Work-ticket constants shared across the control plane, the CLI and the wire
models.

Dependency-free on purpose: `qiita_common.actions` imports the models, so a
model cannot import from it. Mirrors `assembly_constants` — the contract layer
both Python services depend on.
"""

# The operator-facing explanation of `force` on a work-ticket submission, in ONE
# place. It appears on the CLI flag of every gesture that offers it, in the 409
# body the route returns when it refuses, and on the wire model's field — three
# surfaces that must agree, and previously drifted because each carried its own
# copy. The wire semantics that are NOT operator-facing (force is privileged for
# every action, a no-op outside the sequenced_pool COMPLETED gate, and never
# relaxes the in-flight gate) stay on WorkTicketCreate's docstring.
FORCE_RESUBMIT_EXPLANATION = (
    "Submits anyway instead of being refused, when a COMPLETED ticket already"
    " exists for this sequenced_pool and action. That is the only thing it"
    " changes: it does NOT re-load the pool's reads, because each prep_sample's"
    " reads are numbered once and the read-loading step refuses to renumber, so"
    " a forced re-run stops there. To load a pool's reads again, delete the pool"
    " (`qiita delete-sequenced-pool`, which removes its tickets too) and submit"
    " fresh. Requires wet_lab_admin or system_admin."
)
