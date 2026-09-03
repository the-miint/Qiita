"""Work-ticket constants shared across the control plane, the CLI and the wire
models.

Dependency-free on purpose: `qiita_common.actions` imports the models, so a
model cannot import from it. Mirrors `assembly_constants` — the contract layer
both Python services depend on.
"""

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
    " first run already in place, registers it again, and the lake does not"
    " deduplicate — so both copies remain. To load a pool's reads again"
    " cleanly, delete the pool (`qiita delete-sequenced-pool`, which removes"
    " its tickets too) and submit fresh. Requires wet_lab_admin or"
    " system_admin."
)
