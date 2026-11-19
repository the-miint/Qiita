"""Amplicon constants shared across components.

The amplicon workflow's `amplicon_load` job (compute orchestrator) stages the
DuckLake tables under these names; the control plane reads the staged membership
back to mint the run's public identifiers, and reads the registered tables to
serve a feature table. Neither side can see the other's code, so the names live
here.
"""

# One row per (prep_sample_idx, processing_idx, feature_idx) with its count.
# This is the WRITE target (register-files). Reads go through the exclusion-aware
# view below, never this base table — a blocked ASV must not reach a feature table.
AMPLICON_MEMBERSHIP_TABLE = "amplicon_membership"
# The read surface: `amplicon_membership` ANTI JOIN the resolved blocklist on
# feature_idx (data plane `ensure_exclusion_tables`), mirroring `alignment_visible`.
AMPLICON_MEMBERSHIP_VISIBLE_TABLE = "amplicon_membership_visible"
# One row per ASV feature_idx: its sequence hash, length, and the sequence itself.
# An ASV is short (a single 64 KB chunk at most), so its bytes are stored inline here
# rather than in a separate chunk table — unlike a reference genome, which is chunked.
AMPLICON_SEQUENCE_TABLE = "amplicon_sequence"

# register-files names a staged table after its file stem.
AMPLICON_MEMBERSHIP_BASENAME = f"{AMPLICON_MEMBERSHIP_TABLE}.parquet"
AMPLICON_SEQUENCE_BASENAME = f"{AMPLICON_SEQUENCE_TABLE}.parquet"
