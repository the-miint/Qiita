"""Amplicon constants shared across components.

The amplicon workflow's `amplicon_load` job (compute orchestrator) stages the
DuckLake tables under these names; the control plane reads the staged membership
back to mint the run's public identifiers, and reads the registered tables to
serve a feature table. Neither side can see the other's code, so the names live
here.
"""

# One row per (prep_sample_idx, processing_idx, feature_idx) with its count.
AMPLICON_MEMBERSHIP_TABLE = "amplicon_membership"
# One row per ASV feature_idx: its sequence hash, length, and the sequence itself.
# An ASV is short (a single 64 KB chunk at most), so its bytes are stored inline here
# rather than in a separate chunk table — unlike a reference genome, which is chunked.
AMPLICON_SEQUENCE_TABLE = "amplicon_sequence"

# register-files names a staged table after its file stem.
AMPLICON_MEMBERSHIP_BASENAME = f"{AMPLICON_MEMBERSHIP_TABLE}.parquet"
AMPLICON_SEQUENCE_BASENAME = f"{AMPLICON_SEQUENCE_TABLE}.parquet"
