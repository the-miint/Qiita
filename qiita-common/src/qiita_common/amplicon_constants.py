"""Amplicon constants shared across components.

The amplicon workflow's `amplicon_load` job (compute orchestrator) stages the
DuckLake tables under these names; the control plane reads the staged membership
back to mint the run's public identifiers, and reads the registered tables to
serve a feature table. Neither side can see the other's code, so the names live
here.
"""

# One row per (prep_sample_idx, processing_idx, feature_idx) with its count.
AMPLICON_MEMBERSHIP_TABLE = "amplicon_membership"
# One row per ASV feature_idx: its sequence hash and length.
AMPLICON_SEQUENCE_TABLE = "amplicon_sequence"
# The ASV bytes, chunked, keyed by feature_idx.
AMPLICON_SEQUENCE_CHUNKS_TABLE = "amplicon_sequence_chunks"

# register-files names a staged table after its file stem.
AMPLICON_MEMBERSHIP_BASENAME = f"{AMPLICON_MEMBERSHIP_TABLE}.parquet"
AMPLICON_SEQUENCE_BASENAME = f"{AMPLICON_SEQUENCE_TABLE}.parquet"
# The chunks stage as a directory of Parquet files named after the table.
AMPLICON_SEQUENCE_CHUNKS_BASENAME = AMPLICON_SEQUENCE_CHUNKS_TABLE
