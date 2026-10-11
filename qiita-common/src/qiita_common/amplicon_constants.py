"""DuckLake table names of the amplicon workflow's output.

The compute orchestrator's `amplicon_load` job stages these tables (register-files
names a table after its file stem); the control plane signs DoGet tickets naming
the membership and the chunks to serve a run's feature table.
"""

# One row per (prep_sample_idx, processing_idx, feature_idx) with its count.
AMPLICON_MEMBERSHIP_TABLE = "amplicon_membership"
# One row per ASV feature_idx: its sequence hash and length.
AMPLICON_SEQUENCE_TABLE = "amplicon_sequence"
# The ASV bytes, chunked, keyed by feature_idx.
AMPLICON_SEQUENCE_CHUNKS_TABLE = "amplicon_sequence_chunks"
