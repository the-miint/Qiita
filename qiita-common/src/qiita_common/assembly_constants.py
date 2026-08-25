"""Assembly constants shared across components.

Single-sourced here, in the contract layer, because they cross components: the
compute orchestrator's native jobs write the `kind` set, and the control plane,
the DuckLake DDL, and the Postgres table comment point here rather than
enumerating members. `bin_quality` joins `assembly_membership` on `kind`, so a
drift between producers would silently break that join.

Plain module constants, not a `StrEnum` with a Postgres twin: `kind` is a TEXT
column with no `CREATE TYPE` twin (the set is extensible — a future
'plasmid'/'small_circular' kind is intended), so this module is the fail-fast
guard, not a DB CHECK. Adding a kind here needs no migration.
"""

KIND_LCG = "LCG"  # a circular genome (large circular genome)
KIND_MAG = "MAG"  # a refined metagenome-assembled bin
KIND_UNBINNED = "UNBINNED"  # a noLCG contig that no refined bin claimed

# The two DuckLake tables an assembly DoGet ticket may target: one row per
# deduped contig (hash + length), and the contig bytes in chunks.
#
# Here rather than in either caller because three components name them: the
# control plane signs a ticket for one (`routes/assembly.py`) and mirrors both in
# the CP-side DoGet allowlist (`routes/reference.py`), and the orchestrator asks
# for one by name (`data_plane_client.open_assembly_chunk_stream`). The data
# plane's `ALLOWED_TABLES` is the Rust half of the pair; neither language can
# import the other, so `test_cp_doget_allowlist_matches_the_rust_one_exactly`
# parses the Rust const and fails on drift.
#
# `assembly_membership` is deliberately NOT here: it is on neither allowlist. The
# data plane reads it to resolve which contigs one run produced
# (`build_assembly_run_query`), but no ticket can name it as a table.
ASSEMBLED_SEQUENCE_TABLE = "assembled_sequence"
ASSEMBLED_SEQUENCE_CHUNKS_TABLE = "assembled_sequence_chunks"
