"""The `kind` value set for `assembly_membership` / `bin_quality`.

Single-sourced here, in the contract layer, because the set crosses components:
the compute orchestrator's native jobs write it, and the control plane, the
DuckLake DDL, and the Postgres table comment point here rather than enumerating
members. `bin_quality` joins `assembly_membership` on `kind`, so a drift between
producers would silently break that join.

Plain module constants, not a `StrEnum` with a Postgres twin: `kind` is a TEXT
column with no `CREATE TYPE` twin (the set is extensible — a future
'plasmid'/'small_circular' kind is intended), so this module is the fail-fast
guard, not a DB CHECK. Adding a kind here needs no migration.
"""

KIND_LCG = "LCG"  # a circular genome (large circular genome)
KIND_MAG = "MAG"  # a refined metagenome-assembled bin
KIND_UNBINNED = "UNBINNED"  # a noLCG contig that no refined bin claimed
