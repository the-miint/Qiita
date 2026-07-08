"""Assembly-scoped shared vocabulary for the long-read-assembly native jobs.

The closed `kind` value set stored in the DuckLake/Postgres assembly tables,
single-sourced here so `assembly_hash` (producer of `bin_map.kind`) and
`assembly_load` (writer of `assembly_membership.kind` / `bin_quality.kind`) stay
in lockstep — `bin_quality` joins `assembly_membership` on `kind`, so a drift
between the two would silently break that join. Plain module constants, not a
cross-language enum: `kind` is a TEXT column with no Postgres ENUM twin
(deliberately extensible — a future 'plasmid'/'small_circular' kind is intended),
so the shared Python constant is the fail-fast guard, not a DB CHECK.

A private shared helper, not a dispatchable native job: it exports neither
`Inputs` nor `execute`, and its leading-underscore name exempts it from the
boot-time job scan (`scan_native_jobs`).
"""

from __future__ import annotations

KIND_LCG = "LCG"  # a circular genome (large circular genome)
KIND_MAG = "MAG"  # a refined metagenome-assembled bin
