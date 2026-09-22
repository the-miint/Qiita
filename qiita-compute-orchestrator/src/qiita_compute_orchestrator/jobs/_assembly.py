"""The `assemble` step's output basenames, shared by the native assembly jobs.

The `kind` value set those jobs also share lives in the contract layer,
`qiita_common.assembly_constants` — these basenames do not, because they are
written by a container entrypoint in this repo's `workflows/` tree and no other
component resolves them. `CONTIG_ATTRIBUTES_FILE` is the exception and lives in
the contract layer for exactly that reason: the control plane resolves it too,
against the same genomes_dir, when it writes the Postgres membership rows.

A private shared helper, not a dispatchable native job: it exports neither
`Inputs` nor `execute`, and its leading-underscore name exempts it from the
boot-time job scan (`scan_native_jobs`).
"""

from __future__ import annotations

# Basenames the `assemble` step writes into genomes_dir; it owns when each file is
# present and when it is not (workflows/long-read-assembly/assemble.sh). The
# spellings are pinned against that script in
# tests/test_long_read_assembly_entrypoint_pins.py.
LCG_FILE = "circular.fa"  # every circular contig, as one multi-FASTA
NOLCG_FILE = "noLCG.fa"  # the non-circular contigs; the binning input
