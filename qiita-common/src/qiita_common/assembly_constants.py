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

from pathlib import Path
from typing import Any

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

# Per-contig attributes the assembler reported, one row per contig across BOTH
# published FASTAs, keyed on the assembler's own contig id (`bin_map.contig_id`).
# Written by both arms of assemble.sh, into the genomes_dir the assemble step
# already publishes. The entrypoints normalize the circularity call and derive
# myloasm's depth scalar; the rest is what the tool reported. The file is absent
# for a run assembled before it existed — the directory itself is not a signal,
# since assemble.sh creates it unconditionally — so both readers key on the file.
CONTIG_ATTRIBUTES_FILE = "contig_attributes.tsv"

# The attribute columns, in the order the entrypoints write them. Both readers go
# through `contig_attribute_table_sql` below, which names them, so the order is a
# contract on the writers rather than on the parse; the two entrypoints must
# agree with each other, and the workflow's entrypoint pins hold them to it.
#
#   contig_id    the assembler's own id; joins bin_map.contig_id
#   raw_name     the full header (myloasm) or GFA segment name (hifiasm_meta),
#                verbatim, so a normalized value can always be traced back
#   circularity  yes | possibly | no. myloasm states it in the header;
#                hifiasm_meta encodes it in the segment name and has no
#                `possibly`, so that value is myloasm-only.
#   depth        myloasm: the mean of the header's `depth-A-B-C` triple, which is
#                the scalar myloasm itself derives from it (the `avg_cov` its
#                circularity gate tests). hifiasm_meta: the S-line's `dp:f` tag.
#                The two assemblers compute coverage differently; `raw_name` is
#                what makes the difference recoverable.
#   mult         myloasm's k-mer multiplicity. Empty for hifiasm_meta, which has
#                no counterpart, and empty below 1 kb where myloasm reports 0.00
#                for absence of signal rather than a measured zero.
CONTIG_ATTRIBUTE_COLUMNS = ("contig_id", "raw_name", "circularity", "depth", "mult")

# The two attribute columns DuckDB must not be left to sniff. Both are empty for
# every row of a hifiasm_meta assembly (it reports no `mult`, and `depth` only
# where the S-line carried a `dp:f` tag); an all-empty column auto-detects as
# VARCHAR, so declaring them keeps the reader's output types the same for both
# assemblers instead of varying with the data.
_CONTIG_ATTRIBUTE_TYPES = {"depth": "DOUBLE", "mult": "DOUBLE"}


def register_contig_attribute_table(conn: Any, path: Path) -> None:
    """Register `path` as the TEMP TABLE `contig_attribute`, keyed on `contig_id`.

    Both writers of `assembly_membership` LEFT JOIN this table — the control
    plane's `write_assembly_membership` and the orchestrator's `assembly_load` —
    and they are copies of one table, so they read the sidecar identically.

    An absent file registers the table EMPTY rather than changing either join, so
    each statement has one shape and a contig simply gets NULL attributes. Absent
    is the normal state for a run whose assemble step predates the sidecar, which
    a resumed ticket can still reach.
    """
    columns = ", ".join(
        f"{name} {_CONTIG_ATTRIBUTE_TYPES.get(name, 'VARCHAR')}"
        for name in CONTIG_ATTRIBUTE_COLUMNS
    )
    if not path.is_file():
        conn.execute(f"CREATE TEMP TABLE contig_attribute ({columns})")
        return
    # `columns=` rather than auto_detect so an all-empty column still arrives as
    # its declared type; the header is still required to match.
    types = ", ".join(
        f"'{name}': '{_CONTIG_ATTRIBUTE_TYPES.get(name, 'VARCHAR')}'"
        for name in CONTIG_ATTRIBUTE_COLUMNS
    )
    conn.execute(
        "CREATE TEMP TABLE contig_attribute AS SELECT * FROM read_csv(?,"
        f" delim='\t', header=true, columns={{{types}}})",
        [str(path)],
    )
