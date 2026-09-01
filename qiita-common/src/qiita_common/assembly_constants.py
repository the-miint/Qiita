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

# The attribute columns, in the order the entrypoints write them. That ORDER is
# the parse contract, not decoration: `register_contig_attribute_table` below
# binds by position, so both entrypoints must write these five in this sequence.
# The reader checks the sidecar's header row against this tuple, and the
# workflow's entrypoint pins hold the two writers to it.
#
#   contig_id    the assembler's own id; joins bin_map.contig_id
#   raw_name     the header's first token (myloasm) or the GFA segment name
#                (hifiasm_meta), verbatim. For myloasm that is the id before
#                truncation, so the `_circular-`/`_depth-` fields the cut
#                discards stay readable and a normalized value can be traced
#                back. It is NOT the whole header line: myloasm writes `mult=`
#                after a space, which read_fastx returns as a separate column
#   circularity  yes | possibly | no. myloasm states it in the header;
#                hifiasm_meta encodes it in the segment name and has no
#                `possibly`, so that value is myloasm-only.
#   depth        myloasm: the mean of the header's `depth-A-B-C` triple. Per
#                myloasm's source (polishing_mod.rs) that triple is
#                `min_read_depth_multi` and the same function averages it into
#                the `avg_cov` its circularity gate tests -- read off the
#                source, not probed. hifiasm_meta: the S-line's `dp:f` tag.
#                The two assemblers compute coverage differently; `raw_name` is
#                what makes the difference recoverable.
#   mult         myloasm's k-mer multiplicity. Empty for hifiasm_meta, which has
#                no counterpart, and empty below 1 kb where myloasm reports 0.00
#                for absence of signal rather than a measured zero.
CONTIG_ATTRIBUTE_COLUMNS = ("contig_id", "raw_name", "circularity", "depth", "mult")

# The two attribute columns DuckDB must not be left to sniff. `mult` is empty on
# EVERY row a hifiasm_meta assembly writes (it has no counterpart to myloasm's
# k-mer multiplicity), and `depth` on any S-line carrying no `dp:f` tag. An
# all-empty column auto-detects as VARCHAR, so declaring the type keeps the
# reader's output the same for both assemblers instead of varying with the data.
_CONTIG_ATTRIBUTE_TYPES = {"depth": "DOUBLE", "mult": "DOUBLE"}


def register_contig_attribute_table(conn: Any, path: Path) -> None:
    """Register `path` as the TEMP TABLE `contig_attribute`, keyed on `contig_id`.

    Both writers of `assembly_membership` LEFT JOIN this table — the control
    plane's `write_assembly_membership` and the orchestrator's `assembly_load` —
    so they read the sidecar identically and derive the same rows from it. How
    each one CONVERGES on a re-run differs, and is stated at those two sites.

    An absent file registers the table EMPTY rather than changing either join, so
    each statement has one shape and a contig simply gets NULL attributes. Absent
    is the normal state for a run whose assemble step predates the sidecar, which
    a resumed ticket can still reach.

    A repeated `contig_id` raises. Both callers LEFT JOIN this table AFTER
    grouping their rows down to the membership key, so a second row for one
    contig re-multiplies a key that was just collapsed — reaching Postgres as the
    `cardinality_violation` the grouping exists to prevent, from a statement that
    looks correct. Neither producer can emit one (myloasm_split rejects a repeated
    cut id; GFA segment names are unique), so this fires only when a producer
    broke, and it says so here rather than three layers away.
    """
    columns_list = CONTIG_ATTRIBUTE_COLUMNS
    columns = ", ".join(
        f"{name} {_CONTIG_ATTRIBUTE_TYPES.get(name, 'VARCHAR')}" for name in columns_list
    )
    if not path.is_file():
        conn.execute(f"CREATE TEMP TABLE contig_attribute ({columns})")
        return
    # `columns=` binds POSITIONALLY and does not check the header (probed: a file
    # whose header is reordered is read without complaint, so `raw_name` takes
    # `circularity`'s values and vice versa; a file whose header is renamed
    # outright is also accepted). So read row 1 as data and compare it — reading
    # it as a header is the one thing that cannot see it.
    header = conn.execute(
        "SELECT * FROM read_csv(?, delim='\t', header=false, "
        "columns=" + "{" + ", ".join(f"'c{i}': 'VARCHAR'" for i in range(len(columns_list))) + "}"
        ", auto_detect=false) LIMIT 1",
        [str(path)],
    ).fetchone()
    if header != CONTIG_ATTRIBUTE_COLUMNS:
        raise ValueError(
            f"{path} header is {header!r}, expected {CONTIG_ATTRIBUTE_COLUMNS!r}; "
            "the columns are bound by position, so reading it would silently "
            "assign each value to the wrong attribute"
        )
    # `columns=` rather than auto_detect so an all-empty column still arrives as
    # its declared type rather than one that varies with the data.
    types = ", ".join(
        f"'{name}': '{_CONTIG_ATTRIBUTE_TYPES.get(name, 'VARCHAR')}'"
        for name in CONTIG_ATTRIBUTE_COLUMNS
    )
    conn.execute(
        "CREATE TEMP TABLE contig_attribute AS SELECT * FROM read_csv(?,"
        f" delim='\t', header=true, columns={{{types}}})",
        [str(path)],
    )
    (repeated,) = conn.execute(
        "SELECT count(*) FROM (SELECT contig_id FROM contig_attribute"
        " GROUP BY contig_id HAVING count(*) > 1)"
    ).fetchone()
    if repeated:
        raise ValueError(
            f"{path} repeats {repeated} contig_id(s); the attribute join would "
            "duplicate a membership row the write upserts on"
        )


# The attribute half of the membership join, shared by the two writers of
# qiita.assembly_membership. They cannot share the whole statement — the control
# plane joins three Parquets and streams rows back, the orchestrator joins a TEMP
# TABLE and COPYs to Parquet with the run scalars stamped in — but these three
# fragments carry the part that must agree, and a previous round found the two
# copies had already diverged on how they converge.
#
# `min(...)` picks ONE representative contig per membership key rather than
# aggregating each attribute separately: a per-column aggregate could return one
# contig's circularity beside another's depth, describing a contig that does not
# exist. Which contig wins is lexicographic and therefore arbitrary — it matters
# only when a bin holds duplicate (identical) contigs whose reports disagree.
CONTIG_ATTRIBUTE_REPRESENTATIVE_SQL = "min(bm.contig_id) AS attr_contig_id"


def contig_attribute_projection(alias: str) -> str:
    """The four attribute columns, selected off `contig_attribute` alias `a`.

    Named rather than `a.*` so the projection's column ORDER is fixed here: the
    orchestrator COPYs this straight to a Parquet whose column order must match
    the DuckLake table (`ducklake.rs::ensure_assembly_tables`).
    """
    return ", ".join(f"{alias}.{name} AS {name}" for name in CONTIG_ATTRIBUTE_COLUMNS[1:])


def contig_attribute_join(member_alias: str, attr_alias: str = "a") -> str:
    """LEFT JOIN of the registered `contig_attribute` table onto the representative.

    LEFT so a contig absent from the sidecar keeps its membership row with NULL
    attributes, which is the state of every run assembled before the sidecar
    existed. `register_contig_attribute_table` rejects a repeated `contig_id`, so
    this join cannot re-multiply a row the caller's GROUP BY just collapsed.
    """
    return (
        f" LEFT JOIN contig_attribute {attr_alias}"
        f" ON {attr_alias}.contig_id = {member_alias}.attr_contig_id"
    )
